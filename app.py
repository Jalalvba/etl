#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
appds.py — DS-only XLSX loader (keep+rename only mapped headers)

Logic (same as main.py unified rule):
  • Strict Drive pick: regex + XLSX MIME + ".xlsx".
  • Read with fixed header row (0-based, default 1 → Excel row 2).
  • Keep ONLY headers in DS_HEADERS_MAP (left = Excel label); rename to canonical (right).
  • Drop everything else.
  • Accept if AT LEAST ONE mapped header exists; else error.
  • Build DS docs (lines + lines_sig) and upsert to Mongo.

Required env:
  • GOOGLE_CREDENTIALS_BASE64
  • GOOGLE_DRIVE_FOLDER_ID
  • MONGODB_URI
  • MONGODB_DB

Optional env:
  • DS_FILENAME_REGEX   (default matches DS terms)
  • DS_HEADER_ROW       (default "1" → Excel row 2)
"""

from __future__ import annotations

import os, sys, io, re, json, base64, hashlib, warnings
from typing import Any, List, Dict, Optional, Tuple
from datetime import datetime, date, timedelta, timezone as TZ

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ── Quiet noisy libs ──────────────────────────────────────────────────────────
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

# ── UTF-8 console ────────────────────────────────────────────────────────────
if sys.getdefaultencoding().lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

# ── Constants ────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# File name regex (can override via DS_FILENAME_REGEX)
DS_FILENAME_REGEX_DEFAULT = r"(?i)\b(DS|Devis[ _-]?Service|Bon[ _-]?de[ _-]?Réparation)\b"

# Fixed header row (0-based). Default=1 → Excel row 2.
DEFAULT_DS_HEADER_ROW = 1

# Header map (LEFT = Excel label, RIGHT = canonical name)
DS_HEADERS_MAP: Dict[str, str] = {
    "Date DS": "date_ds",
    "Description": "description_ds",
    "Désignation article": "designation_article_ds",
    "Founisseur": "fournisseur_ds",  # keep typo per source
    "Immatriculation": "imm",
    "KM": "km_ds",
    "N°DS": "nds_ds",
    "Prix Unitaire ds": "prix_unitaire_ds_ds",
    "Qté": "qte_ds",
    "ENTITE": "entite_ds",
    "Technicein": "technicien",      # keep typo per source
    "Code art": "code_art",
}

# DS downstream fields (used in lines)
DS_LINE_FIELDS = [
    "date_ds","description_ds","designation_article_ds","fournisseur_ds","imm","km_ds",
    "nds_ds","prix_unitaire_ds_ds","qte_ds","entite_ds","technicien","code_art","imm_norm","ww_norm"
]

# ── Small utils ───────────────────────────────────────────────────────────────
_ALNUM = re.compile(r"[^0-9A-Za-z]")
_EXCEL_EPOCH = datetime(1899, 12, 30)

def log(msg: str) -> None:
    ts = datetime.now(TZ.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[{ts}] {msg}")

def _txt(x: Any) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    s = str(x).strip()
    return s or None

def canon_plate(x: Any) -> Optional[str]:
    if pd.isna(x): return None
    return _ALNUM.sub("", str(x)).lower() or None

def _iso_date_from_any(v: Any) -> Optional[str]:
    if pd.isna(v): return None
    if isinstance(v, date) and not isinstance(v, datetime):
        d = v
    elif isinstance(v, datetime):
        d = v.date()
    elif isinstance(v, (int, float)) and not pd.isna(v):
        try:
            serial = int(v)
            d = (_EXCEL_EPOCH + timedelta(days=serial)).date() if serial > 0 else None
        except Exception:
            d = None
    else:
        dts = pd.to_datetime(str(v).strip(), errors="coerce", dayfirst=True)
        d = dts.date() if pd.notna(dts) else None
    if not d or not (2000 <= d.year <= 2035): return None
    return d.strftime("%Y-%m-%d")

def _sha256_json(obj: Any) -> str:
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

# ── Mongo plumbing ───────────────────────────────────────────────────────────
def get_client_db(uri: str, dbname: str):
    log("STEP 6: Connect to MongoDB ────────────────────────────────────────────")
    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=20000,
            socketTimeoutMS=300000,
            connectTimeoutMS=20000,
            maxPoolSize=100,
            compressors="zstd,snappy,zlib",
            retryWrites=True,
            appname="etl-upsert-ds-only",
        )
        db = client.get_database(dbname, write_concern=WriteConcern(w=1))
        client.admin.command("ping")
        log("Mongo ping OK")
        return client, db
    except Exception as e:
        raise RuntimeError(f"Mongo connection failed: {e}")

def ensure_ds_indexes(db):
    log("STEP 9: Ensure DS indexes ─────────────────────────────────────────────")
    for f in ("nds_ds","imm_norm","ww_norm","vehicle_id","lines_sig"):
        db.ds.create_index(f)
    log("Indexes ensured")

# ── Google Drive client ──────────────────────────────────────────────────────
def build_drive(creds_b64: str):
    log("STEP 2: Authenticate to Google Drive ──────────────────────────────────")
    try:
        info = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        log("Drive auth OK")
        return svc
    except Exception as e:
        raise RuntimeError(f"Drive auth failed: {e}")

def list_folder_files(drive, folder_id: str) -> List[Dict[str, Any]]:
    q = f"'{folder_id}' in parents and trashed=false"
    fields = "nextPageToken, files(id,name,mimeType,modifiedTime)"
    files, page_token = [], None
    while True:
        res = drive.files().list(q=q, fields=fields, orderBy="modifiedTime desc",
                                 pageSize=1000, pageToken=page_token).execute()
        files.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    log(f"Folder listing: {len(files)} files")
    return files

def _looks_like_xlsx(name: str, mime: str) -> bool:
    return name.lower().endswith(".xlsx") and mime == XLSX_MIME

def pick_latest_ds_xlsx(drive, folder_id: str, name_regex: str) -> Tuple[str, str]:
    """
    Newest file whose NAME matches regex AND mime is XLSX AND .xlsx extension.
    Returns (file_id, file_name). Raises if none.
    """
    log("STEP 3: Pick DS XLSX from Drive ──────────────────────────────────────")
    rx = re.compile(name_regex)
    files = list_folder_files(drive, folder_id)
    for f in files:
        fid, name, mime = f.get("id"), f.get("name",""), f.get("mimeType","")
        if rx.search(name) and _looks_like_xlsx(name, mime):
            log(f"Candidate: {name} (mime ok)")
            return fid, name
    raise RuntimeError("No valid DS XLSX found (regex + MIME + .xlsx).")

def download_xlsx(drive, file_id: str, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    req = drive.files().get_media(fileId=file_id)
    with open(out_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        raise RuntimeError("Downloaded XLSX is empty")
    return out_path

# ── Read+rename (keep only mapped headers) ───────────────────────────────────
def read_keep_rename_ds(path: str, header_row: int) -> pd.DataFrame:
    """
    Read sheet with headers at header_row.
    Keep only headers in DS_HEADERS_MAP; rename to canonical.
    Accept if ≥1 mapped header is present; else error.
    """
    log("STEP 5: Read DS (keep+rename only mapped headers) ─────────────────────")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")
    present = [c for c in df.columns if c in DS_HEADERS_MAP]
    if not present:
        raise RuntimeError(f"DS: none of the expected headers found at row {header_row}. Found={list(df.columns)}")
    df = df[present].rename(columns={c: DS_HEADERS_MAP[c] for c in present}).copy()
    log(f"DS: kept -> {list(df.columns)}")
    return df

# ── DS transform to docs ─────────────────────────────────────────────────────
def normalize_ds_df(df: pd.DataFrame) -> pd.DataFrame:
    if "date_ds" in df.columns:
        df["date_ds"] = df["date_ds"].map(_iso_date_from_any)
    if "imm" in df.columns:
        df["imm_norm"] = df["imm"].map(canon_plate)
    if "ww" in df.columns:
        df["ww_norm"] = df["ww"].map(canon_plate)
    return df

def canonicalize_lines(df_group: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, row in df_group.iterrows():
        rows.append({k: _txt(row.get(k)) if k in df_group.columns else None for k in DS_LINE_FIELDS})
    rows.sort(key=lambda r: (
        r.get("code_art") or "",
        r.get("designation_article_ds") or "",
        r.get("qte_ds") or "",
        r.get("prix_unitaire_ds_ds") or "",
        r.get("description_ds") or "",
        r.get("entite_ds") or "",
        r.get("technicien") or "",
    ))
    return rows

def build_ds_docs(df_raw: pd.DataFrame) -> List[Dict[str, Any]]:
    log("STEP 7: Build DS documents ────────────────────────────────────────────")
    df = normalize_ds_df(df_raw.copy())
    # Keep rows that have an identifier
    mask = df.apply(lambda r: bool(_txt(r.get("imm_norm")) or _txt(r.get("ww_norm"))), axis=1)
    df = df[mask].copy()
    if df.empty:
        return []
    docs: List[Dict[str, Any]] = []
    for ds_no, g in df.groupby("nds_ds", dropna=False, sort=False):
        _id = _txt(ds_no)
        if not _id:
            continue
        lines = canonicalize_lines(g)
        imm_norm = _txt(g.get("imm_norm").dropna().iloc[0]) if "imm_norm" in g and not g["imm_norm"].dropna().empty else None
        ww_norm  = _txt(g.get("ww_norm").dropna().iloc[0])  if "ww_norm"  in g and not g["ww_norm"].dropna().empty  else None
        vehicle_id = imm_norm or ww_norm
        dt_series = pd.to_datetime(g.get("date_ds"), errors="coerce", utc=True).dropna()
        date_event = dt_series.max() if not dt_series.empty else None
        docs.append({
            "_id": _id,
            "ds_no": _id,
            "vehicle_id": vehicle_id,
            "imm_norm": imm_norm,
            "ww_norm": ww_norm,
            "date_event": date_event,
            "lines": lines,
            "lines_sig": _sha256_json(lines),
        })
    log(f"DS docs: {len(docs)}")
    return docs

# ── Upsert helpers ───────────────────────────────────────────────────────────
def _preload_sig(db, ids: List[str]) -> Dict[str, Optional[str]]:
    if not ids: return {}
    cur = db.ds.find({"_id": {"$in": ids}}, {"_id": 1, "lines_sig": 1})
    return {str(d["_id"]): d.get("lines_sig") for d in cur}

def upsert_ds(db, docs: List[Dict[str, Any]]) -> Dict[str,int]:
    log("STEP 8: Upsert DS ─────────────────────────────────────────────────────")
    if not docs:
        log("No DS docs to upsert")
        return {"inserted":0,"updated":0,"skipped":0}
    sig_map = _preload_sig(db, [d["_id"] for d in docs])
    now = datetime.now(TZ.utc)
    ins=upd=skp=0
    ops: List[UpdateOne] = []
    for d in docs:
        prev = sig_map.get(d["_id"], None)
        if prev is None:
            ins += 1
        elif prev == d.get("lines_sig"):
            skp += 1
            continue
        else:
            upd += 1
        ops.append(UpdateOne({"_id": d["_id"]}, {"$set": {**d, "updated_at": now}}, upsert=True))
    if ops:
        db.ds.bulk_write(ops, ordered=False, bypass_document_validation=True)
    log(f"DS → inserted={ins} updated={upd} skipped={skp}")
    return {"inserted":ins,"updated":upd,"skipped":skp}

# ── Orchestration ────────────────────────────────────────────────────────────
def main():
    log("STEP 1: Load configuration ─────────────────────────────────────────────")
    load_dotenv(override=False)

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db  = os.getenv("MONGODB_DB")
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not (mongo_uri and mongo_db and creds_b64 and folder_id):
        raise SystemExit("[ERROR] Missing MONGODB_URI / MONGODB_DB / GOOGLE_CREDENTIALS_BASE64 / GOOGLE_DRIVE_FOLDER_ID")

    ds_regex = os.getenv("DS_FILENAME_REGEX", DS_FILENAME_REGEX_DEFAULT)
    try:
        header_row = int(os.getenv("DS_HEADER_ROW", str(DEFAULT_DS_HEADER_ROW)))
    except ValueError:
        header_row = DEFAULT_DS_HEADER_ROW

    log(f"Using DS regex: {ds_regex}")
    log(f"Using DS header row (0-based): {header_row}")

    # Drive
    drive = build_drive(creds_b64)

    # Pick newest DS xlsx
    fid, name = pick_latest_ds_xlsx(drive, folder_id, ds_regex)

    # Download
    log("STEP 4: Download DS XLSX ──────────────────────────────────────────────")
    local_path = "data/ds.xlsx"
    os.makedirs("data", exist_ok=True)
    if os.path.exists(local_path):
        try: os.remove(local_path)
        except Exception: pass
    download_xlsx(drive, fid, local_path)
    log(f"Downloaded: {name} → {local_path}")

    # Read + rename (keep only mapped headers)
    df_ds = read_keep_rename_ds(local_path, header_row=header_row)

    # Build docs
    ds_docs = build_ds_docs(df_ds)

    # Upsert
    client, db = get_client_db(mongo_uri, mongo_db)
    try:
        stats = upsert_ds(db, ds_docs)
        ensure_ds_indexes(db)
    finally:
        try: client.close()
        except Exception: pass

    log(f"UPSERT DS: {stats}")
    log("DONE")

# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(e)
        sys.exit(2)
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}")
        sys.exit(3)
