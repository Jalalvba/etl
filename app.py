#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
appds.py — DS-only loader (STRICT headers; XLSX-only) matching main.py DS contract 1:1
"""

from __future__ import annotations

import os, sys, io, re, json, base64, hashlib, warnings, unicodedata
from dataclasses import dataclass
from typing import Any, List, Dict, Tuple, Optional
from datetime import datetime, date, timedelta, timezone as TZ

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ── Quiet the noise ───────────────────────────────────────────────────────────
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

# ── Constants ────────────────────────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DS_FILENAME_REGEX_DEFAULT = r"(?i)\b(DS|Devis[ _-]?Service|Bon[ _-]?de[ _-]?(?:R[eé]paration))\b"
DS_HEADER_ROW = 1

# Header map: LEFT = expected Excel label, RIGHT = canonical name
DS_HEADERS_MAP: Dict[str, str] = {
    "Date DS": "date_ds",
    "Description": "description_ds",
    "Désignation Consomation": "designation_article_ds",  # your sheet's current label
    "Founisseur": "fournisseur_ds",
    "Immatriculation": "imm",
    "KM": "km_ds",
    "N°DS": "nds_ds",
    "Prix Unitaire ds": "prix_unitaire_ds_ds",
    "Qté": "qte_ds",
    "ENTITE": "entite_ds",
    "Technicein": "technicien",
    "Code art": "code_art",
    # "WW": "ww",  # uncomment if present in DS
}

DS_LINE_FIELDS = [
    "date_ds","description_ds","designation_article_ds","fournisseur_ds","imm","km_ds",
    "nds_ds","prix_unitaire_ds_ds","qte_ds","entite_ds","technicien","code_art","imm_norm","ww_norm"
]

# ── Small utils ───────────────────────────────────────────────────────────────
_ALNUM = re.compile(r"[^0-9A-Za-z]")
_EXCEL_EPOCH = datetime(1899, 12, 30)
_EXCEL_ESC_RE = re.compile(r"_x([0-9A-Fa-f]{4})_")  # Excel XML escape pattern

def log(msg: str) -> None:
    ts = datetime.now(TZ.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[{ts}] {msg}")

class Stepper:
    def __init__(self) -> None: self.i = 0
    def step(self, title: str) -> None:
        self.i += 1
        sep = "─" * max(8, 64 - len(title))
        log(f"STEP {self.i}: {title} {sep}")

STEP = Stepper()

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

def _norm_label(s: str) -> str:
    """Strict normalization: lowercase, strip accents, remove punctuation, collapse spaces."""
    if s is None: return ""
    s = " ".join(str(s).strip().split())
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^0-9a-z ]+", "", s)
    s = " ".join(s.split())
    return s

# ── Decode Excel XML escapes in cell values ───────────────────────────────────
def _decode_excel_escapes_to_text(s: Any) -> Any:
    """Turn '_x000A_' / '_x000D_' / '_x0009_' etc. into readable text.
       LF/CR/TAB → single space; other codes → their Unicode char (if printable).
       Collapse runs of whitespace and trim."""
    if not isinstance(s, str):
        return s
    def repl(m):
        code = m.group(1)
        code_lower = code.lower()
        if code_lower in ("000a","000d","0009"):  # LF, CR, TAB → space
            return " "
        try:
            ch = chr(int(code, 16))
            return " " if ord(ch) < 32 else ch
        except Exception:
            return " "
    s = _EXCEL_ESC_RE.sub(repl, s)
    s = re.sub(r"[ \t\r\n]+", " ", s).strip()
    return s

def _clean_dataframe_excel_escapes(df: pd.DataFrame) -> pd.DataFrame:
    # Only touch object (string) columns for speed and safety
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].map(_decode_excel_escapes_to_text)
    return df

# ── Errors ───────────────────────────────────────────────────────────────────
class ConfigError(RuntimeError): ...
class DriveError(RuntimeError): ...
class TransformError(RuntimeError): ...
class UpsertError(RuntimeError): ...

# ── Config ───────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Config:
    mongo_uri: str
    mongo_db: str
    creds_b64: str
    drive_folder_id: str
    ds_regex: str

def load_config() -> Config:
    STEP.step("Load configuration")
    load_dotenv(override=False)

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db  = os.getenv("MONGODB_DB")
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not (mongo_uri and mongo_db and creds_b64 and folder_id):
        raise ConfigError("Missing one of: MONGODB_URI, MONGODB_DB, GOOGLE_CREDENTIALS_BASE64, GOOGLE_DRIVE_FOLDER_ID")

    ds_regex = os.getenv("DS_FILENAME_REGEX", DS_FILENAME_REGEX_DEFAULT)
    log(f"Using DS regex: {ds_regex}")

    return Config(mongo_uri, mongo_db, creds_b64, folder_id, ds_regex)

# ── Drive ────────────────────────────────────────────────────────────────────
def make_drive(creds_b64: str):
    STEP.step("Authenticate to Google Drive")
    try:
        info = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        log("Drive auth OK")
        return svc
    except Exception as e:
        raise DriveError(f"Drive auth failed: {e}")

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

def download_xlsx(drive, file_id: str, out_path: str) -> str:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    req = drive.files().get_media(fileId=file_id)
    with open(out_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        raise DriveError("Downloaded XLSX is empty")
    return out_path

def pick_and_fetch_ds(drive, folder_id: str, name_regex: str) -> Tuple[str, str, str]:
    STEP.step("Find valid XLSX for ds.xlsx")
    rx = re.compile(name_regex)
    files = list_folder_files(drive, folder_id)
    if not files:
        raise DriveError("Folder is empty")

    for f in files:
        fid  = f.get("id")
        name = f.get("name", "")
        mime = f.get("mimeType", "")
        if not (rx.search(name) and _looks_like_xlsx(name, mime)):
            continue
        log(f"Candidate: {name} (mime ok)")
        tmp = "data/.tmp_ds.xlsx"
        download_xlsx(drive, fid, tmp)

        final_path = "data/ds.xlsx"
        try:
            if os.path.exists(final_path):
                os.remove(final_path)
        except Exception:
            pass
        os.replace(tmp, final_path)
        log(f"Accepted: {name}")
        return fid, name, final_path

    raise DriveError("No XLSX matched regex + MIME + extension for DS.")

# ── STRICT keep+rename reader for DS ─────────────────────────────────────────
def read_ds_strict(path: str, header_row: int, header_map: Dict[str, str]) -> pd.DataFrame:
    """
    Strict matching with value cleanup:
      - normalize expected labels and sheet columns (lowercase, strip accents, no punct, collapse spaces)
      - exact equality on normalized strings
      - keep+rename matched columns; error if none matched
      - decode Excel XML escapes in **cell values** so '_x000A_' etc. don't leak to UI
    """
    STEP.step("Read DS (keep+rename, STRICT headers)")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")

    # Clean encoded control chars in values (before slicing/renaming is fine)
    df = _clean_dataframe_excel_escapes(df)

    # Build normalized lookup for expected headers
    norm_expected_to_canonical: Dict[str, str] = {}
    for raw, canonical in header_map.items():
        norm_expected_to_canonical[_norm_label(raw)] = canonical

    # Build normalized lookup for actual columns
    norm_col_to_actual: Dict[str, str] = {}
    for c in df.columns:
        norm_col_to_actual[_norm_label(c)] = c

    # Exact normalized matches
    matched: Dict[str, Tuple[str, float]] = {}
    for ne, canonical in norm_expected_to_canonical.items():
        if ne in norm_col_to_actual:
            actual = norm_col_to_actual[ne]
            matched[canonical] = (actual, 1.0)
            log(f"DS: matched '{canonical}' <- '{actual}' (strict)")

    if not matched:
        raise TransformError(f"DS: none of the expected headers found (strict normalized match) at row {header_row}.")

    keep_cols = [actual for (actual, _) in matched.values()]
    rename_map = {actual: canonical for canonical, (actual, _) in matched.items()}
    out = df[keep_cols].rename(columns=rename_map).copy()
    log(f"DS: kept -> {list(out.columns)}")
    return out

# ── DS doc building ──────────────────────────────────────────────────────────
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
    STEP.step("Build DS documents")
    df = normalize_ds_df(df_raw.copy())
    mask = df.apply(lambda r: bool(_txt(r.get("imm_norm")) or _txt(r.get("ww_norm"))), axis=1)
    df = df[mask].copy()
    if df.empty:
        log("DS: no valid rows after vehicle id filter (imm_norm/ww_norm)")
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
            "nds_ds": _id,            # indexable top-level field
            "vehicle_id": vehicle_id,
            "imm_norm": imm_norm,
            "ww_norm": ww_norm,
            "date_event": date_event,
            "lines": lines,
            "lines_sig": _sha256_json(lines),
        })
    log(f"DS docs: {len(docs)}")
    return docs

# ── Mongo plumbing ───────────────────────────────────────────────────────────
def get_client_db(uri: str, dbname: str):
    STEP.step("Connect to MongoDB")
    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=20000,
            socketTimeoutMS=300000,
            connectTimeoutMS=20000,
            maxPoolSize=100,
            compressors=["zstd","snappy","zlib"],
            retryWrites=True,
            appname="etl-upsert-ds",
        )
        db = client.get_database(dbname, write_concern=WriteConcern(w=1))
        client.admin.command("ping")
        log("Mongo ping OK")
        return client, db
    except Exception as e:
        raise UpsertError(f"Mongo connection failed: {e}")

def ensure_ds_indexes(db) -> None:
    STEP.step("Ensure ds indexes")
    try:
        for f in ("nds_ds","imm_norm","ww_norm","vehicle_id","lines_sig"):
            db.ds.create_index(f)
        log("ds indexes ensured")
    except Exception as e:
        raise UpsertError(f"Creating ds indexes failed: {e}")

def _preload_sig(db, ids: List[str]) -> Dict[str, Optional[str]]:
    if not ids: return {}
    cur = db.ds.find({"_id": {"$in": ids}}, {"_id": 1, "lines_sig": 1})
    return {str(d["_id"]): d.get("lines_sig") for d in cur}

def upsert_ds(db, docs: List[Dict[str, Any]]) -> Dict[str,int]:
    STEP.step("Upsert ds")
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
        ops.append(UpdateOne(
            {"_id": d["_id"]},
            {
                "$set": {**d, "updated_at": now},
                "$setOnInsert": {"created_at": now},
            },
            upsert=True
        ))
    if ops:
        db.ds.bulk_write(ops, ordered=False, bypass_document_validation=True)
    log(f"ds → inserted={ins} updated={upd} skipped={skp}")
    return {"inserted":ins,"updated":upd,"skipped":skp}

# ── Orchestration ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RunCfg:
    mongo_uri: str; mongo_db: str; creds_b64: str; drive_folder_id: str; ds_regex: str

def run(cfg: RunCfg) -> None:
    log("BEGIN DS-only load")
    drive = make_drive(cfg.creds_b64)
    _, ds_name, ds_path = pick_and_fetch_ds(drive, cfg.drive_folder_id, cfg.ds_regex)
    log(f"DS   → {ds_name}")
    df_ds = read_ds_strict(ds_path, DS_HEADER_ROW, DS_HEADERS_MAP)
    ds_docs = build_ds_docs(df_ds)
    client, db = get_client_db(cfg.mongo_uri, cfg.mongo_db)
    try:
        stats = upsert_ds(db, ds_docs)
        ensure_ds_indexes(db)
    finally:
        try: client.close()
        except Exception: pass
    log(f"UPSERT DS: {stats}")
    log("END DS-only load")

# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if sys.getdefaultencoding().lower() != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
    try:
        cfg = load_config()
        run(RunCfg(cfg.mongo_uri, cfg.mongo_db, cfg.creds_b64, cfg.drive_folder_id, cfg.ds_regex))
    except (ConfigError, DriveError, TransformError, UpsertError) as e:
        log(f"ERROR: {e}")
        sys.exit(2)
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}")
        sys.exit(3)
