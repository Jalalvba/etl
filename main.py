#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
main.py — Unified XLSX loader for DS, CP, PARC (single-sheet, fuzzy keep+rename)

Unified rule (applies to DS, CP, PARC):
  • Fixed header row per dataset.
  • Keep ONLY headers listed in that dataset's map (left = Excel label).
  • Fuzzy-match headers (accents, spacing, small typos), then rename to canonical (right).
  • Drop everything else.
  • Accept if AT LEAST ONE mapped header matches; else error.
  • Upsert to Mongo with change detection (doc_sig / lines_sig).

Strict Drive pick:
  • Name must match regex AND MIME is XLSX AND ".xlsx" extension.

Header rows (0-based):
  • DS   = 1  (Excel row 2)
  • CP   = 7  (Excel row 8)
  • PARC = 7  (Excel row 8)

Env (required):
  • GOOGLE_CREDENTIALS_BASE64
  • GOOGLE_DRIVE_FOLDER_ID
  • MONGODB_URI
  • MONGODB_DB

Env (optional):
  • DS_FILENAME_REGEX, CP_FILENAME_REGEX, PARC_FILENAME_REGEX
"""

from __future__ import annotations

import os, sys, io, re, json, base64, hashlib, warnings, unicodedata, difflib
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

# File name regexes
DS_FILENAME_REGEX_DEFAULT   = r"(?i)\b(DS|Devis[ _-]?Service|Bon[ _-]?de[ _-]?Réparation)\b"
CP_FILENAME_REGEX_DEFAULT   = r"(?i)\b(CP|Car\s*Policy|Contrat\s*Parc)\b"
PARC_FILENAME_REGEX_DEFAULT = r"(?i)\b(PARC|Fleet|Parc\s*Auto)\b"

# Fixed header rows (0-based)
DS_HEADER_ROW   = 1  # Excel row 2
CP_HEADER_ROW   = 7  # Excel row 8
PARC_HEADER_ROW = 7  # Excel row 8

# Header maps: LEFT = exact Excel label, RIGHT = canonical name
DS_HEADERS_MAP: Dict[str, str] = {
    "Date DS": "date_ds",
    "Description": "description_ds",
    "Désignation article": "designation_article_ds",
    "Founisseur": "fournisseur_ds",
    "Immatriculation": "imm",
    "KM": "km_ds",
    "N°DS": "nds_ds",
    "Prix Unitaire ds": "prix_unitaire_ds_ds",
    "Qté": "qte_ds",
    "ENTITE": "entite_ds",
    "Technicein": "technicien",
    "Code art": "code_art",
}

CP_HEADERS_MAP: Dict[str, str] = {
    "Immatriculation": "imm",
    "VIN": "vin",
    "WW": "ww",
    "Client": "client",
    "Date début": "date_debut",
    "Date fin": "date_fin",
    # If your sheet says "Date début contrat" etc., fuzzy match will catch it.
}

PARC_HEADERS_MAP: Dict[str, str] = {
    "Immatriculation": "imm",
    "VIN": "vin",
    "WW": "ww",
    "Modèle": "modele",
    "Date MEC": "date_mec",
    "Prestataire": "prestataire",
    "Locataire": "locataire_parc",
    "Etat véhicule": "etat_vehicule",
}

# DS downstream fields (after rename)
DS_LINE_FIELDS = [
    "date_ds","description_ds","designation_article_ds","fournisseur_ds","imm","km_ds",
    "nds_ds","prix_unitaire_ds_ds","qte_ds","entite_ds","technicien","code_art","imm_norm","ww_norm"
]

# ── Small utils ───────────────────────────────────────────────────────────────
_ALNUM = re.compile(r"[^0-9A-Za-z]")
_EXCEL_EPOCH = datetime(1899, 12, 30)

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

# ── Fuzzy header normalizer ──────────────────────────────────────────────────
def _norm_label(s: str) -> str:
    """Normalize a header: trim, lowercase, strip accents, drop punctuation, collapse spaces."""
    if s is None:
        return ""
    s = " ".join(str(s).strip().split())  # collapse inner spaces
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^0-9a-z ]+", "", s)  # keep alnum + space
    s = " ".join(s.split())
    return s

# ── Errors ───────────────────────────────────────────────────────────────────
class ConfigError(RuntimeError): ...
class DriveError(RuntimeError): ...
class TransformError(RuntimeError): ...
class UpsertError(RuntimeError): ...

# ── Logging ──────────────────────────────────────────────────────────────────
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

# ── Config ───────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Config:
    mongo_uri: str
    mongo_db: str
    creds_b64: str
    drive_folder_id: str
    ds_regex: str
    cp_regex: str
    parc_regex: str

def load_config() -> Config:
    STEP.step("Load configuration")
    load_dotenv(override=False)

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db  = os.getenv("MONGODB_DB")
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not (mongo_uri and mongo_db and creds_b64 and folder_id):
        raise ConfigError("Missing one of: MONGODB_URI, MONGODB_DB, GOOGLE_CREDENTIALS_BASE64, GOOGLE_DRIVE_FOLDER_ID")

    ds_regex   = os.getenv("DS_FILENAME_REGEX",   DS_FILENAME_REGEX_DEFAULT)
    cp_regex   = os.getenv("CP_FILENAME_REGEX",   CP_FILENAME_REGEX_DEFAULT)
    parc_regex = os.getenv("PARC_FILENAME_REGEX", PARC_FILENAME_REGEX_DEFAULT)
    log(f"Using DS regex: {ds_regex}")
    log(f"Using CP regex: {cp_regex}")
    log(f"Using PARC regex: {parc_regex}")

    return Config(mongo_uri, mongo_db, creds_b64, folder_id, ds_regex, cp_regex, parc_regex)

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

def pick_and_fetch(drive, folder_id: str, name_regex: str,
                   temp_name: str, final_name: str) -> Tuple[str, str, str]:
    STEP.step(f"Find valid XLSX for {final_name}")
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
        tmp = f"data/{temp_name}"
        download_xlsx(drive, fid, tmp)

        final_path = f"data/{final_name}"
        try:
            if os.path.exists(final_path):
                os.remove(final_path)
        except Exception:
            pass
        os.replace(tmp, final_path)
        log(f"Accepted: {name}")
        return fid, name, final_path

    raise DriveError("No XLSX matched regex + MIME + extension.")

# ── Fuzzy keep+rename reader ─────────────────────────────────────────────────
def read_keep_rename(path: str, header_row: int, header_map: Dict[str, str], label: str,
                     fuzzy_threshold: float = 0.85) -> pd.DataFrame:
    """
    Read with headers at header_row, keep only keys in header_map (LEFT),
    fuzzy-match (accents/spaces/small typos) to rename to canonical (RIGHT).
    Accept if ≥1 mapped header matched; else error.
    """
    STEP.step(f"Read {label} (keep+rename, fuzzy headers)")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")

    expected = list(header_map.keys())
    norm_expected = {_norm_label(k): k for k in expected}

    cols = list(df.columns)
    norm_cols = {_norm_label(c): c for c in cols}

    # 1) exact normalized matches
    matches: Dict[str, Tuple[str, float]] = {}  # canonical -> (original_col, score)
    used_cols = set()
    for ne, raw_expected in norm_expected.items():
        if ne in norm_cols:
            orig = norm_cols[ne]
            canon = header_map[raw_expected]
            matches[canon] = (orig, 1.0)
            used_cols.add(orig)

    # 2) fuzzy for remaining
    remaining_norm_exp = [ne for ne, raw in norm_expected.items() if header_map[raw] not in matches]
    remaining_cols = [c for c in cols if c not in used_cols]
    norm_rem_cols = [(c, _norm_label(c)) for c in remaining_cols]

    for ne in remaining_norm_exp:
        raw_expected = norm_expected[ne]
        canon = header_map[raw_expected]
        best_score = -1.0
        best_col = None
        for orig, normed in norm_rem_cols:
            score = difflib.SequenceMatcher(None, ne, normed).ratio()
            if score > best_score:
                best_score = score
                best_col = orig
        if best_col is not None and best_score >= fuzzy_threshold:
            matches[canon] = (best_col, best_score)
            used_cols.add(best_col)

    if not matches:
        raise TransformError(f"{label}: none of the expected headers found (even with fuzzy matching) at row {header_row}.")

    for canon, (orig, score) in matches.items():
        log(f"{label}: matched '{canon}' <- '{orig}' ({score:.2f})")

    keep_cols = [orig for (orig, _) in matches.values()]
    rename_map = {orig: canon for canon, (orig, _) in matches.items()}
    out = df[keep_cols].rename(columns=rename_map).copy()
    log(f"{label}: kept -> {list(out.columns)}")
    return out

# ── DS doc building (fed by renamed columns) ─────────────────────────────────
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
    if df.empty: return []
    docs: List[Dict[str, Any]] = []
    for ds_no, g in df.groupby("nds_ds", dropna=False, sort=False):
        _id = _txt(ds_no)
        if not _id: continue
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

# ── CP / PARC simple docs ────────────────────────────────────────────────────
def build_row_docs(df: pd.DataFrame, pk_cols: List[str], label: str) -> List[Dict[str, Any]]:
    STEP.step(f"Build {label} documents")
    have_all_pks = all(c in df.columns for c in pk_cols)
    docs: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        body = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        if have_all_pks:
            _id = "|".join(str(body.get(c) or "").strip() for c in pk_cols)
        else:
            _id = _sha256_json(body)
        doc = {**body, "_id": _id, "doc_sig": _sha256_json(body)}
        docs.append(doc)
    log(f"{label} docs: {len(docs)}")
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
            compressors="zstd,snappy,zlib",
            retryWrites=True,
            appname="etl-upsert-main",
        )
        db = client.get_database(dbname, write_concern=WriteConcern(w=1))
        client.admin.command("ping")
        log("Mongo ping OK")
        return client, db
    except Exception as e:
        raise UpsertError(f"Mongo connection failed: {e}")

def ensure_indexes(db) -> None:
    STEP.step("Ensure indexes")
    try:
        for f in ("nds_ds","imm_norm","ww_norm","vehicle_id","lines_sig"):
            db.ds.create_index(f)
        for f in ("doc_sig",):
            db.cp.create_index(f)
            db.parc.create_index(f)
        log("Indexes ensured")
    except Exception as e:
        raise UpsertError(f"Creating indexes failed: {e}")

def _preload_sig(db, coll: str, ids: List[str], field: str) -> Dict[str, Optional[str]]:
    if not ids: return {}
    cur = db[coll].find({"_id": {"$in": ids}}, {"_id": 1, field: 1})
    return {str(d["_id"]): d.get(field) for d in cur}

def upsert_with_sig(db, coll: str, docs: List[Dict[str, Any]], sig_field: str) -> Dict[str,int]:
    STEP.step(f"Upsert {coll}")
    if not docs:
        log("No docs to upsert")
        return {"inserted":0,"updated":0,"skipped":0}
    sig_map = _preload_sig(db, coll, [d["_id"] for d in docs], sig_field)
    now = datetime.now(TZ.utc)
    ins=upd=skp=0
    ops: List[UpdateOne] = []
    for d in docs:
        prev = sig_map.get(d["_id"], None)
        if prev is None:
            ins += 1
        elif prev == d.get(sig_field):
            skp += 1
            continue
        else:
            upd += 1
        ops.append(UpdateOne({"_id": d["_id"]}, {"$set": {**d, "updated_at": now}}, upsert=True))
    if ops:
        db[coll].bulk_write(ops, ordered=False, bypass_document_validation=True)
    log(f"{coll} → inserted={ins} updated={upd} skipped={skp}")
    return {"inserted":ins,"updated":upd,"skipped":skp}

# ── Orchestration ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class RunCfg:
    mongo_uri: str; mongo_db: str; creds_b64: str; drive_folder_id: str
    ds_regex: str; cp_regex: str; parc_regex: str

def run(cfg: RunCfg) -> None:
    log("BEGIN main load (DS/CP/PARC)")
    drive = make_drive(cfg.creds_b64)

    # DS
    _, ds_name, ds_path = pick_and_fetch(drive, cfg.drive_folder_id, cfg.ds_regex,
                                         temp_name=".tmp_ds.xlsx",   final_name="ds.xlsx")
    log(f"DS   → {ds_name}")
    df_ds = read_keep_rename(ds_path, DS_HEADER_ROW, DS_HEADERS_MAP, "DS")
    ds_docs = build_ds_docs(df_ds)

    # CP
    _, cp_name, cp_path = pick_and_fetch(drive, cfg.drive_folder_id, cfg.cp_regex,
                                         temp_name=".tmp_cp.xlsx",   final_name="cp.xlsx")
    log(f"CP   → {cp_name}")
    df_cp = read_keep_rename(cp_path, CP_HEADER_ROW, CP_HEADERS_MAP, "CP")
    cp_docs = build_row_docs(df_cp, pk_cols=["client","imm"], label="CP")

    # PARC
    _, parc_name, parc_path = pick_and_fetch(drive, cfg.drive_folder_id, cfg.parc_regex,
                                             temp_name=".tmp_parc.xlsx", final_name="parc.xlsx")
    log(f"PARC → {parc_name}")
    df_parc = read_keep_rename(parc_path, PARC_HEADER_ROW, PARC_HEADERS_MAP, "PARC")
    parc_docs = build_row_docs(df_parc, pk_cols=["imm"], label="PARC")

    client, db = get_client_db(cfg.mongo_uri, cfg.mongo_db)
    try:
        ds_stats   = upsert_with_sig(db, "ds",   ds_docs,   sig_field="lines_sig")
        cp_stats   = upsert_with_sig(db, "cp",   cp_docs,   sig_field="doc_sig")
        parc_stats = upsert_with_sig(db, "parc", parc_docs, sig_field="doc_sig")
        ensure_indexes(db)
    finally:
        try: client.close()
        except Exception: pass

    log(f"UPSERT DS   : {ds_stats}")
    log(f"UPSERT CP   : {cp_stats}")
    log(f"UPSERT PARC : {parc_stats}")
    log("END main load")

# ── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if sys.getdefaultencoding().lower() != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
    try:
        c = load_config()
        run(RunCfg(c.mongo_uri, c.mongo_db, c.creds_b64, c.drive_folder_id, c.ds_regex, c.cp_regex, c.parc_regex))
    except (ConfigError, DriveError, TransformError, UpsertError) as e:
        log(f"ERROR: {e}")
        sys.exit(2)
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}")
        sys.exit(3)
