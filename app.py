#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
app.py — DS-only loader that matches mainapp.py's DS contract 1:1

Behavior tied to .env:
- GOOGLE_CREDENTIALS_BASE64: base64-encoded service account JSON (required)
- GOOGLE_SHEET_ID: if set, export this Google Sheet to XLSX
- else if GOOGLE_DRIVE_FOLDER_ID: pick the newest **DS-matching** file in that folder
  - if Google Sheet → export to XLSX
  - else (xlsx/ods) → download file content
- (optional) DS_FILENAME_REGEX: regex to match DS filenames (default below)
- (optional) DS_SHEET_NAME: force a specific sheet/tab name (for XLSX)
- (optional) DS_HEADER_ROW: zero-based header row (default "1" → Excel row 2)
- MONGODB_URI, MONGODB_DB: Mongo target

Process:
1) Delete data/ds.xlsx and data/ds.ods
2) Download/export DS to data/ds.xlsx (or .ods), **DS-only**
3) Preview first 5 raw rows (no header) so you can confirm header placement
4) Read with header row = 2 (header=1) unless overridden
5) Clean Excel artifacts in text (e.g., _x0009_, non-breaking/zero-width spaces)
6) Build DS docs (exact same fields/rules as mainapp.py)
7) Upsert to Mongo + ensure ds indexes
"""

import os, sys, io, re, json, base64, hashlib
from datetime import datetime, date, timedelta, timezone as TZ
from typing import Any, Optional, List, Dict, Tuple

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern
from dotenv import load_dotenv

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ---------- UTF-8 console ----------
if sys.getdefaultencoding().lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

# ---------- Constants ----------
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
ALLOWED_XLSX = (".xlsx", ".xls")
ALLOWED_ODS  = (".ods",)

HEADERS_MAP_DS = {
    "Date DS":"date_ds",
    "Description":"description_ds",
    "Désignation article":"designation_article_ds",
    "Founisseur":"fournisseur_ds",       # keep exact typo
    "Immatriculation":"imm",
    "KM":"km_ds",
    "N°DS":"nds_ds",
    "Prix Unitaire ds":"prix_unitaire_ds_ds",
    "Qté":"qte_ds",
    "ENTITE":"entite_ds",
    "Technicein":"technicien",           # keep exact typo
    "Code art":"code_art",
}

DS_LINE_FIELDS = [
    "date_ds","description_ds","designation_article_ds","fournisseur_ds","imm","km_ds",
    "nds_ds","prix_unitaire_ds_ds","qte_ds","entite_ds","technicien","code_art","imm_norm","ww_norm"
]

_ALNUM = re.compile(r"[^0-9A-Za-z]")
_EXCEL_EPOCH = datetime(1899, 12, 30)

# Default DS filename matcher (tunable via .env: DS_FILENAME_REGEX)
DS_FILENAME_REGEX_DEFAULT = r"(?i)\b(DS|Devis[ _-]?Service|Bon[ _-]?de[ _-]?Réparation)\b"

# ---------- Text cleaning ----------
_excel_escape_re = re.compile(r"_x([0-9A-Fa-f]{4})_")

def _decode_excel_escapes(s: str) -> str:
    def sub(m):
        try:
            cp = int(m.group(1), 16)
            return chr(cp)
        except Exception:
            return m.group(0)
    return _excel_escape_re.sub(sub, s)

def clean_text(v: Any) -> Any:
    if not isinstance(v, str):
        return v
    s = _decode_excel_escapes(v)
    # Replace tabs/newlines and non-breaking/zero-width spaces with normal space
    s = re.sub(r"[\t\r\n\u00A0\u202F\u2007\u200B\u200C\u200D]+", " ", s)
    # Handle literal strings like '\t', '\n', '\r'
    s = s.replace("\\t", " ").replace("\\n", " ").replace("\\r", " ")
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def clean_object_columns_inplace(df: pd.DataFrame) -> None:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(clean_text)

# ---------- Helpers ----------
def _txt(x: Any) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    s = str(x).strip()
    return s or None

def canon_plate(x: Any) -> Optional[str]:
    if pd.isna(x): return None
    return _ALNUM.sub("", str(x)).lower() or None

def _parse_any_to_date(v: Any) -> Optional[date]:
    if pd.isna(v): return None
    if isinstance(v, date) and not isinstance(v, datetime): return v
    if isinstance(v, datetime): return v.date()
    if isinstance(v, (int, float)) and not pd.isna(v):
        try:
            serial = int(v)
            if serial > 0: return (_EXCEL_EPOCH + timedelta(days=serial)).date()
        except Exception:
            pass
    s = str(v).strip()
    if not s: return None
    d = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return d.date() if pd.notna(d) else None

def _sanitize_range(d: Optional[date], min_year=2000, max_year=2035) -> Optional[date]:
    if not d: return None
    return d if (min_year <= d.year <= max_year) else None

def _iso(d: Optional[date]) -> Optional[str]:
    return d.strftime("%Y-%m-%d") if d else None

def normalize_dates_inplace_df(df: pd.DataFrame):
    if "date_ds" in df.columns:
        df["date_ds"] = df["date_ds"].map(_parse_any_to_date).map(_sanitize_range).map(_iso)

def apply_norms_inplace_df(df: pd.DataFrame):
    if "imm" in df.columns: df["imm_norm"] = df["imm"].map(canon_plate)
    if "ww"  in df.columns: df["ww_norm"]  = df["ww"].map(canon_plate)

def canonicalize_lines(df_group: pd.DataFrame):
    rows = []
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

def hash_lines(lines: List[Dict[str, Any]]) -> str:
    payload = json.dumps(lines, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

# ---------- ODS reader (header row aware) ----------
def read_first_sheet_ods_with_header(path: str, header_row: int = 1) -> pd.DataFrame:
    from pyexcel_ods3 import get_data
    book = get_data(path)
    for _, rows in book.items():
        if not rows: return pd.DataFrame()
        if len(rows) <= header_row: return pd.DataFrame()
        headers = [str(x) if x is not None else "" for x in rows[header_row]]
        width = len(headers)
        cols = {h: [] for h in headers}
        for r in rows[header_row+1:]:
            r = (r or [])
            if len(r) < width: r = r + [None]*(width-len(r))
            elif len(r) > width: r = r[:width]
            for j, h in enumerate(headers):
                cols[h].append(r[j] if j < len(r) else None)
        return pd.DataFrame(cols)
    return pd.DataFrame()

def read_table(path: str, header_row: int = 1, sheet_name: Optional[str] = None) -> pd.DataFrame:
    ext = os.path.splitext(path.lower())[1]
    if ext == ".ods":
        return read_first_sheet_ods_with_header(path, header_row=header_row)
    if ext in (".xlsx", ".xls"):
        kw = dict(header=header_row, engine="openpyxl")
        if sheet_name:
            kw["sheet_name"] = sheet_name
        return pd.read_excel(path, **kw)
    raise RuntimeError(f"Unsupported extension: {ext}")

def preview_first_rows(path: str, sheet_name: Optional[str] = None, n: int = 5):
    """Print first n rows raw (header=None) to confirm header placement."""
    ext = os.path.splitext(path.lower())[1]
    try:
        if ext == ".ods":
            from pyexcel_ods3 import get_data
            book = get_data(path)
            for sname, rows in book.items():
                if sheet_name and sname != sheet_name:
                    continue
                print(f"[PREVIEW] {sname}:")
                for i, r in enumerate(rows[:n]):
                    print(f"Row {i}: {r}")
                break
        else:
            kw = dict(header=None, nrows=n, engine="openpyxl")
            if sheet_name:
                kw["sheet_name"] = sheet_name
            df = pd.read_excel(path, **kw)
            print(f"[PREVIEW] First {n} rows (raw, no header) from sheet={sheet_name or 'FIRST'}:")
            for i in range(min(n, len(df))):
                print(f"Row {i}: {list(df.iloc[i].values)}")
    except Exception as e:
        print(f"[WARN] Preview failed: {e}")

# ---------- Mongo ----------
def get_client_db(uri: str, dbname: str):
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=20000,
        socketTimeoutMS=300000,
        connectTimeoutMS=20000,
        maxPoolSize=100,
        compressors="zstd,snappy,zlib",
        retryWrites=True,
        appname="etl-upsert-ds",
    )
    db = client.get_database(dbname, write_concern=WriteConcern(w=1))
    return client, db

def ensure_ds_indexes(db):
    for f in ("nds_ds","imm_norm","ww_norm","vehicle_id","lines_sig"):
        db.ds.create_index(f)

# ---------- Google auth + Drive helpers (DS-only picking) ----------
def build_drive_service(creds_b64: str):
    """Create Drive service using base64 service-account JSON from .env."""
    data = base64.b64decode(creds_b64).decode("utf-8")
    info = json.loads(data)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def export_google_sheet_to_xlsx(drive, file_id: str, out_path: str):
    req = drive.files().export(fileId=file_id,
                               mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    print(f"[DL] Exported Google Sheet → {out_path}")

def download_file_content(drive, file_id: str, out_path: str):
    req = drive.files().get_media(fileId=file_id)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    print(f"[DL] Downloaded file → {out_path}")

def list_files_in_folder(drive, folder_id: str) -> List[Dict[str, Any]]:
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
    return files

def pick_latest_ds_only(drive, folder_id: str, pattern: str) -> Tuple[str, str, str]:
    """Return (file_id, name, mimeType) of newest file whose NAME matches DS regex."""
    files = list_files_in_folder(drive, folder_id)
    if not files:
        raise RuntimeError("No files found in the folder.")
    ds_files = [f for f in files if re.search(pattern, f.get("name", ""))]
    if not ds_files:
        raise RuntimeError(
            "No DS file matched DS_FILENAME_REGEX in the folder. "
            "Rename your DS file to include 'DS' (or set DS_FILENAME_REGEX in .env)."
        )
    f = ds_files[0]  # already sorted by modifiedTime desc
    return f["id"], f["name"], f["mimeType"]

# ---------- DS doc builder ----------
def build_ds_docs_from_dataframe(raw_ds: pd.DataFrame) -> List[Dict[str, Any]]:
    present = [c for c in HEADERS_MAP_DS if c in raw_ds.columns]
    if not present:
        print("[WARN] ds: expected headers not found; present=", list(raw_ds.columns))
        return []

    df = raw_ds[present].rename(columns=HEADERS_MAP_DS).copy()
    clean_object_columns_inplace(df)
    normalize_dates_inplace_df(df)
    apply_norms_inplace_df(df)

    def has_key(row): return bool(_txt(row.get("imm_norm")) or _txt(row.get("ww_norm")))
    mask = df.apply(has_key, axis=1)
    skipped = int((~mask).sum())
    if skipped:
        print(f"[INFO] ds: skipped rows without imm_norm/ww_norm = {skipped}")
    df = df[mask].copy()
    if df.empty:
        return []

    docs: List[Dict[str, Any]] = []
    latest_dt = None

    for ds_no, g in df.groupby("nds_ds", dropna=False, sort=False):
        _id = _txt(ds_no)
        if not _id:
            continue
        lines = canonicalize_lines(g)
        lines_sig = hash_lines(lines)

        imm_norm = _txt(g["imm_norm"].dropna().iloc[0]) if "imm_norm" in g and not g["imm_norm"].dropna().empty else None
        ww_norm  = _txt(g["ww_norm"].dropna().iloc[0])  if "ww_norm"  in g and not g["ww_norm"].dropna().empty  else None

        vehicle_id = imm_norm or ww_norm  # exact rule

        dt_series = pd.to_datetime(g["date_ds"], errors="coerce", utc=True).dropna()
        date_event = dt_series.max() if not dt_series.empty else None
        if date_event is not None:
            latest_dt = date_event if latest_dt is None else max(latest_dt, date_event)

        doc = {
            "_id": _id,
            "ds_no": _id,
            "vehicle_id": vehicle_id,
            "imm_norm": imm_norm,
            "ww_norm": ww_norm,
            "date_event": date_event,
            "lines": lines,
            "lines_sig": lines_sig,
        }
        docs.append(doc)

    if latest_dt:
        print(f"[DONE] latest DS date={latest_dt}")
    return docs

# ---------- Main ----------
def main():
    load_dotenv(override=False)

    mongo_uri = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or "mongodb://localhost:27017"
    mongo_db  = os.getenv("MONGODB_DB")  or os.getenv("MONGO_DB")  or "avis_db"
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    sheet_id  = os.getenv("GOOGLE_SHEET_ID", "").strip()
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    ds_sheet  = os.getenv("DS_SHEET_NAME", "").strip() or None
    ds_header = os.getenv("DS_HEADER_ROW", "1").strip()
    try:
        header_row = int(ds_header)
    except ValueError:
        header_row = 1

    ds_name_regex = os.getenv("DS_FILENAME_REGEX", DS_FILENAME_REGEX_DEFAULT)

    if not creds_b64:
        print("[ERROR] GOOGLE_CREDENTIALS_BASE64 is required in .env")
        sys.exit(1)

    # 1) Clean old DS files
    os.makedirs("data", exist_ok=True)
    for old in ("data/ds.xlsx", "data/ds.ods"):
        try:
            if os.path.exists(old):
                os.remove(old)
                print(f"[CLEAN] removed {old}")
        except Exception as e:
            print(f"[WARN] cannot remove {old}: {e}")

    # 2) Drive auth
    drive = build_drive_service(creds_b64)

    # 3) Decide source and download/export (DS-only)
    out_path = "data/ds.xlsx"  # default target
    if sheet_id:
        print(f"[SRC] Using GOOGLE_SHEET_ID={sheet_id} (export to XLSX)")
        export_google_sheet_to_xlsx(drive, sheet_id, out_path)
    elif folder_id:
        print(f"[SRC] Using GOOGLE_DRIVE_FOLDER_ID={folder_id} (DS-only; name regex)")
        fid, name, mime = pick_latest_ds_only(drive, folder_id, ds_name_regex)
        print(f"[PICK] {name}  mime={mime}")
        if mime == "application/vnd.google-apps.spreadsheet":
            out_path = "data/ds.xlsx"
            export_google_sheet_to_xlsx(drive, fid, out_path)
        else:
            _, ext = os.path.splitext(name.lower())
            if ext in ALLOWED_ODS:
                out_path = "data/ds.ods"
            else:
                out_path = "data/ds.xlsx"
            download_file_content(drive, fid, out_path)
    else:
        print("[ERROR] Provide either GOOGLE_SHEET_ID or GOOGLE_DRIVE_FOLDER_ID in .env")
        sys.exit(2)

    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        print("[ERROR] Download/export failed or produced empty file.")
        sys.exit(3)

    # 4) Preview first rows to confirm header placement
    preview_first_rows(out_path, sheet_name=ds_sheet, n=5)

    # 5) Read with headers on row 2 (header=1) unless overridden
    try:
        raw_ds = read_table(out_path, header_row=header_row, sheet_name=ds_sheet)
    except Exception as e:
        print(f"[ERROR] failed reading {out_path}: {e}")
        sys.exit(4)
    print(f"[INFO] ds: read shape={raw_ds.shape}")

    # 6) Build docs (exact contract)
    ds_docs = build_ds_docs_from_dataframe(raw_ds)
    if not ds_docs:
        print("[INFO] ds: nothing to push (headers missing or no keys).")
        sys.exit(0)

    # 7) Upsert to Mongo + indexes
    client, db = get_client_db(mongo_uri, mongo_db)
    try:
        ops = [
            UpdateOne({"_id": d["_id"]}, {"$set": {**d, "updated_at": datetime.now(TZ.utc)}}, upsert=True)
            for d in ds_docs
        ]
        if ops:
            db.ds.bulk_write(ops, ordered=False, bypass_document_validation=True)
            print("[OK] ds upserted; bulk write executed.")
        ensure_ds_indexes(db)
    finally:
        try: client.close()
        except Exception: pass

if __name__ == "__main__":
    main()
