#!/usr/bin/env python3  # Use the system's Python 3 interpreter to run this script
# -*- coding: utf-8 -*-  # Ensure the source file is interpreted as UTF-8 (handles accents in headers)

"""
parc.py — PARC loader with strict headers, date normalization, signature-based
upserts, and protective indexes.

Env (must be set via real env or .env file):
  - MONGODB_URI                → MongoDB connection string
  - MONGODB_DB                 → Database name
  - GOOGLE_CREDENTIALS_BASE64  → Base64 of a Google Service Account JSON (Drive readonly scope)
  - GOOGLE_DRIVE_FOLDER_ID     → Drive folder ID containing the PARC XLSX
"""  # Docstring describing purpose, behavior, and required environment variables

# ── Standard library imports ─────────────────────────────────────────
import os, re, io, json, base64, hashlib, unicodedata, warnings  # OS ops, regex, streams, JSON, base64, hashing, Unicode, warnings control
from datetime import datetime, date, timedelta, timezone as TZ    # Time/date utilities with explicit timezone handling

# ── Third-party imports ─────────────────────────────────────────────
import pandas as pd                                               # DataFrame operations and Excel reading
from dotenv import load_dotenv                                    # Loads .env into environment variables for local dev
from pymongo import MongoClient, UpdateOne, ASCENDING             # MongoDB client, bulk ops, and index direction constants
from pymongo.write_concern import WriteConcern                    # WriteConcern (we use w=1 for simplicity)
from google.oauth2 import service_account                         # Service account credentials for Google APIs
from googleapiclient.discovery import build                       # Google API client builder (Drive v3)
from googleapiclient.http import MediaIoBaseDownload              # Chunked download helper for Drive files

# Quiet a noisy warning from openpyxl that isn’t actionable for us
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ── Constants ────────────────────────────────────────────────────────
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"  # MIME type for .xlsx files on Drive
PARC_HEADER_ROW = 7                                                               # Zero-based row index where headers live in PARC sheet
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]                       # Read-only scope for Drive access

# Mapping of EXACT visible labels in the sheet → canonical field names used in our DB
PARC_HEADERS_MAP = {
    "Immatriculation": "imm",              # Vehicle registration plate (raw)
    "N° de chassis": "vin",                # VIN (raw)
    "WW": "ww",                            # Temporary registration (raw)
    "Date MCE": "date_mec",                # Mise en circulation (first registration) date
    "Locataire": "locataire_parc",         # Tenant or holder in PARC context
    "Etat véhicule": "etat_vehicule",      # Vehicle state
    "Modèle": "modele",                    # Model name/trim
    "Client": "client",                    # Client name if present in PARC export
    "Société": "societe",                  # Company/legal entity if provided
}

# ── Precompiled regexes & time constants ────────────────────────────
_ALNUM = re.compile(r"[^0-9A-Za-z]")                                              # Non-alphanumeric match (used to normalize plates/VIN/WW)
_EXCEL_EPOCH = datetime(1899, 12, 30, tzinfo=TZ.utc)                              # Excel serial date epoch for Windows Excel
_EXCEL_ESC_RE = re.compile(r"_x([0-9A-Fa-f]{4})_")                                # Excel XML escapes pattern (e.g., _x000A_)

# ── Logging helper ──────────────────────────────────────────────────
def log(msg): print(f"[{datetime.now(TZ.utc).strftime('%Y-%m-%d %H:%M:%SZ')}] {msg}")  # Simple UTC timestamped logger

# ── Tiny value helpers ──────────────────────────────────────────────
def _txt(x):
    """Normalize any Python value to a stripped string or None (treats NaN/'' as None)."""
    return None if pd.isna(x) or x is None else (str(x).strip() or None)

def canon_plate(x):
    """Normalize a plate/WW to alphanumeric lowercase (imm_norm/ww_norm)."""
    if pd.isna(x) or not x: return None
    return _ALNUM.sub("", str(x)).lower() or None

def canon_vin(x):
    """Normalize a VIN to alphanumeric uppercase (vin_norm)."""
    if pd.isna(x) or not x: return None
    s = _ALNUM.sub("", str(x)).upper()
    return s or None

def _iso_date_from_any(v):
    """
    Convert many date representations (dd-mm-yyyy, dd/mm/yyyy, free-form strings,
    and Excel serials) into ISO 'YYYY-MM-DD'. Returns None if parsing fails.
    """
    if pd.isna(v) or v in ("", None): return None
    if isinstance(v, datetime): return v.strftime("%Y-%m-%d")
    if isinstance(v, date): return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    # explicit day-first formats
    if re.match(r"^\d{1,2}[-/]\d{1,2}[-/]\d{4}$", s):
        d = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return d.strftime("%Y-%m-%d") if pd.notna(d) else None
    # general parse (still day-first biased)
    d = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(d): return d.strftime("%Y-%m-%d")
    # Excel serial (e.g., 45500)
    try:
        return (_EXCEL_EPOCH + timedelta(days=int(float(s)))).strftime("%Y-%m-%d")
    except Exception:
        return None

def _norm_label(s):
    """
    Normalize a column header for comparison: lower, strip accents, collapse spaces,
    and drop punctuation. This makes matching robust to minor formatting differences.
    """
    if s is None: return ""
    s = " ".join(str(s).split()).lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return re.sub(r"[^0-9a-z ]+", "", s).strip()

def _sha256_json(obj):
    """Deterministic SHA-256 over compact JSON; used as a content signature."""
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()

# ── Excel XML escape cleanup ────────────────────────────────────────
def _decode_excel_escapes_to_text(s):
    """
    Convert Excel XML escape sequences (e.g., _x000A_) into readable text.
    Control chars (LF/CR/TAB) become a single space. Collapses whitespace at the end.
    """
    if not isinstance(s, str): return s
    def repl(m):
        code = m.group(1).lower()
        if code in ("000a","000d","0009"):  # LF, CR, TAB → space
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
    """Apply Excel XML escape cleanup to all object columns in the DataFrame."""
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].map(_decode_excel_escapes_to_text)
    return df

# ── Google Drive I/O ────────────────────────────────────────────────
def make_drive(creds_b64):
    """Instantiate a Drive v3 client from base64-encoded service account JSON."""
    info = json.loads(base64.b64decode(creds_b64))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def download_xlsx(drive, fid, out):
    """Stream-download a Drive file (by fileId) to the given local path."""
    req = drive.files().get_media(fileId=fid)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out

def pick_latest_parc(drive, folder_id):
    """
    Find the newest XLSX in the Drive folder whose name contains the whole word 'PARC'
    (case-insensitive). Download it to data/parc.xlsx and return that path.
    """
    rx = re.compile(r"(?i)\bPARC\b")
    files = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,modifiedTime)",
        orderBy="modifiedTime desc"
    ).execute().get("files", [])
    for f in files:
        if rx.search(f["name"]) and f["mimeType"] == XLSX_MIME:
            log(f"Candidate: {f['name']}")
            path = download_xlsx(drive, f["id"], "data/.tmp_parc.xlsx")
            os.replace(path, "data/parc.xlsx")  # atomic move to stable path
            log(f"Accepted: {f['name']}")
            return "data/parc.xlsx"
    raise RuntimeError("No matching PARC XLSX found")

# ── Strict header reader & normalization ────────────────────────────
def read_strict(path, header_row, header_map, label):
    """
    Read an Excel file with headers at `header_row`, clean text artifacts,
    and keep only the columns defined in `header_map` (mapped to canonical names).
    """
    log(f"Read {label}")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")
    df = _clean_dataframe_excel_escapes(df)

    norm_expected = {_norm_label(k): (k, v) for k, v in header_map.items()}
    norm_actual = {_norm_label(c): c for c in df.columns}

    matched = {}
    for ne, (orig_label, canon) in norm_expected.items():
        if ne in norm_actual:
            matched[canon] = norm_actual[ne]

    if not matched:
        raise RuntimeError(f"{label}: no expected headers found")

    df = df[list(matched.values())].rename(columns={v: k for k, v in matched.items()})
    log(f"{label}: kept -> {list(df.columns)}")
    return df

# ── PARC dataframe → Mongo documents ────────────────────────────────
def build_parc_docs(df):
    """
    Convert a cleaned PARC DataFrame into MongoDB documents:
    - Compute imm_norm / vin_norm / ww_norm
    - Normalize date_mec
    - Choose _id by priority: imm_norm → vin_norm → ww_norm
    - Compute doc_sig for change detection
    """
    df = df.copy()
    df["imm_norm"] = df["imm"].map(canon_plate) if "imm" in df else None
    df["vin_norm"] = df["vin"].map(canon_vin) if "vin" in df else None
    if "ww" in df:
        df["ww_norm"] = df["ww"].map(canon_plate)
    if "date_mec" in df:
        df["date_mec"] = df["date_mec"].map(_iso_date_from_any)

    docs = []
    for _, r in df.iterrows():
        rec = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}
        _id = rec.get("imm_norm") or rec.get("vin_norm") or rec.get("ww_norm")
        if not _id:
            continue  # skip rows without any usable identifier
        rec["_id"] = _id
        rec["vehicle_id"] = _id  # convenience mirror for joins/queries
        rec["doc_sig"] = _sha256_json(rec)
        docs.append(rec)
    log(f"PARC docs: {len(docs)}")
    return docs

# ── MongoDB plumbing ─────────────────────────────────────────────────
def get_db(uri, dbname):
    """Create a Mongo client, ping it, and return (client, db)."""
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=20000,
        compressors=["zstd","snappy","zlib"],
    )
    client.admin.command("ping")
    return client, client.get_database(dbname, write_concern=WriteConcern(w=1))

def ensure_indexes_parc(db):
    """Create helpful indexes (id lookups + signature checks)."""
    db.parc.create_index([("imm_norm", ASCENDING)], sparse=True)
    db.parc.create_index([("vin_norm", ASCENDING)], sparse=True)
    db.parc.create_index([("ww_norm", ASCENDING)], sparse=True)
    db.parc.create_index([("doc_sig", ASCENDING)])

def upsert_with_sig(db, coll, docs, sig_field):
    """
    Bulk upsert with signature skip:
    - Fetch existing signatures for _id set
    - Skip unchanged docs
    - Upsert changed/new docs with created_at/updated_at timestamps
    """
    if not docs:
        log(f"{coll} → inserted=0 updated=0 skipped=0 (no docs)")
        return {"inserted": 0, "updated": 0, "skipped": 0}

    ids = [d["_id"] for d in docs if "_id" in d]
    exist = {x["_id"]: x.get(sig_field) for x in db[coll].find({"_id": {"$in": ids}}, {"_id": 1, sig_field: 1})}

    now = datetime.now(TZ.utc)
    ins=upd=skp=0; ops=[]
    for d in docs:
        prev = exist.get(d["_id"])
        if prev == d.get(sig_field):
            skp += 1
            continue
        if prev is None: ins += 1
        else: upd += 1
        payload = {**d, "updated_at": now}
        payload.setdefault("created_at", now)
        ops.append(UpdateOne({"_id": d["_id"]}, {"$set": payload}, upsert=True))

    if ops:
        db[coll].bulk_write(ops, ordered=False)

    log(f"{coll} → inserted={ins} updated={upd} skipped={skp}")
    return {"inserted":ins,"updated":upd,"skipped":skp}

# ── Main orchestration ───────────────────────────────────────────────
def run(cfg):
    """End-to-end pipeline: fetch → read → normalize → build docs → index → upsert."""
    log("BEGIN PARC load")
    drive = make_drive(cfg["GOOGLE_CREDENTIALS_BASE64"])
    parc_path = pick_latest_parc(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"])
    df_parc = read_strict(parc_path, PARC_HEADER_ROW, PARC_HEADERS_MAP, "PARC")
    parc_docs = build_parc_docs(df_parc)

    client, db = get_db(cfg["MONGODB_URI"], cfg["MONGODB_DB"])
    try:
        ensure_indexes_parc(db)
        upsert_with_sig(db, "parc", parc_docs, "doc_sig")
    finally:
        client.close()
    log("END PARC load")

# ── CLI entrypoint ───────────────────────────────────────────────────
if __name__ == "__main__":
    load_dotenv()  # Load local .env if present (handy for dev)
    cfg = {
        "MONGODB_URI": os.getenv("MONGODB_URI"),
        "MONGODB_DB": os.getenv("MONGODB_DB"),
        "GOOGLE_CREDENTIALS_BASE64": os.getenv("GOOGLE_CREDENTIALS_BASE64"),
        "GOOGLE_DRIVE_FOLDER_ID": os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
    }
    run(cfg)  # Kick off the pipeline
