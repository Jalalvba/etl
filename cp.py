#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
cp.py — CP loader for long-term rental contracts.

This script automates:
  • Fetching the latest CP Excel file from Google Drive (or a local copy)
  • Reading only expected headers with strict validation
  • Normalizing plates, VINs, and dates
  • Deduplicating rows (latest contract per imm_norm)
  • Building stable MongoDB documents
  • Upserting them by SHA-256 signature
  • Ensuring indexes and cleaning old duplicates

Safe for production: idempotent, signature-based, and logs all operations.
"""

# ─────────────────────────────────────────────────────────────────────────────
# Imports
# ─────────────────────────────────────────────────────────────────────────────
import os, re, io, json, base64, hashlib, unicodedata, warnings, argparse, time
from datetime import datetime, date, timedelta, timezone as TZ
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
from pymongo.write_concern import WriteConcern
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ─────────────────────────────────────────────────────────────────────────────
# Constants and configuration
# ─────────────────────────────────────────────────────────────────────────────
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DEFAULT_CP_HEADER_ROW = 7  # row index (0-based) where headers start
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# Expected columns from the CP Excel file
CP_HEADERS_MAP = {
    "IMM": "imm",
    "NUM chassis": "vin",
    "WW": "ww",
    "Client": "client",
    "Date début contrat": "date_debut_cp",
    "Libellé version long": "modele_long",
    "Date fin contrat": "date_fin_cp",
}

# Regex helpers for normalization
_ALNUM = re.compile(r"[^0-9A-Za-z]")
_EXCEL_EPOCH = datetime(1899, 12, 30, tzinfo=TZ.utc)
_EXCEL_ESC_RE = re.compile(r"_x([0-9A-Fa-f]{4})_")

# ─────────────────────────────────────────────────────────────────────────────
# Logging helper
# ─────────────────────────────────────────────────────────────────────────────
def log(msg):
    """UTC timestamped print to make logs consistent and grep-friendly."""
    print(f"[{datetime.now(TZ.utc).strftime('%Y-%m-%d %H:%M:%SZ')}] {msg}")

# ─────────────────────────────────────────────────────────────────────────────
# Basic normalization helpers
# ─────────────────────────────────────────────────────────────────────────────
def _txt(x):
    """Return stripped text or None."""
    return None if pd.isna(x) or x is None else (str(x).strip() or None)

def canon_plate(x):
    """Normalize a registration number: keep only alphanumerics, lowercase."""
    if pd.isna(x) or not x: return None
    return _ALNUM.sub("", str(x)).lower() or None

def canon_vin(x):
    """Normalize VIN: uppercase, remove non-alphanumerics."""
    if pd.isna(x) or not x: return None
    s = _ALNUM.sub("", str(x)).upper()
    return s or None

def _iso_date_from_any(v):
    """Convert Excel or text date formats to ISO (YYYY-MM-DD)."""
    if pd.isna(v) or v in ("", None): return None
    if isinstance(v, datetime): return v.date().isoformat()
    if isinstance(v, date): return v.isoformat()

    s = str(v).strip()
    # Try standard formats first
    d = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(d): return d.date().isoformat()

    # Try Excel serial numbers
    try:
        serial = float(s)
        d = _EXCEL_EPOCH + timedelta(days=int(serial))
        return d.date().isoformat()
    except Exception:
        return None

def _norm_label(s):
    """Simplify column labels for fuzzy matching (case/accents/spacing)."""
    if s is None: return ""
    s = " ".join(str(s).split()).lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return re.sub(r"[^0-9a-z ]+", "", s).strip()

def _sha256_json(obj):
    """Stable SHA256 hash of a JSON object (used as doc signature)."""
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()

# ─────────────────────────────────────────────────────────────────────────────
# Excel-specific cleaning
# ─────────────────────────────────────────────────────────────────────────────
def _decode_excel_escapes_to_text(s):
    """Decode weird Excel XML escapes like '_x000A_' (linefeeds)."""
    if not isinstance(s, str): return s
    def repl(m):
        code = m.group(1).lower()
        if code in ("000a","000d","0009"):
            return " "
        try:
            ch = chr(int(code, 16))
            return " " if ord(ch) < 32 else ch
        except Exception:
            return " "
    s = _EXCEL_ESC_RE.sub(repl, s)
    return re.sub(r"[ \t\r\n]+", " ", s).strip()

def _clean_dataframe_excel_escapes(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Excel escape decoding to all string columns."""
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].map(_decode_excel_escapes_to_text)
    return df

# ─────────────────────────────────────────────────────────────────────────────
# Google Drive I/O
# ─────────────────────────────────────────────────────────────────────────────
def make_drive(creds_b64):
    """Instantiate a read-only Google Drive service from base64 credentials."""
    info = json.loads(base64.b64decode(creds_b64))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def download_xlsx(drive, fid, out, retries=3):
    """Download a file by Drive fileId with retry."""
    req = drive.files().get_media(fileId=fid)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    for attempt in range(1, retries+1):
        try:
            with open(out, "wb") as f:
                downloader = MediaIoBaseDownload(f, req)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
            return out
        except HttpError as e:
            if attempt == retries: raise
            log(f"Retry {attempt}/{retries}: {e}")
            time.sleep(1.5 * attempt)
    return out

def pick_latest_cp(drive, folder_id):
    """Find the latest XLSX file in Drive folder whose name contains 'CP'."""
    rx = re.compile(r"(?i)\bCP\b")
    files = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,modifiedTime)",
        orderBy="modifiedTime desc"
    ).execute().get("files", [])
    for f in files:
        if rx.search(f["name"]) and f["mimeType"] == XLSX_MIME:
            log(f"Candidate: {f['name']} (modified {f.get('modifiedTime')})")
            path = download_xlsx(drive, f["id"], "data/.tmp_cp.xlsx")
            os.replace(path, "data/cp.xlsx")
            log(f"Accepted: {f['name']}")
            return "data/cp.xlsx"
    raise RuntimeError("No matching CP XLSX found")

# ─────────────────────────────────────────────────────────────────────────────
# Reading and normalization
# ─────────────────────────────────────────────────────────────────────────────
def read_strict(path, header_row, header_map, label):
    """Read Excel with strict header verification."""
    log(f"Read {label} from {path}")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")
    df = _clean_dataframe_excel_escapes(df)

    # Build normalized mappings
    norm_expected = {_norm_label(k): (k, v) for k, v in header_map.items()}
    norm_actual = {_norm_label(c): c for c in df.columns}

    matched = {}
    for ne, (orig_label, canon) in norm_expected.items():
        if ne in norm_actual:
            matched[canon] = norm_actual[ne]

    if not matched:
        raise RuntimeError(f"{label}: expected headers not found, check --header-row")

    df = df[list(matched.values())].rename(columns={v: k for k, v in matched.items()})
    log(f"{label}: kept -> {list(df.columns)}")
    return df

# ─────────────────────────────────────────────────────────────────────────────
# CP-specific transformations
# ─────────────────────────────────────────────────────────────────────────────
def process_cp(df):
    """Normalize and deduplicate CP rows."""
    df = df.copy()
    df["imm_norm"] = df["imm"].map(canon_plate)
    df["vin_norm"] = df["vin"].map(canon_vin)
    df["ww_norm"]  = df["ww"].map(canon_plate)
    df["date_debut_cp"] = df["date_debut_cp"].map(_iso_date_from_any)
    df["date_fin_cp"]   = df["date_fin_cp"].map(_iso_date_from_any)
    df["_sort_date"] = pd.to_datetime(df["date_fin_cp"], errors="coerce")

    # Keep latest by date_fin_cp per imm_norm
    df = df.sort_values(by="_sort_date", ascending=False, na_position="last").drop(columns="_sort_date")
    total_rows = len(df)
    df_unique = df.drop_duplicates(subset=["imm_norm"], keep="first")
    dropped = total_rows - len(df_unique)
    log(f"CP stats: total_rows={total_rows} kept={len(df_unique)} dropped={dropped}")
    return df_unique

def build_cp_docs(df):
    """Convert dataframe rows into MongoDB-ready documents."""
    docs = []
    for _, r in df.iterrows():
        body = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}

        # Primary key priority: imm_norm → vin_norm → ww_norm
        _id = body.get("imm_norm") or body.get("vin_norm") or body.get("ww_norm")
        if not _id:  # skip unidentifiable vehicles
            continue

        # Signature (hash of key business fields)
        body["_id"] = _id
        body["doc_sig"] = _sha256_json({
            "imm_norm": body.get("imm_norm"),
            "vin_norm": body.get("vin_norm"),
            "ww_norm":  body.get("ww_norm"),
            "imm":      body.get("imm"),
            "vin":      body.get("vin"),
            "ww":       body.get("ww"),
            "client":   body.get("client"),
            "modele_long": body.get("modele_long"),
            "date_debut_cp": body.get("date_debut_cp"),
            "date_fin_cp":   body.get("date_fin_cp"),
        })

        # Drop None fields for cleaner storage
        body = {k: v for k, v in body.items() if v is not None}
        docs.append(body)

    log(f"CP docs built: {len(docs)}")
    return docs

# ─────────────────────────────────────────────────────────────────────────────
# MongoDB plumbing
# ─────────────────────────────────────────────────────────────────────────────
def get_db(uri, dbname):
    """Connect to MongoDB with compression and ping check."""
    client = MongoClient(uri, serverSelectionTimeoutMS=20000, compressors=["zstd","snappy","zlib"])
    client.admin.command("ping")
    return client, client.get_database(dbname, write_concern=WriteConcern(w=1))

def ensure_indexes_cp(db):
    """Create standard non-unique indexes for CP collection."""
    db.cp.create_index([("imm_norm", ASCENDING)], name="imm_norm", sparse=True)
    db.cp.create_index([("vin_norm", ASCENDING)], name="vin_norm", sparse=True)
    db.cp.create_index([("ww_norm",  ASCENDING)], name="ww_norm",  sparse=True)
    db.cp.create_index([("doc_sig",  ASCENDING)], name="doc_sig")
    db.cp.create_index([("date_fin_cp", ASCENDING)], name="date_fin_cp", sparse=True)

def upsert_with_sig(db, coll, docs, sig_field, dry_run=False):
    """Upsert documents only if their signature changed."""
    if not docs:
        log(f"{coll} → inserted=0 updated=0 skipped=0 (no docs)")
        return

    ids = [d["_id"] for d in docs]
    existing = {x["_id"]: x.get(sig_field) for x in db[coll].find({"_id": {"$in": ids}}, {"_id": 1, sig_field: 1})}
    now = datetime.now(TZ.utc)

    ins = upd = skp = 0
    ops = []
    for d in docs:
        prev = existing.get(d["_id"])
        cur  = d.get(sig_field)
        if prev == cur:
            skp += 1
            continue
        if prev is None: ins += 1
        else: upd += 1
        payload = {**d, "updated_at": now}
        payload.setdefault("created_at", now)
        ops.append(UpdateOne({"_id": d["_id"]}, {"$set": payload}, upsert=True))

    if dry_run:
        log(f"{coll} (dry-run) → would insert={ins} update={upd} skip={skp}")
        return

    try:
        if ops: db[coll].bulk_write(ops, ordered=False)
    except BulkWriteError as e:
        log(f"Bulk write error: {e.details}")
        raise
    log(f"{coll} → inserted={ins} updated={upd} skipped={skp}")

def drop_old_cp_duplicates(db):
    """Remove redundant old CP docs with same imm_norm."""
    log("Clean old CP duplicates (keep latest per imm_norm)")
    dup_keys = [r["_id"] for r in db.cp.aggregate([
        {"$group": {"_id":"$imm_norm","n":{"$sum":1}}},
        {"$match":{"n":{"$gt":1}}}
    ]) if r["_id"]]
    removed = 0
    for key in dup_keys:
        rows = list(db.cp.find({"imm_norm": key}))
        def sort_key(r):
            d = pd.to_datetime(r.get("date_fin_cp"), errors="coerce")
            return (d if pd.notna(d) else pd.Timestamp.min)
        rows.sort(key=sort_key, reverse=True)
        to_delete = [r["_id"] for r in rows[1:]]
        if to_delete:
            db.cp.delete_many({"_id": {"$in": to_delete}})
            removed += len(to_delete)
    log(f"Removed {removed} old CP duplicates")

# ─────────────────────────────────────────────────────────────────────────────
# Main entrypoint
# ─────────────────────────────────────────────────────────────────────────────
def run(cfg, local_xlsx=None, header_row=DEFAULT_CP_HEADER_ROW, dry_run=False):
    """Main execution pipeline."""
    # Validate environment first
    for k in ("MONGODB_URI", "MONGODB_DB", "GOOGLE_CREDENTIALS_BASE64", "GOOGLE_DRIVE_FOLDER_ID"):
        if not cfg.get(k) and not (local_xlsx and k in {"MONGODB_URI","MONGODB_DB"}):
            raise RuntimeError(f"Missing required env: {k}")

    log("BEGIN CP load")

    # 1) Acquire XLSX (Drive or local)
    if local_xlsx:
        cp_path = local_xlsx
        if not os.path.exists(cp_path):
            raise FileNotFoundError(f"--local-xlsx path not found: {cp_path}")
        log(f"Using local XLSX: {cp_path}")
    else:
        drive = make_drive(cfg["GOOGLE_CREDENTIALS_BASE64"])
        cp_path = pick_latest_cp(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"])

    # 2) Parse, normalize, and deduplicate
    df_cp = read_strict(cp_path, header_row, CP_HEADERS_MAP, "CP")
    df_cp_unique = process_cp(df_cp)
    cp_docs = build_cp_docs(df_cp_unique)

    # 3) MongoDB operations
    client, db = get_db(cfg["MONGODB_URI"], cfg["MONGODB_DB"])
    try:
        ensure_indexes_cp(db)
        drop_old_cp_duplicates(db)
        upsert_with_sig(db, "cp", cp_docs, "doc_sig", dry_run=dry_run)
    finally:
        client.close()

    log("END CP load")

# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(description="CP loader")
    ap.add_argument("--local-xlsx", help="Path to local CP.xlsx (bypass Drive)", default=None)
    ap.add_argument("--header-row", type=int, default=DEFAULT_CP_HEADER_ROW)
    ap.add_argument("--dry-run", action="store_true")
    return ap.parse_args()

if __name__ == "__main__":
    load_dotenv()
    args = parse_args()
    cfg = {
        "MONGODB_URI": os.getenv("MONGODB_URI"),
        "MONGODB_DB": os.getenv("MONGODB_DB"),
        "GOOGLE_CREDENTIALS_BASE64": os.getenv("GOOGLE_CREDENTIALS_BASE64"),
        "GOOGLE_DRIVE_FOLDER_ID": os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
    }
    run(cfg, local_xlsx=args.local_xlsx, header_row=args.header_row, dry_run=args.dry_run)
