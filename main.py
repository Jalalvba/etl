#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
main.py — DS / CP / PARC loader with strict headers and CP deduplication

Highlights:
  • DS, CP, PARC loaded from Google Drive (XLSX)
  • Strict header normalization (lowercase, accents stripped, punctuation removed)
  • Excel XML escapes decoded in cell values (e.g., '_x000A_' → space) to avoid UI artifacts
  • CP deduplicated by imm_norm, keeping only row with latest 'Date fin contrat'
  • Dates like '03-09-2025' normalized to '2025-09-03'
  • Upserts with content signatures (skip unchanged)
"""

import os, re, io, json, base64, hashlib, unicodedata, warnings
from datetime import datetime, date, timedelta, timezone as TZ
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ── Constants ────────────────────────────────────────────────────────
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
DS_HEADER_ROW, CP_HEADER_ROW, PARC_HEADER_ROW = 1, 7, 7
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# Exact labels expected in the XLSX files (row headers at the given header rows)
DS_HEADERS_MAP = {
    "Date DS": "date_ds", "Description": "description_ds",
    "Désignation Consomation": "designation_article_ds", "Founisseur": "fournisseur_ds",
    "Immatriculation": "imm", "KM": "km_ds", "N°DS": "nds_ds",
    "Prix Unitaire ds": "prix_unitaire_ds_ds", "Qté": "qte_ds",
    "ENTITE": "entite_ds", "Technicein": "technicien", "Code art": "code_art",
}
CP_HEADERS_MAP = {
    "IMM": "imm", "NUM chassis": "vin", "WW": "ww", "Client": "client",
    "Date début contrat": "date_debut_cp", "Libellé version long": "modele_long",
    "Date fin contrat": "date_fin_cp",
}
PARC_HEADERS_MAP = {
    "Immatriculation": "imm", "N° de chassis": "vin", "WW": "ww",
    "Modèle": "modele", "Date MCE": "date_mec", "Locataire": "locataire_parc",
    "Etat véhicule": "etat_vehicule",
}

_ALNUM = re.compile(r"[^0-9A-Za-z]")
_EXCEL_EPOCH = datetime(1899, 12, 30, tzinfo=TZ.utc)
_EXCEL_ESC_RE = re.compile(r"_x([0-9A-Fa-f]{4})_")  # Excel XML escape (LF/CR/TAB etc.)

# ── Helpers ──────────────────────────────────────────────────────────
def log(msg): print(f"[{datetime.now(TZ.utc).strftime('%Y-%m-%d %H:%M:%SZ')}] {msg}")

def _txt(x): return None if pd.isna(x) or x is None else (str(x).strip() or None)

def canon_plate(x):
    if pd.isna(x) or not x: return None
    return _ALNUM.sub("", str(x)).lower()

def canon_vin(x):
    if pd.isna(x) or not x: return None
    s = _ALNUM.sub("", str(x)).upper()
    return s or None

def _iso_date_from_any(v):
    """Convert strings like '03-09-2025' or Excel serials to ISO YYYY-MM-DD."""
    if pd.isna(v) or v in ("", None): return None
    if isinstance(v, datetime): return v.strftime("%Y-%m-%d")
    if isinstance(v, date): return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    if re.match(r"^\d{1,2}[-/]\d{1,2}[-/]\d{4}$", s):
        d = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return d.strftime("%Y-%m-%d") if pd.notna(d) else None
    d = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(d): return d.strftime("%Y-%m-%d")
    try:
        return (_EXCEL_EPOCH + timedelta(days=int(float(s)))).strftime("%Y-%m-%d")
    except Exception:
        return None

def _norm_label(s):
    if s is None: return ""
    s = " ".join(str(s).split())
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return re.sub(r"[^0-9a-z ]+", "", s).strip()

def _sha256_json(obj):
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()

# ── Decode Excel XML escapes in cell values ──────────────────────────
def _decode_excel_escapes_to_text(s):
    """
    Turn '_x000A_'/'_x000D_'/'_x0009_' into readable text.
    LF/CR/TAB → single space (keeps UI tidy); other codes → their Unicode char if printable.
    Collapse repeated whitespace and trim.
    """
    if not isinstance(s, str): return s
    def repl(m):
        code = m.group(1).lower()
        if code in ("000a","000d","0009"):  # \n, \r, \t
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
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].map(_decode_excel_escapes_to_text)
    return df

# ── Drive download ───────────────────────────────────────────────────
def make_drive(creds_b64):
    info = json.loads(base64.b64decode(creds_b64))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def download_xlsx(drive, fid, out):
    req = drive.files().get_media(fileId=fid)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return out

def pick(drive, folder_id, regex, tmp, final):
    rx = re.compile(regex)
    files = drive.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,modifiedTime)",
        orderBy="modifiedTime desc"
    ).execute().get("files", [])
    for f in files:
        if rx.search(f["name"]) and f["mimeType"] == XLSX_MIME:
            log(f"Candidate: {f['name']}")
            path = download_xlsx(drive, f["id"], f"data/{tmp}")
            os.replace(path, f"data/{final}")
            log(f"Accepted: {f['name']}")
            return f["id"], f["name"], f"data/{final}"
    raise RuntimeError("No matching XLSX found")

# ── Strict header reader (+ value cleanup) ──────────────────────────
def read_strict(path, header_row, header_map, label):
    log(f"Read {label}")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")
    # decode Excel XML escapes in ALL string cells
    df = _clean_dataframe_excel_escapes(df)

    norm_expected = {_norm_label(k): v for k, v in header_map.items()}
    norm_actual = {_norm_label(c): c for c in df.columns}
    matched = {v: norm_actual[n] for n, v in norm_expected.items() if n in norm_actual}
    if not matched:
        raise RuntimeError(f"{label}: no expected headers found")
    df = df[list(matched.values())].rename(columns={v: k for k, v in matched.items()})
    log(f"{label}: kept -> {list(df.columns)}")
    return df

# ── DS building ──────────────────────────────────────────────────────
def build_ds_docs(df):
    df = df.copy()
    df["date_ds"] = df["date_ds"].map(_iso_date_from_any)
    df["imm_norm"] = df["imm"].map(canon_plate)
    docs = []
    for ds_no, g in df.groupby("nds_ds", dropna=False):
        imm_norm = None
        if "imm_norm" in g and not g["imm_norm"].dropna().empty:
            imm_norm = _txt(g["imm_norm"].dropna().iloc[0])
        # lines as plain records (already cleaned); if you prefer full canonicalization,
        # we can sort and pick DS_LINE_FIELDS like in appds.py
        lines = g.to_dict(orient="records")
        date_event = pd.to_datetime(g["date_ds"], errors="coerce", utc=True).max()
        date_event = date_event.to_pydatetime() if pd.notna(date_event) else None
        docs.append({
            "_id": _txt(ds_no),
            "vehicle_id": imm_norm,
            "imm_norm": imm_norm,
            "date_event": date_event,
            "lines": lines,
            "lines_sig": _sha256_json(lines),
        })
    log(f"DS docs: {len(docs)}")
    return docs

# ── CP handling ──────────────────────────────────────────────────────
def process_cp(df):
    df = df.copy()
    df["imm_norm"] = df["imm"].map(canon_plate)
    df["date_debut_cp"] = df["date_debut_cp"].map(_iso_date_from_any)
    df["date_fin_cp"] = df["date_fin_cp"].map(_iso_date_from_any)
    total_rows = len(df)
    df_sorted = df.sort_values(by="date_fin_cp", ascending=False, na_position="last")
    df_unique = df_sorted.drop_duplicates(subset=["imm_norm"], keep="first")
    dropped = total_rows - len(df_unique)
    log(f"CP stats: total_rows={total_rows} kept={len(df_unique)} dropped={dropped}")
    return df_unique

def build_cp_docs(df):
    docs = []
    for _, r in df.iterrows():
        body = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}
        _id = body.get("imm_norm")
        if not _id:
            _id = canon_vin(body.get("vin")) or canon_plate(body.get("ww"))
        body["_id"] = _id
        body["doc_sig"] = _sha256_json(body)
        docs.append(body)
    log(f"CP docs: {len(docs)}")
    return docs

# ── PARC handling ───────────────────────────────────────────────────
def build_parc_docs(df):
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
            continue
        rec["_id"] = _id
        rec["vehicle_id"] = _id
        rec["doc_sig"] = _sha256_json(rec)
        docs.append(rec)
    log(f"PARC docs: {len(docs)}")
    return docs

# ── Mongo plumbing ───────────────────────────────────────────────────
def get_db(uri, dbname):
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=20000,
        compressors=["zstd","snappy","zlib"],  # list, not comma-string
    )
    client.admin.command("ping")
    return client, client.get_database(dbname, write_concern=WriteConcern(w=1))

def upsert_with_sig(db, coll, docs, sig_field):
    if not docs:
        log(f"{coll} → inserted=0 updated=0 skipped=0 (no docs)")
        return {"inserted": 0, "updated": 0, "skipped": 0}
    ids = [d["_id"] for d in docs if "_id" in d]
    sig_map = {x["_id"]: x.get(sig_field) for x in db[coll].find({"_id": {"$in": ids}}, {"_id": 1, sig_field: 1})}
    now = datetime.now(TZ.utc)
    ins=upd=skp=0; ops=[]
    for d in docs:
        prev = sig_map.get(d["_id"])
        if prev == d.get(sig_field):
            skp += 1
            continue
        if prev is None:
            ins += 1
        else:
            upd += 1
        ops.append(UpdateOne({"_id": d["_id"]}, {"$set": {**d, "updated_at": now}}, upsert=True))
    if ops: db[coll].bulk_write(ops, ordered=False)
    log(f"{coll} → inserted={ins} updated={upd} skipped={skp}")
    return {"inserted":ins,"updated":upd,"skipped":skp}

def drop_old_cp_duplicates(db):
    log("Clean old CP duplicates (keep only latest per imm_norm)")
    pipeline = [
        {"$sort": {"date_fin_cp": -1}},
        {"$group": {"_id": "$imm_norm", "keep": {"$first": "$_id"}, "remove": {"$push": "$_id"}}},
    ]
    dupes = list(db.cp.aggregate(pipeline))
    to_delete = [r for d in dupes for r in d["remove"][1:]]
    if to_delete:
        db.cp.delete_many({"_id": {"$in": to_delete}})
        log(f"Removed {len(to_delete)} old CP duplicates")
    else:
        log("No CP duplicates found")

# ── Main orchestration ───────────────────────────────────────────────
def run(cfg):
    log("BEGIN main load")
    drive = make_drive(cfg["GOOGLE_CREDENTIALS_BASE64"])

    _,_,ds_path = pick(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"], r"(?i)DS", ".tmp_ds.xlsx", "ds.xlsx")
    df_ds = read_strict(ds_path, DS_HEADER_ROW, DS_HEADERS_MAP, "DS")
    ds_docs = build_ds_docs(df_ds)

    _,_,cp_path = pick(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"], r"(?i)CP", ".tmp_cp.xlsx", "cp.xlsx")
    df_cp = read_strict(cp_path, CP_HEADER_ROW, CP_HEADERS_MAP, "CP")
    df_cp_unique = process_cp(df_cp)
    cp_docs = build_cp_docs(df_cp_unique)

    _,_,parc_path = pick(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"], r"(?i)PARC", ".tmp_parc.xlsx", "parc.xlsx")
    df_parc = read_strict(parc_path, PARC_HEADER_ROW, PARC_HEADERS_MAP, "PARC")
    parc_docs = build_parc_docs(df_parc)

    client, db = get_db(cfg["MONGODB_URI"], cfg["MONGODB_DB"])
    try:
        drop_old_cp_duplicates(db)
        upsert_with_sig(db, "ds", ds_docs, "lines_sig")
        upsert_with_sig(db, "cp", cp_docs, "doc_sig")
        upsert_with_sig(db, "parc", parc_docs, "doc_sig")
    finally:
        client.close()
    log("END main load")

# ── Entrypoint ───────────────────────────────────────────────────────
if __name__ == "__main__":
    load_dotenv()
    cfg = {
        "MONGODB_URI": os.getenv("MONGODB_URI"),
        "MONGODB_DB": os.getenv("MONGODB_DB"),
        "GOOGLE_CREDENTIALS_BASE64": os.getenv("GOOGLE_CREDENTIALS_BASE64"),
        "GOOGLE_DRIVE_FOLDER_ID": os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
    }
    run(cfg)
