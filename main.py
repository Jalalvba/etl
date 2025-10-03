#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, io, re, time, argparse, math, hashlib, json, base64
from datetime import datetime, date, timedelta, timezone as TZ
from typing import Any, Optional, Iterable, List, Tuple, Dict, Set
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import AutoReconnect, NetworkTimeout
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

# ---------- Timing helper ----------
class StepTimer:
    def __init__(self): self.t0 = time.perf_counter(); self.steps: List[Tuple[str, float]] = []
    @staticmethod
    def _ts(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    @contextmanager
    def stage(self, name: str):
        print(f"[{self._ts()}] [START] {name}"); t = time.perf_counter()
        try: yield
        finally:
            dt = time.perf_counter()-t; self.steps.append((name, dt))
            print(f"[{self._ts()}] [END]   {name} — {dt:.2f}s")
    def summary(self):
        total = time.perf_counter()-self.t0
        print("\n========== EXECUTION SUMMARY ==========")
        for name, dt in self.steps: print(f"  • {name:<35} {dt:>8.2f}s")
        print("---------------------------------------")
        print(f"  TOTAL                               {total:>8.2f}s")
        print("=======================================\n")

# ---------- Header maps ----------
HEADERS_MAP = {
    "cp": {
        "Client":"client_cp",
        "Date début contrat":"date_debut_cp",
        "Date fin contrat":"date_fin_cp",
        "IMM":"imm",
        "Marque":"marque",
        "Modèle":"modele",
        "Libellé version long":"modele_long",
        "NUM chassis":"vin",
        "WW":"ww",
    },
    "ds": {
        "Date DS":"date_ds",
        "Description":"description_ds",
        "Désignation article":"designation_article_ds",
        "Founisseur":"fournisseur_ds",
        "Immatriculation":"imm",
        "KM":"km_ds",
        "N°DS":"nds_ds",
        "Prix Unitaire ds":"prix_unitaire_ds_ds",
        "Qté":"qte_ds",
        "ENTITE":"entite_ds",
        "Technicein":"technicien",
        "Code art":"code_art",
    },
    "parc": {
        "Immatriculation":"imm",
        "Marque":"marque",
        "Modèle":"modele",
        "Numéro WW":"ww",
        "N° de chassis":"vin",
        "Etat véhicule":"etat_vehicule",
        "Client":"client_parc",
        "Locataire":"locataire_parc",
        "Date MCE":"date_mce_parc",
    },
}

# ---------- Helpers / normalization ----------
_ALNUM = re.compile(r"[^0-9A-Za-z]")
_EXCEL_EPOCH = datetime(1899, 12, 30)

def _txt(x: Any) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    s = str(x).strip(); return s or None

def canon_plate(x: Any) -> Optional[str]:
    if pd.isna(x): return None
    return _ALNUM.sub("", str(x)).lower() or None

def canon_vin(x: Any) -> Optional[str]:
    if pd.isna(x): return None
    s = str(x).upper()
    return _ALNUM.sub("", s).replace("I","").replace("O","").replace("Q","") or None

def _parse_any_to_date(v: Any) -> Optional[date]:
    if pd.isna(v): return None
    if isinstance(v, date) and not isinstance(v, datetime): return v
    if isinstance(v, datetime): return v.date()
    if isinstance(v, (int, float)) and not pd.isna(v):
        try:
            serial = int(v)
            if serial > 0: return (_EXCEL_EPOCH + timedelta(days=serial)).date()
        except Exception: pass
    s = str(v).strip()
    if not s: return None
    if re.fullmatch(r"\d{1,6}", s):
        try:
            serial = int(s)
            if serial > 0: return (_EXCEL_EPOCH + timedelta(days=serial)).date()
        except Exception: return None
    m = re.match(r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})$", s)
    if m:
        dd, mm, yy = map(int, m.groups())
        try: return date(yy, mm, dd)
        except ValueError: return None
    m = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$", s)
    if m:
        yy, mm, dd = map(int, m.groups())
        try: return date(yy, mm, dd)
        except ValueError: return None
    d = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return d.date() if pd.notna(d) else None

def _sanitize_range(d: Optional[date], min_year=2000, max_year=2035) -> Optional[date]:
    if not d: return None
    return d if (min_year <= d.year <= max_year) else None

def _iso(d: Optional[date]) -> Optional[str]:
    return d.strftime("%Y-%m-%d") if d else None

def json_sig(obj: Any) -> str:
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

# ---------- Google auth + Drive helpers (always output .xlsx) ----------
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
GSHEET_MIME = "application/vnd.google-apps.spreadsheet"
XLSX_MIME   = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

def build_drive_service(creds_b64: str):
    info = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def _atomic_write_request(req, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()

def list_folder(drive, folder_id: str) -> List[Dict[str, str]]:
    q = f"'{folder_id}' in parents and trashed=false"
    res = drive.files().list(
        q=q, fields="files(id,name,mimeType,modifiedTime)", orderBy="modifiedTime desc", pageSize=1000
    ).execute()
    return res.get("files", [])

def pick_by_basename(files: List[Dict[str,str]], base: str) -> Optional[Dict[str,str]]:
    """Pick the most recent file whose name starts with base (case-insensitive). Prefer xlsx/Sheets."""
    base = base.lower()
    candidates = []
    for f in files:
        name = (f.get("name") or "").lower()
        if name == base or name.startswith(base + ".") or name.startswith(base + " "):
            candidates.append(f)
    if not candidates:  # fallback: contains
        for f in files:
            name = (f.get("name") or "").lower()
            if base in name:
                candidates.append(f)
    if not candidates: return None
    # prefer xlsx/Sheets first
    def score(f):
        mt = f.get("mimeType","")
        if mt == XLSX_MIME: return 0
        if mt == GSHEET_MIME: return 1
        return 2
    candidates.sort(key=score)
    return candidates[0]

def export_sheet_xlsx(drive, file_id: str, out_path_xlsx: str):
    req = drive.files().export(fileId=file_id, mimeType=XLSX_MIME)
    _atomic_write_request(req, out_path_xlsx)
    print(f"[DL] Exported Google Sheet → {out_path_xlsx}")

def ensure_xlsx_download(drive, file_meta: Dict[str,str], prefer_basename: str, out_dir: str) -> str:
    """Guaranteed .xlsx output, regardless of source mime (Sheet/.xlsx/other → convert via export)."""
    name = file_meta.get("name") or prefer_basename
    mime = file_meta.get("mimeType", "")
    fid  = file_meta["id"]
    out_path = os.path.join(out_dir, f"{prefer_basename}.xlsx")

    if mime == GSHEET_MIME:
        export_sheet_xlsx(drive, fid, out_path)
        print(f"[OK] Saved {name} (Sheet) → {out_path}")
        return out_path

    if mime == XLSX_MIME or name.lower().endswith(".xlsx"):
        req = drive.files().get_media(fileId=fid)
        _atomic_write_request(req, out_path)
        print(f"[DL] Downloaded XLSX → {out_path}")
        return out_path

    # Other format: convert by copying to Sheet then export .xlsx
    copied = drive.files().copy(fileId=fid, body={"mimeType": GSHEET_MIME, "name": f"{prefer_basename}-autoconvert"}).execute()
    sheet_id = copied["id"]
    try:
        export_sheet_xlsx(drive, sheet_id, out_path)
        print(f"[OK] Converted {name} → Sheet → {out_path}")
    finally:
        try:
            drive.files().delete(fileId=sheet_id).execute()
        except Exception:
            pass
    return out_path

# ---------- Readers (xlsx only) ----------
def read_cp_parc(path: str) -> pd.DataFrame:
    return pd.read_excel(path, header=7, engine="openpyxl")  # row 8

def read_ds(path: str) -> pd.DataFrame:
    return pd.read_excel(path, header=1, engine="openpyxl")  # row 2

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
        appname="etl-upsert",
    )
    db = client.get_database(dbname, write_concern=WriteConcern(w=1))
    return client, db

# ---------- Bulk ----------
def _chunked(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n): yield seq[i:i+n]

def _do_bulk(coll, chunk, label, idx, total_batches, bypass):
    t0 = time.perf_counter()
    coll.bulk_write(chunk, ordered=False, bypass_document_validation=bypass)
    dt = time.perf_counter() - t0
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rate = len(chunk)/dt if dt > 0 else float('inf')
    print(f"[{now}] [BULK] {label} batch {idx}/{total_batches} items={len(chunk)} dt={dt:.2f}s rate={rate:,.0f}/s")
    return dt, len(chunk)

def bulk_exec(coll, ops, batch_size=1000, label="ops", workers=4, max_retries=5, sleep_base=0.8, bypass=True):
    if not ops: return 0
    batches = list(_chunked(ops, batch_size))
    total_batches, total_items = len(batches), len(ops)
    start, completed, total_dt = time.perf_counter(), 0, 0.0

    if workers <= 1:
        for i, chunk in enumerate(batches, 1):
            attempt = 0
            while True:
                try:
                    dt, done = _do_bulk(coll, chunk, label, i, total_batches, bypass)
                    completed += done; total_dt += dt
                    elapsed = time.perf_counter()-start
                    avg = total_dt / max(i, 1)
                    eta = elapsed + (total_batches - i)*avg
                    print(f"        progress={completed:,}/{total_items:,} elapsed={elapsed:.1f}s ETA≈{eta:.1f}s")
                    break
                except (AutoReconnect, NetworkTimeout) as e:
                    if attempt >= max_retries: print(f"[ERROR] {label} batch {i} failed: {e}"); raise
                    delay = sleep_base*(2**attempt); print(f"[RETRY] {label} batch {i} → sleep {delay:.1f}s ({e})")
                    time.sleep(delay); attempt += 1
        return total_items

    i = 0
    with ThreadPoolExecutor(max_workers=workers) as exe:
        while i < total_batches:
            window = []
            for _ in range(min(workers, total_batches - i)):
                idx = i + 1
                window.append(exe.submit(_do_bulk, coll, batches[i], label, idx, total_batches, bypass))
                i += 1
            for fut in as_completed(window):
                dt, done = fut.result()
                completed += done; total_dt += dt
                elapsed = time.perf_counter()-start
                finished = math.ceil(completed / batch_size)
                avg = total_dt / max(finished, 1)
                eta = elapsed + max(total_batches - finished, 0)*avg
                print(f"        progress={completed:,}/{total_items:,} elapsed={elapsed:.1f}s ETA≈{eta:.1f}s")
    return total_items

# ---------- Normalization ----------
DATE_TARGETS = {"cp":["date_debut_cp","date_fin_cp"], "parc":["date_mce_parc"], "ds":["date_ds"]}

def normalize_dates_inplace(df: pd.DataFrame, tag: str):
    for c in DATE_TARGETS.get(tag, []):
        if c in df.columns:
            df[c] = df[c].map(_parse_any_to_date).map(_sanitize_range).map(_iso)

def apply_norms_inplace(df: pd.DataFrame):
    if "imm" in df.columns: df["imm_norm"] = df["imm"].map(canon_plate)
    if "ww"  in df.columns: df["ww_norm"]  = df["ww"].map(canon_plate)
    if "vin" in df.columns: df["vin_norm"] = df["vin"].map(canon_vin)

# ---------- Index helpers ----------
def drop_indexes(db):
    for coll in ("cp","parc","ds"):
        try: db[coll].drop_indexes(); print(f"[INFO] dropped indexes on {coll}")
        except Exception as e: print(f"[WARN] drop indexes {coll}: {e}")

def ensure_indexes(db):
    for f in ("imm_norm","vin_norm","ww_norm"):
        db.cp.create_index(f); db.parc.create_index(f)
    for f in ("nds_ds","imm_norm","ww_norm","vehicle_id","lines_sig"):
        db.ds.create_index(f)
    db.cp.create_index("doc_sig"); db.parc.create_index("doc_sig")

# ---------- Existing signatures ----------
def fetch_existing_sig_map(db, coll_name: str, ids: List[str], sig_field: str = "doc_sig"):
    sig_map: Dict[str, Optional[str]] = {}; present_ids: Set[str] = set()
    if not ids:
        print(f"[INFO] {coll_name}: existing matches loaded = 0"); return sig_map, present_ids
    CH = 5000
    for i in range(0, len(ids), CH):
        chunk = ids[i:i+CH]
        cur = db[coll_name].find({"_id": {"$in": chunk}}, {"_id": 1, sig_field: 1})
        for doc in cur:
            _id = str(doc["_id"]); present_ids.add(_id); sig_map[_id] = doc.get(sig_field)
    print(f"[INFO] {coll_name}: existing matches loaded = {len(present_ids):,}")
    return sig_map, present_ids

# ---------- DS helpers ----------
DS_LINE_FIELDS = [
    "date_ds","description_ds","designation_article_ds","fournisseur_ds","imm","km_ds",
    "nds_ds","prix_unitaire_ds_ds","qte_ds","entite_ds","technicien","code_art","imm_norm","ww_norm"
]

def canonicalize_lines(df_group: pd.DataFrame) -> List[Dict[str, Any]]:
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

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Drive .xlsx → MongoDB (cp/parc/ds). Always re-download; no RL; no local DS reuse.")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--mongo-uri", default=None)
    ap.add_argument("--mongo-db", default=None)
    ap.add_argument("--cp-batch", type=int, default=1500)
    ap.add_argument("--parc-batch", type=int, default=1500)
    ap.add_argument("--ds-batch", type=int, default=3000)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--drop-indexes", action="store_true")
    args = ap.parse_args()

    timer = StepTimer()

    # Config
    with timer.stage("Load .env and resolve config"):
        BASE = os.path.abspath(os.path.dirname(__file__))
        load_dotenv(os.path.join(BASE, ".env"), override=False)
        resolved_uri = args.mongo_uri or os.getenv("MONGODB_URI") or os.getenv("MONGO_URI") or "mongodb://localhost:27017"
        resolved_db  = args.mongo_db  or os.getenv("MONGODB_DB")  or os.getenv("MONGO_DB")  or "avis_db"
        creds_b64    = os.getenv("GOOGLE_CREDENTIALS_BASE64")
        folder_id    = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        if not creds_b64 or not folder_id:
            print("[ERROR] GOOGLE_CREDENTIALS_BASE64 and GOOGLE_DRIVE_FOLDER_ID are required")
            sys.exit(2)
        print(f"[INFO] DB={resolved_db}; workers={args.workers}; drop_indexes={args.drop_indexes}")

    # Erase local cp/parc/ds
    with timer.stage("Erase local cp/parc/ds"):
        os.makedirs(args.data_dir, exist_ok=True)
        for fn in ("cp.xlsx","parc.xlsx","ds.xlsx"):
            p = os.path.join(args.data_dir, fn)
            try:
                if os.path.exists(p):
                    os.remove(p); print(f"[CLEAN] removed {p}")
            except Exception as e:
                print(f"[WARN] cannot remove {p}: {e}")

    # Drive auth + download (cp/parc/ds)
    with timer.stage("Download cp/parc/ds from Drive (mandatory)"):
        drive = build_drive_service(creds_b64)
        files = list_folder(drive, folder_id)

        def fetch_and_save(base) -> str:
            f = pick_by_basename(files, base)
            if not f:
                print(f"[ERROR] {base.upper()}: not found in Drive folder")
                sys.exit(3)
            print(f"[PICK] {base.upper()}: {f.get('name')}  mime={f.get('mimeType')}  modified={f.get('modifiedTime')}")
            out = ensure_xlsx_download(drive, f, base, args.data_dir)
            print(f"[OK] Saved {f.get('name')} → {out}")
            return out

        cp_path   = fetch_and_save("cp")
        parc_path = fetch_and_save("parc")
        ds_path   = fetch_and_save("ds")

    with timer.stage("Read + rename + normalize (cp/parc/ds)"):
        dfs: Dict[str, pd.DataFrame] = {}

        # CP
        raw = read_cp_parc(cp_path)
        present = [c for c in HEADERS_MAP["cp"] if c in raw.columns]
        if not present:
            print("[WARN] cp: expected headers not found"); dfs["cp"] = pd.DataFrame()
        else:
            df = raw[present].rename(columns=HEADERS_MAP["cp"])
            normalize_dates_inplace(df, "cp"); apply_norms_inplace(df)
            dfs["cp"] = df
            print(f"[OK] cp rows={len(df):,}")

        # PARC
        raw = read_cp_parc(parc_path)
        present = [c for c in HEADERS_MAP["parc"] if c in raw.columns]
        if not present:
            print("[WARN] parc: expected headers not found"); dfs["parc"] = pd.DataFrame()
        else:
            df = raw[present].rename(columns=HEADERS_MAP["parc"])
            normalize_dates_inplace(df, "parc"); apply_norms_inplace(df)
            dfs["parc"] = df
            print(f"[OK] parc rows={len(df):,}")

        # DS
        raw = read_ds(ds_path)
        present = [c for c in HEADERS_MAP["ds"] if c in raw.columns]
        if not present:
            print("[WARN] ds: expected headers not found"); dfs["ds"] = pd.DataFrame()
        else:
            df = raw[present].rename(columns=HEADERS_MAP["ds"])
            normalize_dates_inplace(df, "ds"); apply_norms_inplace(df)
            dfs["ds"] = df
            print(f"[OK] ds rows={len(df):,}")

    # Connect Mongo
    with timer.stage("Connect to MongoDB"):
        client, db = get_client_db(resolved_uri, resolved_db)
        print(f"[INFO] Connected (w=1)")

    try:
        if args.drop_indexes:
            with timer.stage("Drop indexes (pre-write)"):
                drop_indexes(db)

        now = datetime.now(TZ.utc)

        # Upsert PARC
        with timer.stage("Upsert PARC"):
            inserted = migrated = updated = skipped = 0
            parc_df = dfs.get("parc", pd.DataFrame())
            if parc_df.empty:
                print("[INFO] parc: nothing to push")
            else:
                req = [
                    "imm","marque","modele","ww","vin",
                    "etat_vehicule","client_parc","locataire_parc","date_mce_parc",
                    "imm_norm","vin_norm","ww_norm",
                ]
                for c in req:
                    if c not in parc_df.columns: parc_df[c] = pd.NA

                ids, keys = [], []
                for _, r in parc_df.iterrows():
                    vk = _txt(r.get("vin_norm")) or _txt(r.get("imm_norm")) or _txt(r.get("ww_norm"))
                    keys.append(vk)
                    if vk: ids.append(vk)
                ids = list(dict.fromkeys([i for i in ids if i]))

                sig_map, present = fetch_existing_sig_map(db, "parc", ids, sig_field="doc_sig")
                ops: List[UpdateOne] = []
                for i in range(len(parc_df)):
                    _id = keys[i]
                    if not _id: continue
                    row = parc_df.iloc[i].to_dict()
                    doc = {k: _txt(row.get(k)) for k in req}
                    body = {**doc, "_id": _id}
                    sig = json_sig(body); prev = sig_map.get(_id, None); exists = _id in present
                    if not exists:
                        ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "doc_sig": sig, "updated_at": now}}, upsert=True)); inserted += 1
                    elif prev is None:
                        ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "doc_sig": sig, "updated_at": now}})); migrated += 1
                    elif prev != sig:
                        ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "doc_sig": sig, "updated_at": now}})); updated += 1
                    else:
                        skipped += 1
                if ops: bulk_exec(db.parc, ops, batch_size=args.parc_batch, label="parc", workers=args.workers, bypass=True)
                print(f"[OK] parc inserted={inserted:,} migrated={migrated:,} updated={updated:,} skipped={skipped:,}")

        # Upsert CP
        with timer.stage("Upsert CP"):
            inserted = migrated = updated = skipped = 0
            cp_df = dfs.get("cp", pd.DataFrame())
            if cp_df.empty:
                print("[INFO] cp: nothing to push")
            else:
                req = ["client_cp","date_debut_cp","date_fin_cp","imm","marque","modele","modele_long","vin","ww",
                       "imm_norm","vin_norm","ww_norm"]
                for c in req:
                    if c not in cp_df.columns: cp_df[c] = pd.NA

                ids, keys = [], []
                for _, r in cp_df.iterrows():
                    vk = _txt(r.get("vin_norm")) or _txt(r.get("imm_norm")) or _txt(r.get("ww_norm"))
                    keys.append(vk)
                    if vk: ids.append(vk)
                ids = list(dict.fromkeys([i for i in ids if i]))

                sig_map, present = fetch_existing_sig_map(db, "cp", ids, sig_field="doc_sig")
                ops: List[UpdateOne] = []
                for i in range(len(cp_df)):
                    _id = keys[i]
                    if not _id: continue
                    row = cp_df.iloc[i].to_dict()
                    doc = {k: _txt(row.get(k)) for k in req}
                    body = {**doc, "_id": _id}
                    sig = json_sig(body); prev = sig_map.get(_id, None); exists = _id in present
                    if not exists:
                        ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "doc_sig": sig, "updated_at": now}}, upsert=True)); inserted += 1
                    elif prev is None:
                        ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "doc_sig": sig, "updated_at": now}})); migrated += 1
                    elif prev != sig:
                        ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "doc_sig": sig, "updated_at": now}})); updated += 1
                    else:
                        skipped += 1
                if ops: bulk_exec(db.cp, ops, batch_size=args.cp_batch, label="cp", workers=args.workers, bypass=True)
                print(f"[OK] cp inserted={inserted:,} migrated={migrated:,} updated={updated:,} skipped={skipped:,}")

        # Upsert DS
        with timer.stage("Upsert DS"):
            ds_df = dfs.get("ds", pd.DataFrame()); inserted = migrated = updated = skipped = 0
            latest_dt: Optional[datetime] = None
            if ds_df.empty:
                print("[INFO] ds: nothing to push")
            else:
                def has_key(row): return bool(_txt(row.get("imm_norm")) or _txt(row.get("ww_norm")))
                mask = ds_df.apply(has_key, axis=1)
                skipped_no_key = int((~mask).sum())
                ds_df = ds_df[mask].copy()
                if skipped_no_key: print(f"[INFO] ds: skipped rows without imm_norm/ww_norm = {skipped_no_key:,}")
                if ds_df.empty:
                    print("[INFO] ds: nothing to push after key filtering.")
                else:
                    file_ids = ds_df["nds_ds"].dropna().map(lambda x: str(x).strip()).replace("", pd.NA).dropna().unique().tolist()
                    sig_map, present = fetch_existing_sig_map(db, "ds", file_ids, sig_field="lines_sig")
                    ops: List[UpdateOne] = []
                    for ds_no, g in ds_df.groupby("nds_ds", dropna=False, sort=False):
                        _id = _txt(ds_no)
                        if not _id: continue
                        lines = canonicalize_lines(g); lines_sig = hash_lines(lines)
                        imm_norm = _txt(g["imm_norm"].dropna().iloc[0]) if "imm_norm" in g and not g["imm_norm"].dropna().empty else None
                        ww_norm  = _txt(g["ww_norm"].dropna().iloc[0])  if "ww_norm"  in g and not g["ww_norm"].dropna().empty  else None
                        vehicle_id = imm_norm or ww_norm
                        dt_series = pd.to_datetime(g["date_ds"], errors="coerce", utc=True).dropna()
                        date_event = dt_series.max() if not dt_series.empty else None
                        if date_event is not None: latest_dt = date_event if latest_dt is None else max(latest_dt, date_event)
                        body = {"_id": _id, "ds_no": _id, "vehicle_id": vehicle_id, "imm_norm": imm_norm, "ww_norm": ww_norm,
                                "date_event": date_event, "lines": lines, "lines_sig": lines_sig}
                        prev = sig_map.get(_id, None); exists = _id in present
                        if not exists:
                            ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "updated_at": now}}, upsert=True)); inserted += 1
                        elif prev is None:
                            ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "updated_at": now}})); migrated += 1
                        elif prev != lines_sig:
                            ops.append(UpdateOne({"_id": _id}, {"$set": {**body, "updated_at": now}})); updated += 1
                        else:
                            skipped += 1
                    if ops: bulk_exec(db.ds, ops, batch_size=args.ds_batch, label="ds", workers=args.workers, bypass=True)
                    print(f"[OK] ds inserted={inserted:,} migrated={migrated:,} updated={updated:,} skipped={skipped:,}")
                    print(f"[DONE] latest DS date={latest_dt}")

        with timer.stage("Ensure indexes (post-write)"):
            ensure_indexes(db)

    finally:
        try: client.close(); print("[INFO] Mongo client closed.")
        except Exception: pass

    timer.summary()

if __name__ == "__main__":
    main()
