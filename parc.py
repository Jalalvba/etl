#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parc.py — PARC loader with:
  • strict headers
  • date / plate / VIN normalization
  • signature-based upserts
  • protective indexes

Env (via real env or .env):
  - MONGODB_URI
  - MONGODB_DB
  - GOOGLE_CREDENTIALS_BASE64
  - GOOGLE_DRIVE_FOLDER_ID
"""

from __future__ import annotations

import os
import re
import warnings
from datetime import datetime, timezone as TZ

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.write_concern import WriteConcern

from helper import (
    log,
    _txt,
    canon_plate,
    canon_vin,
    _iso_date_from_any,
    _norm_label,
    _sha256_json,
    _clean_dataframe_excel_escapes,
    make_drive,
    list_folder_files,
    _looks_like_xlsx,
    download_xlsx,
    ConfigError,
    DriveError,
    TransformError,
    UpsertError,
)

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ── Constants ────────────────────────────────────────────────────────

PARC_HEADER_ROW = 7  # 0-based index
PARC_FILENAME_REGEX = r"(?i)\bPARC\b"

PARC_HEADERS_MAP = {
    "Immatriculation": "imm",
    "N° de chassis": "vin",
    "WW": "ww",
    "Date MCE": "date_mec",
    "Locataire": "locataire_parc",
    "Etat véhicule": "etat_vehicule",
    "Modèle": "modele",
    "Client": "client",
    "Société": "societe",
}

# ── Drive: pick latest PARC file ─────────────────────────────────────

def pick_latest_parc(drive, folder_id: str) -> str:
    """
    Find the newest XLSX in the Drive folder whose name contains whole word 'PARC',
    using shared list_folder_files() and _looks_like_xlsx().
    Download to data/parc.xlsx and return that path.
    """
    rx = re.compile(PARC_FILENAME_REGEX)
    files = list_folder_files(drive, folder_id)
    if not files:
        raise DriveError("PARC: Drive folder is empty")

    for f in files:  # newest first
        fid  = f.get("id")
        name = f.get("name", "")
        mime = f.get("mimeType", "")
        if not (rx.search(name) and _looks_like_xlsx(name, mime)):
            continue

        log(f"PARC candidate: {name} ({f.get('modifiedTime')})")
        tmp = "data/.tmp_parc.xlsx"
        download_xlsx(drive, fid, tmp)

        final_path = "data/parc.xlsx"
        try:
            if os.path.exists(final_path):
                os.remove(final_path)
        except Exception:
            pass
        os.replace(tmp, final_path)
        log(f"PARC accepted: {name}")
        return final_path

    raise DriveError("No matching PARC XLSX found in Drive folder")

# ── Strict reader ────────────────────────────────────────────────────

def read_parc_strict(path: str, header_row: int, header_map: dict, label: str) -> pd.DataFrame:
    """
    Read PARC Excel with strict header verification:
      • clean Excel XML escapes
      • normalize labels
      • keep+rename only expected columns
    """
    log(f"Read {label} from {path}")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")
    df = _clean_dataframe_excel_escapes(df)

    norm_expected = {_norm_label(k): (k, v) for k, v in header_map.items()}
    norm_actual = {_norm_label(c): c for c in df.columns}

    matched = {}
    for ne, (orig_label, canon) in norm_expected.items():
        if ne in norm_actual:
            matched[canon] = norm_actual[ne]

    if not matched:
        raise TransformError(f"{label}: no expected headers found (check header_row={header_row})")

    df = df[list(matched.values())].rename(columns={v: k for k, v in matched.items()})
    log(f"{label}: kept -> {list(df.columns)}")
    return df

# ── PARC dataframe → Mongo docs ─────────────────────────────────────

def build_parc_docs(df_raw: pd.DataFrame) -> list[dict]:
    """
    Convert cleaned PARC dataframe into MongoDB documents:

      • imm_norm / vin_norm / ww_norm
      • date_mec → ISO
      • _id priority: imm_norm → vin_norm → ww_norm
      • vehicle_id mirror
      • doc_sig for change detection
    """
    df = df_raw.copy()

    if "imm" in df.columns:
        df["imm_norm"] = df["imm"].map(canon_plate)
    if "vin" in df.columns:
        df["vin_norm"] = df["vin"].map(canon_vin)
    if "ww" in df.columns:
        df["ww_norm"] = df["ww"].map(canon_plate)
    else:
        df["ww_norm"] = None

    if "date_mec" in df.columns:
        df["date_mec"] = df["date_mec"].map(_iso_date_from_any)

    docs: list[dict] = []
    for _, r in df.iterrows():
        rec = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}

        _id = rec.get("imm_norm") or rec.get("vin_norm") or rec.get("ww_norm")
        if not _id:
            continue

        rec["_id"] = _id
        rec["vehicle_id"] = _id

        # You can restrict this signature to a subset of fields later if needed
        rec["doc_sig"] = _sha256_json(rec)

        docs.append(rec)

    log(f"PARC docs: {len(docs)}")
    return docs

# ── MongoDB plumbing ────────────────────────────────────────────────

def get_db(uri: str, dbname: str):
    """
    Connect to MongoDB and return (client, db) with basic write concern.
    """
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=20000,
        compressors=["zstd", "snappy", "zlib"],
    )
    client.admin.command("ping")
    db = client.get_database(dbname, write_concern=WriteConcern(w=1))
    return client, db


def ensure_indexes_parc(db) -> None:
    """
    Create helpful PARC indexes:
      • imm_norm / vin_norm / ww_norm
      • doc_sig
    """
    db.parc.create_index([("imm_norm", ASCENDING)], sparse=True)
    db.parc.create_index([("vin_norm", ASCENDING)], sparse=True)
    db.parc.create_index([("ww_norm", ASCENDING)], sparse=True)
    db.parc.create_index([("doc_sig", ASCENDING)])
    log("PARC indexes ensured")

def upsert_with_sig(db, coll: str, docs: list[dict], sig_field: str) -> dict:
    """
    Bulk upsert with signature skip:
      • load existing signatures for _id set
      • skip unchanged docs
      • upsert changed/new docs with created_at/updated_at
    """
    if not docs:
        log(f"{coll} → inserted=0 updated=0 skipped=0 (no docs)")
        return {"inserted": 0, "updated": 0, "skipped": 0}

    ids = [d["_id"] for d in docs if "_id" in d]
    existing = {
        x["_id"]: x.get(sig_field)
        for x in db[coll].find({"_id": {"$in": ids}}, {"_id": 1, sig_field: 1})
    }

    now = datetime.now(TZ.utc)
    ins = upd = skp = 0
    ops: list[UpdateOne] = []

    for d in docs:
        prev = existing.get(d["_id"])
        cur = d.get(sig_field)

        if prev == cur:
            skp += 1
            continue

        if prev is None:
            ins += 1
        else:
            upd += 1

        payload = {**d, "updated_at": now}
        payload.setdefault("created_at", now)

        ops.append(UpdateOne({"_id": d["_id"]}, {"$set": payload}, upsert=True))

    if ops:
        db[coll].bulk_write(ops, ordered=False)

    log(f"{coll} → inserted={ins} updated={upd} skipped={skp}")
    return {"inserted": ins, "updated": upd, "skipped": skp}

# ── Main orchestration ──────────────────────────────────────────────

def run(cfg: dict) -> None:
    """
    End-to-end PARC pipeline:
      • fetch XLSX from Drive
      • read & normalize
      • build docs
      • ensure indexes
      • upsert with signature
    """
    for k in ("MONGODB_URI", "MONGODB_DB", "GOOGLE_CREDENTIALS_BASE64", "GOOGLE_DRIVE_FOLDER_ID"):
        if not cfg.get(k):
            raise ConfigError(f"Missing required env: {k}")

    log("BEGIN PARC load")

    drive = make_drive(cfg["GOOGLE_CREDENTIALS_BASE64"])
    parc_path = pick_latest_parc(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"])
    df_parc = read_parc_strict(parc_path, PARC_HEADER_ROW, PARC_HEADERS_MAP, "PARC")
    parc_docs = build_parc_docs(df_parc)

    client, db = get_db(cfg["MONGODB_URI"], cfg["MONGODB_DB"])
    try:
        ensure_indexes_parc(db)
        upsert_with_sig(db, "parc", parc_docs, "doc_sig")
    finally:
        client.close()

    log("END PARC load")

# ── CLI entrypoint ──────────────────────────────────────────────────

if __name__ == "__main__":
    load_dotenv()

    cfg = {
        "MONGODB_URI": os.getenv("MONGODB_URI"),
        "MONGODB_DB": os.getenv("MONGODB_DB"),
        "GOOGLE_CREDENTIALS_BASE64": os.getenv("GOOGLE_CREDENTIALS_BASE64"),
        "GOOGLE_DRIVE_FOLDER_ID": os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
    }

    run(cfg)
