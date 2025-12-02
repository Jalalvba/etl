#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
cp.py — CP loader for long-term rental contracts.

Pipeline:
  • Fetch latest CP Excel (Drive or local override)
  • Strict header read
  • Normalize plates, VINs, dates
  • Deduplicate (latest contract per imm_norm)
  • Build stable Mongo documents
  • Upsert by SHA-256 signature
  • Ensure indexes and clean old duplicates
"""

import os
import re
import io
import warnings
import argparse
from datetime import datetime, timedelta, timezone as TZ

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
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

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CP_HEADER_ROW = 7  # 0-based header row index in Excel

CP_HEADERS_MAP = {
    "IMM": "imm",
    "NUM chassis": "vin",
    "WW": "ww",
    "Client": "client",
    "Date début contrat": "date_debut_cp",
    "Libellé version long": "modele_long",
    "Date fin contrat": "date_fin_cp",
}

# ─────────────────────────────────────────────────────────────────────────────
# Drive: pick latest CP file
# ─────────────────────────────────────────────────────────────────────────────

def pick_latest_cp(drive, folder_id: str) -> str:
    """
    Find the newest XLSX in the folder whose name contains whole word 'CP'.
    Uses shared list_folder_files() and download_xlsx() from helper.py.
    """
    rx = re.compile(r"(?i)\bCP\b")
    files = list_folder_files(drive, folder_id)
    if not files:
        raise DriveError("CP: Drive folder is empty")

    for f in files:  # already sorted newest-first by modifiedTime
        fid = f.get("id")
        name = f.get("name", "")
        mime = f.get("mimeType", "")
        if not (rx.search(name) and _looks_like_xlsx(name, mime)):
            continue
        log(f"CP candidate: {name} ({f.get('modifiedTime')})")
        tmp = "data/.tmp_cp.xlsx"
        download_xlsx(drive, fid, tmp)
        final_path = "data/cp.xlsx"
        try:
            if os.path.exists(final_path):
                os.remove(final_path)
        except Exception:
            pass
        os.replace(tmp, final_path)
        log(f"CP accepted: {name}")
        return final_path

    raise DriveError("No matching CP XLSX found in Drive folder")

# ─────────────────────────────────────────────────────────────────────────────
# Reading and normalization
# ─────────────────────────────────────────────────────────────────────────────

def read_strict(path: str, header_row: int, header_map: dict, label: str) -> pd.DataFrame:
    """
    Read Excel with strict header verification:
      • decode Excel escapes
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
        raise TransformError(f"{label}: expected headers not found (check header_row={header_row})")

    df = df[list(matched.values())].rename(columns={v: k for k, v in matched.items()})
    log(f"{label}: kept -> {list(df.columns)}")
    return df


def process_cp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize and deduplicate CP rows:
      • imm_norm / vin_norm / ww_norm
      • date_debut_cp / date_fin_cp (ISO)
      • keep latest date_fin_cp per imm_norm
    """
    df = df.copy()

    df["imm_norm"] = df["imm"].map(canon_plate)
    df["vin_norm"] = df["vin"].map(canon_vin)
    df["ww_norm"]  = df["ww"].map(canon_plate)

    df["date_debut_cp"] = df["date_debut_cp"].map(_iso_date_from_any)
    df["date_fin_cp"]   = df["date_fin_cp"].map(_iso_date_from_any)

    df["_sort_date"] = pd.to_datetime(df["date_fin_cp"], errors="coerce")
    df = df.sort_values(by="_sort_date", ascending=False, na_position="last").drop(columns="_sort_date")

    total_rows = len(df)
    df_unique = df.drop_duplicates(subset=["imm_norm"], keep="first")
    dropped = total_rows - len(df_unique)
    log(f"CP stats: total_rows={total_rows} kept={len(df_unique)} dropped={dropped}")

    return df_unique


def build_cp_docs(df: pd.DataFrame) -> list[dict]:
    """
    Convert CP dataframe rows into MongoDB documents with doc_sig.
    """
    docs: list[dict] = []

    for _, r in df.iterrows():
        rec = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}

        _id = rec.get("imm_norm") or rec.get("vin_norm") or rec.get("ww_norm")
        if not _id:
            continue

        rec["_id"] = _id

        rec["doc_sig"] = _sha256_json({
            "imm_norm": rec.get("imm_norm"),
            "vin_norm": rec.get("vin_norm"),
            "ww_norm":  rec.get("ww_norm"),
            "imm":      rec.get("imm"),
            "vin":      rec.get("vin"),
            "ww":       rec.get("ww"),
            "client":   rec.get("client"),
            "modele_long":   rec.get("modele_long"),
            "date_debut_cp": rec.get("date_debut_cp"),
            "date_fin_cp":   rec.get("date_fin_cp"),
        })

        rec = {k: v for k, v in rec.items() if v is not None}
        docs.append(rec)

    log(f"CP docs built: {len(docs)}")
    return docs

# ─────────────────────────────────────────────────────────────────────────────
# MongoDB plumbing
# ─────────────────────────────────────────────────────────────────────────────

def get_db(uri: str, dbname: str):
    """Connect to MongoDB with ping & simple write concern."""
    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=20000,
        compressors=["zstd", "snappy", "zlib"],
    )
    client.admin.command("ping")
    db = client.get_database(dbname, write_concern=WriteConcern(w=1))
    return client, db


def ensure_indexes_cp(db) -> None:
    """
    Create basic indexes for CP lookup & signature checks.
    """
    db.cp.create_index([("imm_norm", ASCENDING)], name="imm_norm", sparse=True)
    db.cp.create_index([("vin_norm", ASCENDING)], name="vin_norm", sparse=True)
    db.cp.create_index([("ww_norm",  ASCENDING)], name="ww_norm",  sparse=True)
    db.cp.create_index([("doc_sig",  ASCENDING)], name="doc_sig")
    db.cp.create_index([("date_fin_cp", ASCENDING)], name="date_fin_cp", sparse=True)


def upsert_with_sig(db, coll: str, docs: list[dict], sig_field: str, dry_run: bool = False) -> None:
    """
    Upsert documents only if their signature changed.
    """
    if not docs:
        log(f"{coll} → inserted=0 updated=0 skipped=0 (no docs)")
        return

    ids = [d["_id"] for d in docs]
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

    if dry_run:
        log(f"{coll} (dry-run) → would insert={ins} update={upd} skip={skp}")
        return

    try:
        if ops:
            db[coll].bulk_write(ops, ordered=False)
    except BulkWriteError as e:
        log(f"Bulk write error: {e.details}")
        raise

    log(f"{coll} → inserted={ins} updated={upd} skipped={skp}")


def drop_old_cp_duplicates(db) -> None:
    """
    Remove redundant old CP docs with same imm_norm, keeping the latest.
    """
    log("Clean old CP duplicates (keep latest per imm_norm)")

    dup_keys = [
        r["_id"]
        for r in db.cp.aggregate([
            {"$group": {"_id": "$imm_norm", "n": {"$sum": 1}}},
            {"$match": {"n": {"$gt": 1}}},
        ])
        if r["_id"]
    ]

    removed = 0
    for key in dup_keys:
        rows = list(db.cp.find({"imm_norm": key}))
        def sort_key(r):
            d = pd.to_datetime(r.get("date_fin_cp"), errors="coerce")
            return d if pd.notna(d) else pd.Timestamp.min
        rows.sort(key=sort_key, reverse=True)
        to_delete = [r["_id"] for r in rows[1:]]
        if to_delete:
            db.cp.delete_many({"_id": {"$in": to_delete}})
            removed += len(to_delete)

    log(f"Removed {removed} old CP duplicates")

# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run(cfg: dict, local_xlsx: str | None = None,
        header_row: int = DEFAULT_CP_HEADER_ROW,
        dry_run: bool = False) -> None:
    """
    End-to-end CP loader:
      • fetch XLSX
      • read & normalize
      • build docs
      • ensure indexes
      • upsert with signature
      • clean duplicates
    """
    # Basic env validation (Drive not required when using --local-xlsx)
    for k in ("MONGODB_URI", "MONGODB_DB"):
        if not cfg.get(k):
            raise ConfigError(f"Missing required env: {k}")
    if not local_xlsx:
        for k in ("GOOGLE_CREDENTIALS_BASE64", "GOOGLE_DRIVE_FOLDER_ID"):
            if not cfg.get(k):
                raise ConfigError(f"Missing required env: {k}")

    log("BEGIN CP load")

    # 1) Acquire XLSX
    if local_xlsx:
        cp_path = local_xlsx
        if not os.path.exists(cp_path):
            raise FileNotFoundError(f"--local-xlsx not found: {cp_path}")
        log(f"Using local XLSX: {cp_path}")
    else:
        drive = make_drive(cfg["GOOGLE_CREDENTIALS_BASE64"])
        cp_path = pick_latest_cp(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"])

    # 2) Parse, normalize, deduplicate
    df_cp = read_strict(cp_path, header_row, CP_HEADERS_MAP, "CP")
    df_cp_unique = process_cp(df_cp)
    cp_docs = build_cp_docs(df_cp_unique)

    # 3) Mongo operations
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
    # Allow UTF-8 logs everywhere
    if not isinstance(getattr(__import__("sys"), "stdout"), io.TextIOBase):
        import sys as _sys
        _sys.stdout = io.TextIOWrapper(_sys.stdout.buffer, encoding="utf-8")
        _sys.stderr = io.TextIOWrapper(_sys.stderr.buffer, encoding="utf-8")

    load_dotenv()
    args = parse_args()
    cfg = {
        "MONGODB_URI": os.getenv("MONGODB_URI"),
        "MONGODB_DB": os.getenv("MONGODB_DB"),
        "GOOGLE_CREDENTIALS_BASE64": os.getenv("GOOGLE_CREDENTIALS_BASE64"),
        "GOOGLE_DRIVE_FOLDER_ID": os.getenv("GOOGLE_DRIVE_FOLDER_ID"),
    }
    run(cfg, local_xlsx=args.local_xlsx, header_row=args.header_row, dry_run=args.dry_run)
