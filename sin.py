#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sin.py — SIN loader for accident / claim missions.

Pipeline:
  • Fetch latest SIN Excel (Drive or local override)
  • Strict header read on SIN sheet
  • Normalize plates, dates
  • Keep only the latest mission per immatriculation (imm_norm)
  • Build stable Mongo documents (one doc per vehicle)
  • Upsert by SHA-256 signature
  • Ensure useful indexes
"""

from __future__ import annotations

import os
import re
import io
import warnings
import argparse
from datetime import datetime, timezone as TZ

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
from pymongo.write_concern import WriteConcern

from helper import (
    log,
    _txt,
    canon_plate,
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

# ─────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────

DEFAULT_SIN_HEADER_ROW = 7  # 0-based header row index
SIN_SHEET_NAME = "Mission Sinistre"   # adapte si le nom de l’onglet est différent
SIN_FILENAME_REGEX = r"(?i)\bSIN\b"

# IMPORTANT: only real headers present in sin.xlsx
SIN_HEADERS_MAP = {
    "Reference": "ref_sin",
    "Date sinistre": "date_sin",
    "Immatriculation": "imm",
    "Interlocuteur client": "interlocuteur_client",
    "Téléphone client": "tel_client",
    "Conducteur": "conducteur",
    "Téléphone conducteur": "tel_conducteur",
    "Documents de bases": "documents_de_bases",
    "Lieu accident": "lieu_accident",
    "Statut déclaration": "statut_declaration",
    "Date déclaration": "date_declaration",
    "Statut sinistre": "statut_sinistre",
    "Epave": "epave",
}

# Only the two real date fields we have
DATE_FIELDS = [
    "date_sin",
    "date_declaration",
]

# ─────────────────────────────────────────────────────────────────────
# Drive: pick latest SIN file
# ─────────────────────────────────────────────────────────────────────

def pick_latest_sin(drive, folder_id: str) -> str:
    """
    Find the newest XLSX in the folder whose name contains whole word 'SIN'.
    Download to data/sin.xlsx and return that path.
    """
    rx = re.compile(SIN_FILENAME_REGEX)
    files = list_folder_files(drive, folder_id)
    if not files:
        raise DriveError("SIN: Drive folder is empty")

    for f in files:  # already sorted newest-first by modifiedTime
        fid = f.get("id")
        name = f.get("name", "")
        mime = f.get("mimeType", "")
        if not (rx.search(name) and _looks_like_xlsx(name, mime)):
            continue

        log(f"SIN candidate: {name} ({f.get('modifiedTime')})")
        tmp = "data/.tmp_sin.xlsx"
        download_xlsx(drive, fid, tmp)
        final_path = "data/sin.xlsx"
        try:
            if os.path.exists(final_path):
                os.remove(final_path)
        except Exception:
            pass
        os.replace(tmp, final_path)
        log(f"SIN accepted: {name}")
        return final_path

    raise DriveError("No matching SIN XLSX found in Drive folder")

# ─────────────────────────────────────────────────────────────────────
# Reading and normalization
# ─────────────────────────────────────────────────────────────────────

def read_sin_strict(
    path: str,
    header_row: int,
    header_map: dict,
    label: str,
    sheet_name: str = SIN_SHEET_NAME,
) -> pd.DataFrame:
    """
    Read Excel with strict header verification on the SIN sheet:
      • decode Excel escapes
      • normalize labels
      • keep+rename only expected columns
    """
    log(f"Read {label} from {path} (sheet={sheet_name!r}, header_row={header_row})")
    df = pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=header_row,
        engine="openpyxl",
    )
    df = _clean_dataframe_excel_escapes(df)

    norm_expected = {_norm_label(k): (k, v) for k, v in header_map.items()}
    norm_actual = {_norm_label(c): c for c in df.columns}

    matched: dict[str, str] = {}
    for ne, (orig_label, canon) in norm_expected.items():
        if ne in norm_actual:
            matched[canon] = norm_actual[ne]

    if not matched:
        raise TransformError(f"{label}: expected headers not found (check header_row={header_row})")

    df = df[list(matched.values())].rename(columns={v: k for k, v in matched.items()})
    log(f"{label}: kept -> {list(df.columns)}")
    return df

# ─────────────────────────────────────────────────────────────────────
# Business rule: keep only latest mission per imm_norm
# ─────────────────────────────────────────────────────────────────────

def process_sin_last_mission(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize SIN data and keep only the *latest* mission per vehicle:

      • imm_norm from 'imm'
      • DATE_FIELDS → ISO strings
      • sort_date = date_declaration if present, else date_sin
      • sort descending by sort_date
      • drop duplicates on imm_norm, keep first (latest)

    Result: one row per imm_norm (last mission).
    """
    df = df_raw.copy()

    # Normalize plate
    if "imm" in df.columns:
        df["imm_norm"] = df["imm"].map(canon_plate)
    else:
        df["imm_norm"] = None

    # Normalize date fields
    for col in DATE_FIELDS:
        if col in df.columns:
            df[col] = df[col].map(_iso_date_from_any)

    # Build a sort key: prefer date_declaration, fallback to date_sin
    def _pick_sort_date(row):
        d1 = row.get("date_declaration")
        d2 = row.get("date_sin")
        return d1 or d2 or None

    df["_sort_date"] = df.apply(_pick_sort_date, axis=1)
    df["_sort_date"] = pd.to_datetime(df["_sort_date"], errors="coerce")

    df = df.sort_values(by="_sort_date", ascending=False, na_position="last")

    total_rows = len(df)
    df_unique = df.drop_duplicates(subset=["imm_norm"], keep="first")
    dropped = total_rows - len(df_unique)
    log(f"SIN stats: total_rows={total_rows} kept={len(df_unique)} dropped={dropped}")

    df_unique = df_unique.drop(columns=["_sort_date"])
    return df_unique

# ─────────────────────────────────────────────────────────────────────
# SIN dataframe → Mongo docs
# ─────────────────────────────────────────────────────────────────────

def build_sin_docs(df_processed: pd.DataFrame) -> list[dict]:
    """
    Convert processed SIN dataframe into MongoDB documents.

    Assumes df_processed already:
      • has imm_norm
      • has dates normalized to ISO
      • is deduplicated (one row per imm_norm)

    Document rules:
      • _id = imm_norm  (one doc per vehicle)
      • vehicle_id = imm_norm
      • fields = last sinistre info (Reference, Statut sinistre, Epave, etc.)
      • doc_sig based on all business fields (except _id / timestamps / doc_sig)
    """
    df = df_processed.copy()

    docs: list[dict] = []
    for _, r in df.iterrows():
        rec = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}

        imm_norm = rec.get("imm_norm")
        if not imm_norm:
            continue

        rec["_id"] = imm_norm
        rec["vehicle_id"] = imm_norm

        sig_payload = {
            k: rec.get(k)
            for k in rec.keys()
            if k not in {"_id", "created_at", "updated_at", "doc_sig"}
        }
        rec["doc_sig"] = _sha256_json(sig_payload)

        rec = {k: v for k, v in rec.items() if v is not None}
        docs.append(rec)

    log(f"SIN docs built (one per imm_norm): {len(docs)}")
    return docs

# ─────────────────────────────────────────────────────────────────────
# MongoDB plumbing
# ─────────────────────────────────────────────────────────────────────

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


def ensure_indexes_sin(db) -> None:
    """
    Create basic indexes for SIN lookup & signature checks.
    """
    db.sin.create_index([("imm_norm", ASCENDING)], name="imm_norm", sparse=True)
    db.sin.create_index([("vehicle_id", ASCENDING)], name="vehicle_id", sparse=True)
    db.sin.create_index([("date_sin", ASCENDING)], name="date_sin", sparse=True)
    db.sin.create_index([("date_declaration", ASCENDING)], name="date_declaration", sparse=True)
    db.sin.create_index([("client", ASCENDING)], name="client", sparse=True)
    db.sin.create_index([("doc_sig", ASCENDING)], name="doc_sig")
    log("SIN indexes ensured")


def upsert_with_sig(
    db,
    coll: str,
    docs: list[dict],
    sig_field: str,
    dry_run: bool = False,
) -> None:
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

# ─────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────

def run(
    cfg: dict,
    local_xlsx: str | None = None,
    header_row: int = DEFAULT_SIN_HEADER_ROW,
    sheet_name: str = SIN_SHEET_NAME,
    dry_run: bool = False,
) -> None:
    """
    End-to-end SIN loader.
    """
    for k in ("MONGODB_URI", "MONGODB_DB"):
        if not cfg.get(k):
            raise ConfigError(f"Missing required env: {k}")
    if not local_xlsx:
        for k in ("GOOGLE_CREDENTIALS_BASE64", "GOOGLE_DRIVE_FOLDER_ID"):
            if not cfg.get(k):
                raise ConfigError(f"Missing required env: {k}")

    log("BEGIN SIN load")

    if local_xlsx:
        sin_path = local_xlsx
        if not os.path.exists(sin_path):
            raise FileNotFoundError(f"--local-xlsx not found: {sin_path}")
        log(f"Using local SIN XLSX: {sin_path}")
    else:
        drive = make_drive(cfg["GOOGLE_CREDENTIALS_BASE64"])
        sin_path = pick_latest_sin(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"])

    df_sin_raw = read_sin_strict(
        sin_path,
        header_row=header_row,
        header_map=SIN_HEADERS_MAP,
        label="SIN",
        sheet_name=sheet_name,
    )
    df_sin_last = process_sin_last_mission(df_sin_raw)
    sin_docs = build_sin_docs(df_sin_last)

    client, db = get_db(cfg["MONGODB_URI"], cfg["MONGODB_DB"])
    try:
        ensure_indexes_sin(db)
        upsert_with_sig(db, "sin", sin_docs, "doc_sig", dry_run=dry_run)
    finally:
        client.close()

    log("END SIN load")

# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def parse_args():
    ap = argparse.ArgumentParser(description="SIN loader")
    ap.add_argument(
        "--local-xlsx",
        help="Path to local sin.xlsx (bypass Drive)",
        default=None,
    )
    ap.add_argument(
        "--header-row",
        type=int,
        default=DEFAULT_SIN_HEADER_ROW,
        help="0-based header row index for the sheet",
    )
    ap.add_argument(
        "--sheet-name",
        type=str,
        default=SIN_SHEET_NAME,
        help="Excel sheet name containing SIN data",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to MongoDB, just log planned operations",
    )
    return ap.parse_args()


if __name__ == "__main__":
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
    run(
        cfg,
        local_xlsx=args.local_xlsx,
        header_row=args.header_row,
        sheet_name=args.sheet_name,
        dry_run=args.dry_run,
    )
