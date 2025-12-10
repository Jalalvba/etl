#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rl.py — RL Loader (Replacement Vehicles)
Same architecture as sin.py — uses the SAME Drive folder:

    GOOGLE_CREDENTIALS_BASE64
    GOOGLE_DRIVE_FOLDER_ID

In that folder, the loader picks the newest XLSX whose filename contains “RL”.
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

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

DEFAULT_RL_HEADER_ROW = 7   # Excel row 8 (0-based)
RL_SHEET_NAME = 0           # first sheet

# Regex to detect RL files inside the SAME Drive folder
RL_FILENAME_REGEX = r"(?i)\bRL\b|\bREMPLACEMENT\b"

RL_HEADERS_MAP = {
    "Reference": "reference_rl",
    "Date": "date_rl",
    "Immatriculation": "imm",
    "Date début": "date_debut_rl",
    "Date fin": "date_fin_rl",
    "Nbr jours": "nbr_jours_rl",
    "Motif": "motif_rl",
}

DATE_FIELDS = ["date_rl", "date_debut_rl", "date_fin_rl"]

# ─────────────────────────────────────────────────────────────
# PICK LATEST RL FILE FROM SAME GOOGLE DRIVE FOLDER
# ─────────────────────────────────────────────────────────────

def pick_latest_rl(drive, folder_id: str) -> str:
    """
    Search the SAME Drive folder as sin.py (GOOGLE_DRIVE_FOLDER_ID),
    pick the newest XLSX containing RL in its name.
    Save locally as data/rl.xlsx.
    """
    rx = re.compile(RL_FILENAME_REGEX)
    files = list_folder_files(drive, folder_id)
    if not files:
        raise DriveError("RL: Drive folder is empty")

    for f in files:  # sorted newest-first
        name = f.get("name", "")
        mime = f.get("mimeType", "")
        fid = f.get("id", "")

        if not (_looks_like_xlsx(name, mime) and rx.search(name)):
            continue

        log(f"RL candidate: {name} ({f.get('modifiedTime')})")

        tmp = "data/.tmp_rl.xlsx"
        final_path = "data/rl.xlsx"

        download_xlsx(drive, fid, tmp)

        if os.path.exists(final_path):
            try:
                os.remove(final_path)
            except Exception:
                pass

        os.replace(tmp, final_path)
        log(f"RL accepted: {name}")

        return final_path

    raise DriveError("RL: No matching RL excel file found in Drive folder")

# ─────────────────────────────────────────────────────────────
# STRICT HEADER READ
# ─────────────────────────────────────────────────────────────

def read_rl_strict(path, header_row, header_map, label, sheet_name):
    log(f"Read {label} from {path} (header_row={header_row})")

    df = pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=header_row,
        engine="openpyxl",
    )

    df = _clean_dataframe_excel_escapes(df)

    norm_expected = {_norm_label(k): (k, v) for k, v in header_map.items()}
    norm_actual = {_norm_label(c): c for c in df.columns}

    matched = {}
    for ne, (orig, canon) in norm_expected.items():
        if ne in norm_actual:
            matched[canon] = norm_actual[ne]

    if not matched:
        raise TransformError("RL: Expected headers not found")

    df = df[list(matched.values())].rename(columns={v: k for k, v in matched.items()})
    log(f"RL: kept → {list(df.columns)}")
    return df

# ─────────────────────────────────────────────────────────────
# NORMALIZE
# ─────────────────────────────────────────────────────────────

def normalize_rl(df):
    df = df.copy()

    for col in DATE_FIELDS:
        if col in df:
            df[col] = df[col].map(_iso_date_from_any)

    df["imm_norm"] = df["imm"].map(canon_plate)

    if "nbr_jours_rl" in df:
        df["nbr_jours_rl"] = pd.to_numeric(df["nbr_jours_rl"], errors="coerce")

    df = df[df["reference_rl"].map(lambda v: bool(_txt(v)))]
    df = df[df["imm_norm"].map(lambda v: bool(_txt(v)))]

    log(f"RL normalized rows → {len(df)}")
    return df

# ─────────────────────────────────────────────────────────────
# BUILD DOCUMENTS
# ─────────────────────────────────────────────────────────────

def build_rl_docs(df):
    docs = []

    for _, r in df.iterrows():
        rec = {k: (None if pd.isna(v) else v) for k, v in r.to_dict().items()}

        ref = _txt(rec.get("reference_rl"))
        if not ref:
            continue

        rec["_id"] = ref
        imm_norm = _txt(rec.get("imm_norm"))
        if imm_norm:
            rec["vehicle_id"] = imm_norm

        sig_payload = {
            k: rec[k]
            for k in rec
            if k not in {"_id", "created_at", "updated_at", "doc_sig"}
        }

        rec["doc_sig"] = _sha256_json(sig_payload)
        rec = {k: v for k, v in rec.items() if v is not None}
        docs.append(rec)

    log(f"RL docs built → {len(docs)}")
    return docs

# ─────────────────────────────────────────────────────────────
# MONGO
# ─────────────────────────────────────────────────────────────

def get_db(uri, dbname):
    client = MongoClient(uri, serverSelectionTimeoutMS=20000)
    client.admin.command("ping")
    db = client.get_database(dbname, write_concern=WriteConcern(w=1))
    return client, db

def ensure_indexes_rl(db):
    db.rl.create_index([("imm_norm", ASCENDING)], name="imm_norm")
    db.rl.create_index([("vehicle_id", ASCENDING)], name="vehicle_id")
    db.rl.create_index([("reference_rl", ASCENDING)], name="reference_rl")
    db.rl.create_index([("doc_sig", ASCENDING)], name="doc_sig")
    log("RL indexes ensured")

def upsert_with_sig(db, docs, sig_field="doc_sig"):
    if not docs:
        log("RL → nothing to upsert")
        return

    ids = [d["_id"] for d in docs]
    existing = {
        x["_id"]: x.get(sig_field)
        for x in db.rl.find({"_id": {"$in": ids}}, {"_id": 1, sig_field: 1})
    }

    now = datetime.now(TZ.utc)
    ins = upd = skp = 0
    ops = []

    for d in docs:
        prev = existing.get(d["_id"])
        cur = d[sig_field]

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
        try:
            db.rl.bulk_write(ops, ordered=False)
        except BulkWriteError as e:
            raise UpsertError(str(e))

    log(f"RL → inserted={ins}, updated={upd}, skipped={skp}")

# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

def run(cfg, local_xlsx=None, header_row=DEFAULT_RL_HEADER_ROW, sheet_name=RL_SHEET_NAME, dry_run=False):
    log("BEGIN RL load")

    if not cfg.get("MONGODB_URI") or not cfg.get("MONGODB_DB"):
        raise ConfigError("Missing MongoDB env vars")

    if not local_xlsx:
        if not cfg.get("GOOGLE_CREDENTIALS_BASE64"):
            raise ConfigError("Missing GOOGLE_CREDENTIALS_BASE64")
        if not cfg.get("GOOGLE_DRIVE_FOLDER_ID"):
            raise ConfigError("Missing GOOGLE_DRIVE_FOLDER_ID")

    if local_xlsx:
        rl_path = local_xlsx
        if not os.path.exists(rl_path):
            raise FileNotFoundError(f"Local RL XLSX not found: {rl_path}")
        log(f"Using local RL XLSX: {rl_path}")

    else:
        drive = make_drive(cfg["GOOGLE_CREDENTIALS_BASE64"])
        rl_path = pick_latest_rl(drive, cfg["GOOGLE_DRIVE_FOLDER_ID"])

    df_raw = read_rl_strict(
        rl_path,
        header_row=header_row,
        header_map=RL_HEADERS_MAP,
        label="RL",
        sheet_name=sheet_name,
    )

    df_norm = normalize_rl(df_raw)
    rl_docs = build_rl_docs(df_norm)

    client, db = get_db(cfg["MONGODB_URI"], cfg["MONGODB_DB"])
    try:
        ensure_indexes_rl(db)
        upsert_with_sig(db, rl_docs, "doc_sig")
    finally:
        client.close()

    log("END RL load")

# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

def parse_args():
    ap = argparse.ArgumentParser(description="RL loader")
    ap.add_argument("--local-xlsx", help="Use a local rl.xlsx instead of Drive")
    ap.add_argument("--header-row", type=int, default=DEFAULT_RL_HEADER_ROW)
    ap.add_argument("--sheet-name", default=RL_SHEET_NAME)
    ap.add_argument("--dry-run", action="store_true")
    return ap.parse_args()

# ─────────────────────────────────────────────────────────────

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
