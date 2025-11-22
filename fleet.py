#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fleet.py — Fleet DS Loader (XLSX) aligned with ds.py contract.

Purpose:
    Load “Fleet DS” Excel files from Google Drive, apply STRICT header matching,
    normalize rows exactly like ds.py, compute missing unit prices
    (prix_unitaire_ds_ds = mt_ht_ds / qte_ds), then build DS-like documents
    stored in MongoDB (collection: fleet).

Structure of each Mongo document:
    {
        _id: <nds_ds>,
        ds_no: <nds_ds>,
        nds_ds: <nds_ds>,
        vehicle_id: <imm_norm or ww_norm>,
        imm_norm: <...>,
        ww_norm: <...>,
        date_event: <max(date_ds)>,
        lines: [ { ... DS_LINE_FIELDS ... } ],
        lines_sig: <sha256(lines)>,
        created_at,
        updated_at
    }

Input Fleet Excel columns (exact expected labels):
    - Date DS
    - N°DS
    - Code art
    - Désignation article
    - Qté
    - Mt HT DS
    - Immatriculation
    - KM
    - Description
"""

from __future__ import annotations

import os
import sys
import io
import re
import warnings
from dataclasses import dataclass
from typing import Any, List, Dict, Tuple, Optional
from datetime import datetime, timezone as TZ

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern

from helper import (
    log,
    Stepper,
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
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

# ─────────────────────────────────────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

FLEET_FILENAME_REGEX_DEFAULT = r"(?i)YFACFLEETDS"
FLEET_HEADER_ROW = 1  # row 2 in Excel

# STRICT header map (Excel label → canonical field name)
FLEET_HEADERS_MAP: Dict[str, str] = {
    "Date DS": "date_ds",
    "N°DS": "nds_ds",
    "Code art": "code_art",
    "Désignation article": "designation_article_ds",
    "Qté": "qte_ds",
    "Mt HT DS": "mt_ht_ds",            # per line (not total DS)
    "Immatriculation": "imm",
    "KM": "km_ds",
    "Description": "description_ds",
}

# EXACT same DS_LINE_FIELDS as ds.py
DS_LINE_FIELDS = [
    "date_ds", "description_ds", "designation_article_ds", "fournisseur_ds",
    "imm", "km_ds", "nds_ds", "prix_unitaire_ds_ds", "qte_ds",
    "entite_ds", "technicien", "code_art", "imm_norm", "ww_norm",
]

STEP = Stepper()

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    mongo_uri: str
    mongo_db: str
    creds_b64: str
    drive_folder_id: str
    fleet_regex: str


def load_config() -> Config:
    STEP.step("Load configuration")
    load_dotenv(override=False)

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db  = os.getenv("MONGODB_DB")
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if not (mongo_uri and mongo_db and creds_b64 and folder_id):
        raise ConfigError(
            "Missing one of the required environment variables: "
            "MONGODB_URI, MONGODB_DB, GOOGLE_CREDENTIALS_BASE64, GOOGLE_DRIVE_FOLDER_ID"
        )

    fleet_regex = os.getenv("FLEET_FILENAME_REGEX", FLEET_FILENAME_REGEX_DEFAULT)
    log(f"Using Fleet regex: {fleet_regex}")

    return Config(mongo_uri, mongo_db, creds_b64, folder_id, fleet_regex)

# ─────────────────────────────────────────────────────────────────────────────
#  DRIVE SELECTION + DOWNLOAD
# ─────────────────────────────────────────────────────────────────────────────

def pick_and_fetch_fleet(drive, folder_id: str, name_regex: str) -> Tuple[str, str, str]:
    STEP.step("Find valid XLSX for fleet.xlsx")
    rx = re.compile(name_regex)
    files = list_folder_files(drive, folder_id)
    if not files:
        raise DriveError("Drive folder is empty")

    for f in files:
        fid  = f.get("id")
        name = f.get("name", "")
        mime = f.get("mimeType", "")

        if rx.search(name) and _looks_like_xlsx(name, mime):
            tmp = "data/.tmp_fleet.xlsx"
            download_xlsx(drive, fid, tmp)

            final_path = "data/fleet.xlsx"
            if os.path.exists(final_path):
                try:
                    os.remove(final_path)
                except Exception:
                    pass

            os.replace(tmp, final_path)
            log(f"Fleet accepted: {name}")
            return fid, name, final_path

    raise DriveError("No XLSX fleet file matched regex + MIME.")

# ─────────────────────────────────────────────────────────────────────────────
#  STRICT READER
# ─────────────────────────────────────────────────────────────────────────────

def read_fleet_strict(path: str, header_row: int, header_map: Dict[str, str]) -> pd.DataFrame:
    STEP.step("Read Fleet (STRICT keep+rename headers)")

    df = pd.read_excel(path, header=header_row, engine="openpyxl")
    df = _clean_dataframe_excel_escapes(df)

    norm_expected = { _norm_label(k): v for k,v in header_map.items() }
    norm_actual   = { _norm_label(c): c for c in df.columns }

    matched = {}
    for nlabel, canonical in norm_expected.items():
        if nlabel in norm_actual:
            matched[canonical] = norm_actual[nlabel]
            log(f"FLEET: matched '{canonical}' <- '{norm_actual[nlabel]}'")

    if not matched:
        raise TransformError("No expected Fleet headers found (strict mode).")

    keep_cols = [matched[c] for c in matched]
    rename = {matched[c]: c for c in matched}

    out = df[keep_cols].rename(columns=rename).copy()
    log(f"FLEET: kept columns -> {list(out.columns)}")
    return out

# ─────────────────────────────────────────────────────────────────────────────
#  NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

def normalize_fleet_df(df: pd.DataFrame) -> pd.DataFrame:
    STEP.step("Normalize Fleet rows")

    if "date_ds" in df.columns:
        df["date_ds"] = df["date_ds"].map(_iso_date_from_any)

    if "imm" in df.columns:
        df["imm_norm"] = df["imm"].map(canon_plate)

    # Fleet files do not have WW, but keep a placeholder for DS contract compatibility
    df["ww_norm"] = None

    # Convert Mt HT DS & Qté to numeric
    if "mt_ht_ds" in df.columns:
        df["mt_ht_ds"] = pd.to_numeric(df["mt_ht_ds"], errors="coerce")

    if "qte_ds" in df.columns:
        df["qte_ds"] = pd.to_numeric(df["qte_ds"], errors="coerce")

    # Compute prix_unitaire_ds_ds = MtHT / Qté
    df["prix_unitaire_ds_ds"] = df.apply(
        lambda r:
            (r["mt_ht_ds"] / r["qte_ds"])
            if pd.notnull(r.get("mt_ht_ds"))
            and pd.notnull(r.get("qte_ds"))
            and r.get("qte_ds") not in (0, None)
            else None,
        axis=1,
    )

    # Convert to clean string (same behavior as ds.py)
    df["prix_unitaire_ds_ds"] = df["prix_unitaire_ds_ds"].map(_txt)

    # Keep only rows with a valid vehicle identifier
    df = df[df["imm_norm"].map(lambda x: bool(_txt(x)))].copy()

    if df.empty:
        log("Fleet: no valid rows after imm_norm filter")

    return df

# ─────────────────────────────────────────────────────────────────────────────
#  LINES BUILDING
# ─────────────────────────────────────────────────────────────────────────────

def canonicalize_lines(df_group: pd.DataFrame) -> List[Dict[str, Any]]:
    rows = []

    for _, r in df_group.iterrows():
        rows.append({
            k: _txt(r.get(k)) if k in df_group.columns else None
            for k in DS_LINE_FIELDS
        })

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

# ─────────────────────────────────────────────────────────────────────────────
#  BUILD FLEET DOCUMENTS
# ─────────────────────────────────────────────────────────────────────────────

def build_fleet_docs(df_raw: pd.DataFrame) -> List[Dict[str, Any]]:
    STEP.step("Build Fleet documents")

    df = normalize_fleet_df(df_raw.copy())
    if df.empty:
        return []

    docs = []

    for ds_no, g in df.groupby("nds_ds", dropna=False, sort=False):
        _id = _txt(ds_no)
        if not _id:
            continue

        lines = canonicalize_lines(g)

        imm_norm = _txt(g["imm_norm"].dropna().iloc[0]) if not g["imm_norm"].dropna().empty else None
        ww_norm  = None

        vehicle_id = imm_norm or ww_norm

        dt_series = pd.to_datetime(g["date_ds"], errors="coerce", utc=True).dropna()
        date_event = dt_series.max().to_pydatetime() if not dt_series.empty else None

        docs.append({
            "_id": _id,
            "ds_no": _id,
            "nds_ds": _id,
            "vehicle_id": vehicle_id,
            "imm_norm": imm_norm,
            "ww_norm": ww_norm,
            "date_event": date_event,
            "lines": lines,
            "lines_sig": _sha256_json(lines),
        })

    log(f"Fleet docs: {len(docs)}")
    return docs

# ─────────────────────────────────────────────────────────────────────────────
#  MONGO LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def get_client_db(uri: str, dbname: str):
    STEP.step("Connect to MongoDB")
    try:
        client = MongoClient(uri,
            serverSelectionTimeoutMS=20000,
            socketTimeoutMS=300000,
            connectTimeoutMS=20000,
            maxPoolSize=100,
            compressors=["zstd","snappy","zlib"],
            retryWrites=True,
            appname="etl-upsert-fleet",
        )
        db = client.get_database(dbname, write_concern=WriteConcern(w=1))
        client.admin.command("ping")
        return client, db
    except Exception as e:
        raise UpsertError(f"Mongo connection failed: {e}")

def ensure_fleet_indexes(db):
    STEP.step("Ensure fleet indexes")
    for f in ("nds_ds","imm_norm","ww_norm","vehicle_id","lines_sig"):
        db.fleet.create_index(f)
    log("fleet indexes ensured")

def _preload_sig_fleet(db, ids: List[str]):
    if not ids:
        return {}
    cur = db.fleet.find({"_id": {"$in": ids}}, {"_id":1, "lines_sig":1})
    return {str(d["_id"]): d.get("lines_sig") for d in cur}

def upsert_fleet(db, docs: List[Dict[str, Any]]):
    STEP.step("Upsert fleet")
    if not docs:
        return {"inserted":0,"updated":0,"skipped":0}

    sig_map = _preload_sig_fleet(db, [d["_id"] for d in docs])
    now = datetime.now(TZ.utc)

    ins=upd=skp=0
    ops=[]

    for d in docs:
        prev = sig_map.get(d["_id"])
        if prev is None:
            ins += 1
        elif prev == d["lines_sig"]:
            skp += 1
            continue
        else:
            upd += 1

        ops.append(UpdateOne(
            {"_id": d["_id"]},
            {
                "$set": {**d, "updated_at": now},
                "$setOnInsert": {"created_at": now}
            },
            upsert=True
        ))

    if ops:
        db.fleet.bulk_write(ops, ordered=False, bypass_document_validation=True)

    return {"inserted":ins,"updated":upd,"skipped":skp}

# ─────────────────────────────────────────────────────────────────────────────
#  ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RunCfg:
    mongo_uri: str
    mongo_db: str
    creds_b64: str
    drive_folder_id: str
    fleet_regex: str

def run(cfg: RunCfg):
    log("BEGIN FLEET-only load")

    drive = make_drive(cfg.creds_b64)
    _, name, path = pick_and_fetch_fleet(drive, cfg.drive_folder_id, cfg.fleet_regex)
    log(f"Fleet XLSX: {name}")

    df = read_fleet_strict(path, FLEET_HEADER_ROW, FLEET_HEADERS_MAP)
    docs = build_fleet_docs(df)

    client, db = get_client_db(cfg.mongo_uri, cfg.mongo_db)
    try:
        stats = upsert_fleet(db, docs)
        ensure_fleet_indexes(db)
    finally:
        client.close()

    log(f"UPSERT FLEET: {stats}")
    log("END FLEET-only load")

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if sys.getdefaultencoding().lower() != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

    try:
        cfg = load_config()
        run(RunCfg(
            cfg.mongo_uri,
            cfg.mongo_db,
            cfg.creds_b64,
            cfg.drive_folder_id,
            cfg.fleet_regex,
        ))
    except (ConfigError,DriveError,TransformError,UpsertError) as e:
        log(f"ERROR: {e}")
        sys.exit(2)
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}")
        sys.exit(3)
