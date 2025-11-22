#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ds.py — DS-only loader (STRICT headers; XLSX-only) matching main.py DS contract 1:1
"""

from __future__ import annotations

import os
import sys
import io
import re
from dataclasses import dataclass
from typing import Any, List, Dict, Tuple, Optional
from datetime import datetime, timezone as TZ, date, timedelta

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

# ── Quiet pandas/openpyxl warnings (optional, already done in helper but safe) ─
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

# ── Constants ────────────────────────────────────────────────────────────────

DS_FILENAME_REGEX_DEFAULT = r"(?i)\b(DS|Devis[ _-]?Service|Bon[ _-]?de[ _-]?(?:R[eé]paration))\b"
DS_HEADER_ROW = 1  # 0-based index (row 2 in Excel UI)

# Header map: LEFT = Excel label, RIGHT = canonical field name
DS_HEADERS_MAP: Dict[str, str] = {
    "Date DS": "date_ds",
    "Description": "description_ds",
    "Désignation Consomation": "designation_article_ds",
    "Founisseur": "fournisseur_ds",
    "Immatriculation": "imm",
    "KM": "km_ds",
    "N°DS": "nds_ds",
    "Prix Unitaire ds": "prix_unitaire_ds_ds",
    "Qté": "qte_ds",
    "ENTITE": "entite_ds",
    "Technicein": "technicien",
    "Code art": "code_art",
    # "WW": "ww",  # if you ever add WW to DS sheet
}

DS_LINE_FIELDS = [
    "date_ds", "description_ds", "designation_article_ds", "fournisseur_ds",
    "imm", "km_ds", "nds_ds", "prix_unitaire_ds_ds", "qte_ds",
    "entite_ds", "technicien", "code_art", "imm_norm", "ww_norm",
]

# ── Stepper for pretty logs ──────────────────────────────────────────────────

STEP = Stepper()

# ── Config ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Config:
    mongo_uri: str
    mongo_db: str
    creds_b64: str
    drive_folder_id: str
    ds_regex: str


def load_config() -> Config:
    STEP.step("Load configuration")
    load_dotenv(override=False)

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db  = os.getenv("MONGODB_DB")
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not (mongo_uri and mongo_db and creds_b64 and folder_id):
        raise ConfigError(
            "Missing one of: MONGODB_URI, MONGODB_DB, GOOGLE_CREDENTIALS_BASE64, GOOGLE_DRIVE_FOLDER_ID"
        )

    ds_regex = os.getenv("DS_FILENAME_REGEX", DS_FILENAME_REGEX_DEFAULT)
    log(f"Using DS regex: {ds_regex}")

    return Config(
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
        creds_b64=creds_b64,
        drive_folder_id=folder_id,
        ds_regex=ds_regex,
    )

# ── Drive ────────────────────────────────────────────────────────────────────

def pick_and_fetch_ds(drive, folder_id: str, name_regex: str) -> Tuple[str, str, str]:
    """
    Find latest DS XLSX in folder matching regex + XLSX MIME, then download
    it to data/ds.xlsx using shared Drive helpers.
    """
    STEP.step("Find valid XLSX for ds.xlsx")
    rx = re.compile(name_regex)
    files = list_folder_files(drive, folder_id)
    if not files:
        raise DriveError("Folder is empty")

    for f in files:  # newest first
        fid  = f.get("id")
        name = f.get("name", "")
        mime = f.get("mimeType", "")
        if not (rx.search(name) and _looks_like_xlsx(name, mime)):
            continue

        log(f"DS candidate: {name} (mime ok)")
        tmp = "data/.tmp_ds.xlsx"
        download_xlsx(drive, fid, tmp)

        final_path = "data/ds.xlsx"
        try:
            if os.path.exists(final_path):
                os.remove(final_path)
        except Exception:
            pass
        os.replace(tmp, final_path)
        log(f"DS accepted: {name}")
        return fid, name, final_path

    raise DriveError("No XLSX matched regex + MIME + extension for DS.")

# ── STRICT keep+rename reader for DS ─────────────────────────────────────────

def read_ds_strict(path: str, header_row: int, header_map: Dict[str, str]) -> pd.DataFrame:
    """
    Strict DS reader:
      - read Excel with header_row
      - clean Excel XML escapes in values
      - normalize labels (lowercase, no accents, no punctuation, collapse spaces)
      - keep+rename only expected columns
      - error if none matched
    """
    STEP.step("Read DS (keep+rename, STRICT headers)")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")

    # Clean encoded control chars in values
    df = _clean_dataframe_excel_escapes(df)

    # Normalized lookup: expected label → canonical name
    norm_expected_to_canonical: Dict[str, str] = {}
    for raw, canonical in header_map.items():
        norm_expected_to_canonical[_norm_label(raw)] = canonical

    # Normalized lookup: actual column label → original column name
    norm_col_to_actual: Dict[str, str] = {}
    for c in df.columns:
        norm_col_to_actual[_norm_label(c)] = c

    matched: Dict[str, str] = {}
    for ne, canonical in norm_expected_to_canonical.items():
        if ne in norm_col_to_actual:
            actual = norm_col_to_actual[ne]
            matched[canonical] = actual
            log(f"DS: matched '{canonical}' <- '{actual}' (strict)")

    if not matched:
        raise TransformError(
            f"DS: none of the expected headers found (strict normalized match) at row {header_row}."
        )

    keep_cols = [actual for actual in matched.values()]
    rename_map = {actual: canonical for canonical, actual in matched.items()}
    out = df[keep_cols].rename(columns=rename_map).copy()
    log(f"DS: kept -> {list(out.columns)}")
    return out

# ── DS doc building ──────────────────────────────────────────────────────────

def normalize_ds_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize basic DS fields:
      - date_ds → ISO date
      - imm → imm_norm
      - ww → ww_norm (if present)
    """
    if "date_ds" in df.columns:
        df["date_ds"] = df["date_ds"].map(_iso_date_from_any)
    if "imm" in df.columns:
        df["imm_norm"] = df["imm"].map(canon_plate)
    if "ww" in df.columns:
        df["ww_norm"] = df["ww"].map(canon_plate)
    return df


def canonicalize_lines(df_group: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Build DS lines array with stable sorting and full DS_LINE_FIELDS.

    Missing fields (e.g. code_art / entite_ds) are set to None so that
    DS + Fleet can share the same schema and signature behavior.
    """
    rows: List[Dict[str, Any]] = []
    for _, row in df_group.iterrows():
        rows.append({
            k: _txt(row.get(k)) if k in df_group.columns else None
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


def build_ds_docs(df_raw: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Group by DS number and build Mongo-ready documents with lines_sig.
    """
    STEP.step("Build DS documents")

    df = normalize_ds_df(df_raw.copy())

    # Must have at least one vehicle id (imm_norm or ww_norm)
    mask = df.apply(lambda r: bool(_txt(r.get("imm_norm")) or _txt(r.get("ww_norm"))), axis=1)
    df = df[mask].copy()
    if df.empty:
        log("DS: no valid rows after vehicle id filter (imm_norm/ww_norm)")
        return []

    docs: List[Dict[str, Any]] = []

    for ds_no, g in df.groupby("nds_ds", dropna=False, sort=False):
        _id = _txt(ds_no)
        if not _id:
            continue

        lines = canonicalize_lines(g)

        imm_norm = (
            _txt(g.get("imm_norm").dropna().iloc[0])
            if "imm_norm" in g and not g["imm_norm"].dropna().empty
            else None
        )
        ww_norm = (
            _txt(g.get("ww_norm").dropna().iloc[0])
            if "ww_norm" in g and not g["ww_norm"].dropna().empty
            else None
        )

        vehicle_id = imm_norm or ww_norm

        dt_series = pd.to_datetime(g.get("date_ds"), errors="coerce", utc=True).dropna()
        date_event = dt_series.max() if not dt_series.empty else None

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

    log(f"DS docs: {len(docs)}")
    return docs

# ── Mongo plumbing ───────────────────────────────────────────────────────────

def get_client_db(uri: str, dbname: str):
    STEP.step("Connect to MongoDB")
    try:
        client = MongoClient(
            uri,
            serverSelectionTimeoutMS=20000,
            socketTimeoutMS=300000,
            connectTimeoutMS=20000,
            maxPoolSize=100,
            compressors=["zstd", "snappy", "zlib"],
            retryWrites=True,
            appname="etl-upsert-ds",
        )
        db = client.get_database(dbname, write_concern=WriteConcern(w=1))
        client.admin.command("ping")
        log("Mongo ping OK")
        return client, db
    except Exception as e:
        raise UpsertError(f"Mongo connection failed: {e}")


def ensure_ds_indexes(db) -> None:
    STEP.step("Ensure ds indexes")
    try:
        for f in ("nds_ds", "imm_norm", "ww_norm", "vehicle_id", "lines_sig"):
            db.ds.create_index(f)
        log("ds indexes ensured")
    except Exception as e:
        raise UpsertError(f"Creating ds indexes failed: {e}")


def _preload_sig(db, ids: List[str]) -> Dict[str, Optional[str]]:
    if not ids:
        return {}
    cur = db.ds.find({"_id": {"$in": ids}}, {"_id": 1, "lines_sig": 1})
    return {str(d["_id"]): d.get("lines_sig") for d in cur}


def upsert_ds(db, docs: List[Dict[str, Any]]) -> Dict[str, int]:
    STEP.step("Upsert ds")

    if not docs:
        log("No DS docs to upsert")
        return {"inserted": 0, "updated": 0, "skipped": 0}

    sig_map = _preload_sig(db, [d["_id"] for d in docs])
    now = datetime.now(TZ.utc)

    ins = upd = skp = 0
    ops: List[UpdateOne] = []

    for d in docs:
        prev = sig_map.get(d["_id"], None)
        if prev is None:
            ins += 1
        elif prev == d.get("lines_sig"):
            skp += 1
            continue
        else:
            upd += 1

        ops.append(
            UpdateOne(
                {"_id": d["_id"]},
                {
                    "$set": {**d, "updated_at": now},
                    "$setOnInsert": {"created_at": now},
                },
                upsert=True,
            )
        )

    if ops:
        db.ds.bulk_write(ops, ordered=False, bypass_document_validation=True)

    log(f"ds → inserted={ins} updated={upd} skipped={skp}")
    return {"inserted": ins, "updated": upd, "skipped": skp}

# ── Orchestration ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RunCfg:
    mongo_uri: str
    mongo_db: str
    creds_b64: str
    drive_folder_id: str
    ds_regex: str


def run(cfg: RunCfg) -> None:
    log("BEGIN DS-only load")

    drive = make_drive(cfg.creds_b64)
    _, ds_name, ds_path = pick_and_fetch_ds(drive, cfg.drive_folder_id, cfg.ds_regex)
    log(f"DS   → {ds_name}")

    df_ds = read_ds_strict(ds_path, DS_HEADER_ROW, DS_HEADERS_MAP)
    ds_docs = build_ds_docs(df_ds)

    client, db = get_client_db(cfg.mongo_uri, cfg.mongo_db)
    try:
        stats = upsert_ds(db, ds_docs)
        ensure_ds_indexes(db)
    finally:
        try:
            client.close()
        except Exception:
            pass

    log(f"UPSERT DS: {stats}")
    log("END DS-only load")

# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if sys.getdefaultencoding().lower() != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

    try:
        cfg = load_config()
        run(
            RunCfg(
                cfg.mongo_uri,
                cfg.mongo_db,
                cfg.creds_b64,
                cfg.drive_folder_id,
                cfg.ds_regex,
            )
        )
    except (ConfigError, DriveError, TransformError, UpsertError) as e:
        log(f"ERROR: {e}")
        sys.exit(2)
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}")
        sys.exit(3)
