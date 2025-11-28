#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
cb.py — Loader for Purchase Orders (CB/YBONC) from Google Drive → MongoDB 'cb'

- Finds latest CB XLSX in a Drive folder (regex on file name)
- Downloads it to data/cb.xlsx
- STRICT header matching using CB_HEADERS_MAP (all CB headers)
- Normalises plates, dates, numeric fields
- Groups by BC number (N° BC) → ONE Mongo document per BC
- Each document has:
    • static/root fields (site, type BC, supplier, invoice info, etc.)
    • imm_norm
    • lines[] with article-level details
    • total_ht_cb (root) based on sheet total
    • doc_sig = sha256(JSON(lines))
"""

from __future__ import annotations

import os
import sys
import io
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
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

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

# Default regex to detect the CB file on Drive (can be overridden with CB_FILENAME_REGEX)
CB_FILENAME_REGEX_DEFAULT = r"(?i)\b(YBONC|Bon[ _-]?de[ _-]?Commande|BC)\b"

CB_HEADER_ROW = 1  # 0-based index: row 2 in Excel UI

# Excel header label (left) -> canonical field name (right)
CB_HEADERS_MAP: Dict[str, str] = {
    "Site": "site_cb",
    "N° BC": "num_bc",
    "Type BC": "type_bc_cb",
    "Immatriculation": "imm",
    "Date BC": "date_cb",
    "Code frs": "code_frs_cb",
    "Fournisseurs": "supplier_cb",
    "Cat. Article": "cat_article_cb",
    "Code article": "code_article_cb",
    "Description article": "description_article_cb",
    "Nature article": "nature_article_cb",
    "Sous-nature article": "sous_nature_article_cb",
    "PU": "unit_price_cb",
    "Qté": "qty_cb",
    "Prix brut pièce": "brut_price_cb",
    "Remise article": "remise_article_cb",
    "Montant HT": "amount_ht_cb",
    "Total HT": "total_ht_cb",
    "Entité": "entity_cb",
    "Marque": "marque_cb",
    "Signé": "signed_cb",
    "Commande Ferme": "firm_order_cb",
    "Réceptionné": "received_cb",
    "Cde origine": "origin_order_cb",
    "Soldé": "closed_cb",
    "Achteur": "buyer_cb",
    "Crée par": "created_by_cb",
    "DS": "ds_cb",
    "Type DS": "type_ds_cb",
    "Client DS": "client_ds_cb",
    "KM": "km_cb",
    "Ville": "city_cb",
    "N° facture": "invoice_no_cb",
    "Date récéption": "invoice_date_cb",
    "N° Facture fourn": "invoice_supplier_no_cb",
    "Total facture": "invoice_total_cb",
    "N° Avoir": "avoir_no_cb",
    "MT Devise": "amount_devise_cb",
    "Devise": "devise_cb",
    "MT MAD": "amount_mad_cb",
}

# Fields that make up ONE line in lines[]
CB_LINE_FIELDS: List[str] = [
    "cat_article_cb",
    "code_article_cb",
    "description_article_cb",
    "nature_article_cb",
    "sous_nature_article_cb",
    "marque_cb",
    "unit_price_cb",
    "qty_cb",
    "brut_price_cb",
    "remise_article_cb",
    "amount_ht_cb",
    "total_ht_cb",
]

STEP = Stepper()

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    mongo_uri: str
    mongo_db: str
    creds_b64: str
    drive_folder_id: str
    cb_regex: str


def load_config() -> Config:
    """
    Load configuration from environment:
      - MONGODB_URI
      - MONGODB_DB
      - GOOGLE_CREDENTIALS_BASE64
      - CB_GOOGLE_DRIVE_FOLDER_ID or GOOGLE_DRIVE_FOLDER_ID
      - CB_FILENAME_REGEX (optional, default CB_FILENAME_REGEX_DEFAULT)
    """
    STEP.step("Load configuration")
    load_dotenv(override=False)

    mongo_uri = os.getenv("MONGODB_URI")
    mongo_db = os.getenv("MONGODB_DB")
    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")

    # Prefer dedicated CB folder id, fallback to the global one (same as ds.py)
    folder_id = os.getenv("CB_GOOGLE_DRIVE_FOLDER_ID") or os.getenv("GOOGLE_DRIVE_FOLDER_ID")

    if not (mongo_uri and mongo_db and creds_b64 and folder_id):
        raise ConfigError(
            "Missing one of: MONGODB_URI, MONGODB_DB, GOOGLE_CREDENTIALS_BASE64, "
            "CB_GOOGLE_DRIVE_FOLDER_ID/GOOGLE_DRIVE_FOLDER_ID"
        )

    cb_regex = os.getenv("CB_FILENAME_REGEX", CB_FILENAME_REGEX_DEFAULT)
    log(f"Using CB regex: {cb_regex}")

    return Config(
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
        creds_b64=creds_b64,
        drive_folder_id=folder_id,
        cb_regex=cb_regex,
    )

# ---------------------------------------------------------------------------
# DRIVE PICKER
# ---------------------------------------------------------------------------

def pick_and_fetch_cb(drive, folder_id: str, name_regex: str):
    """
    Find latest CB XLSX in folder matching regex + XLSX MIME, then download
    it to data/cb.xlsx using shared Drive helpers.
    """
    STEP.step("Find valid XLSX for cb.xlsx")
    rx = re.compile(name_regex)
    files = list_folder_files(drive, folder_id)
    if not files:
        raise DriveError("CB folder is empty")

    for f in files:  # newest first (list_folder_files already sorts desc)
        fid = f.get("id")
        name = f.get("name", "")
        mime = f.get("mimeType", "")

        if not (rx.search(name) and _looks_like_xlsx(name, mime)):
            continue

        log(f"CB candidate: {name} (mime ok)")
        tmp = "data/.tmp_cb.xlsx"
        download_xlsx(drive, fid, tmp)

        final_path = "data/cb.xlsx"
        try:
            if os.path.exists(final_path):
                os.remove(final_path)
        except Exception:
            pass
        os.replace(tmp, final_path)
        log(f"CB accepted: {name}")
        return fid, name, final_path

    raise DriveError("No XLSX matched regex + MIME + extension for CB.")

# ---------------------------------------------------------------------------
# EXCEL READER (STRICT HEADERS)
# ---------------------------------------------------------------------------

def read_cb_strict(path: str, header_row: int, header_map: Dict[str, str]) -> pd.DataFrame:
    """
    Strict CB reader:
      - read Excel with header_row
      - clean XML escapes
      - normalize labels (lowercase, no accents, etc.) via _norm_label
      - keep+rename only expected columns (CB_HEADERS_MAP)
      - error if nothing matched
    """
    STEP.step("Read CB (keep+rename, STRICT headers)")
    df = pd.read_excel(path, header=header_row, engine="openpyxl")

    # Clean encoded control characters in values
    df = _clean_dataframe_excel_escapes(df)

    # Expected: normalized label -> canonical field name
    norm_expected_to_canonical: Dict[str, str] = {
        _norm_label(lbl): canonical
        for lbl, canonical in header_map.items()
    }

    # Actual: normalized label -> original column name
    norm_label_to_actual: Dict[str, str] = {}
    for col in df.columns:
        norm_label_to_actual[_norm_label(col)] = col

    matched: Dict[str, str] = {}
    for nlabel, canonical in norm_expected_to_canonical.items():
        if nlabel in norm_label_to_actual:
            actual = norm_label_to_actual[nlabel]
            matched[canonical] = actual
            log(f"CB: matched '{canonical}' <- '{actual}' (strict)")

    if not matched:
        raise TransformError(
            f"CB: none of the expected headers found (strict normalized match) at row {header_row + 1}"
        )

    keep_cols = [matched[c] for c in matched]
    rename_map = {matched[c]: c for c in matched}

    out = df[keep_cols].rename(columns=rename_map).copy()
    log(f"CB: kept columns -> {list(out.columns)}")
    return out

# ---------------------------------------------------------------------------
# NORMALISATION
# ---------------------------------------------------------------------------

def normalize_cb_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize CB fields:
      - date_cb / invoice_date_cb → ISO string
      - imm → imm_norm (canon_plate)
      - numeric columns → float where possible
      - filter rows without imm_norm (no vehicle)
    """
    STEP.step("Normalize CB rows")

    if "date_cb" in df.columns:
        df["date_cb"] = df["date_cb"].map(_iso_date_from_any)

    if "invoice_date_cb" in df.columns:
        df["invoice_date_cb"] = df["invoice_date_cb"].map(_iso_date_from_any)

    if "imm" in df.columns:
        df["imm_norm"] = df["imm"].map(canon_plate)

    # Numeric-like columns
    numeric_cols = [
        "unit_price_cb",
        "qty_cb",
        "brut_price_cb",
        "remise_article_cb",
        "amount_ht_cb",
        "total_ht_cb",
        "km_cb",
        "invoice_total_cb",
        "amount_devise_cb",
        "amount_mad_cb",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Require imm_norm to identify a vehicle
    if "imm_norm" in df.columns:
        df = df[df["imm_norm"].map(lambda v: bool(_txt(v)))].copy()

    if df.empty:
        log("CB: no valid rows after imm_norm filter")

    return df

# ---------------------------------------------------------------------------
# BUILD DOCUMENTS (GROUP BY BC)
# ---------------------------------------------------------------------------

def _first_non_empty(series) -> Optional[str]:
    """Return the first non-empty string from a pandas Series, or None."""
    if series is None:
        return None
    for v in series.tolist():
        s = _txt(v)
        if s:
            return s
    return None


def build_cb_docs(df_raw: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Build one Mongo document per BC (num_bc), with detail lines.

    For each num_bc:
      - root fields (static):
          site_cb, num_bc, type_bc_cb, imm, imm_norm, date_cb,
          code_frs_cb, supplier_cb, entity_cb, marque_cb, signed_cb,
          firm_order_cb, received_cb, origin_order_cb, closed_cb,
          buyer_cb, created_by_cb, ds_cb, type_ds_cb, client_ds_cb,
          km_cb, city_cb,
          invoice_* fields, devise / MT MAD / MT Devise, etc.
      - lines: list of article-level fields (CB_LINE_FIELDS)
      - total_ht_cb (root): first non-empty total_ht_cb in group,
                            otherwise sum(amount_ht_cb)
      - doc_sig: sha256(JSON(lines))
      - _id: num_bc
    """
    STEP.step("Build CB documents")

    df = normalize_cb_df(df_raw.copy())
    if df.empty:
        log("CB: empty after normalization")
        return []

    if "num_bc" not in df.columns:
        log("CB: no 'num_bc' column, cannot group")
        return []

    docs: List[Dict[str, Any]] = []

    # fields considered as "static" at BC level
    static_fields = [
        "site_cb",
        "num_bc",
        "type_bc_cb",
        "imm",
        "date_cb",
        "code_frs_cb",
        "supplier_cb",
        "entity_cb",
        "marque_cb",
        "signed_cb",
        "firm_order_cb",
        "received_cb",
        "origin_order_cb",
        "closed_cb",
        "buyer_cb",
        "created_by_cb",
        "ds_cb",
        "type_ds_cb",
        "client_ds_cb",
        "km_cb",
        "city_cb",
        "invoice_no_cb",
        "invoice_date_cb",
        "invoice_supplier_no_cb",
        "invoice_total_cb",
        "avoir_no_cb",
        "amount_devise_cb",
        "devise_cb",
        "amount_mad_cb",
    ]

    for bc_no, g in df.groupby("num_bc", dropna=True, sort=False):
        _id = _txt(bc_no)
        if not _id:
            continue

        doc: Dict[str, Any] = {}

        # static fields → first non-empty value in group
        for f in static_fields:
            if f in g.columns:
                doc[f] = _first_non_empty(g[f])

        # imm_norm (technical)
        if "imm_norm" in g.columns:
            doc["imm_norm"] = _first_non_empty(g["imm_norm"])

        # ---- lines: one entry per row ----
        lines: List[Dict[str, Any]] = []
        for _, row in g.iterrows():
            line = {}
            for k in CB_LINE_FIELDS:
                if k in g.columns:
                    line[k] = _txt(row.get(k))
                else:
                    line[k] = None
            lines.append(line)

        # stable sort lines (by code + article + qty + price)
        lines.sort(
            key=lambda r: (
                r.get("code_article_cb") or "",
                r.get("description_article_cb") or "",
                r.get("qty_cb") or "",
                r.get("unit_price_cb") or "",
            )
        )

        # ---- total_ht_cb at root ----
        root_total = _first_non_empty(g.get("total_ht_cb")) if "total_ht_cb" in g.columns else None
        if root_total is None:
            # fallback = sum Montant HT
            total = 0.0
            if "amount_ht_cb" in g.columns:
                for v in g["amount_ht_cb"]:
                    try:
                        total += float(v)
                    except Exception:
                        pass
            doc["total_ht_cb"] = str(total) if total else None
        else:
            doc["total_ht_cb"] = root_total

        # ---- amount_ht_cb at root (optional aggregate) ----
        if "amount_ht_cb" in g.columns:
            total_amt = 0.0
            for v in g["amount_ht_cb"]:
                try:
                    total_amt += float(v)
                except Exception:
                    pass
            doc["amount_ht_sum_cb"] = total_amt

        # ---- signature & id ----
        doc_sig = _sha256_json(lines)

        doc.update(
            {
                "_id": _id,
                "doc_sig": doc_sig,
                "lines": lines,
            }
        )

        docs.append(doc)

    log(f"CB docs: {len(docs)}")
    return docs

# ---------------------------------------------------------------------------
# MONGO HELPERS
# ---------------------------------------------------------------------------

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
            appname="etl-upsert-cb",
        )
        db = client.get_database(dbname, write_concern=WriteConcern(w=1))
        client.admin.command("ping")
        log("Mongo ping OK")
        return client, db
    except Exception as e:
        raise UpsertError(f"Mongo connection failed: {e}")


def ensure_cb_indexes(db) -> None:
    STEP.step("Ensure cb indexes")
    try:
        for field in ("imm_norm", "num_bc", "date_cb", "doc_sig", "supplier_cb", "city_cb"):
            db.cb.create_index(field)
        log("cb indexes ensured")
    except Exception as e:
        raise UpsertError(f"Creating cb indexes failed: {e}")


def _preload_sig_cb(db, ids: List[str]) -> Dict[str, Optional[str]]:
    if not ids:
        return {}
    cur = db.cb.find({"_id": {"$in": ids}}, {"_id": 1, "doc_sig": 1})
    return {str(d["_id"]): d.get("doc_sig") for d in cur}


def upsert_cb(db, docs: List[Dict[str, Any]]) -> Dict[str, int]:
    STEP.step("Upsert cb")

    if not docs:
        log("No CB docs to upsert")
        return {"inserted": 0, "updated": 0, "skipped": 0}

    sig_map = _preload_sig_cb(db, [d["_id"] for d in docs])
    now = datetime.now(TZ.utc)

    inserted = updated = skipped = 0
    ops: List[UpdateOne] = []

    for d in docs:
        prev_sig = sig_map.get(d["_id"], None)
        if prev_sig is None:
            inserted += 1
        elif prev_sig == d.get("doc_sig"):
            skipped += 1
            continue
        else:
            updated += 1

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
        db.cb.bulk_write(ops, ordered=False, bypass_document_validation=True)

    log(f"cb → inserted={inserted} updated={updated} skipped={skipped}")
    return {"inserted": inserted, "updated": updated, "skipped": skipped}

# ---------------------------------------------------------------------------
# ORCHESTRATION
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RunCfg:
    mongo_uri: str
    mongo_db: str
    creds_b64: str
    drive_folder_id: str
    cb_regex: str


def run(cfg: RunCfg) -> None:
    log("BEGIN CB load")

    drive = make_drive(cfg.creds_b64)
    _, cb_name, cb_path = pick_and_fetch_cb(drive, cfg.drive_folder_id, cfg.cb_regex)
    log(f"CB   → {cb_name}")

    df_cb = read_cb_strict(cb_path, CB_HEADER_ROW, CB_HEADERS_MAP)
    cb_docs = build_cb_docs(df_cb)

    client, db = get_client_db(cfg.mongo_uri, cfg.mongo_db)
    try:
        stats = upsert_cb(db, cb_docs)
        ensure_cb_indexes(db)
    finally:
        try:
            client.close()
        except Exception:
            pass

    log(f"UPSERT CB: {stats}")
    log("END CB load")

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

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
                cfg.cb_regex,
            )
        )
    except (ConfigError, DriveError, TransformError, UpsertError) as e:
        log(f"ERROR: {e}")
        sys.exit(2)
    except Exception as e:
        log(f"FATAL: {type(e).__name__}: {e}")
        sys.exit(3)
