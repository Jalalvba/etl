#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
helper.py — Shared helpers for CP / DS / Fleet / Parc loaders.

Contains:
  • Logging + Stepper
  • Small value/normalization helpers
  • Excel date handling
  • JSON hashing
  • Excel XML escape cleanup
  • Google Drive helpers
  • Common error classes

All scripts (cp.py, ds/appds.py, fleet.py, parc.py) should import from here
instead of redefining their own versions.
"""

from __future__ import annotations

import os
import re
import json
import base64
import hashlib
import unicodedata
import warnings
from typing import Any, Dict, List, Optional
from datetime import datetime, date, timedelta, timezone as TZ

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ── Quiet noisy libs ─────────────────────────────────────────────────────────────
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

# ── Constants ────────────────────────────────────────────────────────────────────

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# Regex helpers for normalization
_ALNUM = re.compile(r"[^0-9A-Za-z]")
_EXCEL_EPOCH = datetime(1899, 12, 30)  # same as ds/appds.py + fleet.py
_EXCEL_ESC_RE = re.compile(r"_x([0-9A-Fa-f]{4})_")  # Excel XML escapes (e.g. _x000A_)


# ── Logging / stepper ───────────────────────────────────────────────────────────

def log(msg: str) -> None:
    """UTC timestamped log line, consistent across all scripts."""
    ts = datetime.now(TZ.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[{ts}] {msg}")


class Stepper:
    """
    Simple step counter to structure logs:

        STEP = Stepper()
        STEP.step("Load configuration")
        STEP.step("Fetch Excel from Drive")
    """

    def __init__(self) -> None:
        self.i = 0

    def step(self, title: str) -> None:
        self.i += 1
        sep = "─" * max(8, 64 - len(title))
        log(f"STEP {self.i}: {title} {sep}")


# ── Small value / normalization helpers ─────────────────────────────────────────

def _txt(x: Any) -> Optional[str]:
    """
    Normalize any value to stripped string or None.
    Treats NaN / None / '' as None.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    return s or None


def canon_plate(x: Any) -> Optional[str]:
    """
    Normalize plate/WW to lowercase alphanumeric (imm_norm / ww_norm).
    """
    if x is None or pd.isna(x):
        return None
    s = _ALNUM.sub("", str(x))
    s = s.lower()
    return s or None


def canon_vin(x: Any) -> Optional[str]:
    """
    Normalize VIN to uppercase alphanumeric (vin_norm).
    """
    if x is None or pd.isna(x):
        return None
    s = _ALNUM.sub("", str(x))
    s = s.upper()
    return s or None


def _iso_date_from_any(v: Any) -> Optional[str]:
    """
    Convert various date representations to ISO 'YYYY-MM-DD'.

    Handles:
      • datetime / date
      • Excel serials (int/float)
      • Strings in many formats (day-first)

    Returns:
      • ISO date string
      • or None if parsing fails or year is outside [2000, 2035]
        (same behaviour as ds/appds.py & fleet.py).
    """
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None

    # Already date/datetime
    if isinstance(v, date) and not isinstance(v, datetime):
        d = v
    elif isinstance(v, datetime):
        d = v.date()
    # Excel serial (numbers)
    elif isinstance(v, (int, float)) and not pd.isna(v):
        try:
            serial = int(v)
            d = (_EXCEL_EPOCH + timedelta(days=serial)).date() if serial > 0 else None
        except Exception:
            d = None
    else:
        # Free-form string – bias dayfirst=True (Morocco format)
        dts = pd.to_datetime(str(v).strip(), errors="coerce", dayfirst=True)
        d = dts.date() if pd.notna(dts) else None

    # Safety window (you can relax this later if needed)
    if not d or not (2000 <= d.year <= 2035):
        return None

    return d.strftime("%Y-%m-%d")


def _norm_label(s: Any) -> str:
    """
    Normalize a column header / label for comparison:
      • strip → collapse spaces
      • lowercase
      • strip accents
      • remove punctuation
      • collapse spaces again
    Same logic as ds/appds.py & fleet.py strict header match.
    """
    if s is None:
        return ""
    s = " ".join(str(s).strip().split())
    s = s.lower()
    s = "".join(
        c
        for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    s = re.sub(r"[^0-9a-z ]+", "", s)
    s = " ".join(s.split())
    return s


def _sha256_json(obj: Any) -> str:
    """
    Deterministic SHA-256 of compact JSON.
    Used as doc signature (doc_sig / lines_sig).
    """
    payload = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ── Excel XML escape cleanup ────────────────────────────────────────────────────

def _decode_excel_escapes_to_text(s: Any) -> Any:
    """
    Convert Excel XML escapes like '_x000A_' / '_x000D_' / '_x0009_' into readable text.

      • LF/CR/TAB → single space
      • Other codes → Unicode char if printable, space otherwise
      • Collapse whitespace and trim

    Used before writing data to Mongo or UI.
    """
    if not isinstance(s, str):
        return s

    def repl(m):
        code = m.group(1)
        code_lower = code.lower()
        if code_lower in ("000a", "000d", "0009"):  # LF, CR, TAB → space
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
    """
    Apply Excel XML escape cleanup to all object (string) columns.
    """
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].map(_decode_excel_escapes_to_text)
    return df


# ── Google Drive helpers ────────────────────────────────────────────────────────

def make_drive(creds_b64: str):
    """
    Instantiate a Drive v3 client from base64-encoded service account JSON.

    Shared by cp.py / ds/appds.py / fleet.py / parc.py.
    """
    try:
        info = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        log("Drive auth OK")
        return svc
    except Exception as e:
        raise DriveError(f"Drive auth failed: {e}")


def list_folder_files(drive, folder_id: str) -> List[Dict[str, Any]]:
    """
    List all non-trashed files in a Drive folder, newest first.
    """
    q = f"'{folder_id}' in parents and trashed=false"
    fields = "nextPageToken, files(id,name,mimeType,modifiedTime)"
    files: List[Dict[str, Any]] = []
    page_token = None

    while True:
        res = drive.files().list(
            q=q,
            fields=fields,
            orderBy="modifiedTime desc",
            pageSize=1000,
            pageToken=page_token,
        ).execute()
        files.extend(res.get("files", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break

    log(f"Folder listing: {len(files)} files")
    return files


def _looks_like_xlsx(name: str, mime: str) -> bool:
    """
    True if the file is an XLSX (extension + MIME).
    """
    return name.lower().endswith(".xlsx") and mime == XLSX_MIME


def download_xlsx(drive, file_id: str, out_path: str) -> str:
    """
    Download a Drive file by id to local path.
    Raises DriveError if the file is empty.

    Used by DS / Fleet / CP / Parc loaders.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    req = drive.files().get_media(fileId=file_id)

    with open(out_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        raise DriveError("Downloaded XLSX is empty")

    return out_path


# ── Common error classes ────────────────────────────────────────────────────────

class ConfigError(RuntimeError):
    """Configuration problem (missing env vars, etc.)."""
    ...


class DriveError(RuntimeError):
    """Issues while talking to Google Drive (auth, download, etc.)."""
    ...


class TransformError(RuntimeError):
    """Issues while transforming Excel → DataFrame → docs."""
    ...


class UpsertError(RuntimeError):
    """Issues while writing to MongoDB."""
    ...
