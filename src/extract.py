from __future__ import annotations

import os
import json
from typing import List

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

from transform import normalize_column_names  # 👈 thêm

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
CHECKPOINT_FILE = "checkpoint.json"


# =========================
# 🔐 Google Sheets Service
# =========================
def get_sheets_service():
    credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    creds = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=creds)


# =========================
# 📄 Sheet Metadata
# =========================
def list_sheet_names(spreadsheet_id: str) -> list[str]:
    service = get_sheets_service()
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = metadata.get("sheets", [])
    return [s["properties"]["title"] for s in sheets]


# =========================
# 🧱 Helpers
# =========================
def make_unique_headers(headers: list[str]) -> list[str]:
    seen = {}
    result = []

    for i, h in enumerate(headers):
        name = str(h).strip() if str(h).strip() else f"unnamed_{i+1}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        result.append(name)

    return result


def _normalize_table(values: List[List[str]], header_idx: int) -> pd.DataFrame:
    raw_headers = values[header_idx]
    rows = values[header_idx + 1 :]

    max_cols = max(len(raw_headers), *(len(r) for r in rows)) if rows else len(raw_headers)

    padded_headers = raw_headers + [
        f"unnamed_{i+1}" for i in range(len(raw_headers), max_cols)
    ]
    headers = make_unique_headers(padded_headers)

    normalized_rows = []
    for row in rows:
        if len(row) < max_cols:
            row = row + [""] * (max_cols - len(row))
        elif len(row) > max_cols:
            row = row[:max_cols]
        normalized_rows.append(row)

    df = pd.DataFrame(normalized_rows, columns=headers)

    # drop empty rows
    df = df.dropna(how="all")
    df = df[(df.astype(str).apply(lambda s: s.str.strip()).ne("").any(axis=1))]

    return df.reset_index(drop=True)


# =========================
# 📥 Extract Raw Data
# =========================
def read_sheet_raw(spreadsheet_id: str, range_name: str) -> List[List[str]]:
    service = get_sheets_service()
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return result.get("values", [])


# =========================
# 🧠 Checkpoint (Incremental)
# =========================
def load_checkpoint(sheet_name: str) -> str:
    if not os.path.exists(CHECKPOINT_FILE):
        return "1970-01-01 00:00:00"

    with open(CHECKPOINT_FILE, "r") as f:
        data = json.load(f)

    return data.get(sheet_name, "1970-01-01 00:00:00")


def save_checkpoint(sheet_name: str, df: pd.DataFrame):
    if df.empty or "updated_at" not in df.columns:
        return

    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
    max_time = df["updated_at"].max()

    if pd.isna(max_time):
        return

    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
    else:
        data = {}

    data[sheet_name] = str(max_time)

    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)


def filter_incremental(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    if "updated_at" not in df.columns:
        raise ValueError(f"Sheet {sheet_name} thiếu cột updated_at")

    last_updated = load_checkpoint(sheet_name)

    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
    last_time = pd.to_datetime(last_updated, utc=True)

    # loại date rác
    df = df[df["updated_at"].notna()]
    df = df[df["updated_at"] > pd.Timestamp("2000-01-01", tz="UTC")]

    df = df[df["updated_at"] > last_time]

    return df


# =========================
# 🚀 Main Extract Function
# =========================
def read_sheet_by_header_keyword(
    spreadsheet_id: str,
    sheet_name: str,
    header_first_cell: str,
    range_name: str | None = None,
    incremental: bool = False,
) -> pd.DataFrame:
    if range_name is None:
        range_name = f"{sheet_name}!A:ZZ"

    values = read_sheet_raw(spreadsheet_id, range_name)
    if not values:
        return pd.DataFrame()

    # =========================
    # 🔍 Detect header (robust)
    # =========================
    header_idx = None
    for i, row in enumerate(values):
        row_upper = [str(c).strip().upper() for c in row]

        if header_first_cell.strip().upper() in row_upper:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(
            f"Không tìm thấy header bắt đầu bằng '{header_first_cell}' trong sheet {sheet_name}"
        )

    df = _normalize_table(values, header_idx)

    # =========================
    # 🔥 FIX HEADER LỆCH (multi-header)
    # =========================
    # nếu row đầu chứa updated_at → promote lên header
    first_row_values = [str(x).strip().lower() for x in df.iloc[0].values]

    if "updated_at" in first_row_values:
        df.columns = df.iloc[0]
        df = df[1:].reset_index(drop=True)

    # =========================
    # 🔥 NORMALIZE COLUMN NAME
    # =========================
    df = normalize_column_names(df)

    # =========================
    # 🔥 INCREMENTAL
    # =========================
    if incremental:
        df = filter_incremental(df, sheet_name)

    return df