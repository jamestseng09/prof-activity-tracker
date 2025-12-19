import os
import json
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials


TAIPEI_TZ = ZoneInfo("Asia/Taipei")

SOURCE_SHEET_NAME = "INST_MASTER"
SNAPSHOT_SHEET_NAME = "INST_MONTHLY_SNAPSHOT"


def last_day_of_previous_month(tz=TAIPEI_TZ) -> date:
    """Return last day of previous month in the given timezone."""
    now = datetime.now(tz)
    first_of_this_month = now.replace(day=1).date()
    return first_of_this_month - timedelta(days=1)


def get_env_json(name: str) -> dict:
    raw = os.environ.get(name, "").strip()
    if not raw:
        raise RuntimeError(f"Missing environment variable: {name}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Env var {name} is not valid JSON. "
            f"Did you paste the full service account JSON into the GitHub secret?"
        ) from e


def open_gspread_client() -> gspread.Client:
    creds_info = get_env_json("GOOGLE_SERVICE_ACCOUNT_JSON")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


def ensure_worksheet(sh: gspread.Spreadsheet, title: str, rows: int = 2000, cols: int = 30) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def normalize_header(s: str) -> str:
    return (s or "").strip()


def main():
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("Missing environment variable: SHEET_ID")

    gc = open_gspread_client()
    sh = gc.open_by_key(sheet_id)

    ws_src = sh.worksheet(SOURCE_SHEET_NAME)
    records = ws_src.get_all_records()  # list[dict] keyed by header

    ws_snap = ensure_worksheet(sh, SNAPSHOT_SHEET_NAME)

    # Snapshot header (stable, sales-friendly)
    snapshot_header = [
        "snapshot_date",
        "inst_id",
        "Country",
        "Institution",
        "Division / Centre",
        "Key Contact (Name)",
        "Key Contact (Title)",
        "Contact Role",
        "Decision Level",
        "Buying Likelihood (6-18m)",
        "Procurement Signals (What to watch)",
        "Research / Capability Keywords",
        "Research Category Code",
        "Machines",
        "Official URL",
        "News / Updates URL",
        "Last Signal Date",
        "Activity Status",
        "Notes",
    ]

    # Write header if sheet is empty
    existing = ws_snap.get_all_values()
    if not existing:
        ws_snap.append_row(snapshot_header, value_input_option="RAW")
    else:
        # If header row exists but differs, we still keep the current sheet as-is.
        # We will append rows in the order of snapshot_header to avoid breaking later charts.
        pass

    snap_date = last_day_of_previous_month()
    snap_date_str = snap_date.isoformat()

    # Helper to read from records with slight header variations
    def pick(rec: dict, key: str) -> str:
        # exact match first
        if key in rec:
            return str(rec.get(key, "")).strip()
        # try relaxed matches
        k_norm = normalize_header(key).lower()
        for rk in rec.keys():
            if normalize_header(rk).lower() == k_norm:
                return str(rec.get(rk, "")).strip()
        return ""

    rows_to_append = []
    for rec in records:
        inst_id = pick(rec, "inst_id")
        # Skip blank lines / incomplete rows
        if not inst_id:
            continue

        row = [
            snap_date_str,
            inst_id,
            pick(rec, "Country"),
            pick(rec, "Institution"),
            pick(rec, "Division / Centre"),
            pick(rec, "Key Contact (Name)"),
            pick(rec, "Key Contact (Title)"),
            pick(rec, "Contact Role"),
            pick(rec, "Decision Level"),
            pick(rec, "Buying Likelihood (6-18m)"),
            pick(rec, "Procurement Signals (What to watch)"),
            pick(rec, "Research / Capability Keywords"),
            pick(rec, "Research Category Code"),
            pick(rec, "Machines"),
            pick(rec, "Official URL"),
            pick(rec, "News / Updates URL"),
            pick(rec, "Last Signal Date"),
            pick(rec, "Activity Status"),
            pick(rec, "Notes"),
        ]
        rows_to_append.append(row)

    if rows_to_append:
        ws_snap.append_rows(rows_to_append, value_input_option="RAW")

    print(f"✅ Appended {len(rows_to_append)} institution snapshot rows for {snap_date_str}.")


if __name__ == "__main__":
    main()
