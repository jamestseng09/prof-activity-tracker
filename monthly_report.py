import os, json, datetime
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter, defaultdict

# ------------ CONFIG ------------
PROF_SHEET = "PROF_MASTER"
MONTHLY_SNAPSHOT_SHEET = "MONTHLY_SNAPSHOT"
EXEC_SUMMARY_SHEET = "EXEC_SUMMARY"

# Column header names in PROF_MASTER
COL_COUNTRY = "Country"
COL_UNIVERSITY = "University"
COL_MACHINES = "Machines"
COL_STATUS = "activity_status"
COL_DAYS = "days_since_last_pub"

VALID_COUNTRIES = {"Singapore", "Malaysia"}
VALID_STATUSES = ["HIGHLY ACTIVE", "ACTIVE", "STABLE", "DORMANT", "STAGNANT"]

def now_utc_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat()

def month_str(today=None):
    if today is None:
        today = datetime.date.today()
    return today.strftime("%Y-%m")

def load_gspread():
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def safe_int(x, default=999999):
    try:
        return int(x)
    except Exception:
        return default

def normalize(s):
    return (s or "").strip()

def main():
    # --- Connect ---
    gc = load_gspread()
    sh = gc.open_by_key(os.environ["SHEET_ID"])

    prof_ws = sh.worksheet(PROF_SHEET)
    snap_ws = sh.worksheet(MONTHLY_SNAPSHOT_SHEET)
    exec_ws = sh.worksheet(EXEC_SUMMARY_SHEET)

    month = month_str()

    # --- Read PROF_MASTER ---
    profs = prof_ws.get_all_records()  # <-- THIS defines "profs"

    # --- Group rows by country (skip junk/header rows) ---
    by_country = defaultdict(list)

    for p in profs:
        country = normalize(p.get(COL_COUNTRY))

        # skip empty, header-like, or unknown buckets
        if not country or country == COL_COUNTRY:
            continue
        if country not in VALID_COUNTRIES:
            continue

        by_country[country].append(p)

    # --- Build monthly snapshot + exec summary ---
    snapshot_rows = []
    exec_rows = []

    for country, rows in by_country.items():
        total = len(rows)

        # Status counts (only count valid statuses, ignore blanks)
        status_counts = Counter()
        for r in rows:
            st = normalize(r.get(COL_STATUS))
            if st in VALID_STATUSES:
                status_counts[st] += 1

        ha = status_counts.get("HIGHLY ACTIVE", 0)
        a = status_counts.get("ACTIVE", 0)
        s = status_counts.get("STABLE", 0)
        d = status_counts.get("DORMANT", 0)
        stg = status_counts.get("STAGNANT", 0)

        # Top targets: (HIGHLY ACTIVE or ACTIVE) + days_since_last_pub <= 90
        top_targets = 0
        for r in rows:
            st = normalize(r.get(COL_STATUS))
            days = safe_int(r.get(COL_DAYS))
            if st in ("HIGHLY ACTIVE", "ACTIVE") and days <= 90:
                top_targets += 1

        # Top universities (by count)
        uni_counts = Counter()
        for r in rows:
            uni = normalize(r.get(COL_UNIVERSITY))
            if uni:
                uni_counts[uni] += 1
        top_unis_text = " | ".join([u for u, _ in uni_counts.most_common(5)])

        # Top machine signals (split by ";" first, then "," fallback)
        machine_counter = Counter()
        for r in rows:
            machines = normalize(r.get(COL_MACHINES))
            if not machines:
                continue
            # your Machines strings often contain ";" and ","
            parts = [x.strip() for x in machines.replace(";", ",").split(",") if x.strip()]
            machine_counter.update(parts)

        top_machines_text = " | ".join([m for m, _ in machine_counter.most_common(8)])

        summary = (
            f"{month} — {country} academic activity summary: "
            f"{total} professors tracked. "
            f"{ha} highly active and {a} active (priority outreach), {s} stable. "
            f"{d + stg} dormant/stagnant (monitor). "
            f"Top university clusters: {top_unis_text if top_unis_text else 'N/A'}. "
            f"Most frequent equipment signals: {top_machines_text if top_machines_text else 'N/A'}."
        )

        # MONTHLY_SNAPSHOT columns (match your sheet headers)
        snapshot_rows.append([
            month,
            country,
            total,
            ha,
            a,
            s,
            d,
            stg,
            top_targets,
            top_unis_text,
            top_machines_text
        ])

        # EXEC_SUMMARY columns: month, summary_text, created_at_utc
        exec_rows.append([month, summary, now_utc_iso()])

    # --- Write outputs (append history) ---
    if snapshot_rows:
        snap_ws.append_rows(snapshot_rows, value_input_option="USER_ENTERED")

    if exec_rows:
        exec_ws.append_rows(exec_rows, value_input_option="USER_ENTERED")

    print(f"[OK] Monthly report written for {month}. Countries: {list(by_country.keys())}")

if __name__ == "__main__":
    main()
