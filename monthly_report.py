import os, json, datetime
import gspread
from google.oauth2.service_account import Credentials
from collections import Counter, defaultdict

# ------------ CONFIG ------------
PROF_SHEET = "PROF_MASTER"
MONTHLY_SNAPSHOT_SHEET = "MONTHLY_SNAPSHOT"
EXEC_SUMMARY_SHEET = "EXEC_SUMMARY"

# Your statuses in PROF_MASTER column N
STATUSES = ["HIGHLY ACTIVE", "ACTIVE", "STABLE", "DORMANT", "STAGNANT"]

def now_utc_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def month_str_taiwan(today=None):
    # Use today's date (UTC runner), month label is fine (YYYY-MM)
    # If you want Taiwan-month exactly, it still matches monthly cadence.
    if today is None:
        today = datetime.date.today()
    return today.strftime("%Y-%m")

def load_gspread():
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default

def main():
    gc = load_gspread()
    sh = gc.open_by_key(os.environ["SHEET_ID"])

    prof_ws = sh.worksheet(PROF_SHEET)
    snap_ws = sh.worksheet(MONTHLY_SNAPSHOT_SHEET)
    exec_ws = sh.worksheet(EXEC_SUMMARY_SHEET)

    month = month_str_taiwan()

    profs = prof_ws.get_all_records()

    # ---- Aggregate by country (you can later split by MY/SG if needed) ----
    by_country = defaultdict(list)
    for p in profs:
        country = (p.get("Country") or "").strip()
        if country:
            by_country[country].append(p)

    # ---- For each country, compute metrics & write rows ----
    snapshot_rows = []
    exec_rows = []

    for country, rows in by_country.items():
        total = len(rows)
        status_counts = Counter((r.get("activity_status") or "").strip() for r in rows)

        # Top targets = HIGHLY ACTIVE + ACTIVE with recent activity (e.g. days <= 90)
        top_targets = []
        for r in rows:
            st = (r.get("activity_status") or "").strip()
            days = safe_int(r.get("days_since_last_pub"), 999999)
            if st in ["HIGHLY ACTIVE", "ACTIVE"] and days <= 90:
                top_targets.append(r)

        # Top universities (by number of profs in this country)
        uni_counts = Counter((r.get("University") or "").strip() for r in rows if (r.get("University") or "").strip())
        top_unis = [u for u, c in uni_counts.most_common(5) if u]
        top_unis_text = " | ".join(top_unis)

        # Top machine signals (count frequency of machine names in column K)
        machine_counter = Counter()
        for r in rows:
            machines = (r.get("Machines") or "").strip()
            if machines:
                # Split by comma; normalize spaces
                parts = [p.strip() for p in machines.split(",") if p.strip()]
                machine_counter.update(parts)

        top_machines = [m for m, c in machine_counter.most_common(8)]
        top_machines_text = " | ".join(top_machines)

        # Build executive summary (deterministic, but professional)
        ha = status_counts.get("HIGHLY ACTIVE", 0)
        a = status_counts.get("ACTIVE", 0)
        s = status_counts.get("STABLE", 0)
        d = status_counts.get("DORMANT", 0)
        stg = status_counts.get("STAGNANT", 0)

        summary = (
            f"{month} — {country} academic activity summary: "
            f"{total} professors tracked. "
            f"{ha} are highly active and {a} active (priority outreach pool), "
            f"with {s} stable. "
            f"{d + stg} are dormant or stagnant (monitor-only). "
            f"Top university clusters: {top_unis_text if top_unis_text else 'N/A'}. "
            f"Most frequent equipment signals this month: {top_machines_text if top_machines_text else 'N/A'}."
        )

        snapshot_rows.append([
            month,
            country,
            total,
            ha,
            a,
            s,
            d,
            stg,
            len(top_targets),
            top_unis_text,
            top_machines_text,
            ""
        ])

        exec_rows.append([month, summary, now_utc_iso()])

    # ---- Write to MONTHLY_SNAPSHOT (append, don't overwrite history) ----
    if snapshot_rows:
        snap_ws.append_rows(snapshot_rows, value_input_option="USER_ENTERED")

    # ---- Write to EXEC_SUMMARY (append monthly paragraph) ----
    if exec_rows:
        exec_ws.append_rows(exec_rows, value_input_option="USER_ENTERED")

    print(f"[OK] Monthly report written for {month} ({len(snapshot_rows)} country rows).")

if __name__ == "__main__":
    main()
