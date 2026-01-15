import os, json, datetime
import gspread
from google.oauth2.service_account import Credentials

def main():
    # --- Auth ---
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(os.environ["SHEET_ID"])

    ws_master = sh.worksheet("INST_MASTER")
    ws_snapshot = sh.worksheet("INST_DAILY_SNAPSHOT")

    rows = ws_master.get_all_values()
    header = rows[0]
    data = rows[1:]

    def idx(col):
        return header.index(col)

    # --- Column mapping from INST_MASTER ---
    SCIENTIST_ID = idx("scientist_id")
    TOTAL_WORKS = idx("total_works")
    TOTAL_CITATIONS = idx("total_citations")
    LAST_PUB_DATE = idx("last_pub_date")

    today = datetime.date.today().isoformat()

    out = []

    for r in data:
        if len(r) <= SCIENTIST_ID:
            continue

        scientist_id = r[SCIENTIST_ID].strip()
        if not scientist_id:
            continue

        out.append([
            scientist_id,
            today,                                 # last_check
            r[TOTAL_WORKS] if len(r) > TOTAL_WORKS else "",
            r[TOTAL_CITATIONS] if len(r) > TOTAL_CITATIONS else "",
            r[LAST_PUB_DATE] if len(r) > LAST_PUB_DATE else "",
        ])

    # --- Write header if empty ---
    existing = ws_snapshot.get_all_values()
    if len(existing) == 0:
        ws_snapshot.update(values=[[
            "scientist_id",
            "last_check",
            "total_works",
            "total_citations",
            "last_pub_date",
        ]])

    # --- Append snapshot ---
    if out:
        ws_snapshot.append_rows(out, value_input_option="USER_ENTERED")

if __name__ == "__main__":
    main()
