import os, json, datetime
import gspread
from google.oauth2.service_account import Credentials

def month_end_date(d: datetime.date) -> datetime.date:
    # month end of the previous month if run on the 1st
    first = d.replace(day=1)
    prev_month_last = first - datetime.timedelta(days=1)
    return prev_month_last

def main():
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(os.environ["SHEET_ID"])
    ws_master = sh.worksheet("INST_MASTER")
    ws_log = sh.worksheet("MONTHLY_STATUS_LOG")

    rows = ws_master.get_all_values()
    header = rows[0]
    data = rows[1:]

    # Required columns in PROF_MASTER (by header name)
    def idx(col): return header.index(col)

    # Adjust these header names if your sheet uses slightly different names
    PROF_ID = idx("prof_id")
    COUNTRY = idx("Country")
    UNIV = idx("University")
    DEPT = idx("Department / Centre")
    NAME = idx("Professor Name")
    CAT = idx("Research Category Code")
    MACH = idx("Machines")
    STATUS = idx("activity_status")
    LASTPUB = idx("last_pub_date")
    SOURCE = idx("Google Scholar/Research Gate URL")

    run_day = datetime.date.today()
    m_end = month_end_date(run_day).isoformat()

    out = []
    for r in data:
        prof_id = r[PROF_ID].strip() if len(r) > PROF_ID else ""
        if not prof_id:
            continue
        out.append([
            m_end,
            r[PROF_ID],
            r[COUNTRY],
            r[UNIV],
            r[DEPT],
            r[NAME],
            r[CAT],
            r[MACH],
            r[STATUS],
            r[LASTPUB],
            r[SOURCE],
        ])

    # If sheet is empty, write header first
    existing = ws_log.get_all_values()
    if len(existing) == 0:
        ws_log.update(values=[[
            "month_end","prof_id","country","university","department","professor_name",
            "research_category_code","machines","activity_status","last_pub_date","source"
        ]])

    # Append snapshot rows
    ws_log.append_rows(out, value_input_option="USER_ENTERED")

if __name__ == "__main__":
    main()
