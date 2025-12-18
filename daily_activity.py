import os, json, requests, datetime
import gspread
from google.oauth2.service_account import Credentials

OPENALEX = "https://api.openalex.org"

def get_author(author_id):
    return requests.get(f"{OPENALEX}/authors/{author_id}", timeout=30).json()

def get_new_works(author_id, since_date):
    url = f"{OPENALEX}/works"
    params = {
        "filter": f"author.id:{author_id},from_publication_date:{since_date}",
        "per_page": 200
    }
    return requests.get(url, params=params, timeout=30).json().get("results", [])

def main():
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(os.environ["SHEET_ID"])
    prof_master = sh.worksheet("PROF_MASTER")
    snapshot_ws = sh.worksheet("SNAPSHOT")
    log_ws = sh.worksheet("DAILY_ACTIVITY_LOG")

    today = datetime.date.today().isoformat()

    profs = prof_master.get_all_records()
    snapshot = {r["prof_id"]: r for r in snapshot_ws.get_all_records()}

    log_rows = []
    new_snapshot = []

    for p in profs:
        pid = p["prof_id"]
        openalex_id = (p.get("openalex_id") or "").strip()
        if not openalex_id:
            continue

        author_id = openalex_id.replace("https://openalex.org/", "")
        last_check = snapshot.get(pid, {}).get("last_check", "1900-01-01")

        works = get_new_works(author_id, last_check)
        new_pubs = len(works)

        titles, links = [], []
        last_pub_date = snapshot.get(pid, {}).get("last_pub_date", "")

        for w in works:
            titles.append(w.get("display_name", ""))
            links.append(w.get("doi") or w.get("id"))
            pub_date = w.get("publication_date")
            if pub_date:
                last_pub_date = max(last_pub_date, pub_date)

        author = get_author(author_id)
        total_cites = int(author.get("cited_by_count", 0))
        prev_cites = int(snapshot.get(pid, {}).get("total_citations", 0))
        cite_delta = max(0, total_cites - prev_cites)

        if new_pubs > 0 or cite_delta > 0:
            log_rows.append([
                today,
                pid,
                new_pubs,
                " | ".join(titles),
                " | ".join(links),
                cite_delta,
                "OpenAlex"
            ])

        new_snapshot.append([pid, today, author.get("works_count", ""), total_cites, last_pub_date])

    if log_rows:
        log_ws.append_rows(log_rows, value_input_option="USER_ENTERED")

    snapshot_ws.clear()
    snapshot_ws.append_row(["prof_id","last_check","total_works","total_citations","last_pub_date"])
    if new_snapshot:
        snapshot_ws.append_rows(new_snapshot, value_input_option="USER_ENTERED")

if __name__ == "__main__":
    main()
