import os, json, requests, datetime
import gspread
from google.oauth2.service_account import Credentials

OPENALEX = "https://api.openalex.org"

# Strongly recommended by OpenAlex etiquette + helps reduce blocks
OPENALEX_EMAIL = os.environ.get("OPENALEX_MAILTO", "")
HEADERS = {
    "User-Agent": f"prof-activity-tracker/1.0 (mailto:{OPENALEX_EMAIL})" if OPENALEX_EMAIL else "prof-activity-tracker/1.0"
}

def safe_get_json(url, params=None):
    """Fetch JSON safely. Returns dict or None."""
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    except requests.RequestException as e:
        print(f"[WARN] Request error: {url} -> {e}")
        return None

    if r.status_code != 200:
        # Print a short snippet to diagnose HTML/Cloudflare/etc.
        snippet = (r.text or "")[:200].replace("\n", " ")
        print(f"[WARN] Non-200 from OpenAlex: {r.status_code} url={url} params={params} body={snippet}")
        return None

    try:
        return r.json()
    except Exception:
        snippet = (r.text or "")[:200].replace("\n", " ")
        print(f"[WARN] Non-JSON response from OpenAlex url={url} body={snippet}")
        return None

def normalize_openalex_author_id(openalex_id_raw: str) -> str:
    """Accepts 'A123', 'https://openalex.org/A123' and returns 'A123'."""
    if not openalex_id_raw:
        return ""
    s = openalex_id_raw.strip()
    s = s.replace("https://openalex.org/", "").replace("http://openalex.org/", "")
    # Some people paste full API URL too:
    s = s.replace("https://api.openalex.org/authors/", "").replace("http://api.openalex.org/authors/", "")
    return s

def get_author(author_id):
    if not author_id or not author_id.startswith("A"):
        return None
    url = f"{OPENALEX}/authors/{author_id}"
    return safe_get_json(url)

def get_new_works(author_id, since_date):
    if not author_id or not author_id.startswith("A"):
        return []
    url = f"{OPENALEX}/works"
    params = {
        # IMPORTANT: correct filter for works by author
        "filter": f"authorships.author.id:{author_id},from_publication_date:{since_date}",
        "per_page": 200
    }
    data = safe_get_json(url, params=params)
    if not data:
        return []
    return data.get("results", [])

def main():
    creds_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(os.environ["SHEET_ID"])
    prof_master = sh.worksheet("PROF_MASTER")
    snapshot_ws = sh.worksheet("DAILY_SNAPSHOT")
    log_ws = sh.worksheet("DAILY_ACTIVITY_LOG")

    today = datetime.date.today().isoformat()

    profs = prof_master.get_all_records()
    snapshot = {r["prof_id"]: r for r in snapshot_ws.get_all_records()}

    log_rows = []
    new_snapshot = []

    for p in profs:
        pid = p.get("prof_id", "")
        openalex_id_raw = (p.get("openalex_id") or "").strip()
        if not pid or not openalex_id_raw:
            continue

        author_id = normalize_openalex_author_id(openalex_id_raw)
        if not author_id:
            continue

        last_check = snapshot.get(pid, {}).get("last_check", "1900-01-01")
        works = get_new_works(author_id, last_check)

        new_pubs = len(works)
        titles, links = [], []
        last_pub_date = snapshot.get(pid, {}).get("last_pub_date", "") or ""

        for w in works:
            titles.append(w.get("display_name", ""))
            links.append(w.get("doi") or w.get("id") or "")
            pub_date = w.get("publication_date")
            if pub_date:
                # ISO date strings compare safely lexicographically
                last_pub_date = max(last_pub_date, pub_date)

        author = get_author(author_id)
        if not author:
            # Still write snapshot row with what we have (keeps pipeline moving)
            prev_cites = int(snapshot.get(pid, {}).get("total_citations", 0) or 0)
            new_snapshot.append([pid, today, "", prev_cites, last_pub_date])
            continue

        total_cites = int(author.get("cited_by_count", 0) or 0)
        prev_cites = int(snapshot.get(pid, {}).get("total_citations", 0) or 0)
        cite_delta = max(0, total_cites - prev_cites)

        if new_pubs > 0 or cite_delta > 0:
            log_rows.append([
                today,
                pid,
                new_pubs,
                " | ".join([t for t in titles if t]),
                " | ".join([l for l in links if l]),
                cite_delta,
                "OpenAlex"
            ])

        new_snapshot.append([pid, today, author.get("works_count", ""), total_cites, last_pub_date])

    if log_rows:
        log_ws.append_rows(log_rows, value_input_option="USER_ENTERED")

    # Keep your existing behavior
    snapshot_ws.clear()
    snapshot_ws.append_row(["prof_id","last_check","total_works","total_citations","last_pub_date"])
    if new_snapshot:
        snapshot_ws.append_rows(new_snapshot, value_input_option="USER_ENTERED")

if __name__ == "__main__":
    main()
