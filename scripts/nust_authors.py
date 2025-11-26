import os
import time
import json
import sqlite3
import requests as r
from typing import Dict, Any, Iterable, Optional

# ----------------------
# Config
# ----------------------
NUST_INST_ID = os.getenv("NUST_INST_ID", "I101993903")
YEAR = int(os.getenv("YEAR", "2025"))
DB_PATH = os.getenv("DB_PATH", "../nust_research_authors.sqlite3")
BASE = "https://api.openalex.org"
AUTHORS_URL = f"{BASE}/authors"
WORKS_URL = f"{BASE}/works"
UA = os.getenv("OPENALEX_UA", "NUST-ORCID-Collector/1.0 (contact: you@yourdomain.na)")
HEADERS = {"User-Agent": UA, "Accept": "application/json"}

PER_PAGE = 200
SLEEP_BETWEEN = 0.2     
RETRY_MAX = 5
RETRY_BACKOFF = 1.6      

TOPICS_URL = f"{BASE}/topics"
_topic_cache: dict[str, dict] = {}


# ----------------------
# HTTP helpers
# ----------------------
def get_with_retry(url: str, params: Dict[str, Any], headers: Dict[str, str]) -> r.Response:
    """Robust GET with exponential backoff for 403/429/5xx and prints server error text for debugging."""
    delay = 0.5
    attempt = 0
    while True:
        resp = r.get(url, params=params, headers=headers, timeout=60)
        if resp.status_code == 200:
            return resp

        # Helpful debug
        msg = f"[{resp.status_code}] {url}\nParams: {params}\nResp: {resp.text[:500]}"
        if resp.status_code in (403, 404, 422):
            # 403 often due to UA or invalid select/filters; bubble fast but print server message
            raise r.HTTPError(msg, response=resp)
        if resp.status_code in (429, 500, 502, 503, 504):
            attempt += 1
            if attempt > RETRY_MAX:
                raise r.HTTPError(f"Retries exceeded. {msg}", response=resp)
            time.sleep(delay)
            delay *= RETRY_BACKOFF
            continue

        # Other client errors: raise immediately
        raise r.HTTPError(msg, response=resp)
    


# ----------------------
# SQLite setup
# ----------------------
def init_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.executescript("""
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;

    CREATE TABLE IF NOT EXISTS authors (
        author_id TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        orcid TEXT,
        works_count INTEGER DEFAULT 0,
        last_known_institution_id TEXT,
        last_known_institution_name TEXT
    );

    CREATE TABLE IF NOT EXISTS works_2025 (
        work_id TEXT PRIMARY KEY,
        title TEXT,
        publication_year INTEGER,
        doi TEXT
    );

    CREATE TABLE IF NOT EXISTS authorships_2025 (
        work_id TEXT NOT NULL,
        author_id TEXT NOT NULL,
        author_position TEXT,
        is_nust_affiliation INTEGER DEFAULT 0,
        UNIQUE(work_id, author_id)
    );

    CREATE TABLE IF NOT EXISTS topics (
      topic_id TEXT PRIMARY KEY,
      display_name TEXT NOT NULL,
      subfield_id TEXT, subfield TEXT,
      field_id TEXT, field TEXT,
      domain_id TEXT, domain TEXT
    );

    CREATE TABLE IF NOT EXISTS work_topics_2025 (
      work_id TEXT NOT NULL,
      topic_id TEXT NOT NULL,
      score REAL,
      is_primary INTEGER DEFAULT 0,
      UNIQUE(work_id, topic_id)
    );

    -- optional manual mapping table (populate later if desired)
    CREATE TABLE IF NOT EXISTS dept_mappings (
      topic_id TEXT PRIMARY KEY,
      department TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_authors_orcid ON authors(orcid);
    CREATE INDEX IF NOT EXISTS idx_authorships_work ON authorships_2025(work_id);
    CREATE INDEX IF NOT EXISTS idx_authorships_author ON authorships_2025(author_id);
    CREATE INDEX IF NOT EXISTS idx_work_topics_work ON work_topics_2025(work_id);
    CREATE INDEX IF NOT EXISTS idx_work_topics_topic ON work_topics_2025(topic_id);
    """)
    con.commit()
    return con


def get_topic_details(topic_id: str) -> dict:
    """
    Fetch full topic object (field/subfield/domain) if not present on the work payload.
    Caches by topic_id to avoid repeated API calls.
    """
    if topic_id in _topic_cache:
        return _topic_cache[topic_id]

    # topic_id is like "https://openalex.org/T11636" — pass as-is
    params = {"select": "id,display_name,subfield,field,domain"}  # safe on /topics
    resp = get_with_retry(f"{TOPICS_URL}/{topic_id.split('/')[-1]}", params, HEADERS)
    obj = resp.json()

    # Normalize a compact dict with the pieces we need
    details = {
        "id": obj.get("id"),
        "display_name": obj.get("display_name"),
        "subfield": obj.get("subfield") or {},
        "field": obj.get("field") or {},
        "domain": (obj.get("field") or {}).get("domain") or obj.get("domain") or {},
    }
    _topic_cache[topic_id] = details
    return details


# ----------------------
# Upsert helpers
# ----------------------
def upsert_author(cur: sqlite3.Cursor, a: Dict[str, Any]) -> None:
    author_id = a.get("id")
    name = a.get("display_name", "")
    orcid = (a.get("orcid") or "").replace("https://orcid.org/", "")
    works_count = a.get("works_count", 0)
    lkis = a.get("last_known_institutions") or []
    inst_id = lkis[0].get("id") if lkis else None
    inst_name = lkis[0].get("display_name") if lkis else None

    cur.execute("""
        INSERT INTO authors(author_id, display_name, orcid, works_count,
                            last_known_institution_id, last_known_institution_name)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(author_id) DO UPDATE SET
            display_name=excluded.display_name,
            orcid=excluded.orcid,
            works_count=excluded.works_count,
            last_known_institution_id=excluded.last_known_institution_id,
            last_known_institution_name=excluded.last_known_institution_name
    """, (author_id, name, orcid, works_count, inst_id, inst_name))


def upsert_work(cur: sqlite3.Cursor, wk: Dict[str, Any]) -> None:
    wid = wk.get("id")
    title = wk.get("display_name", "")
    year = wk.get("publication_year", None)
    doi = wk.get("doi", None)
    cur.execute("""
        INSERT INTO works_2025(work_id, title, publication_year, doi)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(work_id) DO UPDATE SET
          title=excluded.title,
          publication_year=excluded.publication_year,
          doi=excluded.doi
    """, (wid, title, year, doi))


def upsert_topic(cur: sqlite3.Cursor, t: dict) -> Optional[str]:
    if not t or "id" not in t:
        return None

    tid = t["id"]  # e.g., "https://openalex.org/T11636"
    tdisp = t.get("display_name")

    # Try to read hierarchy directly from the work payload
    sub = t.get("subfield") or {}
    fld = t.get("field") or {}
    dom = t.get("domain") or {}

    # If missing, enrich from /topics/{id} (cached)
    if not (fld and dom):
        try:
            full = get_topic_details(tid)
            # prefer existing values; fill gaps from resolver
            sub = sub or full.get("subfield") or {}
            fld = fld or full.get("field") or {}
            dom = dom or full.get("domain") or {}
            # also upgrade display_name if missing
            tdisp = tdisp or full.get("display_name")
        except r.HTTPError as e:
            # If enrichment fails, continue with what we have
            # (field/domain will remain NULL — acceptable fallback)
            print(f"Warning: topic enrich failed for {tid}: {e}")

    cur.execute("""
        INSERT INTO topics(topic_id, display_name, subfield_id, subfield, field_id, field, domain_id, domain)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(topic_id) DO UPDATE SET
          display_name=COALESCE(excluded.display_name, topics.display_name),
          subfield_id=COALESCE(excluded.subfield_id, topics.subfield_id),
          subfield=COALESCE(excluded.subfield, topics.subfield),
          field_id=COALESCE(excluded.field_id, topics.field_id),
          field=COALESCE(excluded.field, topics.field),
          domain_id=COALESCE(excluded.domain_id, topics.domain_id),
          domain=COALESCE(excluded.domain, topics.domain)
    """, (
        tid, tdisp,
        sub.get("id"), sub.get("display_name"),
        fld.get("id"), fld.get("display_name"),
        dom.get("id"), dom.get("display_name"),
    ))
    return tid



def attach_work_topics(cur: sqlite3.Cursor, work_id: str, wk: Dict[str, Any]) -> None:
    # primary_topic
    pt = wk.get("primary_topic")
    if pt and "id" in pt:
        tid = upsert_topic(cur, pt)
        cur.execute("""
            INSERT OR IGNORE INTO work_topics_2025(work_id, topic_id, score, is_primary)
            VALUES (?, ?, ?, 1)
        """, (work_id, tid, pt.get("score")))

    # other topics
    for t in wk.get("topics") or []:
        if not t or "id" not in t:
            continue
        tid = upsert_topic(cur, t)
        cur.execute("""
            INSERT OR IGNORE INTO work_topics_2025(work_id, topic_id, score, is_primary)
            VALUES (?, ?, ?, 0)
        """, (work_id, tid, t.get("score")))


def attach_authorships(cur: sqlite3.Cursor, work_id: str, wk: Dict[str, Any]) -> None:
    for au in wk.get("authorships", []) or []:
        au_id = (au.get("author") or {}).get("id")
        if not au_id:
            continue
        insts = au.get("institutions") or []
        is_nust = 1 if any(i.get("id") == NUST_INST_ID for i in insts) else 0
        cur.execute("""
            INSERT OR IGNORE INTO authorships_2025(work_id, author_id, author_position, is_nust_affiliation)
            VALUES (?, ?, ?, ?)
        """, (work_id, au_id, au.get("author_position"), is_nust))


# ----------------------
# API paging
# ----------------------
def iter_authors() -> Iterable[Dict[str, Any]]:
    """
    Yield authors where last_known_institutions includes NUST and they have ORCID.
    Request whole nested object for last_known_institutions to avoid select parsing issues.
    """
    params = {
        "filter": f"last_known_institutions.id:{NUST_INST_ID},has_orcid:true",
        "per-page": PER_PAGE,
        "cursor": "*",
        "select": "id,display_name,orcid,works_count,last_known_institutions",
        "sort": "works_count:desc"
    }
    while True:
        resp = get_with_retry(AUTHORS_URL, params, HEADERS)
        data = resp.json()
        results = data.get("results", [])
        for a in results:
            yield a
        nxt = data.get("meta", {}).get("next_cursor")
        if not nxt:
            break
        params["cursor"] = nxt
        time.sleep(SLEEP_BETWEEN)


def iter_author_works_2025(author_id: str) -> Iterable[Dict[str, Any]]:
    """
    Yield works for a single author, limited to 2025 and requiring the work's authorship to include NUST.
    Request nested objects: authorships, primary_topic, topics.
    """
    params = {
        "filter": (
            f"authorships.author.id:{author_id},"
            f"publication_year:{YEAR},"
            f"authorships.institutions.id:{NUST_INST_ID}"
        ),
        "per-page": PER_PAGE,
        "cursor": "*",
        "select": "id,display_name,publication_year,doi,authorships,primary_topic,topics",
        "sort": "cited_by_count:desc"
    }
    while True:
        resp = get_with_retry(WORKS_URL, params, HEADERS)
        data = resp.json()
        for wk in data.get("results", []):
            yield wk
        nxt = data.get("meta", {}).get("next_cursor")
        if not nxt:
            break
        params["cursor"] = nxt
        time.sleep(SLEEP_BETWEEN)


# ----------------------
# Main
# ----------------------
def main():
    print(f"DB: {DB_PATH}\nInstitution: {NUST_INST_ID}\nYear: {YEAR}\nUser-Agent: {UA}")
    con = init_db(DB_PATH)
    cur = con.cursor()

    # 1) Authors
    author_count = 0
    for a in iter_authors():
        upsert_author(cur, a)
        author_count += 1
        if author_count % 500 == 0:
            con.commit()
    con.commit()
    print(f"Authors (NUST + ORCID): {author_count}")

    # 2) Works (2025, NUST-affiliated in authorships)
    seen_work_ids = set()
    work_count = 0
    link_rows = 0
    topic_rows = 0

    cur.execute("SELECT author_id FROM authors;")
    author_ids = [row[0] for row in cur.fetchall()]

    for aid in author_ids:
        try:
            for wk in iter_author_works_2025(aid):
                wid = wk["id"]
                upsert_work(cur, wk)
                attach_authorships(cur, wid, wk)
                attach_work_topics(cur, wid, wk)

                if wid not in seen_work_ids:
                    seen_work_ids.add(wid)
                    work_count += 1

            con.commit()
        except r.HTTPError as e:
            # Print once and continue (a single bad author should not kill the run)
            print(f"Warning: works fetch failed for {aid}\n{e}\n")
            continue

    # Quick counts
    cur.execute("SELECT COUNT(*) FROM authorships_2025;")
    link_rows = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM work_topics_2025;")
    topic_rows = cur.fetchone()[0]

    con.commit()

    # 3) Example: aggregate by topic domain (no department mapping needed)
    print("\nTop topic domains (2025 works):")
    cur.execute("""
        SELECT t.domain, COUNT(DISTINCT wt.work_id) as n_works
        FROM work_topics_2025 wt
        JOIN topics t ON t.topic_id = wt.topic_id
        GROUP BY t.domain
        ORDER BY n_works DESC NULLS LAST
        LIMIT 15;
    """)
    rows = cur.fetchall()
    for dom, n in rows:
        print(f"{(dom or 'Unknown'):25} {n}")

    con.close()
    print(f"\nDone. Unique NUST-affiliated works in {YEAR}: {len(seen_work_ids)}")
    print(f"Authorship links stored: {link_rows}")
    print(f"Work-topic links stored: {topic_rows}")
    print(f"SQLite written to: {DB_PATH}")


if __name__ == "__main__":
    try:
        main()
    except r.HTTPError as e:
        # Surface helpful error text if OpenAlex rejects a param or UA
        print("\nHTTPError while calling OpenAlex:")
        print(str(e))
        print("\nTips:")
        print("- Ensure your User-Agent includes contact info (email) and is not empty.")
        print("- If error mentions 'Invalid query parameters', try temporarily removing 'select' or 'sort'.")
        print("- Corporate proxies or aggressive AV can also inject 403s; try a different network / VPN off.")
        raise
