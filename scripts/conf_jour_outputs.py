import os
import time
import json
import csv
import sqlite3
from typing import Dict, Any, Iterable
import requests as r

import argparse

# ----------------------
# Config
# ----------------------
NUST_INST_ID = os.getenv("NUST_INST_ID", "I101993903")  # NUST OpenAlex institution id
YEAR_START = int(os.getenv("YEAR_START", "2023"))
YEAR_END   = int(os.getenv("YEAR_END",   "2025"))
DB_PATH    = os.getenv("DB_PATH", "../db_csv/nust_conf_jour_output/nust_research.sqlite3")

BASE = "https://api.openalex.org"
AUTHORS_URL = f"{BASE}/authors"
WORKS_URL   = f"{BASE}/works"

UA = os.getenv("OPENALEX_UA", "NUST-ORCID-Collector/1.0 (contact: 223118001@nust.na)")
HEADERS = {"User-Agent": UA, "Accept": "application/json"}

PER_PAGE = 200
PAGE_SLEEP = 0.2
RETRY_MAX = 5
RETRY_BACKOFF = 1.6

# ----------------------
# Helper function
# ----------------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        default="../db_csv/nust_conf_jour_output/counts_per_person_year.csv",
        help="Path to output CSV (relative to repo root).",
    )
    parser.add_argument(
        "--db-path",
        default="../db_csv/nust_conf_jour_output/nust_research.sqlite3",
        help="Path to SQLite DB file.",
    )
    return parser.parse_args()


# ----------------------
# HTTP helper with retry
# ----------------------
def get_with_retry(url: str, params: Dict[str, Any]) -> r.Response:
    delay = 0.6
    tries = 0
    while True:
        resp = r.get(url, params=params, headers=HEADERS, timeout=60)
        if resp.status_code == 200:
            return resp
        # Helpful diagnostics
        msg = f"[{resp.status_code}] GET {url}\nParams: {params}\nBody: {resp.text[:500]}"
        if resp.status_code in (403, 404, 422):
            raise r.HTTPError(msg, response=resp)
        if resp.status_code in (429, 500, 502, 503, 504):
            tries += 1
            if tries > RETRY_MAX:
                raise r.HTTPError(f"Retries exceeded. {msg}", response=resp)
            time.sleep(delay)
            delay *= RETRY_BACKOFF
            continue
        raise r.HTTPError(msg, response=resp)

# ----------------------
# SQLite setup
# ----------------------
def init_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
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

    CREATE TABLE IF NOT EXISTS works (
      work_id TEXT PRIMARY KEY,
      title TEXT,
      publication_year INTEGER,
      doi TEXT,
      venue_type TEXT   -- 'journal' | 'conference' | 'other'
    );

    -- Fact table: one row per author-work with year + venue_type (for Power BI pivoting)
    CREATE TABLE IF NOT EXISTS fact_authorships_works (
      author_id TEXT NOT NULL,
      work_id   TEXT NOT NULL,
      year      INTEGER,
      venue_type TEXT,
      UNIQUE(author_id, work_id)
    );

    CREATE INDEX IF NOT EXISTS idx_author_orcid ON authors(orcid);
    CREATE INDEX IF NOT EXISTS idx_fact_author_year ON fact_authorships_works(author_id, year);
    CREATE INDEX IF NOT EXISTS idx_fact_venue ON fact_authorships_works(venue_type);

    -- Helpful view for Power BI
    CREATE VIEW IF NOT EXISTS v_counts_per_person_year AS
    SELECT
      a.author_id,
      a.display_name,
      f.year,
      SUM(CASE WHEN f.venue_type = 'journal'    THEN 1 ELSE 0 END) AS journal_count,
      SUM(CASE WHEN f.venue_type = 'conference' THEN 1 ELSE 0 END) AS conference_count,
      COUNT(*) AS total_outputs
    FROM fact_authorships_works f
    JOIN authors a ON a.author_id = f.author_id
    WHERE f.year IS NOT NULL
    GROUP BY a.author_id, a.display_name, f.year;
    """)
    con.commit()
    return con

# ----------------------
# Venue classification
# ----------------------
def classify_venue_type(work: Dict[str, Any]) -> str:
    """
    Prefer primary_location.source.type ('journal' | 'conference').
    Fallback to work.type / type_crossref if needed.
    """
    pl = work.get("primary_location") or {}
    src = pl.get("source") or {}
    t = (src.get("type") or "").lower().strip()
    if t in ("journal", "conference"):
        return t

    # Fallbacks
    wt  = (work.get("type") or "").lower()
    tcr = (work.get("type_crossref") or "").lower()
    if "proceedings" in tcr or "proceedings" in wt or "conference" in tcr or "conference" in wt:
        return "conference"
    if "journal" in tcr or "journal" in wt:
        return "journal"
    return "other"

# ----------------------
# API iteration
# ----------------------
def iter_authors() -> Iterable[Dict[str, Any]]:
    """
    Authors with ORCID whose last_known_institutions includes NUST.
    Request full nested last_known_institutions to avoid dotted select 403s.
    """
    params = {
        "filter": f"last_known_institutions.id:{NUST_INST_ID},has_orcid:true",
        "per-page": PER_PAGE,
        "cursor": "*",
        "select": "id,display_name,orcid,works_count,last_known_institutions",
        "sort": "works_count:desc"
    }
    while True:
        resp = get_with_retry(AUTHORS_URL, params)
        data = resp.json()
        for a in data.get("results", []):
            yield a
        nxt = data.get("meta", {}).get("next_cursor")
        if not nxt:
            break
        params["cursor"] = nxt
        time.sleep(PAGE_SLEEP)

def iter_author_works_year(author_id: str, year: int) -> Iterable[Dict[str, Any]]:
    """
    Works for a single author in a single year AND requiring the work to include NUST in authorships.
    """
    params = {
        "filter": (
            f"authorships.author.id:{author_id},"
            f"publication_year:{year},"
            f"authorships.institutions.id:{NUST_INST_ID}"
        ),
        "per-page": PER_PAGE,
        "cursor": "*",
        # Request whole nested objects to avoid select parsing issues
        "select": "id,display_name,publication_year,doi,authorships,primary_location,type,type_crossref",
        "sort": "cited_by_count:desc"
    }
    while True:
        resp = get_with_retry(WORKS_URL, params)
        data = resp.json()
        for w in data.get("results", []):
            yield w
        nxt = data.get("meta", {}).get("next_cursor")
        if not nxt:
            break
        params["cursor"] = nxt
        time.sleep(PAGE_SLEEP)

# ----------------------
# Upserts
# ----------------------
def upsert_author(cur: sqlite3.Cursor, a: Dict[str, Any]) -> None:
    lkis = a.get("last_known_institutions") or []
    inst_id = lkis[0].get("id") if lkis else None
    inst_nm = lkis[0].get("display_name") if lkis else None
    cur.execute("""
      INSERT INTO authors(author_id, display_name, orcid, works_count, last_known_institution_id, last_known_institution_name)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(author_id) DO UPDATE SET
        display_name=excluded.display_name,
        orcid=excluded.orcid,
        works_count=excluded.works_count,
        last_known_institution_id=excluded.last_known_institution_id,
        last_known_institution_name=excluded.last_known_institution_name
    """, (
        a.get("id"),
        a.get("display_name",""),
        (a.get("orcid") or "").replace("https://orcid.org/",""),
        a.get("works_count",0),
        inst_id, inst_nm
    ))

def upsert_work(cur: sqlite3.Cursor, w: Dict[str, Any]) -> None:
    cur.execute("""
      INSERT INTO works(work_id, title, publication_year, doi, venue_type)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(work_id) DO UPDATE SET
        title=excluded.title,
        publication_year=excluded.publication_year,
        doi=excluded.doi,
        venue_type=excluded.venue_type
    """, (
        w["id"],
        w.get("display_name",""),
        w.get("publication_year"),
        w.get("doi"),
        classify_venue_type(w),
    ))

def upsert_fact(cur: sqlite3.Cursor, w: Dict[str, Any]) -> None:
    wid  = w["id"]
    year = w.get("publication_year")
    vt   = classify_venue_type(w)
    for au in w.get("authorships") or []:
        aid = (au.get("author") or {}).get("id")
        if not aid:
            continue
        cur.execute("""
          INSERT OR IGNORE INTO fact_authorships_works(author_id, work_id, year, venue_type)
          VALUES (?, ?, ?, ?)
        """, (aid, wid, year, vt))

# ----------------------
# Export view to CSV
# ----------------------
def export_counts_csv(db_path: str, out_csv: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    rows = cur.execute("SELECT * FROM v_counts_per_person_year").fetchall()
    cols = [c[1] for c in cur.execute("PRAGMA table_info('v_counts_per_person_year')")]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    con.close()
    print(f"CSV exported: {out_csv}")

# ----------------------
# Main
# ----------------------
def main():
    args = parse_args()
    db_path = args.db_path or DB_PATH
    out_csv = args.out


    print(f"DB: {DB_PATH}\nInstitution: {NUST_INST_ID}\nYears: {YEAR_START}..{YEAR_END}\nUA: {UA}")
    con = init_db(DB_PATH)
    cur = con.cursor()

    # 1) Authors (NUST + ORCID)
    author_ids = []
    count_authors = 0
    for a in iter_authors():
        upsert_author(cur, a)
        author_ids.append(a["id"])
        count_authors += 1
        if count_authors % 500 == 0:
            con.commit()
    con.commit()
    print(f"Authors stored: {count_authors}")

    # 2) Works by author, per year (NUST-affiliated works only)
    seen_works = set()
    for aid in author_ids:
        for yr in range(YEAR_START, YEAR_END + 1):
            try:
                for w in iter_author_works_year(aid, yr):
                    wid = w["id"]
                    upsert_work(cur, w)
                    upsert_fact(cur, w)
                    if wid not in seen_works:
                        seen_works.add(wid)
                con.commit()
            except r.HTTPError as e:
                print(f"Warning: fetch failed for author {aid} year {yr}\n{e}\n")
                continue

    print(f"Unique works stored: {len(seen_works)}")

    # 3) Export counts for Power BI
    con.commit()
    con.close()
    export_counts_csv(db_path, out_csv=out_csv)

if __name__ == "__main__":
    try:
        main()
    except r.HTTPError as e:
        print("\nHTTPError calling OpenAlex:")
        print(str(e))
        print("\nChecklist:")
        print("- Ensure OPENALEX_UA includes a contact email (required).")
        print("- If error says 'Invalid query parameters', temporarily remove 'select' or 'sort' and retry.")
        print("- Some networks/proxies cause 403 — try another network if needed.")
        raise
