"""
TRAPP2 CEO / Leadership Tracker
================================

Builds a database of executives and board members across all tickers in
data/company/*.json, with cross-references:
   - "Where is this person now?"
   - "Where have they been before?" (Wikidata employer/position history)
   - "Which other companies share this person as a board member?"

Output:
   data/leadership/_all_people.json    — flat list of all people across all companies
   data/leadership/by_ticker/*.json    — quick lookup per ticker
   data/leadership/by_person/*.json    — slugified-name file with full career
   data/leadership/_manifest.json      — index + run stats

The "career history" comes from Wikidata's P39 (position held) and P108
(employer) properties on the person's entity. We resolve the person's QID
by their name (matched to the company's QID context), then pull their
career data.

Runs in the nightly workflow after fetch_company_facts.py.
"""

import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# Repo owner — resolved at runtime so the pipeline follows the repos to any
# GitHub account. Actions sets GITHUB_REPOSITORY_OWNER automatically;
# VALUATIO_OWNER (repo variable/env) overrides; TheMostLocal is the fallback.
_GH_OWNER = (__import__("os").environ.get("VALUATIO_OWNER")
             or __import__("os").environ.get("GITHUB_REPOSITORY_OWNER")
             or "TheMostLocal").strip()

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
UA = f"TRAPP2/1.0 (https://github.com/{_GH_OWNER}/TRAPP2; financial-data-app) python-requests"
HEADERS = {"User-Agent": UA, "Accept": "application/sparql-results+json"}

ROOT = Path(__file__).resolve().parent.parent
COMPANY_DIR = ROOT / "data" / "company"
LEADERSHIP_DIR = ROOT / "data" / "leadership"
BY_TICKER_DIR = LEADERSHIP_DIR / "by_ticker"
BY_PERSON_DIR = LEADERSHIP_DIR / "by_person"
LEADERSHIP_DIR.mkdir(parents=True, exist_ok=True)
BY_TICKER_DIR.mkdir(parents=True, exist_ok=True)
BY_PERSON_DIR.mkdir(parents=True, exist_ok=True)


def log(msg):
    print(f"[ceo-tracker] {msg}", flush=True)


def slugify(name):
    """Convert a name to a safe filename: 'Tim Cook' → 'tim-cook'"""
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def sparql_query(query, retries=4):
    """Wikidata SPARQL with hardened retry. Same logic as fetch_company_facts.py."""
    for attempt in range(retries + 1):
        try:
            r = requests.get(
                WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers=HEADERS,
                timeout=45,
            )
            if r.status_code == 429:
                wait = max(int(r.headers.get("Retry-After", 0)), 5 * (attempt + 1))
                print(f"[ceo-tracker]   rate-limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code in (500, 502, 503, 504):
                wait = 3 * (attempt + 1)
                print(f"[ceo-tracker]   server error {r.status_code}, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                return None
            return r.json()
        except requests.RequestException as e:
            wait = 2 * (attempt + 1)
            print(f"[ceo-tracker]   request failed: {e}, waiting {wait}s")
            time.sleep(wait)
        except ValueError:
            return None
    return None


def find_person_qid(name, company_qid=None):
    """
    Resolve a person name to a Wikidata QID. Optionally constrain by company QID
    (the person should have a P39/P108 link to that company) to disambiguate
    common names. Returns the QID or None.
    """
    # Strategy: search for entity with label = name AND instance-of human (Q5).
    # If company_qid given, prefer matches that share an employer link with that company.
    if company_qid:
        query = f"""
        SELECT ?person WHERE {{
          ?person rdfs:label "{name}"@en .
          ?person wdt:P31 wd:Q5 .
          {{ ?person wdt:P108 wd:{company_qid} }} UNION
          {{ ?person wdt:P39 ?pos . ?pos wdt:P642 wd:{company_qid} }} UNION
          {{ ?person wdt:P3320 wd:{company_qid} }} UNION
          {{ wd:{company_qid} wdt:P169 ?person }} UNION
          {{ wd:{company_qid} wdt:P488 ?person }} UNION
          {{ wd:{company_qid} wdt:P3320 ?person }}
        }}
        LIMIT 1
        """
    else:
        # No company anchor — fall back to a label-only match.
        query = f"""
        SELECT ?person (COUNT(?sitelink) AS ?fame) WHERE {{
          ?person rdfs:label "{name}"@en .
          ?person wdt:P31 wd:Q5 .
          OPTIONAL {{ ?sitelink schema:about ?person }}
        }}
        GROUP BY ?person
        ORDER BY DESC(?fame)
        LIMIT 1
        """
    result = sparql_query(query)
    if not result:
        return None
    bindings = result.get("results", {}).get("bindings", [])
    if not bindings:
        return None
    return bindings[0]["person"]["value"].rsplit("/", 1)[-1]


def fetch_person_career(person_qid, person_name):
    """
    Pull a person's career history from Wikidata:
      - P108  employer (companies they have worked for)
      - P39   positions held (chairperson, CEO, board member, etc.)
      - P569  date of birth
      - P19   place of birth
      - P106  occupation
      - P69   educated at
    """
    query = f"""
    SELECT
      (SAMPLE(?dob) AS ?dateOfBirth)
      (SAMPLE(?birthPlaceLabel) AS ?birthPlace)
      (SAMPLE(?image) AS ?image)
      (GROUP_CONCAT(DISTINCT ?occupationLabel; separator="|") AS ?occupations)
      (GROUP_CONCAT(DISTINCT ?educatedAtLabel; separator="|") AS ?education)
      (GROUP_CONCAT(DISTINCT CONCAT(STR(?employerLabel), "::", COALESCE(STR(?employerStart), ""), "::", COALESCE(STR(?employerEnd), "")); separator="|") AS ?employers)
      (GROUP_CONCAT(DISTINCT CONCAT(STR(?positionLabel), "::", COALESCE(STR(?positionOfLabel), ""), "::", COALESCE(STR(?positionStart), ""), "::", COALESCE(STR(?positionEnd), "")); separator="|") AS ?positions)
    WHERE {{
      BIND(wd:{person_qid} AS ?person)
      OPTIONAL {{ ?person wdt:P569 ?dob }}
      OPTIONAL {{ ?person wdt:P19  ?birthPlace .   ?birthPlace rdfs:label ?birthPlaceLabel . FILTER(LANG(?birthPlaceLabel) = "en") }}
      OPTIONAL {{ ?person wdt:P18  ?image }}
      OPTIONAL {{ ?person wdt:P106 ?occupation .   ?occupation rdfs:label ?occupationLabel . FILTER(LANG(?occupationLabel) = "en") }}
      OPTIONAL {{ ?person wdt:P69  ?educatedAt .   ?educatedAt rdfs:label ?educatedAtLabel . FILTER(LANG(?educatedAtLabel) = "en") }}
      OPTIONAL {{
        ?person p:P108 ?employerStmt .
        ?employerStmt ps:P108 ?employer .
        ?employer rdfs:label ?employerLabel . FILTER(LANG(?employerLabel) = "en")
        OPTIONAL {{ ?employerStmt pq:P580 ?employerStart }}
        OPTIONAL {{ ?employerStmt pq:P582 ?employerEnd }}
      }}
      OPTIONAL {{
        ?person p:P39 ?positionStmt .
        ?positionStmt ps:P39 ?position .
        ?position rdfs:label ?positionLabel . FILTER(LANG(?positionLabel) = "en")
        OPTIONAL {{ ?positionStmt pq:P642 ?positionOf . ?positionOf rdfs:label ?positionOfLabel . FILTER(LANG(?positionOfLabel) = "en") }}
        OPTIONAL {{ ?positionStmt pq:P580 ?positionStart }}
        OPTIONAL {{ ?positionStmt pq:P582 ?positionEnd }}
      }}
    }}
    LIMIT 1
    """
    result = sparql_query(query)
    if not result or not result.get("results", {}).get("bindings"):
        return None
    b = result["results"]["bindings"][0]

    def g(k):
        return b.get(k, {}).get("value", "")

    def split_list(raw):
        return [s for s in raw.split("|") if s.strip()] if raw else []

    # Parse employer entries: "Apple Inc.::2011-01-01T::"
    employers = []
    for entry in split_list(g("employers")):
        parts = entry.split("::")
        if len(parts) >= 1 and parts[0]:
            employers.append({
                "company": parts[0],
                "start": parts[1][:10] if len(parts) > 1 and parts[1] else None,
                "end":   parts[2][:10] if len(parts) > 2 and parts[2] else None,
            })

    # Parse position entries: "chief executive officer::Apple Inc.::2011-01-01T::"
    positions = []
    for entry in split_list(g("positions")):
        parts = entry.split("::")
        if len(parts) >= 1 and parts[0]:
            positions.append({
                "title":   parts[0],
                "company": parts[1] if len(parts) > 1 and parts[1] else None,
                "start":   parts[2][:10] if len(parts) > 2 and parts[2] else None,
                "end":     parts[3][:10] if len(parts) > 3 and parts[3] else None,
            })

    return {
        "qid":         person_qid,
        "name":        person_name,
        "dateOfBirth": g("dateOfBirth")[:10] or None,
        "birthPlace":  g("birthPlace") or None,
        "image":       g("image") or None,
        "occupations": split_list(g("occupations")),
        "education":   split_list(g("education")),
        "employers":   employers,
        "positions":   positions,
    }


def main():
    # ── CLI args ──
    # `--start A` and `--end M` filter the company files by their leading letter
    # (case-insensitive). This lets us split the run across multiple workflows
    # (e.g. A-M in one job, N-Z in another) without exceeding the 60-minute
    # GitHub Actions soft-cap. Defaults: no filter (process all).
    #
    # Other supported invocations:
    #   python fetch_ceo_tracker.py --start A --end M
    #   python fetch_ceo_tracker.py --start N --end Z
    #   python fetch_ceo_tracker.py             (full run, unchanged behavior)
    args = sys.argv[1:]
    start_letter = None
    end_letter = None
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--start" and i + 1 < len(args):
            start_letter = args[i + 1].upper()[:1]
            i += 2
        elif a == "--end" and i + 1 < len(args):
            end_letter = args[i + 1].upper()[:1]
            i += 2
        else:
            log(f"unknown arg {a!r}, ignoring")
            i += 1

    if not COMPANY_DIR.exists():
        log(f"No {COMPANY_DIR} — run fetch_company_facts.py first")
        return 1

    company_files = sorted(COMPANY_DIR.glob("*.json"))
    company_files = [f for f in company_files if not f.name.startswith("_")]
    total_files = len(company_files)

    # Apply A-Z range filter
    if start_letter or end_letter:
        s = start_letter or "A"
        e = end_letter or "Z"
        before = len(company_files)
        company_files = [f for f in company_files if s <= f.name[0].upper() <= e]
        log(f"Range filter [{s}-{e}]: {before} → {len(company_files)} files (of {total_files} total)")
    else:
        log(f"Found {len(company_files)} company files (no range filter)")

    all_people = {}       # qid → person record
    by_ticker = {}        # ticker → [person summaries]
    person_to_tickers = {}  # qid → [tickers]

    for cf in company_files:
        try:
            company = json.loads(cf.read_text())
        except Exception as e:
            log(f"  skip {cf.name}: {e}")
            continue
        ticker = company.get("ticker")
        company_qid = company.get("qid")
        if not ticker:
            continue

        # Combine all named people from this company
        people_in_company = []
        for src_field, default_role in (
            ("executives", "Executive"),
            ("board",      "Board Member"),
            ("founders",   "Founder"),
        ):
            for entry in company.get(src_field, []) or []:
                name = (entry.get("name") or "").strip()
                if not name:
                    continue
                people_in_company.append({
                    "name": name,
                    "role_at_company": entry.get("role") or default_role,
                    "source_field": src_field,
                })

        by_ticker[ticker] = []

        for person in people_in_company:
            name = person["name"]
            # Skip if name is the company itself (Wikidata data quality issue — e.g.
            # NFLX's founders list contained "Netflix, Inc." which is the company QID)
            if name.lower() == (company.get("name") or "").lower():
                continue

            # Resolve QID
            qid = find_person_qid(name, company_qid)
            time.sleep(1.5)  # respect Wikidata rate limit

            if not qid:
                log(f"  ? {ticker}: '{name}' — no QID found, skipping career fetch")
                continue

            # Fetch full career if we haven't already
            if qid not in all_people:
                career = fetch_person_career(qid, name)
                time.sleep(1.5)
                if not career:
                    log(f"  ? {ticker}: '{name}' QID={qid} — empty career data")
                    continue
                all_people[qid] = career
                log(f"  ✓ {ticker}: '{name}' QID={qid} ({len(career['positions'])} positions, {len(career['employers'])} employers)")

            # Cross-reference
            person_to_tickers.setdefault(qid, []).append(ticker)
            by_ticker[ticker].append({
                "qid": qid,
                "name": name,
                "role_at_company": person["role_at_company"],
                "source_field": person["source_field"],
            })

    # Write per-ticker files
    for ticker, people_summaries in by_ticker.items():
        # Enrich each summary with the cached career data
        enriched = []
        for ps in people_summaries:
            full = all_people.get(ps["qid"])
            if full:
                enriched.append({**ps, "career": full})
        out = {
            "ticker": ticker,
            "fetchedAt": datetime.utcnow().isoformat() + "Z",
            "people": enriched,
        }
        (BY_TICKER_DIR / f"{ticker}.json").write_text(json.dumps(out, indent=2))

    # Write per-person files
    for qid, career in all_people.items():
        slug = slugify(career["name"])
        out = {
            **career,
            "currentTickers": person_to_tickers.get(qid, []),
        }
        (BY_PERSON_DIR / f"{slug}.json").write_text(json.dumps(out, indent=2))

    # Write master index. CRITICAL: when running with --start/--end filters
    # (e.g. A-M in one job, N-Z in another), we MUST merge with any existing
    # manifest so the second run doesn't wipe out the first run's entries.
    # We do this by reading the current manifest (if present) and merging
    # entries from the previous run that aren't being re-processed in this
    # range. Then write the combined index back.
    existing_index_by_qid = {}
    manifest_path = LEADERSHIP_DIR / "_manifest.json"
    if manifest_path.exists():
        try:
            existing = json.loads(manifest_path.read_text())
            for entry in existing.get("index", []) or []:
                if entry.get("qid"):
                    existing_index_by_qid[entry["qid"]] = entry
        except Exception as e:
            log(f"  could not read existing manifest, starting fresh: {e}")

    # New entries from this run
    new_index_by_qid = {
        qid: {
            "qid": qid,
            "name": career["name"],
            "slug": slugify(career["name"]),
            "tickers": person_to_tickers.get(qid, []),
            "positionsCount": len(career.get("positions", [])),
            "employersCount": len(career.get("employers", [])),
        }
        for qid, career in all_people.items()
    }

    # Merge: this run's entries WIN (they're freshest). Old entries for people
    # NOT touched by this run are preserved.
    merged_index_by_qid = {**existing_index_by_qid, **new_index_by_qid}

    manifest = {
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "totalPeople": len(merged_index_by_qid),
        "totalTickers": len({t for entry in merged_index_by_qid.values() for t in entry.get("tickers", [])}),
        "lastRangeFilter": (
            f"{start_letter or 'A'}-{end_letter or 'Z'}"
            if (start_letter or end_letter) else "ALL"
        ),
        "thisRunPeople": len(all_people),
        "thisRunTickers": len(by_ticker),
        "index": list(merged_index_by_qid.values()),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    prune_orphans()

    log(f"Done: this run = {len(all_people)} people across {len(by_ticker)} tickers; "
        f"manifest total = {len(merged_index_by_qid)} unique people")
    return 0


def prune_orphans():
    """Leadership is derived from data/company/. When a company file is gone
    (ticker left the universe, or company-facts pruned it), drop its by_ticker
    file, remove it from every index entry, and delete people left with no
    current ticker. Range-independent and idempotent."""
    live = {f.stem.upper() for f in COMPANY_DIR.glob("*.json") if not f.name.startswith("_")}
    if not live:
        return  # never wipe leadership because company facts are missing
    gone_t = []
    for f in sorted(BY_TICKER_DIR.glob("*.json")):
        if f.stem.upper() not in live:
            f.unlink()
            gone_t.append(f.stem)
    manifest_path = LEADERSHIP_DIR / "_manifest.json"
    try:
        man = json.loads(manifest_path.read_text())
    except Exception:
        man = None
    gone_p = []
    if man and isinstance(man.get("index"), list):
        kept = []
        for e in man["index"]:
            tks = [t for t in (e.get("tickers") or []) if str(t).upper() in live]
            if tks:
                e["tickers"] = tks
                kept.append(e)
            else:
                gone_p.append(e.get("slug"))
        keep_slugs = {e.get("slug") for e in kept}
        for slug in gone_p:
            if slug and slug not in keep_slugs:
                fp = BY_PERSON_DIR / f"{slug}.json"
                if fp.exists():
                    fp.unlink()
        man["index"] = kept
        man["totalPeople"] = len(kept)
        man["totalTickers"] = len({t for e in kept for t in e.get("tickers", [])})
        if gone_t or gone_p:
            man["prunedAt"] = datetime.utcnow().isoformat() + "Z"
        manifest_path.write_text(json.dumps(man, indent=2))
    if gone_t or gone_p:
        log(f"Pruned orphans: {len(gone_t)} by_ticker file(s), {len(gone_p)} person(s) with no current ticker")


if __name__ == "__main__":
    sys.exit(main())
