"""
TRAPP2 Company Facts Fetcher
==============================

Pulls structured company information from Wikidata (free, no API key, CC0-licensed)
and writes per-ticker JSON files to data/company/<TICKER>.json.

Wikidata properties used (in SPARQL Pxxx form):
    P31     instance-of (filter to "business")
    P249    stock ticker symbol     (matches our ticker)
    P414    stock exchange          (NASDAQ / NYSE / etc.)
    P169    chief executive officer
    P3320   board member
    P488    chairperson
    P1037   director / manager (alt path)
    P112    founder
    P159    headquarters location
    P571    inception date          (IPO ~= founding for many)
    P1128   employees
    P856    official website
    P946    ISIN
    P127    owned by
    P127    operating area
    P3320   board members
    P1342   number of seats
    P452    industry
    P1056   product or material produced
    P2541   operating area (country)

Strategy:
  1. Resolve the Wikidata entity for each ticker using P249 lookup
  2. Pull all available properties in a single SPARQL query
  3. Use the Wikibase API to get linked entity labels (CEO name, board member names, etc.)
  4. Normalize to the schema the app's renderCompanyTab expects

Output schema:
{
  "ticker": "AAPL",
  "qid": "Q312",
  "fetchedAt": "2026-05-18T...",
  "source": "wikidata",
  "name": "Apple Inc.",
  "description": "...",
  "industry": "...",
  "website": "https://www.apple.com",
  "headquarters": "Cupertino, California, US",
  "employees": 161000,
  "isin": "US0378331005",
  "inception": "1976-04-01",
  "executives": [{"name": "Tim Cook", "role": "Chief Executive Officer"}],
  "board":      [{"name": "Arthur D. Levinson", "role": "Chairperson"}, ...],
  "founders":   [{"name": "Steve Jobs"}, ...],
  "products":   [{"name": "iPhone"}, ...],
  "countries":  [{"name": "United States"}, ...],
}

Runs in the nightly workflow. Best-effort: individual ticker failures don't
break the workflow.
"""

import json
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"

UA = f"TRAPP2/1.0 (https://github.com/{_GH_OWNER}/TRAPP2; financial-data-app) python-requests"
HEADERS = {"User-Agent": UA, "Accept": "application/sparql-results+json"}

ROOT = Path(__file__).resolve().parent.parent
COMPANY_DIR = ROOT / "data" / "company"
TICKERS_FILE = ROOT / "data" / "tickers.txt"
COMPANY_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_FILE = COMPANY_DIR / "_manifest.json"

# Skip tickers that can't have meaningful Wikidata company entries.
# These are derivatives / indexes / ETFs / FX / crypto / futures — no company behind them.
NON_COMPANY_PATTERNS = (
    "=F", "=X", "-USD", "-USDT",  # futures, FX, crypto
    "!",                            # TradingView-style continuous futures (CL1!, ES1!, etc.)
    ".PVT",                         # private/unlisted entities (no public Wikidata)
)
NON_COMPANY_PREFIXES = ("^",)  # indexes
NON_COMPANY_TICKERS = {
    # Known ETFs — they have Wikidata entries but they're funds, not companies
    "SPY", "QQQ", "DIA", "IWM", "VOO", "VTI", "VEA", "VWO", "EFA", "EEM",
    "XLK", "XLF", "XLV", "XLE", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC",
    "TLT", "IEF", "SHY", "BND", "AGG", "LQD", "HYG", "JNK", "TIP", "MBB", "EMB",
    "GOVT", "VCSH", "VCIT", "VCLT", "BIV", "BSV", "BLV", "BNDX",
    "GLD", "SLV", "USO", "UNG", "DBA", "DBC", "GSG",
    "IYT", "IYR", "VNQ", "XBI", "IBB", "XHB", "ITB", "XRT",
    "ARKK", "SOXL", "TQQQ", "SQQQ", "UVXY", "VXX", "UUP",
    "RSP", "SPLV", "SPHQ", "MTUM", "QUAL", "USMV", "SCHD",
}

# z178 - listing symbols -> the form Yahoo, the SEC and every book use. Exchange lists (NYSE / Nasdaq "ACT"
# symbols, datahub.io, the app's CSV) write preferreds with "$" and share classes with ".": Yahoo has no
# "ABR$D" or "BRK.B", so those lines fetched nothing ("$" also breaks shell steps and file names). Normalized
# where tickers.txt is read, so the txt files can hold either form:
#   ABR$D -> ABR-PD (preferred)   BRK.B -> BRK-B (class)   XYZ.U -> XYZ-UN (unit)   XYZ.W / .WS -> XYZ-WT (warrant)
#   XYZ.R -> XYZ-RI (right)       exchange suffixes (.L .PA .SW .HK .T .TO ...) and everything else unchanged
def norm_symbol(t):
    import re as _re
    t = (t or "").strip().upper()
    if "$" in t:
        root, ser = t.split("$", 1)
        return f"{root}-P{ser}" if ser else f"{root}-P"
    m = _re.fullmatch(r"([A-Z]{1,5})\.(U|W|WS|R)", t)
    if m:
        return m.group(1) + {"U": "-UN", "W": "-WT", "WS": "-WT", "R": "-RI"}[m.group(2)]
    m = _re.fullmatch(r"([A-Z]{1,5})\.([A-C]|V)", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return t


import re as _re_mod
_re_pref = _re_mod.compile(r"-(P[A-Z]?|UN|WT|RI)$")


def load_tickers_from_file():
    """
    Load tickers from data/tickers.txt — one ticker per line, comments with #.
    Filters out derivatives, indexes, ETFs, FX, crypto, futures, options.
    Returns deduped list of equity tickers.
    """
    if not TICKERS_FILE.exists():
        # In Actions, a missing tickers.txt means a broken/partial checkout. The
        # built-in list (US mega-caps) would be written into THIS repo as if it
        # were its universe - that's how TRAPP2-1 got 106 US company files.
        if __import__("os").environ.get("GITHUB_ACTIONS"):
            print(f"::error::[company-facts] {TICKERS_FILE} not found - refusing the built-in fallback in CI")
            return []
        print(f"[company-facts] WARNING: {TICKERS_FILE} not found, using built-in fallback list")
        return BUILT_IN_TICKERS

    tickers = []
    seen = set()
    for line in TICKERS_FILE.read_text().splitlines():
        # Strip comments
        t = norm_symbol(line.split("#")[0])                 # z178: ABR$D -> ABR-PD, BRK.B -> BRK-B
        if not t:
            continue
        if _re_pref.search(t):                              # z178: preferreds, units, warrants, rights aren't companies
            continue
        # Filter non-companies
        if any(t.endswith(suffix) for suffix in NON_COMPANY_PATTERNS):
            continue
        if any(t.startswith(prefix) for prefix in NON_COMPANY_PREFIXES):
            continue
        # Option contract strings (e.g. SPY261204C00800000) — too long, contain digits at end
        if len(t) > 10 and any(c.isdigit() for c in t[-6:]):
            continue
        if t in NON_COMPANY_TICKERS:
            continue
        if t in seen:
            continue
        seen.add(t)
        tickers.append(t)
    return tickers


# Fallback list when tickers.txt is missing — mirrors the major tickers the app cares about.
BUILT_IN_TICKERS = [
    # Mega-cap tech / "magnificent seven"
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "NVDA",
    # Other mega-caps
    "BRK.B", "JPM", "V", "MA", "WMT", "PG", "JNJ", "UNH", "HD", "BAC",
    "XOM", "CVX", "LLY", "MRK", "ABBV", "PFE", "PEP", "KO", "COST",
    "AVGO", "ORCL", "ADBE", "CRM", "NFLX", "AMD", "INTC", "QCOM", "CSCO",
    "DIS", "T", "VZ", "TMUS", "CMCSA", "MCD", "NKE", "SBUX",
    # Industrials / aero / autos
    "BA", "CAT", "GE", "MMM", "HON", "LMT", "RTX", "F", "GM",
    # Financials
    "GS", "MS", "C", "WFC", "AXP", "BLK", "SCHW", "USB", "PNC",
    # Energy + utilities
    "COP", "SLB", "EOG", "MPC", "PSX", "VLO", "OXY", "NEE", "DUK", "SO",
    # Healthcare
    "ABT", "TMO", "DHR", "BMY", "AMGN", "GILD", "MDT", "ISRG", "REGN",
    # Consumer
    "TGT", "LOW", "CVS", "EL", "CL", "TJX", "ROST",
    # Big tech / SaaS
    "INTU", "AMAT", "NOW", "PYPL", "SHOP", "UBER", "ABNB", "SPOT",
    # Railroads / transportation (your IYT holdings)
    "UNP", "CSX", "NSC", "UPS", "FDX", "ODFL", "JBHT", "CHRW", "EXPD",
    # Communications / media
    "WBD", "PARA",
]

# Wikidata properties → human label
PROP_LABELS = {
    "P169": "Chief Executive Officer",
    "P3320": "Board Member",
    "P488": "Chairperson",
    "P488": "Chairperson",
    "P112": "Founder",
    "P127": "Owner",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def log(msg):
    print(f"[company-facts] {msg}", flush=True)


def sparql_query(query, retries=4):
    """Execute a SPARQL query against Wikidata. Returns parsed JSON results or None.
    
    Wikidata's public SPARQL endpoint is heavily rate-limited and flaky.
    We retry with exponential backoff. Empirically: 1 retry is insufficient at
    sustained scale; 4 retries with backoff catches most transient failures."""
    for attempt in range(retries + 1):
        try:
            r = requests.get(
                WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers=HEADERS,
                timeout=45,
            )
            if r.status_code == 429:
                # Rate limited — back off and retry. The Retry-After header is
                # sometimes missing or unreasonable; use a sane backoff.
                wait = max(int(r.headers.get("Retry-After", 0)), 5 * (attempt + 1))
                log(f"  rate-limited, waiting {wait}s (attempt {attempt + 1}/{retries + 1})")
                time.sleep(wait)
                continue
            if r.status_code in (500, 502, 503, 504):
                # Server-side flake — back off
                wait = 3 * (attempt + 1)
                log(f"  server error {r.status_code}, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log(f"  unexpected status {r.status_code}")
                return None
            return r.json()
        except requests.RequestException as e:
            wait = 2 * (attempt + 1)
            log(f"  SPARQL request failed: {e}, waiting {wait}s")
            time.sleep(wait)
        except ValueError as e:
            # JSON decode error — sometimes endpoint returns HTML on errors
            log(f"  SPARQL JSON decode failed: {e}")
            return None
    log(f"  SPARQL exhausted retries")
    return None


def find_qid_for_ticker(ticker):
    """
    Look up the Wikidata QID for a ticker.

    THREE STRATEGIES tried in order, because Wikidata's P249 (ticker symbol)
    is sparsely populated and only the most-followed companies have it set
    cleanly. Without these fallbacks, the script returns None for ~70% of
    tickers and writes no JSON file — which is exactly the bug the user
    reported (only NFLX worked).

      1. P249 exact match — works for ~30% of tickers (well-curated entries)
      2. Wikidata Search API by ticker — works for ~50% more (uses Elastic
         search index which is less strict than SPARQL P249)
      3. Common-name fallback — last resort, queries by ticker as a name

    Returns (qid, label) or (None, None).
    """
    # Strategy 1: P249 SPARQL lookup (most precise when it works)
    query = f"""
    SELECT ?company ?companyLabel (COUNT(?sitelink) AS ?links) WHERE {{
      ?company wdt:P249 "{ticker}" .
      ?company wdt:P31/wdt:P279* wd:Q4830453 .
      OPTIONAL {{ ?sitelink schema:about ?company . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    GROUP BY ?company ?companyLabel
    ORDER BY DESC(?links)
    LIMIT 1
    """
    result = sparql_query(query)
    if result:
        bindings = result.get("results", {}).get("bindings", [])
        if bindings:
            qid_uri = bindings[0]["company"]["value"]
            qid = qid_uri.rsplit("/", 1)[-1]
            label = bindings[0].get("companyLabel", {}).get("value")
            log(f"  ✓ P249 match: {qid} ({label})")
            return qid, label

    # Strategy 2: Wikidata Search API (Elastic-based, much more forgiving)
    # Returns an array of candidates; we filter to those that look like companies
    # by checking their P31 (instance-of) values match Q4830453 (business).
    try:
        time.sleep(0.5)
        r = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "search": ticker,
                "language": "en",
                "format": "json",
                "type": "item",
                "limit": 10,
            },
            headers=HEADERS,
            timeout=20,
        )
        if r.status_code == 200:
            candidates = r.json().get("search", [])
            for cand in candidates:
                qid = cand.get("id")
                if not qid:
                    continue
                # Verify it's actually a company via a focused SPARQL check
                verify_q = f"""
                ASK {{
                  wd:{qid} wdt:P31/wdt:P279* wd:Q4830453 .
                }}
                """
                time.sleep(0.5)
                vr = sparql_query(verify_q)
                if vr and vr.get("boolean") is True:
                    label = cand.get("label") or cand.get("description") or ticker
                    log(f"  ✓ Search API match: {qid} ({label})")
                    return qid, label
    except Exception as e:
        log(f"  Search API error: {e}")

    # Strategy 3: P414 (stock exchange) + ticker partial-match via SPARQL
    # Some entities have ticker info on the stock-exchange statement rather than
    # via P249. This catches another ~10% of tickers.
    query3 = f"""
    SELECT ?company ?companyLabel WHERE {{
      ?company p:P414 ?ex .
      ?ex pq:P249 "{ticker}" .
      ?company wdt:P31/wdt:P279* wd:Q4830453 .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    """
    result = sparql_query(query3)
    if result:
        bindings = result.get("results", {}).get("bindings", [])
        if bindings:
            qid_uri = bindings[0]["company"]["value"]
            qid = qid_uri.rsplit("/", 1)[-1]
            label = bindings[0].get("companyLabel", {}).get("value")
            log(f"  ✓ P414 qualifier match: {qid} ({label})")
            return qid, label

    return None, None


def fetch_company_facts(qid, ticker):
    """
    Fetch all properties of interest for a company QID in a single SPARQL query.
    Returns a dict of normalized fields.
    """
    # GROUP_CONCAT with separator |  lets us collect multi-value lists (board members,
    # founders, products) in a single result row instead of issuing N queries.
    query = f"""
    SELECT
      ?companyLabel ?description
      ?websiteUrl
      ?isin
      ?inception
      ?employees
      ?industryLabel
      ?hqLabel
      (GROUP_CONCAT(DISTINCT ?ceoLabel; separator="|") AS ?ceos)
      (GROUP_CONCAT(DISTINCT ?chairLabel; separator="|") AS ?chairs)
      (GROUP_CONCAT(DISTINCT ?boardLabel; separator="|") AS ?boards)
      (GROUP_CONCAT(DISTINCT ?founderLabel; separator="|") AS ?founders)
      (GROUP_CONCAT(DISTINCT ?productLabel; separator="|") AS ?products)
      (GROUP_CONCAT(DISTINCT ?countryLabel; separator="|") AS ?countries)
    WHERE {{
      BIND(wd:{qid} AS ?company)
      OPTIONAL {{ ?company schema:description ?description . FILTER(LANG(?description) = "en") }}
      OPTIONAL {{ ?company wdt:P856  ?websiteUrl }}
      OPTIONAL {{ ?company wdt:P946  ?isin }}
      OPTIONAL {{ ?company wdt:P571  ?inception }}
      OPTIONAL {{ ?company wdt:P1128 ?employees }}
      OPTIONAL {{ ?company wdt:P452  ?industry .  ?industry rdfs:label ?industryLabel . FILTER(LANG(?industryLabel) = "en") }}
      OPTIONAL {{ ?company wdt:P159  ?hq .        ?hq       rdfs:label ?hqLabel       . FILTER(LANG(?hqLabel)       = "en") }}
      OPTIONAL {{ ?company wdt:P169  ?ceo .       ?ceo      rdfs:label ?ceoLabel      . FILTER(LANG(?ceoLabel)      = "en") }}
      OPTIONAL {{ ?company wdt:P488  ?chair .     ?chair    rdfs:label ?chairLabel    . FILTER(LANG(?chairLabel)    = "en") }}
      OPTIONAL {{ ?company wdt:P3320 ?board .     ?board    rdfs:label ?boardLabel    . FILTER(LANG(?boardLabel)    = "en") }}
      OPTIONAL {{ ?company wdt:P112  ?founder .   ?founder  rdfs:label ?founderLabel  . FILTER(LANG(?founderLabel)  = "en") }}
      OPTIONAL {{ ?company wdt:P1056 ?product .   ?product  rdfs:label ?productLabel  . FILTER(LANG(?productLabel)  = "en") }}
      OPTIONAL {{ ?company wdt:P2541 ?country .   ?country  rdfs:label ?countryLabel  . FILTER(LANG(?countryLabel)  = "en") }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    GROUP BY ?companyLabel ?description ?websiteUrl ?isin ?inception ?employees ?industryLabel ?hqLabel
    LIMIT 1
    """
    result = sparql_query(query)
    if not result:
        return None
    bindings = result.get("results", {}).get("bindings", [])
    if not bindings:
        return None
    b = bindings[0]

    def get(key):
        return b.get(key, {}).get("value")

    def split_list(key):
        """Parse GROUP_CONCAT result into a list of unique non-empty strings."""
        raw = get(key) or ""
        if not raw:
            return []
        items = [s.strip() for s in raw.split("|") if s.strip()]
        # Deduplicate while preserving order
        seen = set()
        out = []
        for item in items:
            if item not in seen:
                seen.add(item)
                out.append(item)
        return out

    # Normalize executives: CEO + Chair (separate roles), then board members
    executives = []
    for ceo in split_list("ceos"):
        executives.append({"name": ceo, "role": "Chief Executive Officer"})
    for chair in split_list("chairs"):
        # If chair is already in executives as CEO, don't duplicate
        if not any(e["name"] == chair for e in executives):
            executives.append({"name": chair, "role": "Chairperson"})

    board = [{"name": name, "role": "Board Member"} for name in split_list("boards")]
    founders = [{"name": name} for name in split_list("founders")]
    products = [{"name": name} for name in split_list("products")]
    countries = [{"name": name} for name in split_list("countries")]

    # Parse employees (might be "161000" or similar)
    employees = get("employees")
    if employees:
        try:
            employees = int(float(employees))
        except (ValueError, TypeError):
            employees = None

    return {
        "name": get("companyLabel"),
        "description": get("description"),
        "website": get("websiteUrl"),
        "isin": get("isin"),
        "inception": (get("inception") or "")[:10] or None,
        "employees": employees,
        "industry": get("industryLabel"),
        "headquarters": get("hqLabel"),
        "executives": executives,
        "board": board,
        "founders": founders,
        "products": products,
        "countries": countries,
    }


def fetch_ticker(ticker):
    """Resolve a ticker → write its company facts JSON file. Returns (ok, msg)."""
    qid, _label = find_qid_for_ticker(ticker)
    if not qid:
        # Write a lightweight marker so this ticker isn't re-attempted every
        # night (no-QID tickers — ETFs, obscure foreign listings — rarely gain
        # a Wikidata entry suddenly). It still re-checks after the freshness
        # window expires, in case an entry was added.
        try:
            (COMPANY_DIR / f"{ticker}.json").write_text(json.dumps({
                "ticker": ticker, "fetchedAt": datetime.utcnow().isoformat() + "Z",
                "source": "wikidata", "_noEntity": True, "name": None,
            }, indent=2))
        except Exception:
            pass
        return False, "no Wikidata QID found"
    facts = fetch_company_facts(qid, ticker)
    if not facts or not facts.get("name"):
        return False, f"empty facts for QID {qid}"

    output = {
        "ticker": ticker,
        "qid": qid,
        "fetchedAt": datetime.utcnow().isoformat() + "Z",
        "source": "wikidata",
        **facts,
    }
    out_path = COMPANY_DIR / f"{ticker}.json"
    out_path.write_text(json.dumps(output, indent=2))

    n_exec = len(facts["executives"])
    n_board = len(facts["board"])
    n_products = len(facts["products"])
    return True, f"QID={qid}, {n_exec} execs, {n_board} board, {n_products} products"


def _is_fresh(ticker, max_age_days=30):
    """True if this ticker's company JSON exists and was fetched recently —
    company facts (founded, HQ, leadership) rarely change, so we skip fresh ones.
    This is what keeps the nightly run under the 1-hour limit: most nights only
    a handful of new/stale tickers actually need fetching."""
    out_path = COMPANY_DIR / f"{ticker}.json"
    if not out_path.exists():
        return False
    try:
        data = json.loads(out_path.read_text())
        fetched = data.get("fetchedAt") or data.get("_fetchedAt")
        if not fetched:
            # No timestamp — fall back to file mtime.
            age = time.time() - out_path.stat().st_mtime
            return age < max_age_days * 86400
        ts = datetime.fromisoformat(fetched.replace("Z", "+00:00"))
        age_days = (datetime.now(ts.tzinfo) - ts).total_seconds() / 86400
        return age_days < max_age_days
    except Exception:
        return False


def main():
    import os as _os
    tickers = load_tickers_from_file()
    if not tickers:
        log("No tickers to fetch - nothing written")
        return 1

    # Time budget: stop fetching ~10 min before GitHub's 60-min job limit so the
    # commit step always runs and we never lose a half-finished run. The next
    # night picks up whatever's still stale. Override via FACTS_BUDGET_SECS.
    budget_secs = int(_os.environ.get("FACTS_BUDGET_SECS", str(48 * 60)))
    # Freshness window — re-fetch a ticker only if its file is older than this.
    max_age_days = int(_os.environ.get("FACTS_MAX_AGE_DAYS", "30"))
    # Force a full refresh (ignore freshness) with FACTS_FORCE=1.
    force = _os.environ.get("FACTS_FORCE", "") == "1"
    start = time.time()

    # Partition: stale/missing first (these need work), skip the fresh ones.
    to_fetch = tickers if force else [t for t in tickers if not _is_fresh(t, max_age_days)]
    skipped = len(tickers) - len(to_fetch)
    log(f"Company facts: {len(tickers)} total · {skipped} fresh (skipped) · {len(to_fetch)} to fetch · budget {budget_secs}s")

    # Load existing manifest so skipped tickers keep their prior status.
    manifest = {"generatedAt": datetime.utcnow().isoformat() + "Z", "source": "wikidata", "tickers": {}}
    if MANIFEST_FILE.exists():
        try:
            prior = json.loads(MANIFEST_FILE.read_text())
            manifest["tickers"] = prior.get("tickers", {})
        except Exception:
            pass

    success = 0
    fetched_count = 0
    for ticker in to_fetch:
        # Stop if we're near the time budget — let the commit step run.
        if time.time() - start > budget_secs:
            log(f"⏱ Hit time budget after {fetched_count} fetches — stopping; remaining stale tickers will refresh next run.")
            break
        try:
            ok, msg = fetch_ticker(ticker)
            if ok:
                log(f"✓ {ticker:7s} — {msg}")
                success += 1
            else:
                log(f"✗ {ticker:7s} — {msg}")
            manifest["tickers"][ticker] = {
                "ok": ok,
                "msg": msg,
                "fetchedAt": datetime.utcnow().isoformat() + "Z",
            }
        except Exception as e:
            log(f"✗ {ticker:7s} — exception: {type(e).__name__}: {e}")
            manifest["tickers"][ticker] = {"ok": False, "msg": str(e)}
        fetched_count += 1
        time.sleep(1.5)

    pruned = prune_orphans(tickers)
    if pruned:
        for t in pruned:
            manifest["tickers"].pop(t, None)
        manifest["prunedOrphans"] = pruned
    # z99: XTRAPP leadership edits are the system override - applied to every
    # affected company file each run (not only to the few re-fetched tonight).
    try:
        from human_corrections import apply_leadership_to_dir
        human = apply_leadership_to_dir(COMPANY_DIR, tickers)
        if human:
            manifest["humanLeadership"] = human
        log(f"XTRAPP leadership overrides: {sum(human.values())} change(s) across {len(human)} ticker(s)"
            + (f" — {', '.join(sorted(human))}" if human else ""))
    except Exception as e:
        log(f"XTRAPP leadership overrides skipped: {e}")
    manifest["generatedAt"] = datetime.utcnow().isoformat() + "Z"
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2))
    log(f"Done: fetched {fetched_count}, {success} successful, {skipped} skipped as fresh, "
        f"{len(pruned)} orphan file(s) pruned")
    return 0


PRUNE_MIN_UNIVERSE = 20   # a tickers.txt smaller than this looks truncated - don't prune


def prune_orphans(universe):
    """Delete company files whose ticker is no longer in this repo's universe
    (dropped tickers, or a past fallback run). Universe files - fresh, stale or
    _noEntity markers - are never touched. Skipped when the universe looks
    truncated, so a half-uploaded tickers.txt can't empty the folder."""
    keep = {str(t).upper() for t in universe}
    if len(keep) < PRUNE_MIN_UNIVERSE:
        log(f"Prune skipped: universe has only {len(keep)} tickers (< {PRUNE_MIN_UNIVERSE})")
        return []
    pruned = []
    for f in sorted(COMPANY_DIR.glob("*.json")):
        if f.name.startswith("_"):
            continue
        if f.stem.upper() not in keep:
            f.unlink()
            pruned.append(f.stem)
    if pruned:
        log(f"Pruned {len(pruned)} orphan company file(s): {', '.join(pruned[:12])}{' ...' if len(pruned) > 12 else ''}")
    return pruned


if __name__ == "__main__":
    sys.exit(main())
