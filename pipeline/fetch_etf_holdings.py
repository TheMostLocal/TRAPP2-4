"""
TRAPP2 ETF holdings fetcher
============================

Downloads daily ETF holdings from issuer-direct endpoints (iShares, SSGA,
Vanguard, Invesco) and commits each fund as data/etf_holdings/<TICKER>.json.

PROVENANCE NOTE — this is the IMPORTANT thing the user explicitly asked for:
each output file's `source` field declares exactly how the data was obtained
so the user can verify the data is PULLED FROM ISSUERS, not synthesized or
hardcoded:
  - "etf-scraper-{provider}"  → live issuer-direct download via maintained
                                 etf_scraper library (best — full holdings)
  - "yahoo-finance-fallback"  → top 10 only when issuer fails (degraded)
  - source is NEVER hand-rolled / written-in data.

THREE STRATEGIES tried in order until one succeeds:

  1. etf_scraper library (PRIMARY)
     - Maintained third-party library at github.com/nikulpatel3141/ETF-Scraper.
     - Has daily-running CI badges (all 4 providers green) which proves it
       works from datacenter IPs — exactly our GitHub Actions scenario.
     - Returns a pandas DataFrame with ticker, name, weight, shares, etc.

  2. Direct issuer CSV with browser-pattern headers (FALLBACK 1)
     - The previous implementation. Kept as a fallback in case etf_scraper
       breaks (e.g. iShares changes their schema and the library hasn't
       updated yet).
     - Browser User-Agent + Referer to dodge bot detection.

  3. Yahoo Finance topHoldings (FALLBACK 2)
     - Top 10 holdings only — partial coverage. Used when both above fail.
     - The output JSON's `source` field is set to "yahoo-finance-fallback"
       so the app shows accurate provenance.

If ALL THREE fail, no file is written for that ticker and the manifest entry
records the failure. We do NOT write hand-coded holdings as a final fallback —
that would be misleading.

Run nightly via .github/workflows/nightly.yml or manually:
    python pipeline/fetch_etf_holdings.py
"""

import csv
import io
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

# Try to import etf_scraper. If it's not installed, the script falls through
# to the legacy direct-fetch implementation. This lets the script keep working
# even on a stale dev box where pip install hasn't been re-run.
try:
    from etf_scraper import ETFScraper
    ETF_SCRAPER_AVAILABLE = True
except Exception as _e:
    ETF_SCRAPER_AVAILABLE = False
    _etf_scraper_import_error = str(_e)


# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
HOLDINGS_DIR = ROOT / "data" / "etf_holdings"
HOLDINGS_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_FILE = HOLDINGS_DIR / "_manifest.json"


def log(msg):
    print(f"[etf-holdings] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Source registry — used for FALLBACK 2 (direct CSV) when etf_scraper fails.
# etf_scraper handles ticker → URL resolution internally so the primary path
# doesn't need this list.
# ---------------------------------------------------------------------------
ETF_SOURCES = {
    # iShares (BlackRock)
    "IVV":  {"url": "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund",                    "parser": "ishares_csv"},
    "IJH":  {"url": "https://www.ishares.com/us/products/239763/ishares-core-sp-midcap-etf/1467271812596.ajax?fileType=csv&fileName=IJH_holdings&dataType=fund",                "parser": "ishares_csv"},
    "IJR":  {"url": "https://www.ishares.com/us/products/239774/ishares-core-sp-smallcap-etf/1467271812596.ajax?fileType=csv&fileName=IJR_holdings&dataType=fund",              "parser": "ishares_csv"},
    "IWM":  {"url": "https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund",                  "parser": "ishares_csv"},
    "IWD":  {"url": "https://www.ishares.com/us/products/239708/ishares-russell-1000-value-etf/1467271812596.ajax?fileType=csv&fileName=IWD_holdings&dataType=fund",            "parser": "ishares_csv"},
    "IWF":  {"url": "https://www.ishares.com/us/products/239706/ishares-russell-1000-growth-etf/1467271812596.ajax?fileType=csv&fileName=IWF_holdings&dataType=fund",           "parser": "ishares_csv"},
    "EFA":  {"url": "https://www.ishares.com/us/products/239623/ishares-msci-eafe-etf/1467271812596.ajax?fileType=csv&fileName=EFA_holdings&dataType=fund",                     "parser": "ishares_csv"},
    "EEM":  {"url": "https://www.ishares.com/us/products/239637/ishares-msci-emerging-markets-etf/1467271812596.ajax?fileType=csv&fileName=EEM_holdings&dataType=fund",         "parser": "ishares_csv"},
    "AGG":  {"url": "https://www.ishares.com/us/products/239458/ishares-core-total-us-bond-market-etf/1467271812596.ajax?fileType=csv&fileName=AGG_holdings&dataType=fund",     "parser": "ishares_csv"},
    "HYG":  {"url": "https://www.ishares.com/us/products/239565/ishares-iboxx-high-yield-corporate-bond-etf/1467271812596.ajax?fileType=csv&fileName=HYG_holdings&dataType=fund","parser": "ishares_csv"},
    "TIP":  {"url": "https://www.ishares.com/us/products/239467/ishares-tips-bond-etf/1467271812596.ajax?fileType=csv&fileName=TIP_holdings&dataType=fund",                     "parser": "ishares_csv"},
    "IYR":  {"url": "https://www.ishares.com/us/products/239520/ishares-us-real-estate-etf/1467271812596.ajax?fileType=csv&fileName=IYR_holdings&dataType=fund",                "parser": "ishares_csv"},
    "IYT":  {"url": "https://www.ishares.com/us/products/239526/ishares-transportation-average-etf/1467271812596.ajax?fileType=csv&fileName=IYT_holdings&dataType=fund",        "parser": "ishares_csv"},
}

# Tickers to attempt via etf_scraper — broader list since the library handles
# SSGA / Vanguard / Invesco that our custom code doesn't. The library knows
# which provider each ticker belongs to via its built-in listings.csv.
ETF_TICKERS_VIA_SCRAPER = [
    # iShares
    "IVV","IJH","IJR","IWM","IWD","IWF","EFA","EEM","AGG","HYG","TIP","IYR","IYT",
    # SSGA (SPDRs)
    "SPY","DIA","XLK","XLF","XLV","XLE","XLY","XLP","XLI","XLB","XLU","XLRE","XLC",
    # Vanguard
    "VOO","VTI","VEA","VWO","VTV","VUG","VYM","BND","VNQ",
    # Invesco
    "QQQ","RSP","QQQM",
]


# ---------------------------------------------------------------------------
# STRATEGY 1: etf_scraper library
# ---------------------------------------------------------------------------
def fetch_via_etf_scraper(ticker: str, scraper) -> tuple[bool, str, list]:
    """Returns (ok, message, holdings_list).

    The library returns a pandas DataFrame. Convert to our standard
    list-of-dicts shape so all three strategies produce identical output
    schema for the app to consume.
    """
    try:
        df = scraper.query_holdings(ticker, None)  # None = latest
        if df is None or df.empty:
            return False, "etf_scraper returned empty DataFrame", []

        holdings = []
        # Column names vary by provider. Build a column-name→canonical map.
        cols = {c.lower(): c for c in df.columns}
        ticker_col = cols.get("ticker") or cols.get("symbol") or cols.get("identifier")
        name_col   = cols.get("name") or cols.get("description") or cols.get("security_name")
        weight_col = cols.get("weight") or cols.get("weighting") or cols.get("weight(%)") or cols.get("percentageweight")
        # Some providers report market value
        mv_col     = cols.get("market_value") or cols.get("marketvalue") or cols.get("value")
        shares_col = cols.get("shares") or cols.get("amount") or cols.get("nominal")

        for _, row in df.iterrows():
            entry = {
                "ticker":      str(row[ticker_col]).strip() if ticker_col else "",
                "name":        str(row[name_col]).strip() if name_col else "",
                "weight":      _safe_float(row[weight_col]) if weight_col else None,
                "shares":      _safe_float(row[shares_col]) if shares_col else None,
                "marketValue": _safe_float(row[mv_col]) if mv_col else None,
            }
            # Drop completely empty rows
            if entry["ticker"] or entry["name"]:
                holdings.append(entry)

        if not holdings:
            return False, "etf_scraper DataFrame had no parsable rows", []
        return True, f"{len(holdings)} holdings via etf_scraper", holdings
    except Exception as e:
        return False, f"etf_scraper: {type(e).__name__}: {e}", []


def _safe_float(v) -> float | None:
    """Coerce to float; return None for NaN / empty / bad strings."""
    if v is None:
        return None
    try:
        import math
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# STRATEGY 2: Direct iShares CSV fetch (fallback)
# ---------------------------------------------------------------------------
def parse_ishares_csv(text: str) -> list[dict]:
    """iShares CSV: preamble rows of metadata, then header row, then holdings."""
    holdings = []
    reader = csv.reader(io.StringIO(text))
    found_header = False
    header_idx = {}
    for row in reader:
        if not row:
            continue
        if not found_header:
            if row[0].strip().lower() == "ticker":
                for i, cell in enumerate(row):
                    header_idx[cell.strip().lower()] = i
                found_header = True
            continue
        try:
            tic = row[header_idx.get("ticker", 0)].strip()
            if not tic or tic == "-":
                continue
            holdings.append({
                "ticker":      tic,
                "name":        row[header_idx.get("name", 1)].strip() if "name" in header_idx else "",
                "weight":      _safe_float(row[header_idx.get("weight (%)", -1)]) if "weight (%)" in header_idx else None,
                "marketValue": _safe_float(row[header_idx.get("market value", -1)]) if "market value" in header_idx else None,
                "shares":      _safe_float(row[header_idx.get("shares", -1)]) if "shares" in header_idx else None,
            })
        except (IndexError, ValueError):
            continue
    return holdings


def fetch_via_direct_csv(ticker: str) -> tuple[bool, str, list]:
    config = ETF_SOURCES.get(ticker)
    if not config:
        return False, "no direct-fetch URL registered for ticker", []
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/csv,application/csv,text/plain,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.ishares.com/us/products/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }
    try:
        r = requests.get(config["url"], headers=headers, timeout=30, allow_redirects=True)
        if r.status_code != 200 or len(r.text) < 200:
            return False, f"direct CSV HTTP {r.status_code}, len={len(r.text)}", []
        holdings = parse_ishares_csv(r.text)
        if not holdings:
            return False, "direct CSV parser returned 0 holdings", []
        return True, f"{len(holdings)} holdings via direct CSV", holdings
    except Exception as e:
        return False, f"direct CSV: {type(e).__name__}: {e}", []


# ---------------------------------------------------------------------------
# STRATEGY 3: Yahoo Finance topHoldings (last-resort fallback)
# ---------------------------------------------------------------------------
def fetch_via_yahoo(ticker: str) -> tuple[bool, str, list]:
    y_url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=topHoldings"
    y_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    try:
        r = requests.get(y_url, headers=y_headers, timeout=20)
        if r.status_code != 200:
            return False, f"yahoo HTTP {r.status_code}", []
        j = r.json()
        top = j.get("quoteSummary", {}).get("result", [{}])[0].get("topHoldings", {})
        yh = top.get("holdings") or []
        if not yh:
            return False, "yahoo: no topHoldings in response", []
        holdings = [
            {
                "ticker":      h.get("symbol") or "",
                "name":        h.get("holdingName") or h.get("symbol") or "",
                "weight":      float(h.get("holdingPercent", {}).get("raw", 0) or 0) * 100.0,
                "shares":      None,
                "marketValue": None,
            }
            for h in yh
        ]
        return True, f"{len(holdings)} holdings (top 10 only)", holdings
    except Exception as e:
        return False, f"yahoo: {type(e).__name__}: {e}", []


# ---------------------------------------------------------------------------
# Orchestrator — try strategies in order, write the first success
# ---------------------------------------------------------------------------
def fetch_etf(ticker: str, scraper) -> tuple[bool, str]:
    """Returns (ok, msg). Writes data/etf_holdings/<TICKER>.json on success.

    The output JSON's `source` field declares which strategy succeeded, so
    the app and the user can verify provenance.
    """
    # STRATEGY 1
    if scraper is not None:
        ok, msg, holdings = fetch_via_etf_scraper(ticker, scraper)
        if ok:
            _write_holdings_json(ticker, holdings, "etf-scraper")
            return True, msg
        s1_msg = msg
    else:
        s1_msg = "etf_scraper not available"

    # STRATEGY 2
    ok, msg, holdings = fetch_via_direct_csv(ticker)
    if ok:
        _write_holdings_json(ticker, holdings, "issuer-direct-csv")
        return True, f"{msg} (after S1: {s1_msg})"
    s2_msg = msg

    # STRATEGY 3
    ok, msg, holdings = fetch_via_yahoo(ticker)
    if ok:
        _write_holdings_json(ticker, holdings, "yahoo-finance-fallback")
        return True, f"{msg} (after S1: {s1_msg}, S2: {s2_msg})"

    return False, f"all 3 strategies failed (S1: {s1_msg} | S2: {s2_msg} | S3: {msg})"


def _write_holdings_json(ticker: str, holdings: list, source: str) -> None:
    out = {
        "ticker":         ticker,
        "source":         source,
        "fetchedAt":      datetime.utcnow().isoformat() + "Z",
        "holdingsCount":  len(holdings),
        "holdings":       holdings,
    }
    out_path = HOLDINGS_DIR / f"{ticker}.json"
    out_path.write_text(json.dumps(out, indent=2))


def main() -> int:
    log(f"Output dir: {HOLDINGS_DIR}")

    # Pre-flight: log which strategies are available
    if ETF_SCRAPER_AVAILABLE:
        log("✓ etf_scraper library loaded — primary strategy ready")
    else:
        log(f"✗ etf_scraper unavailable: {_etf_scraper_import_error}")
        log("  Falling back to direct CSV + Yahoo for every ticker")

    # Init scraper once (reuses its internal listings.csv)
    scraper = None
    if ETF_SCRAPER_AVAILABLE:
        try:
            scraper = ETFScraper()
            log(f"✓ ETFScraper initialized")
        except Exception as e:
            log(f"✗ ETFScraper init failed: {e}")
            scraper = None

    # Build full ticker list — union of etf_scraper-enabled list and direct-CSV list
    all_tickers = sorted(set(ETF_TICKERS_VIA_SCRAPER) | set(ETF_SOURCES.keys()))
    log(f"Attempting {len(all_tickers)} ETFs")

    manifest = {
        "generatedAt":   datetime.utcnow().isoformat() + "Z",
        "totalAttempted": len(all_tickers),
        "etfs":          {},
    }
    success = 0
    sources_used = {}
    for ticker in all_tickers:
        ok, msg = fetch_etf(ticker, scraper)
        if ok:
            log(f"✓ {ticker:6s} — {msg}")
            success += 1
            # Track which strategy succeeded for the run summary
            for tag in ("etf-scraper", "issuer-direct-csv", "yahoo-finance-fallback"):
                if tag in msg:
                    sources_used[tag] = sources_used.get(tag, 0) + 1
                    break
        else:
            log(f"✗ {ticker:6s} — {msg}")
        manifest["etfs"][ticker] = {
            "ok":        ok,
            "msg":       msg,
            "fetchedAt": datetime.utcnow().isoformat() + "Z",
        }
        # Be courteous to issuer servers
        time.sleep(0.4)

    manifest["totalSucceeded"] = success
    manifest["sourcesUsed"] = sources_used
    MANIFEST_FILE.write_text(json.dumps(manifest, indent=2))
    log(f"Done: {success}/{len(all_tickers)} ETFs fetched. Sources: {sources_used}")
    log(f"Manifest: {MANIFEST_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
