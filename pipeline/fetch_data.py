#!/usr/bin/env python3
# ============================================================
#  >>> DESTINATION: TheMostLocal/TRAPP2-4  ->  pipeline/fetch_data.py
#  TRAPP2-4 - the NYSE equities that weren't in the app (z178). Same output as the other books' fetch_data.py
#  (data/master.csv + data/master.json, same columns), built for ~1,600 tickers instead of ~300.
# ============================================================
"""
TRAPP2-4 — Quote + fundamentals fetcher, sized for a large book.

Same job and the same columns as TRAPP2 / TRAPP2-2 / TRAPP2-3 (see their fetch_data.py), with what ~1,600
tickers need (z178):
  * PARALLEL - FETCH_WORKERS threads (default 6) instead of one ticker at a time; ~1,600 sequential Yahoo calls
    plus profiles ran past the nightly job's hour.
  * STAGGERED PROFILES - fundamentals (.info) are re-fetched every 48-96 hours, spread by ticker, instead of all
    of them every 24 hours; quotes are fetched every run. The balance-sheet / cash-flow gap-fill is computed when
    the profile is refreshed and kept with it, not re-requested every night.
  * A SMALLER CACHE - only the ~60 .info fields this script reads are cached (.profile_cache.json is committed).
  * NOTHING DROPS OUT - a ticker that fails (or isn't reached inside FETCH_BUDGET_MIN) keeps yesterday's row,
    so a Yahoo hiccup doesn't make names vanish from the app.
  * LISTING SYMBOLS - tickers.txt may hold exchange-list symbols (ABR$D, BF.A): each is fetched under its Yahoo
    form (ABR-PD, BF-A); the row's "ticker" is the Yahoo form and "symbolListed" keeps what the list said.
"""
import csv
import hashlib
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
TICKERS_FILE = DATA / "tickers.txt"
MASTER_CSV = DATA / "master.csv"
MASTER_JSON = DATA / "master.json"

# Field profile cache — fundamentals don't change intraday so we re-fetch
# the slow .info dict at most once every 24h per ticker. Quotes refresh every run.
PROFILE_CACHE = DATA / ".profile_cache.json"
PROFILE_TTL_HOURS = 24                     # kept for reference - TRAPP2-4 uses profile_ttl_hours() below
FETCH_WORKERS = int(os.environ.get("FETCH_WORKERS", "6"))
FETCH_BUDGET_MIN = float(os.environ.get("FETCH_BUDGET_MIN", "150"))
_CACHE_LOCK = threading.Lock()


def profile_ttl_hours(ticker):
    """48-96 h, fixed per ticker - about a third of the profiles refresh each night instead of all at once."""
    return 48 + int(hashlib.md5(ticker.encode()).hexdigest()[:4], 16) % 49


# the .info fields this script reads - nothing else is cached (the cache file is committed)
INFO_KEYS = {
    "address", "address1", "annualReportExpenseRatio", "averageVolume", "beta", "city", "companyOfficers", "country",
    "currency", "currentRatio", "debtToEquity", "dividendYield", "earningsGrowth", "ebitda", "enterpriseToEbitda",
    "enterpriseToRevenue", "exchange", "expenseRatio", "financialCurrency", "firstTradeDateEpochUtc", "freeCashflow",
    "fullTimeEmployees", "fundExpenseRatio", "grossMargins", "heldPercentInsiders", "heldPercentInstitutions", "industry",
    "ipoExpectedDate", "longBusinessSummary", "longName", "marketCap", "netExpenseRatio", "netIncomeToCommon",
    "operatingMargins", "payoutRatio", "pegRatio", "phone", "priceToBook", "profitMargins", "quickRatio", "quoteType",
    "returnOnAssets", "returnOnEquity", "revenueGrowth", "sector", "sharesOutstanding", "shortName",
    "shortPercentOfFloat", "state", "stockBasedCompensation", "totalAssets", "totalCash", "totalDebt", "totalRevenue",
    "totalStockholderEquity", "trailingEps", "trailingPE", "trailingPegRatio", "website",
}


def _trim_info(info):
    out = {k: info.get(k) for k in INFO_KEYS if info.get(k) not in (None, "", [])}
    if isinstance(out.get("companyOfficers"), list):
        out["companyOfficers"] = [{"name": (o or {}).get("name", "")} for o in out["companyOfficers"][:1]]
    return out

# Columns in deterministic order for master.csv
COLUMNS = [
    "ticker", "name", "price", "marketcap", "volume", "volumeavg",
    "priceopen", "low", "high", "close", "change", "changepct", "closeyest",
    "date", "high52", "low52", "beta", "shares", "pe", "eps",
    "sector", "industry", "description", "exchange", "ceo", "country",
    "ipodate", "isetf", "isfund", "isactive", "web_url", "image",
    "currency", "employees", "city", "state", "phone", "address",
    "dividend_yield", "fetched_at", "profile_fetched_at",
    "asset_class",  # NEW — Equity / Future / FX / Crypto / Index / Mutual Fund / Private / Option
    # Financial metrics for Research grading + bot engine (camelCase to match app)
    "returnOnEquity", "returnOnAssets", "grossMargin", "operatingMargin",
    "profitMargin", "revenueGrowth", "earningsGrowth", "revenue", "ebitda",
    "freeCashFlow", "netIncome", "stockBasedComp", "priceToBook", "evToEbitda",
    "evToRevenue", "totalDebt", "totalEquity", "totalAssets", "cash",
    # NEW — expense ratio (fund fee) + extra grading ratios
    "expenseRatio", "currentRatio", "quickRatio", "debtToEquity", "payoutRatio",
    "pegRatio", "heldPctInsiders", "heldPctInstitutions", "shortPctFloat",
    "financialCurrency",  # z123: the statements' (reporting) currency - Yahoo info.financialCurrency
]


def log(*args):
    print("[fetch_data]", *args, flush=True)


# Reference tickers the signal engine needs quotes for. Auto-merged with tickers.txt.
def classify_instrument(ticker):
    """
    Classify a ticker by its symbol pattern into asset class + sector for the
    Stock Book. yfinance returns null sector for non-equities, so we infer it
    from the ticker string itself.

    Returns dict with: asset_class, sector, industry, display_ticker.
    The display_ticker is what shows in the app UI (e.g. ^GSPC → "S&P 500").
    """
    t = (ticker or "").strip().upper()

    # === Futures / Commodities (X=F or X!) ===
    if t.endswith("=F") or t.endswith("!"):
        # Detect commodity type from root symbol
        root = t[:-2] if t.endswith("=F") else t.rstrip("!").rstrip("0123456789")
        FUTURES_MAP = {
            # Energy
            "CL": ("Energy", "Crude Oil WTI"),
            "BZ": ("Energy", "Crude Oil Brent"),
            "NG": ("Energy", "Natural Gas"),
            "RB": ("Energy", "RBOB Gasoline"),
            "HO": ("Energy", "Heating Oil"),
            "B0": ("Energy", "Crude Oil Brent (alt)"),
            # Metals
            "GC": ("Metals", "Gold"),
            "SI": ("Metals", "Silver"),
            "HG": ("Metals", "Copper"),
            "PL": ("Metals", "Platinum"),
            "PA": ("Metals", "Palladium"),
            "MGC": ("Metals", "Micro Gold"),
            "SIL": ("Metals", "Silver Micro"),
            # Equity index futures
            "ES": ("Equity Index Future", "S&P 500"),
            "NQ": ("Equity Index Future", "Nasdaq 100"),
            "YM": ("Equity Index Future", "Dow Jones"),
            "RTY": ("Equity Index Future", "Russell 2000"),
            "BTC": ("Equity Index Future", "Bitcoin Future"),
            # Rate futures
            "ZB": ("Treasury Future", "30Y Bond"),
            "ZN": ("Treasury Future", "10Y Note"),
            "ZF": ("Treasury Future", "5Y Note"),
            "ZT": ("Treasury Future", "2Y Note"),
            # Grains
            "ZC": ("Grains", "Corn"),
            "ZS": ("Grains", "Soybeans"),
            "ZW": ("Grains", "Wheat"),
            "ZO": ("Grains", "Oats"),
            "ZR": ("Grains", "Rough Rice"),
            "ZM": ("Grains", "Soybean Meal"),
            "ZL": ("Grains", "Soybean Oil"),
            "KE": ("Grains", "Hard Red Wheat"),
            # Softs
            "CC": ("Softs", "Cocoa"),
            "KC": ("Softs", "Coffee"),
            "CT": ("Softs", "Cotton"),
            "SB": ("Softs", "Sugar"),
            "OJ": ("Softs", "Orange Juice"),
            "LBS": ("Softs", "Lumber"),
            # Livestock
            "GF": ("Livestock", "Feeder Cattle"),
            "HE": ("Livestock", "Lean Hogs"),
            "LE": ("Livestock", "Live Cattle"),
            # Currency futures
            "DX": ("Currency Future", "US Dollar Index"),
        }
        info = FUTURES_MAP.get(root, ("Futures", root))
        return {
            "asset_class": "Future",
            "sector": "Commodities & Futures",
            "industry": info[0],
            "display_name": info[1],
        }

    # === FX (X=X) ===
    if t.endswith("=X"):
        pair = t[:-2]
        return {
            "asset_class": "FX",
            "sector": "Currencies",
            "industry": "Foreign Exchange",
            "display_name": pair if len(pair) >= 6 else f"USD/{pair}",
        }

    # === Crypto (X-USD or X-USDT) ===
    if t.endswith("-USD") or t.endswith("-USDT"):
        base = t.split("-")[0]
        CRYPTO_NAMES = {
            "BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana",
            "DOGE": "Dogecoin", "XRP": "XRP", "BNB": "BNB",
            "USDC": "USD Coin", "USDT": "Tether", "STETH": "Lido Staked Ether",
        }
        return {
            "asset_class": "Crypto",
            "sector": "Cryptocurrency",
            "industry": CRYPTO_NAMES.get(base, base),
            "display_name": CRYPTO_NAMES.get(base, base),
        }

    # === Index (^XXX, Yahoo foreign-exchange suffixes, special formats) ===
    # Yahoo uses .SS (Shanghai), .ME (Moscow), .TA (Tel Aviv), .JO (Johannesburg),
    # etc. for foreign-listed indexes that don't fit the ^XXX pattern.
    INDEX_SUFFIXES = (".SS", ".HK", ".SZ", ".KS", ".KQ", ".SI", ".AX")
    if (t.startswith("^")
        or ".ME" in t
        or "-STRD" in t
        or any(t.endswith(s) for s in INDEX_SUFFIXES)
        or t == "DX-Y.NYB"):
        INDEX_MAP = {
            "^GSPC": "S&P 500", "^DJI": "Dow Jones", "^IXIC": "Nasdaq Composite",
            "^NDX": "Nasdaq 100", "^RUT": "Russell 2000", "^NYA": "NYSE Composite",
            "^VIX": "VIX", "^VIX9D": "VIX 9-Day", "^MOVE": "MOVE Index",
            "^TNX": "US 10-Year Yield", "^FVX": "US 5-Year Yield", "^TYX": "US 30-Year Yield",
            "^FTSE": "FTSE 100", "^GDAXI": "DAX", "^FCHI": "CAC 40",
            "^STOXX50E": "EURO STOXX 50", "^N100": "Euronext 100", "^BFX": "BEL 20",
            "^N225": "Nikkei 225", "^HSI": "Hang Seng", "^KS11": "KOSPI",
            "^TWII": "TAIEX", "^AXJO": "ASX 200", "^BSESN": "BSE Sensex",
            "^KLSE": "FTSE Bursa Malaysia KLCI", "^JKSE": "Jakarta Composite",
            "^NZ50": "NZX 50", "^STI": "Straits Times",
            "^MERV": "S&P MERVAL", "^BVSP": "Bovespa", "^MXX": "IPC Mexico",
            "^IPSA": "S&P IPSA", "^GSPTSE": "S&P/TSX Composite",
            "^TA125.TA": "TA-125", "^CASE30": "EGX 30", "^JN0U.JO": "FTSE/JSE Top 40",
            "^XDE": "USD/EUR Index", "^XDB": "USD/GBP Index", "^XDN": "USD/JPY Index",
            "^XDA": "USD/AUD Index", "^XAX": "AMEX Composite",
            "^BUK100P": "Cboe UK 100",
            "MOEX.ME": "MOEX Russia",
            "000001.SS": "Shanghai Composite",
            "DX-Y.NYB": "US Dollar Index (DXY)",
            "^125904-USD-STRD": "USD-STRD Index",
        }
        display = INDEX_MAP.get(t, t.lstrip("^"))
        return {
            "asset_class": "Index",
            "sector": "Indices",
            "industry": "Stock Index" if not any(k in t for k in ["VIX", "MOVE", "TNX", "FVX", "TYX"]) else "Volatility / Rates",
            "display_name": display,
        }

    # === Mutual fund (5-char ticker ending in X) ===
    if len(t) == 5 and t.endswith("X") and t.isalpha():
        return {
            "asset_class": "Mutual Fund",
            "sector": "Mutual Funds",
            "industry": "Mutual Fund",
            "display_name": t,
        }

    # === Private (X.PVT) ===
    if t.endswith(".PVT"):
        base = t[:-4]
        PVT_NAMES = {
            "SPAX": "SpaceX", "OPAI": "OpenAI", "ANTH": "Anthropic",
            "STRI": "Stripe", "DATB": "Databricks",
        }
        return {
            "asset_class": "Private",
            "sector": "Private Equity",
            "industry": "Private",
            "display_name": PVT_NAMES.get(base, base),
        }

    # === Options (long string with C/P + strike at end) ===
    # Format: ROOT + YYMMDD + (C|P) + 8-digit strike
    if len(t) > 12 and any(c.isdigit() for c in t[-6:]):
        import re
        # Try to parse option symbol
        m = re.match(r"^([A-Z]+)(\d{6})([CP])(\d{8})$", t)
        if m:
            root, dt, cp, strike_x1000 = m.groups()
            strike = int(strike_x1000) / 1000
            exp = f"20{dt[:2]}-{dt[2:4]}-{dt[4:6]}"
            cp_label = "Call" if cp == "C" else "Put"
            return {
                "asset_class": "Option",
                "sector": "Options",
                "industry": f"{root} Options",
                "display_name": f"{root} {exp} {cp_label} ${strike:.0f}",
            }

    # === Default: equity (let yfinance fill sector) ===
    return {
        "asset_class": "Equity",
        "sector": None,    # yfinance will fill
        "industry": None,
        "display_name": None,
    }


REQUIRED_REFERENCE_TICKERS = [
    # US equity / volatility benchmarks
    "SPY", "QQQ", "DIA", "IWM",
    "^VIX", "^VIX9D", "^MOVE",
    # Credit
    "HYG", "LQD", "JNK", "EMB",
    # Sector ETFs (11 SPDRs)
    "XLK", "XLF", "XLV", "XLE", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC",
    # Equity-index futures
    "ES=F", "NQ=F", "YM=F", "RTY=F",
    # Energy futures
    "CL=F", "BZ=F", "NG=F", "RB=F", "HO=F",
    # Metal futures
    "GC=F", "SI=F", "HG=F", "PL=F", "PA=F",
    # Agricultural / grain futures (the user specifically asked for ZC=F)
    "ZC=F", "ZS=F", "ZW=F", "KC=F", "SB=F", "CC=F", "CT=F",
    # Interest rate futures
    "ZB=F", "ZN=F", "ZF=F", "ZT=F",
    # Crypto
    "BTC-USD", "ETH-USD", "SOL-USD",
    # FX pairs (currency)
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X",
    "NZDUSD=X", "USDCNY=X", "USDINR=X", "USDMXN=X",
    "DX=F",  # Dollar index future
    # Global / foreign equity indexes (user wants these in the FX bucket)
    "^GSPTSE",   # Canada (S&P/TSX Composite)
    "^MERV",     # Argentina (S&P MERVAL)
    "^BVSP",     # Brazil (Bovespa)
    "^MXX",      # Mexico (IPC)
    "^FTSE",     # UK (FTSE 100)
    "^GDAXI",    # Germany (DAX)
    "^FCHI",     # France (CAC 40)
    "^STOXX50E", # Eurozone (EURO STOXX 50)
    "^N225",     # Japan (Nikkei 225)
    "^HSI",      # Hong Kong (Hang Seng)
    "^AXJO",     # Australia (ASX 200)
    "^BSESN",    # India (BSE Sensex)
    "^KS11",     # South Korea (KOSPI)
    "^TWII",     # Taiwan (TAIEX)
    # Dollar / treasury / aggregate
    "UUP",
    "TLT", "IEF", "SHY", "BND", "AGG", "GOVT", "TIP",
]


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


LISTED = {}          # Yahoo form -> the symbol as tickers.txt wrote it (when different)


def load_tickers():
    tickers = []
    if TICKERS_FILE.exists():
        for line in TICKERS_FILE.read_text().splitlines():
            raw = line.strip().split("#")[0].strip().upper()
            t = norm_symbol(raw)                           # z178: ABR$D -> ABR-PD, BF.A -> BF-A
            if t:
                tickers.append(t)
                if raw != t:
                    LISTED[t] = raw
    else:
        log(f"⚠ {TICKERS_FILE} missing — create it with one ticker per line")
    # Always include reference tickers (deduped, preserves order)
    tickers = tickers + REQUIRED_REFERENCE_TICKERS
    seen = set()
    out = []
    for t in tickers:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def load_profile_cache():
    if not PROFILE_CACHE.exists():
        return {}
    try:
        return json.loads(PROFILE_CACHE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_profile_cache(cache):
    PROFILE_CACHE.parent.mkdir(parents=True, exist_ok=True)
    PROFILE_CACHE.write_text(json.dumps(cache, separators=(",", ":")))


def profile_is_fresh(cache_entry, ticker=""):
    if not cache_entry or "fetched_at" not in cache_entry:
        return False
    try:
        fetched = datetime.fromisoformat(cache_entry["fetched_at"])
    except (ValueError, TypeError):
        return False
    age_hours = (datetime.now(timezone.utc) - fetched).total_seconds() / 3600
    return age_hours < (profile_ttl_hours(ticker) if ticker else PROFILE_TTL_HOURS)


def safe(d, *keys, default=""):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
        if d is None:
            return default
    return d if d is not None else default


def fmt_num(v):
    if v is None or v == "" or v == "N/A":
        return ""
    try:
        f = float(v)
    except (ValueError, TypeError):
        return str(v)
    # yfinance returns NaN/inf for halted, pre-market or missing quotes. Write a
    # blank, never the string "nan" (the app's CSV parse turned it into NaN).
    if f != f or f in (float("inf"), float("-inf")):
        return ""
    return f"{f:.6f}".rstrip("0").rstrip(".") if "." in str(v) else str(v)


def fetch_quote(ticker, profile_cache):
    """Pull current quote + fundamentals for one ticker. Returns row dict."""
    t = yf.Ticker(ticker)

    # Fast price path: yfinance .fast_info is light. Falls back to history if missing.
    fast = {}
    try:
        fast = dict(t.fast_info) if t.fast_info else {}
    except Exception:
        fast = {}

    price = fast.get("last_price") or fast.get("regular_market_price")
    prev_close = fast.get("previous_close") or fast.get("regular_market_previous_close")
    open_p = fast.get("open")
    day_high = fast.get("day_high")
    day_low = fast.get("day_low")
    high52 = fast.get("year_high")
    low52 = fast.get("year_low")
    vol = fast.get("last_volume") or fast.get("regular_market_volume")

    # Backstop: pull last 2 bars via history if fast_info lacked anything
    if price is None or prev_close is None:
        try:
            hist = t.history(period="5d", interval="1d", auto_adjust=False)
            if len(hist) >= 1:
                price = price or float(hist["Close"].iloc[-1])
                if len(hist) >= 2:
                    prev_close = prev_close or float(hist["Close"].iloc[-2])
                open_p = open_p or float(hist["Open"].iloc[-1])
                day_high = day_high or float(hist["High"].iloc[-1])
                day_low = day_low or float(hist["Low"].iloc[-1])
                vol = vol or float(hist["Volume"].iloc[-1])
        except Exception as e:
            log(f"  ✗ {ticker} history backstop failed: {e}")

    if price is None:
        return None

    change = (price - prev_close) if prev_close else None
    changepct = (change / prev_close) if change is not None and prev_close else None

    # Fundamentals — heavy call; cache it 24h
    cached = profile_cache.get(ticker, {})
    info = cached.get("info") if profile_is_fresh(cached, ticker) else None
    profile_fetched_at = cached.get("fetched_at", "")
    refreshed = False
    if info is None:
        try:
            info = _trim_info(t.info or {})
            profile_fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
            with _CACHE_LOCK:
                profile_cache[ticker] = {"info": info, "fetched_at": profile_fetched_at}
            refreshed = True
        except Exception as e:
            log(f"  ⚠ {ticker} .info failed: {e}")
            info = cached.get("info") or {}

    row = {
        "ticker": ticker,
        "name": safe(info, "longName") or safe(info, "shortName"),
        "price": fmt_num(price),
        "marketcap": fmt_num(safe(info, "marketCap")),
        "volume": fmt_num(vol),
        "volumeavg": fmt_num(safe(info, "averageVolume")),
        "priceopen": fmt_num(open_p),
        "low": fmt_num(day_low),
        "high": fmt_num(day_high),
        "close": fmt_num(price),
        "change": fmt_num(change),
        "changepct": fmt_num(changepct * 100 if changepct is not None else ""),
        "closeyest": fmt_num(prev_close),
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "high52": fmt_num(high52),
        "low52": fmt_num(low52),
        "beta": fmt_num(safe(info, "beta")),
        "shares": fmt_num(safe(info, "sharesOutstanding")),
        "pe": fmt_num(safe(info, "trailingPE")),
        "eps": fmt_num(safe(info, "trailingEps")),
        "sector": safe(info, "sector"),
        "industry": safe(info, "industry"),
        "description": (safe(info, "longBusinessSummary") or "")[:2000],
        "exchange": safe(info, "exchange"),
        "ceo": (info.get("companyOfficers") or [{}])[0].get("name", "") if isinstance(info.get("companyOfficers"), list) and info.get("companyOfficers") else "",
        "country": safe(info, "country"),
        "ipodate": safe(info, "ipoExpectedDate") or safe(info, "firstTradeDateEpochUtc"),
        "isetf": "true" if safe(info, "quoteType") == "ETF" else "false",
        "isfund": "true" if safe(info, "quoteType") in ("MUTUALFUND", "FUND") else "false",
        "isactive": "true",
        "web_url": safe(info, "website"),
        "image": "",  # yfinance dropped logo_url. App synthesizes via Clearbit using web_url.
        "currency": safe(info, "currency"),
        "financialCurrency": safe(info, "financialCurrency"),   # z123: TSM trades in USD, reports in TWD
        "employees": fmt_num(safe(info, "fullTimeEmployees")),
        "city": safe(info, "city"),
        "state": safe(info, "state"),
        "phone": safe(info, "phone"),
        "address": safe(info, "address1") or safe(info, "address"),
        "dividend_yield": fmt_num(safe(info, "dividendYield")),
        # --- Financial metrics for Research grading + the bot engine ---
        # Yahoo's .info already carries these; we just extract them. camelCase
        # so they match what the app reads (no client-side remap needed).
        "returnOnEquity": fmt_num(safe(info, "returnOnEquity")),
        "returnOnAssets": fmt_num(safe(info, "returnOnAssets")),
        "grossMargin": fmt_num(safe(info, "grossMargins")),
        "operatingMargin": fmt_num(safe(info, "operatingMargins")),
        "profitMargin": fmt_num(safe(info, "profitMargins")),
        "revenueGrowth": fmt_num(safe(info, "revenueGrowth")),
        "earningsGrowth": fmt_num(safe(info, "earningsGrowth")),
        "revenue": fmt_num(safe(info, "totalRevenue")),
        "ebitda": fmt_num(safe(info, "ebitda")),
        "freeCashFlow": fmt_num(safe(info, "freeCashflow")),
        "netIncome": fmt_num(safe(info, "netIncomeToCommon")),
        "stockBasedComp": fmt_num(safe(info, "stockBasedCompensation")),
        "priceToBook": fmt_num(safe(info, "priceToBook")),
        "evToEbitda": fmt_num(safe(info, "enterpriseToEbitda")),
        "evToRevenue": fmt_num(safe(info, "enterpriseToRevenue")),
        "totalDebt": fmt_num(safe(info, "totalDebt")),
        "totalEquity": fmt_num(safe(info, "totalStockholderEquity")),
        "totalAssets": fmt_num(safe(info, "totalAssets")),
        "cash": fmt_num(safe(info, "totalCash")),
        # ---- NEW: expense ratio (ETFs/funds) + extra ratios for grading ----
        # Expense ratio is an ANNUAL fund fee (decimal, 0.0095 = 0.95%/yr). yfinance
        # exposes it under a few keys depending on the security; take whichever is
        # present. The app factors it into holding/trading cost AND the grade.
        "expenseRatio": fmt_num(
            safe(info, "annualReportExpenseRatio")
            or safe(info, "netExpenseRatio")
            or safe(info, "expenseRatio")
            or safe(info, "fundExpenseRatio")
        ),
        "currentRatio": fmt_num(safe(info, "currentRatio")),
        "quickRatio": fmt_num(safe(info, "quickRatio")),
        "debtToEquity": fmt_num(safe(info, "debtToEquity")),
        "payoutRatio": fmt_num(safe(info, "payoutRatio")),
        "pegRatio": fmt_num(safe(info, "trailingPegRatio") or safe(info, "pegRatio")),
        "heldPctInsiders": fmt_num(safe(info, "heldPercentInsiders")),
        "heldPctInstitutions": fmt_num(safe(info, "heldPercentInstitutions")),
        "shortPctFloat": fmt_num(safe(info, "shortPercentOfFloat")),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "profile_fetched_at": profile_fetched_at,
    }

    # ---- Balance-sheet / cashflow fallback ----
    # Foreign ADRs (e.g. TSM) often return EMPTY balance-sheet fields from
    # yfinance .info, leaving totalEquity / totalAssets / totalDebt / SBC blank
    # so Research metrics like ROTCE, Debt/Equity and SBC% can't compute. The
    # financial STATEMENTS usually carry the numbers even when .info doesn't.
    # Best-effort: fills only the gaps, never overwrites, never fails the row.
    try:
        _bs_cached = (profile_cache.get(ticker) or {}).get("bs")
        if not refreshed and isinstance(_bs_cached, dict):     # z178: the gap-fill rides with the profile
            for _k, _v in _bs_cached.items():
                if _v and not row.get(_k):
                    row[_k] = _v
        _need_bs = refreshed and ((not row.get("totalEquity")) or (not row.get("totalAssets")) or (not row.get("totalDebt")))
        if _need_bs:
            _bs = t.balance_sheet
            if _bs is not None and not _bs.empty:
                _col = _bs.columns[0]
                def _bsval(*names):
                    for n in names:
                        if n in _bs.index:
                            v = _bs.loc[n, _col]
                            if v == v and v is not None:
                                return float(v)
                    return None
                if not row.get("totalEquity"):
                    row["totalEquity"] = fmt_num(_bsval(
                        "Stockholders Equity", "Total Equity Gross Minority Interest", "Common Stock Equity"))
                if not row.get("totalAssets"):
                    row["totalAssets"] = fmt_num(_bsval("Total Assets"))
                if not row.get("totalDebt"):
                    row["totalDebt"] = fmt_num(_bsval("Total Debt", "Long Term Debt"))
        if refreshed and not row.get("stockBasedComp"):
            _cf = t.cashflow
            if _cf is not None and not _cf.empty and "Stock Based Compensation" in _cf.index:
                v = _cf.loc["Stock Based Compensation", _cf.columns[0]]
                if v == v and v is not None:
                    row["stockBasedComp"] = fmt_num(float(v))
        if refreshed:
            with _CACHE_LOCK:
                if ticker in profile_cache:
                    profile_cache[ticker]["bs"] = {k: row.get(k) for k in ("totalEquity", "totalAssets", "totalDebt", "stockBasedComp") if row.get(k)}
    except Exception as _e:
        pass

    # Inject classifier-derived sector/industry/asset_class for non-equity
    # instruments (futures, FX, crypto, indices, mutual funds, private, options).
    # yfinance returns null sector for these, so we infer from the ticker pattern.
    classification = classify_instrument(ticker)
    row["asset_class"] = classification["asset_class"]
    if classification["sector"] and not row["sector"]:
        row["sector"] = classification["sector"]
    if classification["industry"] and not row["industry"]:
        row["industry"] = classification["industry"]
    if classification["display_name"] and not safe(info, "shortName") and not safe(info, "longName"):
        row["name"] = classification["display_name"]
    if ticker in LISTED:
        row["symbolListed"] = LISTED[ticker]

    return row


def _previous_rows():
    try:
        rows = json.loads(MASTER_JSON.read_text())
        return {r.get("ticker"): r for r in rows if isinstance(r, dict) and r.get("ticker")}
    except Exception:
        return {}


def main():
    tickers = load_tickers()
    if not tickers:
        log("No tickers to fetch. Exiting.")
        return 1
    t0 = time.time()
    log(f"Fetching {len(tickers)} tickers via yfinance · {FETCH_WORKERS} workers · budget {FETCH_BUDGET_MIN:.0f} min…")

    profile_cache = load_profile_cache()
    previous = _previous_rows()
    rows_by = {}
    failed, skipped = [], []
    stop = threading.Event()

    def one(tic):
        if stop.is_set():
            return tic, None, "budget"
        try:
            return tic, fetch_quote(tic, profile_cache), None
        except Exception as e:
            return tic, None, str(e)[:120]

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = [ex.submit(one, t) for t in tickers]
        for n, fu in enumerate(as_completed(futs), 1):
            tic, row, err = fu.result()
            if row:
                rows_by[tic] = row
            elif err == "budget":
                skipped.append(tic)
            else:
                failed.append(tic)
                if len(failed) <= 40:
                    log(f"  ✗ {tic}: {err or 'no price data'}")
            if n % 100 == 0:
                log(f"  … {n}/{len(tickers)} ({len(rows_by)} OK) · {(time.time() - t0) / 60:.1f} min")
                with _CACHE_LOCK:
                    save_profile_cache(profile_cache)
            if not stop.is_set() and (time.time() - t0) / 60 > FETCH_BUDGET_MIN:
                log(f"  ⏱ budget of {FETCH_BUDGET_MIN:.0f} min reached - the rest keep their previous rows")
                stop.set()

    with _CACHE_LOCK:
        save_profile_cache(profile_cache)

    # every ticker keeps a row: fresh, else yesterday's (a Yahoo hiccup must not drop names from the app)
    rows, carried = [], 0
    for t in tickers:
        if t in rows_by:
            rows.append(rows_by[t])
        elif t in previous:
            rows.append(previous[t])
            carried += 1

    DATA.mkdir(parents=True, exist_ok=True)
    with MASTER_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS + ["symbolListed"], extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    MASTER_JSON.write_text(json.dumps(rows, separators=(",", ":")))

    log(f"✓ {len(rows_by)}/{len(tickers)} fetched · {carried} kept their previous row · {len(failed)} failed · "
        f"{len(skipped)} past the budget · {len(rows)} rows written · {(time.time() - t0) / 60:.1f} min")
    return 0


if __name__ == "__main__":
    sys.exit(main())
