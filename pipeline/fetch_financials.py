#!/usr/bin/env python3
"""
TRAPP2 Financial Statements Fetcher
=====================================

Pulls MULTI-YEAR income statement, balance sheet, and cash-flow data from Yahoo
Finance (free, no API key, no rate limit) and writes one JSON file per ticker to
data/financials/<TICKER>.json.

WHY THIS EXISTS
---------------
The app previously fetched these statements live from FMP (Financial Modeling
Prep) on every company-tab open. FMP's free tier exhausts quickly, so NVDA and
other tickers would show BLANK financials once the quota ran out. By fetching
from Yahoo once and committing the result to the repo, the app loads precise,
consistent statements from raw.githubusercontent.com with no per-view API cost.

Financial statements only change at earnings, so this script only needs to run
each earnings season (or weekly is fine — it's cheap). The committed JSON is the
durable, cross-device source of truth.

OUTPUT SHAPE (per ticker) — matches what the app's financials renderer expects,
which mirrors FMP's statement objects so the frontend needs no shape changes:

    {
      "ticker": "NVDA",
      "updated": "2026-06-14T00:00:00Z",
      "source": "yahoo",
      "currency": "USD",
      "income":   [ {calendarYear, date, revenue, costOfRevenue, grossProfit,
                     operatingExpenses, operatingIncome, ebitda, netIncome,
                     epsdiluted, eps}, ... up to 4 years ],
      "balance":  [ {calendarYear, date, cashAndCashEquivalents,
                     shortTermInvestments, totalCurrentAssets, totalAssets,
                     totalDebt, totalLiabilities, totalStockholdersEquity,
                     netDebt}, ... ],
      "cashflow": [ {calendarYear, date, operatingCashFlow, capitalExpenditure,
                     freeCashFlow, stockBasedCompensation, dividendsPaid}, ... ]
    }

USAGE
-----
    python pipeline/fetch_financials.py                 # all tickers in data/tickers.txt
    python pipeline/fetch_financials.py NVDA AAPL MSFT  # specific tickers
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    print("yfinance not installed. Run: pip install yfinance", file=sys.stderr)
    sys.exit(1)

# ---- Paths ----
HERE = Path(__file__).resolve().parent
DATA = HERE.parent / "data"
FIN_DIR = DATA / "financials"
FIN_DIR.mkdir(parents=True, exist_ok=True)
TICKERS_FILE = DATA / "tickers.txt"


def log(msg: str) -> None:
    print(msg, flush=True)


def _num(v):
    """Coerce a pandas/numpy cell to a clean float or None (NaN-safe)."""
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def _col_year(col) -> str:
    """Statement columns are Timestamps; return the 4-digit year as a string."""
    try:
        return str(col.year)
    except AttributeError:
        s = str(col)
        return s[:4] if len(s) >= 4 else s


def _col_date(col) -> str:
    try:
        return col.strftime("%Y-%m-%d")
    except AttributeError:
        return str(col)[:10]


def _row(df, *names):
    """Return a {col: value} accessor for the first matching statement row name.

    Yahoo's statement row labels vary; we try several aliases and return a
    function that pulls that row's value for a given column.
    """
    if df is None or df.empty:
        return lambda col: None
    for n in names:
        if n in df.index:
            series = df.loc[n]
            return lambda col, _s=series: _num(_s.get(col))
    return lambda col: None


def fetch_income(df) -> list:
    """Income statement df → list of FMP-shaped dicts (newest first).
    Works for BOTH annual (t.financials) and quarterly (t.quarterly_financials)."""
    if df is None or getattr(df, "empty", True):
        return []
    revenue = _row(df, "Total Revenue", "Operating Revenue")
    cost = _row(df, "Cost Of Revenue", "Cost of Revenue", "Reconciled Cost Of Revenue")
    gross = _row(df, "Gross Profit")
    opex = _row(df, "Operating Expense", "Total Operating Expenses")
    opinc = _row(df, "Operating Income", "Total Operating Income As Reported")
    ebitda = _row(df, "EBITDA", "Normalized EBITDA")
    netinc = _row(df, "Net Income", "Net Income Common Stockholders",
                  "Net Income From Continuing Operation Net Minority Interest")
    eps_d = _row(df, "Diluted EPS")
    eps_b = _row(df, "Basic EPS")
    out = []
    for col in df.columns:
        out.append({
            "calendarYear": _col_year(col),
            "date": _col_date(col),
            "revenue": revenue(col),
            "costOfRevenue": cost(col),
            "grossProfit": gross(col),
            "operatingExpenses": opex(col),
            "operatingIncome": opinc(col),
            "ebitda": ebitda(col),
            "netIncome": netinc(col),
            "epsdiluted": eps_d(col),
            "eps": eps_d(col) if eps_d(col) is not None else eps_b(col),
        })
    return out


def fetch_balance(df) -> list:
    if df is None or getattr(df, "empty", True):
        return []
    cash = _row(df, "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments")
    sti = _row(df, "Other Short Term Investments", "Short Term Investments")
    tca = _row(df, "Current Assets", "Total Current Assets")
    ta = _row(df, "Total Assets")
    debt = _row(df, "Total Debt", "Long Term Debt And Capital Lease Obligation")
    tl = _row(df, "Total Liabilities Net Minority Interest", "Total Liabilities")
    teq = _row(df, "Stockholders Equity", "Total Equity Gross Minority Interest", "Common Stock Equity")
    netdebt = _row(df, "Net Debt")
    out = []
    for col in df.columns:
        out.append({
            "calendarYear": _col_year(col),
            "date": _col_date(col),
            "cashAndCashEquivalents": cash(col),
            "shortTermInvestments": sti(col),
            "totalCurrentAssets": tca(col),
            "totalAssets": ta(col),
            "totalDebt": debt(col),
            "totalLiabilities": tl(col),
            "totalStockholdersEquity": teq(col),
            "netDebt": netdebt(col),
        })
    return out


def fetch_cashflow(df) -> list:
    if df is None or getattr(df, "empty", True):
        return []
    ocf = _row(df, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities")
    capex = _row(df, "Capital Expenditure", "Purchase Of PPE")
    fcf = _row(df, "Free Cash Flow")
    sbc = _row(df, "Stock Based Compensation")
    div = _row(df, "Cash Dividends Paid", "Common Stock Dividend Paid")
    out = []
    for col in df.columns:
        ocf_v, capex_v = ocf(col), capex(col)
        fcf_v = fcf(col)
        # Derive FCF if Yahoo didn't provide it directly.
        if fcf_v is None and ocf_v is not None and capex_v is not None:
            fcf_v = ocf_v + capex_v  # capex is negative on the statement
        out.append({
            "calendarYear": _col_year(col),
            "date": _col_date(col),
            "operatingCashFlow": ocf_v,
            "capitalExpenditure": capex_v,
            "freeCashFlow": fcf_v,
            "stockBasedCompensation": sbc(col),
            "dividendsPaid": div(col),
        })
    return out


def fetch_one(ticker: str) -> bool:
    ticker = ticker.strip().upper()
    if not ticker:
        return False
    try:
        t = yf.Ticker(ticker)
        # ---- Annual ----
        def _safe(attr):
            try: return getattr(t, attr)
            except Exception: return None
        income = fetch_income(_safe("financials"))
        balance = fetch_balance(_safe("balance_sheet"))
        cashflow = fetch_cashflow(_safe("cashflow"))
        # ---- Quarterly (the part that was missing) ----
        income_q = fetch_income(_safe("quarterly_financials"))
        balance_q = fetch_balance(_safe("quarterly_balance_sheet"))
        cashflow_q = fetch_cashflow(_safe("quarterly_cashflow"))
        # Tag quarterly rows with a readable "Q# YYYY" period from their date.
        def _tag_q(rows):
            for r in rows:
                d = r.get("date") or ""
                try:
                    mo = int(d[5:7]); yr = d[:4]
                    r["period"] = f"Q{(mo - 1) // 3 + 1} {yr}"
                except Exception:
                    r["period"] = "Q"
            return rows
        income_q = _tag_q(income_q); balance_q = _tag_q(balance_q); cashflow_q = _tag_q(cashflow_q)
        if not income and not balance and not cashflow and not income_q and not balance_q and not cashflow_q:
            log(f"  ⚠ {ticker}: no statement data (may be an ETF/FX/index)")
            return False
        # z123: statements are in the REPORTING currency (Yahoo info.financialCurrency). It
        # differs from the trading currency for ADRs (TSM: listed in USD, reports in TWD) and
        # some cross-listings (Glencore: GBp listing, reports in USD). "currency" = the statements'.
        trade_ccy, fin_ccy = None, None
        try:
            fc = t.fast_info.get("currency") if hasattr(t, "fast_info") else None
            if fc:
                trade_ccy = str(fc)
        except Exception:
            pass
        try:
            fin_ccy = (t.info or {}).get("financialCurrency") or None
        except Exception:
            fin_ccy = None
        currency = str(fin_ccy or trade_ccy or "USD")
        payload = {
            "ticker": ticker,
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "yahoo",
            "currency": currency,
            "financialCurrency": fin_ccy,
            "tradingCurrency": trade_ccy,
            "income": income,
            "balance": balance,
            "cashflow": cashflow,
            # Quarterly statements (newest first). Same shape as the annual
            # arrays, plus a "period" label like "Q3 2025".
            "incomeQuarterly": income_q,
            "balanceQuarterly": balance_q,
            "cashflowQuarterly": cashflow_q,
        }
        out_path = FIN_DIR / f"{ticker}.json"
        out_path.write_text(json.dumps(payload, separators=(",", ":")))
        log(f"  ✓ {ticker}: {len(income)}y/{len(income_q)}q income · "
            f"{len(balance)}y/{len(balance_q)}q balance · {len(cashflow)}y/{len(cashflow_q)}q cashflow")
        return True
    except Exception as e:
        log(f"  ✗ {ticker}: {e}")
        return False


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


def load_tickers() -> list:
    if not TICKERS_FILE.exists():
        log(f"No {TICKERS_FILE}; pass tickers as arguments instead.")
        return []
    out = []
    for line in TICKERS_FILE.read_text().splitlines():
        s = norm_symbol(line.split("#")[0])                 # z178: ABR$D -> ABR-PD, BRK.B -> BRK-B (and inline comments)
        if s and not s.endswith(("-UN", "-WT", "-RI")):
            out.append(s)
    return out


def main():
    args = [norm_symbol(a) for a in sys.argv[1:]]
    tickers = args if args else load_tickers()
    if not tickers:
        log("No tickers to process.")
        return
    log(f"Fetching financial statements for {len(tickers)} ticker(s)…")
    ok = 0
    for i, tk in enumerate(tickers, 1):
        if fetch_one(tk):
            ok += 1
        # Gentle pacing so Yahoo doesn't throttle on large universes.
        if i % 25 == 0:
            log(f"  … {i}/{len(tickers)} ({ok} ok)")
            time.sleep(1)
    log(f"Done. {ok}/{len(tickers)} tickers have financial statements in data/financials/.")


if __name__ == "__main__":
    main()
