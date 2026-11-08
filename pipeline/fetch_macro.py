#!/usr/bin/env python3
"""
TRAPP2 — FRED macro series fetcher.

Reads FRED_API_KEY from env. Writes data/macro/<series_id>.json for each series.
Each file: {"series_id", "title", "frequency", "observations": [{"date", "value"}, ...]}

Add free API key from https://fred.stlouisfed.org/docs/api/api_key.html
as a GitHub repo secret named FRED_API_KEY.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import MACRO, log, write_json, utc_now_iso

# Series matter most for regime detection: growth + inflation + liquidity + curve + risk
SERIES = [
    ("GDP",          "Real GDP",                  "quarterly"),
    ("INDPRO",       "Industrial Production",     "monthly"),
    ("UNRATE",       "Unemployment Rate",         "monthly"),
    ("PAYEMS",       "Nonfarm Payrolls",          "monthly"),
    ("CPIAUCSL",     "CPI All Urban",             "monthly"),
    ("PCEPI",        "PCE Price Index",           "monthly"),
    ("DFF",          "Effective Fed Funds Rate",  "daily"),
    ("M2SL",         "M2 Money Stock",            "monthly"),
    ("DGS3MO",       "3-Month Treasury",          "daily"),
    ("DGS2",         "2-Year Treasury",           "daily"),
    ("DGS10",        "10-Year Treasury",          "daily"),
    ("T10Y2Y",       "10Y-2Y Spread",             "daily"),
    ("T10Y3M",       "10Y-3M Spread",             "daily"),
    ("BAMLH0A0HYM2", "High Yield OAS",            "daily"),
    ("VIXCLS",       "VIX",                       "daily"),
    ("DCOILWTICO",   "WTI Crude",                 "daily"),
    ("DEXUSEU",      "USD/EUR",                   "daily"),
]


def fetch_series(series_id, api_key, start="2000-01-01"):
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
    }
    url = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            payload = json.loads(r.read())
    except Exception as e:
        log(f"  ✗ {series_id}: {e}")
        return None
    obs = payload.get("observations", [])
    cleaned = []
    for o in obs:
        d = o.get("date")
        v = o.get("value")
        if not d or v in (".", "", None):
            continue
        try:
            cleaned.append({"date": d, "value": float(v)})
        except (ValueError, TypeError):
            continue
    return {
        "series_id": series_id,
        "fetched_at": utc_now_iso(),
        "observations": cleaned,
    }


def main():
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        log("FRED_API_KEY not set. Skipping macro fetch.")
        log("Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html")
        return 0

    MACRO.mkdir(parents=True, exist_ok=True)
    log(f"Fetching {len(SERIES)} FRED series → {MACRO}")
    n_ok = 0
    for sid, name, freq in SERIES:
        data = fetch_series(sid, api_key)
        if data is None:
            continue
        data["title"] = name
        data["frequency"] = freq
        write_json(MACRO / f"{sid}.json", data, compact=True)
        log(f"  ✓ {sid:14s} {len(data['observations']):>6d} obs · {name}")
        n_ok += 1
        time.sleep(0.15)
    log(f"Wrote {n_ok}/{len(SERIES)} series.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
