# ============================================================
#  >>> DESTINATION: TheMostLocal/TRAPP2-CORE   (new repo, z158)
#  >>> FILE PATH:   pipeline/alpaca_data.py
#
#  ALPACA MARKET DATA - official US stock / ETF data, hundreds of symbols per request (free account).
#  Free plan: consolidated (SIP) history up to 15 minutes ago, real-time from IEX. So:
#    daily(symbols)    -> {T: [(date, o, h, l, c, v)]}  last ~5 sessions; today's bar = SIP to 16 min ago,
#                         its close / high / low brought up to date with IEX's latest trade
#    intraday(symbols) -> {T: [[ts_sec, o, h, l, c, v]]} today's 5-minute bars, 0400 ET on (SIP to 16 min
#                         ago + IEX for the last 16 minutes)
#  Symbols in Yahoo form in and out (BRK-B <-> BRK.B). Anything Alpaca doesn't return is simply absent -
#  the caller falls back to Yahoo for those. Keys: ALPACA_KEY_ID / ALPACA_SECRET_KEY (repo secrets).
# ============================================================
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://data.alpaca.markets"
KEY = (os.environ.get("ALPACA_KEY_ID") or "").strip()
SECRET = (os.environ.get("ALPACA_SECRET_KEY") or "").strip()
BATCH = int(os.environ.get("ALPACA_BATCH", "200"))


def enabled():
    return bool(KEY and SECRET)


def eligible(t):
    """US-listed stocks / ETFs only - indices, FX, futures, crypto and foreign listings stay on Yahoo."""
    t = str(t or "").upper()
    if not t or t.startswith("^") or t.endswith(("=X", "=F", "-USD")) or "." in t:
        return False
    return len(t) <= 7


def _a(t):
    return t.replace("-", ".")


def _y(t):
    return t.replace(".", "-")


def _get(path, params, tries=4):
    url = f"{BASE}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SECRET, "Accept": "application/json"})
    for k in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and k < tries - 1:
                time.sleep(2 + 3 * k)
                continue
            raise
    return {}


def _bars(symbols, timeframe, start, end, feed):
    """{yahoo symbol: [bar dicts]} - multi-symbol, every page."""
    out = {}
    for i in range(0, len(symbols), BATCH):
        chunk = [_a(s) for s in symbols[i:i + BATCH]]
        token = None
        while True:
            p = {"symbols": ",".join(chunk), "timeframe": timeframe, "start": start, "end": end, "limit": 10000,
                 "adjustment": "raw", "feed": feed, "sort": "asc"}
            if token:
                p["page_token"] = token
            j = _get("/v2/stocks/bars", p)
            for s, rows in (j.get("bars") or {}).items():
                out.setdefault(_y(s), []).extend(rows or [])
            token = j.get("next_page_token")
            if not token:
                break
    return out


def _snapshots(symbols, feed="iex"):
    out = {}
    for i in range(0, len(symbols), BATCH):
        j = _get("/v2/stocks/snapshots", {"symbols": ",".join(_a(s) for s in symbols[i:i + BATCH]), "feed": feed})
        for s, v in (j or {}).items():
            if isinstance(v, dict):
                out[_y(s)] = v
    return out


def _ts(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def daily(symbols, now=None):
    now = now or datetime.now(timezone.utc)
    syms = [s for s in symbols if eligible(s)]
    if not (enabled() and syms):
        return {}
    end = (now - timedelta(minutes=16)).strftime("%Y-%m-%dT%H:%M:%SZ")
    start = (now - timedelta(days=9)).strftime("%Y-%m-%dT00:00:00Z")
    bars = _bars(syms, "1Day", start, end, "sip")
    snap = {}
    try:
        snap = _snapshots(syms, "iex")
    except Exception:
        pass
    out = {}
    for s, rows in bars.items():
        # daily bars are stamped at the session's midnight (ET) in UTC - the date part is the session date
        rows = [(str(b["t"])[:10], float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"]), float(b.get("v") or 0))
                for b in rows if b.get("c") is not None]
        if not rows:
            continue
        lt = (snap.get(s) or {}).get("latestTrade") or {}
        if lt.get("p") and lt.get("t") and str(lt["t"])[:10] == rows[-1][0] and _ts(lt["t"]) > _ts(end):
            d, o, h, l, c, v = rows[-1]
            p = float(lt["p"])
            rows[-1] = (d, o, max(h, p), min(l, p), p, v)      # today's bar brought up to the latest IEX trade
        out[s] = rows[-6:]
    return out


def intraday(symbols, day_start_utc, now=None):
    """Today's 5-minute bars from 0400 ET: SIP up to 16 min ago, IEX after that."""
    now = now or datetime.now(timezone.utc)
    syms = [s for s in symbols if eligible(s)]
    if not (enabled() and syms):
        return {}
    cut = now - timedelta(minutes=16)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    sip = _bars(syms, "5Min", day_start_utc.strftime(fmt), cut.strftime(fmt), "sip") if cut > day_start_utc else {}
    iex = {}
    try:
        iex = _bars(syms, "5Min", max(cut, day_start_utc).strftime(fmt), now.strftime(fmt), "iex")
    except Exception:
        pass
    out = {}
    for s in set(sip) | set(iex):
        seen, rows = set(), []
        for b in (sip.get(s) or []) + (iex.get(s) or []):
            ts = int(_ts(b["t"]).timestamp())
            if ts in seen:
                continue                                        # SIP wins where both have the bar
            seen.add(ts)
            rows.append([ts, float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"]), int(b.get("v") or 0)])
        rows.sort(key=lambda r: r[0])
        if rows:
            out[s] = rows
    return out
