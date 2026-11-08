#!/usr/bin/env python3
# ============================================================
#  >>> DESTINATION: TheMostLocal/TRAPP2, TRAPP2-1, TRAPP2-2, TRAPP2-3  (identical file)
#  >>> FILE PATH:   pipeline/fetch_intraday_bars.py
#
#  z131 - TODAY'S 5-MINUTE BARS (open / high / low / close / volume), pre-market and
#  after hours included, for the app's 1D candles + volume and the tape's pre-market /
#  after-hours movers. Runs in the intraday loop after the tape (every 15 minutes,
#  04:00-20:00 ET on NYSE trading days).
#
#    data/bars/YYYY-MM-DD/<T>.json  { date, interval: "5m", updatedAt, pc, b: [[t, o, h, l, c, v], ...] }
#                                   one small file per ticker (~10 KB) - the app loads only the one
#                                   it shows; t = bar START, epoch seconds (UTC); pc = prior close.
#                                   File name = the ticker as in data/history/ (^VIX.json, EURUSD=X.json)
#    data/bars/latest.json          { date, updatedAt, tickers: [...], ext: { T: { s, px, ref, pct, t } } }
#                                 s = "pre" | "post"; px = last extended-hours price;
#                                 ref = the close it is measured from (prior close for
#                                 pre-market, today's 16:00 close for after hours)
#
#  Sessions (ET): pre-market 04:00-09:30, regular 09:30-16:00, after hours 16:00-20:00.
#  Only US listings trade them; Yahoo returns extended bars only for symbols that do,
#  so a ticker without them simply has none. Foreign listings (RMS.PA, 7203.T ...)
#  trade while this loop sleeps and are skipped - their 1D view uses the daily data.
#  Retention: the newest KEEP_DAYS day files. Never writes NaN.
# ============================================================
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
OUT = DATA / "bars"
MASTER = DATA / "master.json"
KEEP_DAYS = int(os.environ.get("BARS_KEEP_DAYS", "3"))
CHUNK = int(os.environ.get("BARS_CHUNK", "40"))
FORCE = os.environ.get("BARS_FORCE", "").strip() not in ("", "0", "false", "False")
PRE_OPEN, REG_OPEN, REG_CLOSE, POST_CLOSE = 4 * 60, 9 * 60 + 30, 16 * 60, 20 * 60

sys.path.insert(0, str(Path(__file__).resolve().parent))
from append_intraday_snapshot import _et_now, session_for, _parse_iso  # noqa: E402  (the tape's own calendar)

FOREIGN = re.compile(r"\.[A-Z]{1,3}$")
NOT_US_EXT = re.compile(r"^\^|=X$|=F$|-USD$")              # indices, FX, futures, crypto: no US pre / after hours


def log(*a):
    print("[bars]", *a, flush=True)


def eligible(t):
    t = str(t or "").upper().strip()
    if not t:
        return False
    if FOREIGN.search(t) and not t.endswith(".NYB"):     # foreign listing: its session is overnight here
        return False
    return True


def num(v):
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def et_minute(ts_sec):
    et = _et_now(datetime.fromtimestamp(ts_sec, tz=timezone.utc))
    return et.hour * 60 + et.minute, et.date().isoformat()


def et_minute_dt(ts_sec):
    return _et_now(datetime.fromtimestamp(ts_sec, tz=timezone.utc))


def frame_rows(frame):
    rows = []
    if frame is None:
        return rows
    for ts, r in frame.iterrows():
        o, h, l, c = (num(r.get(k)) for k in ("Open", "High", "Low", "Close"))
        if None in (o, h, l, c) or c <= 0:
            continue
        v = num(r.get("Volume"))
        rows.append([int(ts.timestamp()), round(o, 6), round(h, 6), round(l, 6), round(c, 6), int(v) if v is not None else 0])
    rows.sort(key=lambda x: x[0])
    return rows


def download(tickers):
    """z158: Alpaca first (official SIP bars to 16 min ago + IEX since, hundreds of symbols per call) for US
    stocks / ETFs; Yahoo for everything else and for anything Alpaca didn't return."""
    out = {}
    try:
        import alpaca_data
        if alpaca_data.enabled():
            now = datetime.now(timezone.utc)
            day0 = _et_now(now).replace(hour=4, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
            got = alpaca_data.intraday(tickers, day0, now)
            out = {t: [[r[0], round(r[1], 6), round(r[2], 6), round(r[3], 6), round(r[4], 6), int(r[5])] for r in rows if r[4] > 0]
                   for t, rows in got.items() if rows}
            out = {t: rows for t, rows in out.items() if rows}          # a symbol with no usable bar goes to Yahoo
            log(f"Alpaca: 5-min bars for {len(out)} of {sum(1 for t in tickers if alpaca_data.eligible(t))} US symbols")
    except Exception as e:
        log(f"Alpaca unavailable ({type(e).__name__}: {str(e)[:120]}) - Yahoo for everything")
        out = {}
    rest = [t for t in tickers if t not in out]
    if rest:
        out.update(_yahoo_download(rest))
    return out


def _yahoo_download(tickers):
    import pandas as pd
    import yfinance as yf
    out = {}
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        try:
            df = yf.download(chunk, period="1d", interval="5m", prepost=True, group_by="ticker",
                             auto_adjust=False, progress=False, threads=True)
        except Exception as e:                                       # one bad chunk must not stop the rest
            log(f"chunk {i // CHUNK + 1}: download failed: {e}")
            continue
        if df is None or getattr(df, "empty", True):
            continue
        multi = isinstance(df.columns, pd.MultiIndex)
        for t in chunk:
            try:
                f = df[t] if multi and t in df.columns.get_level_values(0) else (df if not multi and len(chunk) == 1 else None)
                rows = frame_rows(f)
                if rows:
                    out[t] = rows
            except Exception as e:
                log(f"{t}: {e}")
        time.sleep(1.0)
    return out


def main():
    now_et = _et_now(datetime.now(timezone.utc))
    trading, _close, label = session_for(now_et)
    m = now_et.hour * 60 + now_et.minute
    if not FORCE and (not trading or m < PRE_OPEN or m > POST_CLOSE + 15):
        log(f"skip: {label if not trading else 'outside 04:00-20:15 ET'}")
        return 0
    rows = json.loads(MASTER.read_text())
    rows = rows if isinstance(rows, list) else (rows.get("rows") or [])
    prev, quote = {}, {}
    tickers = []
    for r in rows:
        t = str(r.get("ticker") or "").upper().strip()
        if eligible(t) and t not in prev:
            tickers.append(t)
            prev[t] = num(r.get("closeyest"))
            quote[t] = r                                   # z134: Yahoo's daily bar (official open / close)
    log(f"{len(tickers)} tickers")
    bars = download(tickers)
    if not bars:
        log("no bars this pass")
        return 0
    # the trading date of the bars (the newest bar's ET date)
    newest = max(b[-1][0] for b in bars.values())
    day = et_minute(newest)[1]
    ddir = OUT / day
    ddir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ext, wrote, nbars = {}, 0, 0
    for t, b in bars.items():
        b = [x for x in b if et_minute(x[0])[1] == day]               # today's session only
        if not b:
            continue
        pc = prev.get(t)
        # z134: the official open / close (Yahoo's daily bar, same as the tape) trump the 5-minute bars
        # at 09:30 and 16:00 - the first regular bar opens at the official open, the last regular bar
        # closes at the official close (its high / low widened if the print lies outside them)
        oo = oc = None
        q = quote.get(t) or {}
        if str(q.get("date") or "")[:10] == day:
            oo = num(q.get("priceopen"))
            fa = _parse_iso(q.get("fetched_at"))
            tr, cmin, _ = session_for(et_minute_dt(b[-1][0]))
            if fa is not None and cmin is not None:
                fe = _et_now(fa)
                if fe.strftime("%Y-%m-%d") == day and fe.hour * 60 + fe.minute >= cmin:      # z135: from 1600 ET, not 1605
                    oc = num(q.get("price"))
            reg = [k for k, x in enumerate(b) if REG_OPEN <= et_minute(x[0])[0] < (cmin or REG_CLOSE)]
            if reg and oo and oo > 0 and et_minute(b[reg[0]][0])[0] == REG_OPEN:
                x = b[reg[0]]; x[1] = round(oo, 6); x[2] = max(x[2], x[1]); x[3] = min(x[3], x[1])
            if reg and oc and oc > 0:
                x = b[reg[-1]]; x[4] = round(oc, 6); x[2] = max(x[2], x[4]); x[3] = min(x[3], x[4])
        (ddir / f"{t}.json").write_text(json.dumps({"date": day, "interval": "5m", "updatedAt": stamp, "pc": pc, "oo": oo, "oc": oc, "b": b}, separators=(",", ":")))
        wrote += 1
        nbars += len(b)
        if NOT_US_EXT.search(t):                                       # no US pre-market / after hours to quote
            continue
        last = b[-1]
        lm = et_minute(last[0])[0]
        if lm < REG_OPEN and pc:                                       # pre-market, from the prior close
            ext[t] = {"s": "pre", "px": last[4], "ref": pc, "pct": round(last[4] / pc - 1, 6), "t": last[0]}
        elif lm >= REG_CLOSE:                                          # after hours, from today's 16:00 close
            reg = [x for x in b if REG_OPEN <= et_minute(x[0])[0] < REG_CLOSE]
            if reg:
                ref = reg[-1][4]
                ext[t] = {"s": "post", "px": last[4], "ref": ref, "pct": round(last[4] / ref - 1, 6), "t": last[0]}
    have = sorted(p.stem for p in ddir.glob("*.json"))
    (OUT / "latest.json").write_text(json.dumps({"date": day, "updatedAt": stamp, "tickers": have, "ext": ext}, separators=(",", ":")))
    import shutil
    days = sorted(p for p in OUT.iterdir() if p.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name))
    for p in days[:-KEEP_DAYS]:
        shutil.rmtree(p, ignore_errors=True)
    log(f"{day}: {wrote} tickers · {nbars} bars · "
        f"{sum(1 for v in ext.values() if v['s'] == 'pre')} pre-market / {sum(1 for v in ext.values() if v['s'] == 'post')} after-hours quotes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
