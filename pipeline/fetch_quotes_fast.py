#!/usr/bin/env python3
"""
fetch_quotes_fast.py — 15-minute quote refresh in ONE batched request (z102).
Shared, identical in TRAPP2 / TRAPP2-1 / TRAPP2-2 / TRAPP2-3.

Why: intraday.yml ran the full fetch_data.py every 15 minutes - per-ticker
yfinance calls (fast_info + .info when the profile cache is stale) for ~290
tickers, sequentially. That regularly ran past the job's 12-minute timeout, the
run was killed before the "append intraday tape" + commit steps, and the tape
ended up with 1-3 points a day instead of ~26.

What: take the rows already in data/master.json (fundamentals stay as the
nightly full fetch left them) and refresh only the quote fields from a batched
yf.download(period="5d", interval="1d") - Yahoo updates the current day's daily
bar live, so its Close is the latest price during the session:
    price, close, priceopen, high, low, volume      <- today's bar
    closeyest                                        <- previous bar's close
    change, changepct (percent, like fetch_data.py), date, fetched_at
    marketcap                                        <- scaled by the price move
Tickers the batch can't price keep their previous values.

Exit 0 when >= 80% of rows were refreshed; exit 2 otherwise so the workflow
falls back to the full fetch_data.py (`fetch_quotes_fast.py || fetch_data.py`).
"""
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
MASTER_JSON = DATA / "master.json"
MASTER_CSV = DATA / "master.csv"
CHUNK = 100
MIN_OK = 0.80


def log(*a):
    print("[quotes-fast]", *a, flush=True)


def fmt_num(v):
    if v is None or v == "":
        return ""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if f != f:          # NaN
        return ""
    return f"{f:.6f}".rstrip("0").rstrip(".")


def _num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def bars_for(frame, ticker, single):
    """[(date, open, high, low, close, volume)] oldest->newest, NaN rows dropped."""
    try:
        sub = frame if single else frame[ticker]
    except Exception:
        return []
    out = []
    for idx, row in sub.iterrows():
        c = _num(row.get("Close"))
        if c is None:
            continue
        out.append((str(idx)[:10], _num(row.get("Open")), _num(row.get("High")), _num(row.get("Low")), c, _num(row.get("Volume"))))
    return out


def download(tickers):
    """z158: Alpaca first (official, hundreds of symbols per call) for US stocks / ETFs; Yahoo for the rest
    and for anything Alpaca didn't return."""
    frames = {}
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import alpaca_data
        if alpaca_data.enabled():
            frames = alpaca_data.daily(tickers)
            log(f"  Alpaca: {len(frames)} of {sum(1 for t in tickers if alpaca_data.eligible(t))} US symbols")
    except Exception as e:
        log(f"  Alpaca unavailable ({type(e).__name__}: {str(e)[:120]}) - Yahoo for everything")
        frames = {}
    rest = [t for t in tickers if t not in frames]
    if rest:
        frames.update(_yahoo_download(rest))
    return frames


def _yahoo_download(tickers):
    import yfinance as yf
    frames = {}
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        try:
            df = yf.download(chunk, period="5d", interval="1d", group_by="ticker", auto_adjust=False,
                             threads=True, progress=False)
        except Exception as e:
            log(f"  chunk {i // CHUNK + 1} failed: {type(e).__name__}: {e}")
            continue
        single = len(chunk) == 1
        for t in chunk:
            b = bars_for(df, t, single)
            if b:
                frames[t] = b
    return frames


def _set(r, col, val):
    """Write a fresh pulled value. If a human (XTRAPP) override owns this field,
    the override stays in place and the fresh value goes to _pulled - which is
    what human_corrections.py expects (it re-applies right after this step)."""
    pulled = r.get("_pulled") if isinstance(r.get("_pulled"), dict) else None
    if pulled is not None and col in pulled:
        pulled[col] = val
    else:
        r[col] = val


def refresh(rows, bars, now_iso):
    ok = 0
    for r in rows:
        t = str(r.get("ticker") or "")
        b = bars.get(t)
        if not b:
            continue
        d, o, h, lo, c, v = b[-1]
        prev = b[-2][4] if len(b) >= 2 else _num(r.get("closeyest"))
        pulled = r.get("_pulled") if isinstance(r.get("_pulled"), dict) else {}
        old_px = _num(pulled.get("price", r.get("price")))
        old_mc = _num(pulled.get("marketcap", r.get("marketcap")))
        for col, val in (("price", fmt_num(c)), ("close", fmt_num(c)), ("priceopen", fmt_num(o)),
                         ("high", fmt_num(h)), ("low", fmt_num(lo))):
            _set(r, col, val)
        if v is not None:
            _set(r, "volume", fmt_num(v))
        if prev:
            _set(r, "closeyest", fmt_num(prev))
            _set(r, "change", fmt_num(c - prev))
            _set(r, "changepct", fmt_num((c - prev) / prev * 100))
        if old_mc and old_px:
            _set(r, "marketcap", fmt_num(round(old_mc * c / old_px)))
        _set(r, "date", d)
        r["fetched_at"] = now_iso
        ok += 1
    return ok


def main():
    if not MASTER_JSON.exists():
        log("no data/master.json yet — run the full fetch")
        return 2
    rows = json.loads(MASTER_JSON.read_text())
    tickers = [str(r.get("ticker")) for r in rows if r.get("ticker")]
    if not tickers:
        log("master.json has no tickers")
        return 2
    t0 = datetime.now(timezone.utc)
    bars = download(tickers)
    ok = refresh(rows, bars, datetime.now(timezone.utc).isoformat(timespec="seconds"))
    secs = (datetime.now(timezone.utc) - t0).total_seconds()
    log(f"refreshed {ok}/{len(rows)} rows in {secs:.0f}s")
    if ok < MIN_OK * len(rows):
        log(f"fewer than {MIN_OK:.0%} priced — leaving files untouched, falling back to the full fetch")
        return 2
    MASTER_JSON.write_text(json.dumps(rows, separators=(",", ":")))
    if MASTER_CSV.exists():
        with MASTER_CSV.open(encoding="utf-8") as f:
            header = next(csv.reader(f), None)
        if header:
            with MASTER_CSV.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
                w.writeheader()
                for r in rows:
                    # master.csv stays PURE-PULLED (the app layers corrections itself)
                    pulled = r.get("_pulled") if isinstance(r.get("_pulled"), dict) else {}
                    w.writerow({k: (pulled[k] if k in pulled else r.get(k, "")) for k in header})
    return 0


if __name__ == "__main__":
    sys.exit(main())
