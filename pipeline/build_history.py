#!/usr/bin/env python3
"""
build_history.py — repo-side "backend" for Valuatio.

Runs inside GitHub Actions (no server needed). For every ticker in
data/master.json it writes a compact per-ticker price-history file the app
already knows how to read lazily:

    data/history/<TICKER>.json     →  [["YYYY-MM-DD", close], ...]  (≈1 year)
    data/history_manifest.json     →  ["AAPL", "MSFT", ...]

The app's history manifest loader (fetchHistoryManifest → fetchPerTickerHistory
→ normalizeHistoryArray) consumes exactly this shape. With the repo holding
full-book history, the browser only pulls the tickers it needs, on demand —
nothing big ever has to live in browser storage.

Idempotent: re-running merges new dates into existing files (append-only by
date), trims to MAX_POINTS, and skips tickers Yahoo doesn't know.
"""
import json, os, sys, time
import urllib.parse
import urllib.request

MASTER = "data/master.json"
HIST_DIR = "data/history"
MANIFEST = "data/history_manifest.json"
MAX_POINTS = 280          # ≈ 1 trading year + buffer (matches the app's trim)
RANGE = "1y"
BATCH_SLEEP = 0.35        # be polite to Yahoo (~3 req/s)
UA = {"User-Agent": "Mozilla/5.0 (ValuatioHistoryBot)"}


def yahoo_daily(ticker):
    """Return [[date, close], ...] ascending, or None."""
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/"
           f"{urllib.parse.quote(ticker)}?range={RANGE}&interval=1d")
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=20) as r:
            j = json.load(r)
        res = j["chart"]["result"][0]
        ts = res.get("timestamp") or []
        closes = res["indicators"]["quote"][0].get("close") or []
        out = []
        for t, c in zip(ts, closes):
            if c is None:
                continue
            d = time.strftime("%Y-%m-%d", time.gmtime(t))
            out.append([d, round(float(c), 4)])
        return out or None
    except Exception as e:
        print(f"  ✗ {ticker}: {e}", file=sys.stderr)
        return None


def merge(existing, fresh):
    """Append-only merge by date; existing values are never rewritten."""
    by_date = {row[0]: row[1] for row in existing}
    for d, p in fresh:
        if d not in by_date:          # never rewrite history — epistemic rule
            by_date[d] = p
    rows = sorted(by_date.items())
    return [list(r) for r in rows[-MAX_POINTS:]]


def main():
    with open(MASTER) as f:
        master = json.load(f)
    rows = master if isinstance(master, list) else list(master.values())
    tickers = {r["ticker"].upper() for r in rows if r.get("ticker")}
    # data/extra_tickers.json (optional): ["TICKER", ...] — add any tickers you
    # track that aren't in master.json (e.g. manually-added personal-book
    # tickers) so they get nightly repo history too.
    extra_path = "data/extra_tickers.json"
    if os.path.exists(extra_path):
        try:
            with open(extra_path) as f:
                extra = json.load(f)
            tickers |= {str(t).upper().strip() for t in extra if t}
            print(f"+ {len(extra)} extra tickers from {extra_path}")
        except Exception as e:
            print(f"extra_tickers.json unreadable: {e}", file=sys.stderr)
    tickers = sorted(tickers)
    os.makedirs(HIST_DIR, exist_ok=True)

    ok, fail = [], []
    for i, t in enumerate(tickers, 1):
        path = os.path.join(HIST_DIR, f"{t}.json")
        existing = []
        if os.path.exists(path):
            try:
                with open(path) as f:
                    existing = json.load(f)
                # normalize old object format → array-of-arrays
                if existing and isinstance(existing[0], dict):
                    existing = [[e.get("date"), e.get("price") or e.get("close")]
                                for e in existing if e.get("date")]
            except Exception:
                existing = []
        fresh = yahoo_daily(t)
        if fresh:
            merged = merge(existing, fresh)
            with open(path, "w") as f:
                json.dump(merged, f, separators=(",", ":"))
            ok.append(t)
        elif existing:
            ok.append(t)              # keep serving what we have
            fail.append(t)
        else:
            fail.append(t)
        if i % 25 == 0:
            print(f"  {i}/{len(tickers)} … {len(ok)} ok")
        time.sleep(BATCH_SLEEP)

    with open(MANIFEST, "w") as f:
        json.dump(sorted(ok), f, separators=(",", ":"))
    print(f"Done: {len(ok)} tickers in manifest, {len(fail)} fetch failures "
          f"({', '.join(fail[:10])}{'…' if len(fail) > 10 else ''})")


if __name__ == "__main__":
    main()
