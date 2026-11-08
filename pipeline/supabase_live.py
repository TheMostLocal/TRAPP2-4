#!/usr/bin/env python3
# ============================================================
#  >>> DESTINATION: TheMostLocal/TRAPP2-CORE
#  >>> FILE PATH:   pipeline/supabase_live.py
#
#  z159 - PHASE 2: LIVE DATA IN SUPABASE. After each intraday pass a book pushes what it just wrote to
#  GitHub into the Analytics Supabase project, so the app reads ONE indexed query instead of dozens of
#  raw files behind GitHub's 5-minute cache. GitHub stays the source of truth and the fallback.
#    live_quotes    one row per ticker - the book's latest quote (master.json)
#    live_bars_day  one row per ticker per day - that day's 5-minute bars + official open / close / prior
#                   close / extended-hours quote (data/bars), same shape as the GitHub file
#  Only what changed since the last push is sent (quotes by fetched_at, bar files by modification time);
#  --prune drops bar days older than 4 days. Best-effort: any failure is logged and the pass goes on.
#  Env: SUPABASE_LIVE_URL, SUPABASE_LIVE_KEY (the Analytics project's service key; never an anon key).
# ============================================================
import base64
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
BOOK = (os.environ.get("GITHUB_REPOSITORY") or "").split("/")[-1] or ROOT.name
STATE = Path(os.environ.get("RUNNER_TEMP") or "/tmp") / f"supabase_live_{BOOK}.json"
URL = re.sub(r"/rest/v1/?$", "", (os.environ.get("SUPABASE_LIVE_URL") or "").strip().rstrip("/"))
KEY = (os.environ.get("SUPABASE_LIVE_KEY") or "").strip()
CHUNK = 400


def log(*a):
    print("[live]", *a, flush=True)


def key_kind(k):
    if k.startswith("sb_secret_"):
        return "secret"
    if k.startswith("sb_publishable_"):
        return "publishable"
    if k.count(".") == 2:
        try:
            seg = k.split(".")[1]
            seg += "=" * (-len(seg) % 4)
            return json.loads(base64.urlsafe_b64decode(seg)).get("role") or "jwt"
        except Exception:
            return "jwt"
    return "unrecognized"


def enabled():
    return bool(URL and KEY and key_kind(KEY) in ("service_role", "secret"))


def _req(method, path, body=None, prefer="resolution=merge-duplicates,return=minimal"):
    req = urllib.request.Request(f"{URL}{path}", method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json", "Prefer": prefer})
    for k in range(3):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.status
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and k < 2:
                time.sleep(2 + 3 * k)
                continue
            raise RuntimeError(f"HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:200]}")
    return 0


def upsert(table, rows, conflict):
    n = 0
    for i in range(0, len(rows), CHUNK):
        _req("POST", f"/rest/v1/{table}?on_conflict={conflict}", rows[i:i + CHUNK])
        n += len(rows[i:i + CHUNK])
    return n


def num(v):
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    except (TypeError, ValueError):
        return None


def quote_rows(master, since):
    out, newest = [], since
    for r in master:
        t = str(r.get("ticker") or "").upper()
        fa = str(r.get("fetched_at") or "")
        if not t or not fa or (since and fa <= since):
            continue
        out.append({"ticker": t, "book": BOOK, "price": num(r.get("price")), "change": num(r.get("change")), "changepct": num(r.get("changepct")),
                    "priceopen": num(r.get("priceopen")), "high": num(r.get("high")), "low": num(r.get("low")), "closeyest": num(r.get("closeyest")),
                    "volume": num(r.get("volume")), "marketcap": num(r.get("marketcap")),
                    "currency": r.get("financialCurrency") or r.get("currency") or None, "date": (str(r.get("date") or "")[:10] or None), "fetched_at": fa})
        newest = max(newest or "", fa)
    return out, newest


def bar_rows(seen):
    """Bar files changed since the last push -> live_bars_day rows (same content as the GitHub file)."""
    latest = {}
    try:
        latest = json.loads((DATA / "bars" / "latest.json").read_text())
    except Exception:
        return [], seen
    d = latest.get("date")
    folder = DATA / "bars" / str(d)
    if not d or not folder.is_dir():
        return [], seen
    ext = latest.get("ext") or {}
    out, now_seen = [], dict(seen)
    for f in folder.glob("*.json"):
        key = f"{d}/{f.name}"
        mt = f.stat().st_mtime
        if seen.get(key) == mt:
            continue
        try:
            rec = json.loads(f.read_text())
        except Exception:
            continue
        t = f.stem.upper()
        out.append({"ticker": t, "date": d, "book": BOOK, "pc": num(rec.get("pc")), "oo": num(rec.get("oo")), "oc": num(rec.get("oc")),
                    "ext": ext.get(t) or ext.get(f.stem), "b": rec.get("b") or [], "updated_at": rec.get("updatedAt") or latest.get("updatedAt")})
        now_seen[key] = mt
    return out, now_seen


def main(argv):
    if not URL or not KEY:
        return 0                                                # not configured: GitHub only, silently
    if not enabled():
        log(f"SUPABASE_LIVE_KEY is a '{key_kind(KEY)}' key - writes need the service key; skipped")
        return 0
    t0 = time.time()
    st = {}
    try:
        st = json.loads(STATE.read_text())
    except Exception:
        pass
    try:
        master = json.loads((DATA / "master.json").read_text())
        master = master if isinstance(master, list) else master.get("rows") or []
    except Exception as e:
        log(f"master.json unreadable ({e})")
        master = []
    nq = nb = 0
    try:
        rows, newest = quote_rows(master, st.get("quotesSince"))
        nq = upsert("live_quotes", rows, "ticker") if rows else 0
        st["quotesSince"] = newest
    except Exception as e:
        log(f"quotes: {e}")
    try:
        rows, seen = bar_rows(st.get("bars") or {})
        nb = upsert("live_bars_day", rows, "ticker,date") if rows else 0
        st["bars"] = {k: v for k, v in seen.items() if k[:10] >= (date.today() - timedelta(days=5)).isoformat()}
    except Exception as e:
        log(f"bars: {e}")
    if "--prune" in argv:
        try:
            cut = (date.today() - timedelta(days=4)).isoformat()
            _req("DELETE", f"/rest/v1/live_bars_day?date=lt.{cut}", prefer="return=minimal")
            log(f"pruned bar days before {cut}")
        except Exception as e:
            log(f"prune: {e}")
    try:
        STATE.write_text(json.dumps(st))
    except Exception:
        pass
    log(f"{BOOK}: {nq} quotes · {nb} bar days pushed in {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
