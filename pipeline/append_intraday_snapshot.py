#!/usr/bin/env python3
# ============================================================
#  >>> DESTINATION: TheMostLocal/TRAPP2, TRAPP2-2, TRAPP2-3, TRAPP2-1  (identical file)
#  >>> FILE PATH:   pipeline/append_intraday_snapshot.py
#
#  THE 15-MINUTE INTRADAY TAPE — "GOOGLEFINANCE-in-Sheets" refresh for the app.
#
#  Runs immediately AFTER pipeline/fetch_data.py in the same workflow. It reads
#  the freshly-written data/master.json and appends ONE compact snapshot row per
#  run to a per-trading-day tape:
#
#      data/intraday/YYYY-MM-DD.json   full tape for that session
#        { date, tzNote, prevClose:{TIC:px}, snapshots:[ {t, b, p:{TIC:px}} ], ... }
#
#      data/intraday/latest.json       tiny "current state" pointer the frontend
#                                      polls every cycle (one file, ~40 KB, no
#                                      day-file guessing, no 404 on holidays)
#
#      data/intraday/index.json        which day files exist + their point counts
#
#  WHY THREE FILES
#    latest.json  → the frontend's fast path: what is the price RIGHT NOW and how
#                   old is it. One request, always the same URL.
#    YYYY-MM-DD   → the session tape: draws today's intraday line on a chart.
#    index.json   → lets the app find the most recent SESSION when today is a
#                   holiday/weekend, instead of showing an empty chart.
#
#  CORRECTNESS RULES BAKED IN
#    * US MARKET CALENDAR. Weekends and NYSE holidays are skipped outright, and
#      early-close days (1:00pm ET) are honored. Without this the tape silently
#      records 40 identical "flat" points every July 4th and the app's freshness
#      badge lies about the market being open.
#    * 15-MINUTE BUCKETS keyed on ET minute-of-day. A re-run inside the same
#      bucket REPLACES that bucket rather than appending, so a retried workflow
#      or a queue-lagged double-fire can never double-write a point.
#    * MONOTONIC APPEND. A bucket that is older than the newest bucket already on
#      file is dropped, not inserted. GitHub Actions cron can fire out of order
#      after queue lag; silently back-filling would make the line zig-zag.
#    * STALE-QUOTE GUARD. A row whose own fetched_at is older than
#      MAX_QUOTE_AGE_MIN is excluded from the snapshot. master.json keeps the
#      last good value for a ticker that failed its fetch, so without this guard
#      a dead ticker's stale price is re-stamped with a fresh timestamp every 15
#      minutes and the app believes it is live.
#    * PRIOR CLOSE is carried on the day file (from master.json `closeyest`), so
#      the frontend can draw the % -from-prior-close baseline WITHOUT a second
#      round trip to master.json.
#    * CURRENCY is carried so the frontend can FX-normalize the tape exactly the
#      way it normalizes master.json rows (KRW/GBX/USX all pass through the same
#      normalizeRowToUSD path). An un-normalized tape would repeat the historic
#      +$11.5M SK-Hynix bug on the intraday chart.
#    * RETENTION. Day files older than KEEP_DAYS are deleted so the repo does not
#      grow without bound (~270 KB/session/repo → ~5 MB at 20 sessions).
#
#  Stdlib only. Safe and cheap to run every cycle.
# ============================================================
import json
import os
import sys
from datetime import datetime, timedelta, timezone, date as _date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
MASTER = DATA / "master.json"
INTRADAY_DIR = DATA / "intraday"

BUCKET_MIN = int(os.environ.get("INTRADAY_BUCKET_MIN", "15"))
KEEP_DAYS = int(os.environ.get("INTRADAY_KEEP_DAYS", "20"))
MAX_SNAPSHOTS_PER_DAY = 200
# A quote older than this is not "now" — leave it out of the snapshot.
MAX_QUOTE_AGE_MIN = int(os.environ.get("INTRADAY_MAX_QUOTE_AGE_MIN", "90"))
# Record from 04:00 ET (pre-market) through 20:00 ET (post-market) so the tape
# covers extended hours; the frontend decides what to draw.
TAPE_START_MIN = int(os.environ.get("INTRADAY_START_MIN", str(4 * 60)))
TAPE_END_MIN = int(os.environ.get("INTRADAY_END_MIN", str(20 * 60)))
# Set INTRADAY_FORCE=1 to write a point regardless of session/holiday state.
FORCE = os.environ.get("INTRADAY_FORCE", "").strip() not in ("", "0", "false", "False")

REGULAR_OPEN_MIN = 9 * 60 + 30      # 09:30 ET
REGULAR_CLOSE_MIN = 16 * 60         # 16:00 ET
EARLY_CLOSE_MIN = 13 * 60           # 13:00 ET on half days


# ------------------------------------------------------------------
#  US EASTERN TIME without pytz/zoneinfo-data surprises.
#  zoneinfo is stdlib on 3.9+; on a runner without tzdata we fall back to the
#  US DST rule (2nd Sunday in March → 1st Sunday in November), which has been
#  correct since 2007 and is exactly what the frontend implements.
# ------------------------------------------------------------------
def _et_now(utc_dt=None):
    utc_dt = utc_dt or datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        return utc_dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        pass
    y = utc_dt.year
    # 2nd Sunday of March, 07:00 UTC  →  1st Sunday of November, 06:00 UTC
    def _nth_sunday(month, nth):
        d = _date(y, month, 1)
        d += timedelta(days=(6 - d.weekday()) % 7)      # first Sunday
        return d + timedelta(days=7 * (nth - 1))
    dst_start = datetime.combine(_nth_sunday(3, 2), datetime.min.time(), timezone.utc).replace(hour=7)
    dst_end = datetime.combine(_nth_sunday(11, 1), datetime.min.time(), timezone.utc).replace(hour=6)
    offset = -4 if dst_start <= utc_dt < dst_end else -5
    return utc_dt + timedelta(hours=offset)


# ------------------------------------------------------------------
#  NYSE / NASDAQ holiday calendar, computed (not hardcoded) so it never expires.
#  Rules: New Year's Day, MLK (3rd Mon Jan), Washington's Birthday (3rd Mon Feb),
#  Good Friday, Memorial Day (last Mon May), Juneteenth (Jun 19, from 2022),
#  Independence Day, Labor Day (1st Mon Sep), Thanksgiving (4th Thu Nov),
#  Christmas. Saturday holidays observe Friday; Sunday holidays observe Monday
#  (except New Year's Day falling on Saturday, which the NYSE does NOT observe
#  on the preceding Friday).
# ------------------------------------------------------------------
def _easter(year):
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return _date(year, month, day + 1)


def _observed(d, allow_friday=True):
    if d.weekday() == 5:                       # Saturday
        return d - timedelta(days=1) if allow_friday else None
    if d.weekday() == 6:                       # Sunday
        return d + timedelta(days=1)
    return d


def _nth_weekday(year, month, weekday, nth):
    d = _date(year, month, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(days=7 * (nth - 1))


def _last_weekday(year, month, weekday):
    # Walk back from the LAST day of the month. Starting from day 28 and stepping
    # forward misses the last Monday in a 31-day month (Memorial Day 2027 landed
    # on May 24 instead of May 31 before this fix).
    d = (_date(year, 12, 31) if month == 12
         else _date(year, month + 1, 1) - timedelta(days=1))
    return d - timedelta(days=(d.weekday() - weekday) % 7)


_HOLIDAY_CACHE = {}


def market_holidays(year):
    if year in _HOLIDAY_CACHE:
        return _HOLIDAY_CACHE[year]
    h = set()
    # New Year's Day — a Saturday New Year is NOT observed on Dec 31.
    ny = _observed(_date(year, 1, 1), allow_friday=False)
    if ny:
        h.add(ny)
    h.add(_nth_weekday(year, 1, 0, 3))                       # MLK
    h.add(_nth_weekday(year, 2, 0, 3))                       # Presidents' Day
    h.add(_easter(year) - timedelta(days=2))                 # Good Friday
    h.add(_last_weekday(year, 5, 0))                         # Memorial Day
    if year >= 2022:
        h.add(_observed(_date(year, 6, 19)))                 # Juneteenth
    h.add(_observed(_date(year, 7, 4)))                      # Independence Day
    h.add(_nth_weekday(year, 9, 0, 1))                       # Labor Day
    h.add(_nth_weekday(year, 11, 3, 4))                      # Thanksgiving
    h.add(_observed(_date(year, 12, 25)))                    # Christmas
    _HOLIDAY_CACHE[year] = h
    return h


def early_close_days(year):
    """1:00pm ET closes: day after Thanksgiving, Jul 3 (when Jul 4 is a weekday
    other than Monday), Christmas Eve when it is a weekday."""
    out = set()
    out.add(_nth_weekday(year, 11, 3, 4) + timedelta(days=1))
    jul4 = _date(year, 7, 4)
    if jul4.weekday() < 5 and jul4.weekday() != 0:
        out.add(jul4 - timedelta(days=1))
    xmas_eve = _date(year, 12, 24)
    if xmas_eve.weekday() < 5:
        out.add(xmas_eve)
    return {d for d in out if d.weekday() < 5 and d not in market_holidays(year)}


def session_for(et_dt):
    """Return (is_trading_day, close_minute, label) for an ET datetime."""
    d = et_dt.date()
    if d.weekday() >= 5:
        return False, None, "weekend"
    if d in market_holidays(d.year):
        return False, None, "holiday"
    if d in early_close_days(d.year):
        return True, EARLY_CLOSE_MIN, "early-close"
    return True, REGULAR_CLOSE_MIN, "regular"


# ------------------------------------------------------------------
def _num(v):
    if v in (None, "", "NaN", "nan"):
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def _px(v):
    """Round a price for the tape. Sub-$1 instruments (FX pairs like USDKRW=X at
    0.0006655) need real precision; everything else is noise past 4dp and the
    extra digits are ~20% of the file size."""
    return round(v, 6) if abs(v) < 1 else round(v, 4)


def _parse_iso(s):
    if not s:
        return None
    try:
        t = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(t)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _atomic_write(path, text):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def main():
    now_utc = datetime.now(timezone.utc)
    et = _et_now(now_utc)
    et_min = et.hour * 60 + et.minute
    trading, close_min, label = session_for(et)

    if not FORCE:
        if not trading:
            print(f"SKIP intraday tape — {et.date()} is a {label} (ET {et:%H:%M}).")
            return 0
        if et_min < TAPE_START_MIN or et_min > TAPE_END_MIN:
            print(f"SKIP intraday tape — ET {et:%H:%M} outside "
                  f"{TAPE_START_MIN // 60:02d}:00-{TAPE_END_MIN // 60:02d}:00 tape window.")
            return 0

    if not MASTER.exists():
        print("no data/master.json yet — skipping intraday snapshot", file=sys.stderr)
        return 0
    try:
        rows = json.loads(MASTER.read_text(encoding="utf-8"))
        if isinstance(rows, dict):
            rows = rows.get("rows") or list(rows.values())
    except Exception as e:
        print(f"could not read master.json: {e}", file=sys.stderr)
        return 0
    if not isinstance(rows, list):
        print("master.json is not a row list — skipping", file=sys.stderr)
        return 0

    day = et.strftime("%Y-%m-%d")
    bucket = (et_min // BUCKET_MIN) * BUCKET_MIN
    now_iso = now_utc.isoformat(timespec="seconds")

    pmap, prev, ccy, vol = {}, {}, {}, {}
    opens, closes = {}, {}              # z134: Yahoo's OFFICIAL open / close for today (the daily bar)
    stale = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        t = str(r.get("ticker") or "").strip().upper()
        if not t:
            continue
        px = _num(r.get("price")) or _num(r.get("close")) or _num(r.get("last price"))
        if px is None or px <= 0:
            continue
        # Reject a quote that master.json is merely REMEMBERING from an earlier
        # cycle — otherwise a ticker whose fetch is failing looks permanently live.
        fa = _parse_iso(r.get("fetched_at") or r.get("fetchedat"))
        if fa is not None and (now_utc - fa).total_seconds() > MAX_QUOTE_AGE_MIN * 60:
            stale += 1
            continue
        pmap[t] = _px(px)
        pc = _num(r.get("closeyest")) or _num(r.get("prev_close"))
        if pc and pc > 0:
            prev[t] = _px(pc)
        c = str(r.get("currency") or "").strip()
        if c and c.upper() != "USD":
            ccy[t] = c
        v = _num(r.get("volume"))
        if v and v > 0:
            vol[t] = int(v)
        # z134 / z135: the daily bar fetch_quotes_fast just wrote is TODAY's only once the session has
        # opened (row date == today, ET). Its Open is the official opening print - the chart's 0930
        # point. A quote fetched at or after the close (1600 ET; 1300 on early closes) carries the
        # official close - the 1600 point. The loop runs extra passes at 0930:40 / 0932:30 and
        # 1600:40 / 1602:30 ET to take them the moment Yahoo has them; every later pass re-reads
        # them, so a value Yahoo revises is corrected. Both trump the 15-minute pulls.
        if str(r.get("date") or "")[:10] == day and et_min >= REGULAR_OPEN_MIN:
            po = _num(r.get("priceopen"))
            if po and po > 0:
                opens[t] = _px(po)
            if close_min is not None and fa is not None:
                fe = _et_now(fa)
                if fe.strftime("%Y-%m-%d") == day and fe.hour * 60 + fe.minute >= close_min:
                    closes[t] = _px(px)

    if not pmap:
        print("no fresh prices in master.json — skipping", file=sys.stderr)
        return 0

    INTRADAY_DIR.mkdir(parents=True, exist_ok=True)
    path = INTRADAY_DIR / f"{day}.json"
    doc = {}
    if path.exists():
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            doc = {}
    if not isinstance(doc, dict):
        doc = {}
    doc.setdefault("date", day)
    doc.setdefault("snapshots", [])
    snaps = [s for s in doc["snapshots"] if isinstance(s, dict) and isinstance(s.get("b"), int)]

    newest_b = max((s["b"] for s in snaps), default=-1)
    point = {"t": now_iso, "b": bucket, "p": pmap}
    if vol:
        point["v"] = vol

    if bucket < newest_b:
        # Out-of-order fire (queue lag). Recording it would draw the line
        # backwards; the newer point already covers this instant.
        print(f"SKIP out-of-order bucket {bucket} (newest on file {newest_b}).")
        return 0
    replaced = False
    for i, s in enumerate(snaps):
        if s["b"] == bucket:
            snaps[i] = point
            replaced = True
            break
    if not replaced:
        snaps.append(point)
    snaps.sort(key=lambda s: s["b"])
    if len(snaps) > MAX_SNAPSHOTS_PER_DAY:
        snaps = snaps[-MAX_SNAPSHOTS_PER_DAY:]

    # Prior close is a property of the SESSION, not of any one snapshot. Merge so
    # a ticker that only quoted later in the day still gets its baseline.
    prev_all = dict(doc.get("prevClose") or {})
    prev_all.update(prev)
    ccy_all = dict(doc.get("currency") or {})
    ccy_all.update(ccy)
    open_all = dict(doc.get("open") or {})      # z134: later passes may refine Yahoo's values
    open_all.update(opens)
    close_all = dict(doc.get("close") or {})
    close_all.update(closes)

    doc.update({
        "date": day,
        "snapshots": snaps,
        "prevClose": prev_all,
        "currency": ccy_all,
        "open": open_all,
        "close": close_all,
        "openCloseNote": "official open / close from Yahoo's daily bar (open: from 0930 ET; close: from a quote fetched at or after the close, 1600 ET)",
        "bucketMinutes": BUCKET_MIN,
        "session": label,
        "openMinuteET": REGULAR_OPEN_MIN,
        "closeMinuteET": close_min,
        "tzNote": "snapshot times are UTC ISO-8601; b = ET minute-of-day bucket",
        "updatedAt": now_iso,
        "snapshotCount": len(snaps),
        "tickerCount": len(pmap),
        "repo": os.environ.get("GITHUB_REPOSITORY", ""),
    })
    _atomic_write(path, json.dumps(doc, separators=(",", ":")))

    # --- latest.json: the frontend's one-request fast path -----------------
    latest = {
        "asOf": now_iso,
        "date": day,
        "bucket": bucket,
        "etMinute": et_min,
        "etTime": et.strftime("%H:%M"),
        "session": label,
        "marketOpen": bool(trading and REGULAR_OPEN_MIN <= et_min < (close_min or REGULAR_CLOSE_MIN)),
        "openMinuteET": REGULAR_OPEN_MIN,
        "closeMinuteET": close_min,
        "bucketMinutes": BUCKET_MIN,
        "repo": os.environ.get("GITHUB_REPOSITORY", ""),
        "tickerCount": len(pmap),
        "staleExcluded": stale,
        "p": pmap,
        "prevClose": prev_all,
        "currency": ccy_all,
        "open": open_all,
        "close": close_all,
    }
    _atomic_write(INTRADAY_DIR / "latest.json", json.dumps(latest, separators=(",", ":")))

    # --- retention + index -------------------------------------------------
    cutoff = (et.date() - timedelta(days=KEEP_DAYS)).isoformat()
    days = []
    for f in sorted(INTRADAY_DIR.glob("*.json")):
        if f.name in ("latest.json", "index.json"):
            continue
        stem = f.stem
        if len(stem) != 10 or stem[4] != "-":
            continue
        if stem < cutoff:
            try:
                f.unlink()
                continue
            except Exception:
                pass
        try:
            j = json.loads(f.read_text(encoding="utf-8"))
            days.append({"date": stem, "points": len(j.get("snapshots") or []),
                         "updatedAt": j.get("updatedAt"), "session": j.get("session")})
        except Exception:
            days.append({"date": stem, "points": None})
    days.sort(key=lambda d: d["date"])
    _atomic_write(INTRADAY_DIR / "index.json", json.dumps({
        "updatedAt": now_iso, "keepDays": KEEP_DAYS, "bucketMinutes": BUCKET_MIN,
        "latestDate": days[-1]["date"] if days else day, "days": days,
    }, separators=(",", ":")))

    print(f"OK intraday/{day}.json  bucket={bucket} (ET {et:%H:%M}, {label})  "
          f"{len(pmap)} tickers, {len(snaps)} points today, {stale} stale excluded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
