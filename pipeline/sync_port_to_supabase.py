#!/usr/bin/env python3
"""
sync_port_to_supabase.py — push data/portfolio_data.json -> Supabase
`portfolio_positions` (the shared ANALYTICS/PORT project). Server-side
(GitHub Actions), SERVICE-ROLE key.

WHY THE OLD VERSION PUSHED 0 ROWS:
  It was a copy of the BOT sync — it read d.get("trades") (portfolio_data.json
  has no "trades"; positions live under "portfolio", history under
  "transactions") AND wrote a "trade" key when the table column is "position".
  So it built an empty list AND the wrong shape.

WHAT IT PUSHES (all -> portfolio_positions, jsonb column `position`, key `id`):
  - each portfolio entry            _kind='entry'        (role + ticker + notes -> thesis,
                                                           marketValue/pnl for Long/Short lots)
  - each transaction (immutable)    _kind='transaction'  (stable txn: id, no duplication)
  - cash / goodGlobe index+curve /  _kind='cash'|'index'|'indexCurve'|'snapshot'  (singletons)
    a restore snapshot

FRESH, NO DUPLICATES:
  Every row upserts on `id` (resolution=merge-duplicates) — re-running never
  duplicates. A RECONCILE step then deletes rows that no longer exist locally
  (so a removed position/watch disappears), while KEEPING transaction history
  (txn: ids) and the snapshot singleton. Mirrors the app's own sync exactly.

TIMEZONE:
  updated_at is Eastern wall-clock tagged +00:00 so Supabase's UTC display shows
  local Eastern time (see now_iso).

Env (repo secrets):
  SUPABASE_URL           the shared ANALYTICS/PORT project URL
  SUPABASE_SERVICE_ROLE  the service_role key   (SUPABASE_SERVICE_KEY also accepted)
"""
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# Repo owner — resolved at runtime so the pipeline follows the repos to any
# GitHub account. Actions sets GITHUB_REPOSITORY_OWNER automatically;
# VALUATIO_OWNER (repo variable/env) overrides; TheMostLocal is the fallback.
_GH_OWNER = (__import__("os").environ.get("VALUATIO_OWNER")
             or __import__("os").environ.get("GITHUB_REPOSITORY_OWNER")
             or "TheMostLocal").strip()

# ---- Supabase credentials (same block in every Valuatio sync script) --------
# Picks whichever configured key is actually a SERVICE key (legacy JWT with
# role=service_role, or a new sb_secret_ key), so a wrong value in ONE of the
# two secret names can't silently downgrade writes to anon. Never prints keys.
import base64 as _sb_b64, json as _sb_json, os as _sb_os, re as _sb_re
def _sb_claims(k):
    try:
        seg = k.split(".")[1]; seg += "=" * (-len(seg) % 4)
        return _sb_json.loads(_sb_b64.urlsafe_b64decode(seg))
    except Exception:
        return {}
def _sb_kind(k):
    k = (k or "").strip()
    if not k: return "missing"
    if k.startswith("sb_secret_"): return "secret"
    if k.startswith("sb_publishable_"): return "publishable"
    if k.count(".") == 2: return _sb_claims(k).get("role") or "jwt(no role)"
    return "unrecognized"
def _sb_url():
    u = (_sb_os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
    return _sb_re.sub(r"/rest/v1$", "", u)
def _sb_pick_key():
    names = ("SUPABASE_SERVICE_ROLE", "SUPABASE_SERVICE_KEY", "SUPABASE_KEY", "SUPABASE_ANON_KEY")
    vals = [(n, (_sb_os.environ.get(n) or "").strip()) for n in names]
    have = [(n, v) for n, v in vals if v]
    good = [(n, v) for n, v in have if _sb_kind(v) in ("service_role", "secret")]
    name, key = (good or have or [(None, "")])[0]
    print("[supabase] " + (", ".join(f"{n}={_sb_kind(v)}" for n, v in have) or "no keys set")
          + f" -> using {name or 'none'}")
    if key and _sb_kind(key) not in ("service_role", "secret"):
        print(f"::warning::{name} is a '{_sb_kind(key)}' key, not service_role/secret - "
              "service-only tables (ticker_snapshot, regime_timeline, bot_equity) will reject writes")
    distinct = {v for n, v in have if n in names[:2]}
    if len(distinct) > 1:
        print("::warning::SUPABASE_SERVICE_ROLE and SUPABASE_SERVICE_KEY differ - set both to the same service key")
    ref = _sb_claims(key).get("ref") if key.count(".") == 2 else None
    m = _sb_re.match(r"https://([a-z0-9]+)\.supabase\.co$", _sb_url())
    if ref and m and ref != m.group(1):
        print(f"::error::key belongs to Supabase project '{ref}' but SUPABASE_URL points at '{m.group(1)}' (keys from the other project?)")
    return key
def _sb_finite(obj):
    if isinstance(obj, float):
        return obj if obj == obj and obj not in (float("inf"), float("-inf")) else None
    if isinstance(obj, dict):
        return {k: _sb_finite(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sb_finite(v) for v in obj]
    return obj
# -----------------------------------------------------------------------------
URL = _sb_url()
KEY = _sb_pick_key()
TABLE = "portfolio_positions"
DATA = "data/portfolio_data.json"

RAW = f"https://raw.githubusercontent.com/{_GH_OWNER}"
MASTER_SOURCES = [f"{RAW}/TRAPP2/main/data/master.json",
                  f"{RAW}/TRAPP2-2/main/data/master.json",
                  f"{RAW}/TRAPP2-1/main/data/master.json",
                  f"{RAW}/TRAPP2-3/main/data/master.json",
                  f"{RAW}/TRAPP2-4/main/data/master.json"]  # gap-fill only (first-wins); z180: + TRAPP2-4
ACTIVE_ROLES = {"long", "short", "buy to open", "sell to open"}
SHORT_ROLES = {"short", "sell to open"}


# --------------------------------------------------------------- timezone ----
try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except Exception:
    _ET = None


def _eastern_now():
    if _ET is not None:
        return datetime.now(_ET)
    u = datetime.now(timezone.utc); y = u.year

    def nth_sunday(month, n):
        d = datetime(y, month, 1, tzinfo=timezone.utc)
        return 1 + ((6 - d.weekday()) % 7) + (n - 1) * 7
    start = datetime(y, 3, nth_sunday(3, 2), 7, tzinfo=timezone.utc)
    end = datetime(y, 11, nth_sunday(11, 1), 6, tzinfo=timezone.utc)
    return u + timedelta(hours=(-4 if start <= u < end else -5))


def now_iso():
    """Eastern wall-clock tagged +00:00 so Supabase's UTC display shows local."""
    return _eastern_now().replace(tzinfo=timezone.utc).isoformat()


# ------------------------------------------------------------------ http -----
def _req(method, path, body=None, headers=None):
    h = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}
    if headers:
        h.update(headers)
    data = json.dumps(_sb_finite(body), allow_nan=False).encode() if body is not None else None
    req = urllib.request.Request(URL + path, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def fetch_json(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "valuatio-port-sync"})
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def _num(v):
    try:
        f = float(v)
        return None if (f != f or f in (float("inf"), float("-inf"))) else f
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------- id helpers (app parity) -
def _js_num_str(v):
    n = _num(v)
    n = 0.0 if n is None else n
    return str(int(n)) if n == int(n) else repr(n)


def _b36(n):
    if n == 0:
        return "0"
    digs = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = ""
    while n:
        n, r = divmod(n, 36)
        out = digs[r] + out
    return out


def _stable_txn_id(t):
    tid = t.get("id")
    if tid and str(tid).startswith("txn:"):
        return str(tid)
    if tid and str(tid).strip():
        return f"txn:{tid}"
    key = "|".join([
        str(t.get("type", "") or "").lower(),
        str(t.get("ticker", "") or "").upper(),
        _js_num_str(t.get("qty")),
        _js_num_str(t.get("price")),
        str(t.get("ts") or t.get("date") or ""),
    ])
    h = 0
    for ch in key:
        h = (h << 5) - h + ord(ch)
        h &= 0xFFFFFFFF
        if h & 0x80000000:
            h -= 0x100000000
    return f"txn:c{_b36(h & 0xFFFFFFFF)}"


def _ensure_port_id(rec):
    if rec.get("id") and str(rec["id"]).strip():
        return str(rec["id"])
    tk = str(rec.get("ticker") or rec.get("symbol") or "POS").upper()
    d = rec.get("openDate") or rec.get("date") or rec.get("addedAt") or ""
    rec["id"] = f"{tk}-{d}" if d else f"{tk}-{_b36(abs(hash(json.dumps(rec, sort_keys=True))) & 0xFFFFFFFF)}"
    return rec["id"]


# --------------------------------------------------------------- enrichment --
FX_URL = f"{RAW}/TRAPP2-1/main/data/fx/rates.json"

# Quote-unit currencies: (base currency, factor). Futures on CBOT/ICE soft
# commodities quote in US CENTS (USX), London stocks in PENCE (GBp/GBX), JSE in
# ZA cents, TASE in agorot. The app converts all of these (normalizeRowToUSD);
# the portfolio sync must too, or a corn future marks at $497 instead of $4.97.
_SUBUNIT = {"USX": ("USD", 0.01), "USD.": ("USD", 0.01), "USd": ("USD", 0.01),
            "GBp": ("GBP", 0.01), "GBX": ("GBP", 0.01),
            "ZAc": ("ZAR", 0.01), "ZAC": ("ZAR", 0.01),
            "ILA": ("ILS", 0.01), "ILa": ("ILS", 0.01)}


def _load_fx():
    """USD-per-unit map from TRAPP2-1/data/fx/rates.json (the file the app uses)."""
    d = fetch_json(FX_URL)
    rates = (d.get("rates") if isinstance(d, dict) else {}) or {}
    out = {"USD": 1.0}
    for ccy, v in rates.items():
        up = _num(v.get("usdPer")) if isinstance(v, dict) else None
        if up and up > 0:
            out[str(ccy).upper()] = up
    return out


def _to_usd(price, currency, fx):
    """Local quote -> USD. None when the currency has no loaded rate (the lot then
    marks at cost and is flagged, rather than being valued in the wrong unit)."""
    if price is None:
        return None
    raw = (currency or "USD").strip()
    base, factor = _SUBUNIT.get(raw, (raw.upper() or "USD", 1.0))
    rate = fx.get(base)
    return None if rate is None else price * factor * rate


UNPRICED = {}   # ticker -> local currency it couldn't convert (logged in main)
PRICE_META = {}  # ticker -> {"localPrice", "localCurrency", "fxRate"} for non-USD quotes


def _load_price_map():
    """{TICKER: price in USD}. First repo wins, as before; foreign / sub-unit
    quotes are converted with the same FX file the app uses."""
    fx = _load_fx()
    px = {}
    for url in MASTER_SOURCES:
        d = fetch_json(url)
        rows = d.values() if isinstance(d, dict) else (d if isinstance(d, list) else [])
        for r in rows:
            if not isinstance(r, dict):
                continue
            tk = (r.get("ticker") or r.get("symbol") or "").upper()
            if not tk or tk in px:
                continue
            p = (_num(r.get("price")) or _num(r.get("fmpPrice")) or _num(r.get("close"))
                 or _num(r.get("last")) or _num(r.get("closeyest")))   # prior close if no live quote
            if p is None:
                continue
            ccy = r.get("currency") or "USD"
            usd = _to_usd(p, ccy, fx)
            if usd is None:
                UNPRICED[tk] = ccy
                continue
            px[tk] = usd
            if (ccy or "USD") != "USD":
                PRICE_META[tk] = {"localPrice": p, "localCurrency": ccy, "fxRate": usd / p if p else None}
    return px


def _enrich(rec, price_map, total_mv):
    """Field bridges (schema column paths) + marketValue/pnl for active lots."""
    k = rec.get("_kind")
    if k in ("snapshot", "index", "indexCurve", "cash"):
        return rec
    if k == "transaction":
        to = dict(rec)
        if rec.get("type") is not None and to.get("action") is None:
            to["action"] = rec["type"]
        if rec.get("qty") is not None and to.get("shares") is None:
            to["shares"] = _num(rec["qty"])
        if (rec.get("ts") or rec.get("date")) and to.get("date") is None:
            to["date"] = rec.get("ts") or rec.get("date")
        return to

    out = dict(rec)
    tk = str(rec.get("ticker") or "").upper()
    if not tk:
        return out
    # schema reads shares/avgCost/openDate; app stores qty/costBasis/addedAt.
    if rec.get("qty") is not None and out.get("shares") is None:
        out["shares"] = _num(rec["qty"])
    if rec.get("costBasis") is not None and out.get("avgCost") is None:
        out["avgCost"] = _num(rec["costBasis"])
    if rec.get("addedAt") and out.get("openDate") is None:
        out["openDate"] = rec["addedAt"]
        out.setdefault("open_date", rec["addedAt"])
    # exit date from the most recent sell note; thesis from the first note.
    notes = rec.get("notes") if isinstance(rec.get("notes"), list) else []
    if out.get("exitDate") is None:
        sells = [n for n in notes if isinstance(n, dict) and n.get("soldAction")]
        if sells and sells[-1].get("ts"):
            out["exitDate"] = sells[-1]["ts"]; out["exit_date"] = sells[-1]["ts"]
    if out.get("thesis") is None and notes:
        first = notes[0]
        out["thesis"] = str(first.get("text") if isinstance(first, dict) else first)

    # marketValue / pnl / weight for ACTIVE lots (Long/Short/…) with qty.
    role = str(rec.get("position") or "").lower()
    qty, avg = _num(rec.get("qty")), _num(rec.get("costBasis"))
    if role in ACTIVE_ROLES and qty:
        live = price_map.get(tk)
        price = live if live is not None else avg
        # Provenance: market (USD-converted) mark, or marked at cost because no
        # convertible quote exists (no master row / no FX rate for its currency).
        out["priceSource"] = "market" if live is not None else "cost"
        out["priceCurrency"] = "USD"
        if live is not None:
            out["markPriceUsd"] = round(live, 6)
            meta = PRICE_META.get(tk)
            if meta:   # same provenance fields the app writes (z81)
                out["localCurrency"] = meta["localCurrency"]
                out["localPrice"] = round(meta["localPrice"], 6)
                if meta.get("fxRate"):
                    out["fxRate"] = float(f"{meta['fxRate']:.8g}")
        elif tk in UNPRICED:
            out["priceNote"] = f"no FX rate for {UNPRICED[tk]}"
        if price is not None:
            direction = -1.0 if role in SHORT_ROLES else 1.0
            mv = abs(qty * price)
            out["marketValue"] = round(mv, 2)
            if avg is not None:
                pnl = (price - avg) * qty * direction
                out["pnl"] = round(pnl, 2)
                basis = abs(avg * qty)
                if basis:
                    out["pnlPct"] = round(pnl / basis * 100, 4)
                    out["returnPct"] = out["pnlPct"]
            if total_mv:
                out["weight"] = round(mv / total_mv * 100, 4)
    return out


# ------------------------------------------------------------- build rows ----
def build_records(d):
    recs = []
    for e in (d.get("portfolio") or []):
        if isinstance(e, dict):
            recs.append(dict(e, _kind="entry"))
    for t in (d.get("transactions") or []):
        if isinstance(t, dict):
            r = dict(t, _kind="transaction")
            r["id"] = _stable_txn_id(t)
            recs.append(r)
    if d.get("cashPosition") is not None:
        recs.append({"id": "__cash__", "_kind": "cash", "value": d["cashPosition"]})
    if d.get("goodGlobeIndex") is not None:
        recs.append({"id": "__goodglobe_index__", "_kind": "index", "value": d["goodGlobeIndex"]})
    if d.get("goodGlobeCurve") is not None:
        recs.append({"id": "__goodglobe_curve__", "_kind": "indexCurve", "value": d["goodGlobeCurve"]})
    recs.append({"id": "__portfolio_snapshot__", "_kind": "snapshot",
                 "counts": d.get("counts"), "generatedAt": d.get("generatedAt"), "schema": d.get("schema")})
    return recs


def _existing_ids():
    st, body = _req("GET", f"/rest/v1/{TABLE}?select=id")
    if st == 200:
        try:
            return {r["id"] for r in json.loads(body) if isinstance(r, dict) and "id" in r}
        except Exception:
            return set()
    print(f"  (could not list existing ids: HTTP {st})")
    return set()


# ---- Scheduled-run freshness guard -------------------------------------------
# The app writes Supabase directly whenever you edit, and commits this JSON only
# when you press "Save to Repo". A PUSH of the file is therefore always a
# deliberate, current snapshot and is synced. An hourly SCHEDULED run, however,
# would replay whatever copy sits in the repo - a months-old file would
# overwrite newer rows and its reconcile step would DELETE rows added since.
# So scheduled runs only sync a file generated within the last N hours.
MAX_AGE_H = float(os.environ.get("PORT_SYNC_MAX_AGE_H") or 36)


def _stale_for_schedule(d, label):
    if (os.environ.get("SYNC_TRIGGER") or "").strip() != "schedule":
        return False
    ts = d.get("generatedAt") if isinstance(d, dict) else None
    try:
        gen = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if gen.tzinfo is None:
            gen = gen.replace(tzinfo=timezone.utc)
        age_h = (datetime.now(timezone.utc) - gen).total_seconds() / 3600
    except Exception:
        age_h = None
    if age_h is not None and age_h <= MAX_AGE_H:
        return False
    shown = "unknown age" if age_h is None else f"{age_h:.0f}h old"
    print(f"::notice::{label}: scheduled sync SKIPPED - repo file is {shown} (generatedAt={ts}). "
          f"Only fresh files (<= {MAX_AGE_H:.0f}h) or a direct push are synced, so a stale repo copy "
          "can never overwrite or delete newer rows written by the app.")
    return True
# -----------------------------------------------------------------------------


FAILED = []


def main():
    if not URL or not KEY:
        print("X Missing creds. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE (or SUPABASE_SERVICE_KEY).")
        return 1
    if not os.path.exists(DATA):
        print(f"No {DATA} — nothing to sync.")
        return 0
    d = json.load(open(DATA))
    if not isinstance(d, dict):
        print("portfolio_data.json is not a wrapped snapshot object — skipping.")
        return 0
    if _stale_for_schedule(d, "portfolio"):
        return 0

    records = build_records(d)
    price_map = _load_price_map()
    if UNPRICED:
        print(f"  {len(UNPRICED)} ticker(s) skipped - no FX rate for their currency: "
              + ", ".join(f"{t}({c})" for t, c in sorted(UNPRICED.items())[:12]))
    print(f"Portfolio -> Supabase ({URL}) · {len(records)} record(s), {len(price_map)} prices")

    # total active market value (for weight_pct).
    total_mv = 0.0
    for r in records:
        if r.get("_kind") == "entry" and str(r.get("position") or "").lower() in ACTIVE_ROLES:
            qty = _num(r.get("qty"))
            if qty:
                px = price_map.get(str(r.get("ticker") or "").upper()) or _num(r.get("costBasis"))
                if px:
                    total_mv += abs(qty * px)

    now = now_iso()
    rows, local_ids = [], set()
    for rec in records:
        if rec.get("_kind") in ("entry", "transaction"):
            _ensure_port_id(rec)
            pos = _enrich(rec, price_map, total_mv)
            rows.append({"id": rec["id"], "position": pos, "updated_at": now})
        else:
            rows.append({"id": rec["id"], "position": _enrich(rec, price_map, total_mv), "updated_at": now})
        local_ids.add(rec["id"])

    # upsert (fresh, no duplicates)
    sent = 0
    for i in range(0, len(rows), 100):
        chunk = rows[i:i + 100]
        st, body = _req("POST", f"/rest/v1/{TABLE}?on_conflict=id", chunk,
                        {"Prefer": "resolution=merge-duplicates,return=minimal"})
        if st in (200, 201, 204):
            sent += len(chunk)
        else:
            print(f"::error::portfolio upsert chunk {i} -> HTTP {st}: {body[:240]}")
            FAILED.append("upsert")
    print(f"  upserted {sent}/{len(rows)}")

    # RECONCILE: drop rows gone locally — but keep txn history + the snapshot.
    # Guard: only reconcile when we actually have local records, so an empty or
    # unreadable file can never wipe the table.
    deleted = 0
    if local_ids and not FAILED:   # never delete after a failed upsert
        stale = [i for i in _existing_ids()
                 if i not in local_ids
                 and not (isinstance(i, str) and i.startswith("txn:"))
                 and i != "__portfolio_snapshot__"]
        for i in range(0, len(stale), 50):
            chunk = stale[i:i + 50]
            in_list = ",".join('"%s"' % str(x).replace('"', "") for x in chunk)
            st, body = _req("DELETE", f"/rest/v1/{TABLE}?id=in.({urllib.parse.quote(in_list)})", None,
                            {"Prefer": "return=minimal"})
            if st in (200, 204):
                deleted += len(chunk)
            else:
                print(f"::error::portfolio reconcile delete -> HTTP {st}: {body[:200]}")
                FAILED.append("delete")
    if FAILED:
        print(f"X portfolio sync: {sent} upserted, {deleted} removed, FAILED: {sorted(set(FAILED))}")
        return 1
    print(f"OK portfolio sync complete — {sent} upserted, {deleted} stale row(s) removed")
    return 0


import urllib.parse  # noqa: E402  (used in main's reconcile)

if __name__ == "__main__":
    sys.exit(main())
