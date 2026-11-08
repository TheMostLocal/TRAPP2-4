"""
TRAPP2 pipeline shared helpers.
Math, IO, and small utilities used by compute_signals / classify_regime / compute_transitions.
"""
import json
import math
import statistics
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DATA = ROOT / "data"
HISTORY = DATA / "history"
MACRO = DATA / "macro"


def log(*args):
    print("[trapp2]", *args, flush=True)


def utc_today():
    return datetime.now(timezone.utc).date().isoformat()


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_json(path, default=None):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except (json.JSONDecodeError, OSError):
        return default


def finite(obj):
    """Recursively replace NaN / ±Infinity with None. Python's json writes them as
    bare NaN tokens, which browsers (JSON.parse) and Postgres/PostgREST REJECT —
    one NaN made a whole regime_snapshots upsert batch fail."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: finite(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [finite(v) for v in obj]
    return obj


def write_json(path, data, compact=False):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = finite(data)
    if compact:
        p.write_text(json.dumps(data, separators=(",", ":"), allow_nan=False))
    else:
        p.write_text(json.dumps(data, indent=2, allow_nan=False))


def load_history(ticker):
    """Load per-ticker history → list of (date_iso, price) sorted asc."""
    f = HISTORY / f"{ticker}.json"
    raw = read_json(f)
    if raw is None:
        return []
    data = raw
    if isinstance(raw, dict):
        for key in (ticker, "history", "prices", "data"):
            if isinstance(raw.get(key), list):
                data = raw[key]
                break
        else:
            return []
    if not isinstance(data, list):
        return []
    out = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        d = entry.get("date") or entry.get("Date")
        p = entry.get("price") or entry.get("close") or entry.get("Close") or entry.get("adjClose")
        if d is None or p is None:
            continue
        try:
            fp = float(p)
        except (ValueError, TypeError):
            continue
        if math.isfinite(fp):
            out.append((str(d)[:10], fp))
    out.sort()
    return out


def load_macro_series(series_id):
    """Load a FRED series saved by fetch_macro → list of (date, value)."""
    f = MACRO / f"{series_id}.json"
    raw = read_json(f)
    if not raw:
        return []
    obs = raw.get("observations") if isinstance(raw, dict) else raw
    if not isinstance(obs, list):
        return []
    out = []
    for o in obs:
        if not isinstance(o, dict):
            continue
        d = o.get("date")
        v = o.get("value")
        if not d or v in (None, ".", ""):
            continue
        try:
            fv = float(v)
        except (ValueError, TypeError):
            continue
        if math.isfinite(fv):
            out.append((str(d)[:10], fv))
    out.sort()
    return out


def last_value(series):
    return series[-1][1] if series else None


def returns(history, days):
    """Total return over the last N calendar days. Binary searches for closest bar."""
    if not history or len(history) < 2:
        return None
    end_date, end_price = history[-1]
    if end_price <= 0:
        return None
    target = (datetime.fromisoformat(end_date) - timedelta(days=days)).date().isoformat()
    lo, hi, best = 0, len(history) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if history[mid][0] <= target:
            best = history[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    if not best or best[1] <= 0:
        return None
    return (end_price / best[1]) - 1


def sma(history, days):
    if not history or len(history) < days:
        return None
    return statistics.mean(p for _, p in history[-days:])


def annualized_vol(history, days=21):
    if not history or len(history) < days + 2:
        return None
    closes = [p for _, p in history[-(days + 1):]]
    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0 and closes[i] > 0:
            rets.append(math.log(closes[i] / closes[i - 1]))
    if len(rets) < 5:
        return None
    return statistics.stdev(rets) * math.sqrt(252)


def zscore(value, series):
    if not series or len(series) < 30 or value is None:
        return None
    mean = statistics.mean(series)
    sd = statistics.stdev(series)
    if sd == 0:
        return 0.0
    return (value - mean) / sd


def clamp(v, lo=-1.0, hi=1.0):
    if v is None:
        return None
    return max(lo, min(hi, v))


def squash(z, k=1.0):
    if z is None:
        return None
    return math.tanh(z * k)
