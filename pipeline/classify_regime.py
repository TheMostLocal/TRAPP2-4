#!/usr/bin/env python3
"""
TRAPP2 — Phase 3: Regime Classifier + State Object writer.

Reads signal scores from compute_signals.py, classifies the current regime via
a soft-vote classifier (6 ideal-score profiles, sum-of-squares distance, softmax),
and writes:
  data/regime_current.json   — today's snapshot (the "brain")
  data/regime_history.json   — append-only daily history (Markov input)

In Phase 4 (compute_transitions.py), once regime_history has ~60 rows, the Markov
transition matrix will be computed and attached to regime_current.

Regime definitions:
  expansion        — broad bullish, growth + risk-on + low vol
  risk_on_melt_up  — extreme positive momentum
  inflation_shock  — macro weak, vol elevated, valuation hit
  recession_fear   — growth crashing, liquidity tight, defensive
  risk_off         — flight-to-quality, vol elevated
  neutral          — no dominant signal
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import DATA, log, write_json, read_json, utc_today, utc_now_iso, load_macro_series
from compute_signals import compute_all

# Ideal score profiles per regime.
# Order: macro, liquidity, vol, val, trend, breadth, commodity, crypto.
# Softmax weights derived from sum-squared distance between actual scores and ideals.
#
# commodity reasoning:
#   expansion: copper rallying with broad cyclical demand → +0.3
#   melt_up:   risk appetite high, commodities firm → +0.4
#   inflation_shock: oil ripping, gold/oil ratio low → +0.6 (strongly positive)
#   recession_fear: industrial demand collapsing → -0.5
#   risk_off:  gold dominates oil/copper → -0.3
#
# crypto reasoning:
#   expansion: BTC/ETH appreciating with risk-on flow → +0.3
#   melt_up:   crypto leading equities → +0.7
#   inflation_shock: crypto often sells off on rate fear → -0.2
#   recession_fear: deleveraging in crypto → -0.4
#   risk_off:  risk assets dumped first → -0.5
REGIME_DEFINITIONS = [
    # name              macro  liq    vol    val    trend  breadth  comm   crypto
    ("expansion",       +0.4,  +0.4,  +0.3,  0.0,   +0.5,  +0.4,    +0.3,  +0.3),
    ("risk_on_melt_up", +0.6,  +0.5,  +0.6,  -0.4,  +0.8,  +0.7,    +0.4,  +0.7),
    ("inflation_shock", -0.4,  -0.4,  -0.4,  +0.2,  -0.1,  -0.2,    +0.6,  -0.2),
    ("recession_fear",  -0.6,  -0.5,  -0.7,  -0.1,  -0.5,  -0.5,    -0.5,  -0.4),
    ("risk_off",        -0.2,  -0.4,  -0.5,  0.0,   -0.3,  -0.4,    -0.3,  -0.5),
    ("neutral",          0.0,   0.0,   0.0,  0.0,   0.0,   0.0,     0.0,   0.0),
]

ORDER = ["macro", "liquidity", "volatility", "valuation", "trend", "breadth", "commodity", "crypto"]


def _score_vec(scores):
    """Convert scores dict → fixed-order list, replacing None with 0."""
    return [scores.get(k) if scores.get(k) is not None else 0.0 for k in ORDER]


def classify(scores, signal_confidences):
    """Soft-vote classifier. Returns (regime_label, regime_probs, overall_confidence)."""
    s = _score_vec(scores)
    fits = []
    n_signals = len(ORDER)
    for name, *ideal in REGIME_DEFINITIONS:
        # Sum of squared distances between actual and ideal score profile
        dist = sum((s[i] - ideal[i]) ** 2 for i in range(n_signals))
        # Give 'neutral' a small bias so it wins when everything is near zero
        if name == "neutral":
            dist -= 0.30
        fits.append((name, dist))

    # Softmax: weight = exp(-k * dist), k controls sharpness
    K = 1.5
    weights = [(name, math.exp(-K * dist)) for name, dist in fits]
    total = sum(w for _, w in weights)
    probs = {name: w / total for name, w in weights}
    best = max(probs, key=probs.get)

    # Overall confidence: average per-signal conf, weighted by signal's contribution
    valid_confs = [c for c in signal_confidences.values() if c and c > 0]
    overall = sum(valid_confs) / len(valid_confs) if valid_confs else 0.0

    return best, probs, overall


def top_drivers(scores, signal_confidences, n=3):
    """Return the n signals with largest |score| (most influential right now)."""
    items = []
    for name, val in scores.items():
        if val is None:
            continue
        items.append({
            "signal": name,
            "score": round(val, 3),
            "confidence": round(signal_confidences.get(name, 0.0), 3),
            "weight": round(abs(val) * signal_confidences.get(name, 1.0), 3),
            "direction": "bullish" if val > 0.15 else "bearish" if val < -0.15 else "neutral",
        })
    items.sort(key=lambda x: -x["weight"])
    return items[:n]


def build_snapshot():
    """Run signals → classifier → state object."""
    scores, confs, notes = compute_all()
    regime, probs, overall = classify(scores, confs)
    drivers = top_drivers(scores, confs)

    return {
        "date": utc_today(),
        "regime": regime,
        "confidence": round(overall, 3),
        "scores": {k: (round(v, 3) if v is not None else None) for k, v in scores.items()},
        "signal_confidences": {k: round(v, 3) for k, v in confs.items()},
        "current_probabilities": {k: round(v, 4) for k, v in probs.items()},
        "drivers": drivers,
        "signal_notes": notes,
        "computed_at": utc_now_iso(),
    }


def append_history(snapshot):
    """Append today's snapshot to data/regime_history.json (replace if same date)."""
    hist_path = DATA / "regime_history.json"
    history = read_json(hist_path, default=[])
    if not isinstance(history, list):
        history = []
    # Drop any prior row for today
    history = [row for row in history if row.get("date") != snapshot["date"]]
    history.append(snapshot)
    history.sort(key=lambda r: r.get("date", ""))
    write_json(hist_path, history, compact=True)
    return len(history)


# History is permanent (Markov input), so a row only goes in when its inputs
# were complete. A run with FRED macro missing (fresh repo, failed fetch, no
# FRED_API_KEY) produces a degraded snapshot: still published as
# regime_current.json (flagged), never appended to history.
REQUIRED_MACRO = ("INDPRO", "T10Y2Y", "BAMLH0A0HYM2", "VIXCLS", "DGS10", "CPIAUCSL")
MAX_NULL_SCORES = 2


def input_gaps(snap):
    missing = [s for s in REQUIRED_MACRO if len(load_macro_series(s)) < 2]
    nulls = [k for k, v in (snap.get("scores") or {}).items() if v is None]
    return missing, nulls


def main():
    snap = build_snapshot()
    missing, nulls = input_gaps(snap)
    complete = not missing and len(nulls) <= MAX_NULL_SCORES
    snap["inputs_complete"] = complete
    if not complete:
        snap["inputs_missing"] = {"macro": missing, "null_scores": nulls}
    write_json(DATA / "regime_current.json", snap)
    if complete:
        n = append_history(snap)
    else:
        n = len(read_json(DATA / "regime_history.json", default=[]) or [])
        print(f"::warning::regime: inputs incomplete (missing macro: {missing or 'none'}; "
              f"null scores: {nulls or 'none'}) - regime_current.json written and flagged, "
              f"history NOT appended. Run the Mid-Day Refresh (fetch_macro.py) to restore data/macro.")

    log("")
    log("=" * 60)
    log(f"REGIME: {snap['regime'].upper()}")
    log(f"Confidence: {snap['confidence']}")
    log("")
    log("Scores:")
    for k, v in snap["scores"].items():
        c = snap["signal_confidences"][k]
        marker = "—" if v is None else f"{v:+.3f}"
        log(f"  {k:12s} {marker:>8s}  conf={c:.2f}")
    log("")
    log("Regime probabilities:")
    for name, p in sorted(snap["current_probabilities"].items(), key=lambda kv: -kv[1]):
        bar = "█" * int(p * 30)
        log(f"  {name:18s} {p:.3f}  {bar}")
    log("")
    log("Top drivers:")
    for d in snap["drivers"]:
        log(f"  {d['signal']:12s} {d['score']:+.3f}  weight={d['weight']:.2f}  ({d['direction']})")
    log("")
    log(f"✓ Wrote data/regime_current.json")
    log(f"✓ {'Appended to' if complete else 'NOT appended to'} data/regime_history.json ({n} total snapshots)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
