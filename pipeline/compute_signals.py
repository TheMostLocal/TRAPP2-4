#!/usr/bin/env python3
"""
TRAPP2 — Phase 2: Unified Signal Engine.

Reads data/history/*.json (yfinance) + data/macro/*.json (FRED).
Produces 8 standardized scores in [-1, +1], each with a confidence in [0, 1]:

  macro       — sector ETF momentum (growth proxy)
  liquidity   — yield curve slope + HY credit spread + HYG/LQD ratio
  volatility  — VIX level + VIX9D term structure (inverted)
  valuation   — SPY P/E vs long-run baseline + Fed model spread
  trend       — SPY price vs 50/200 dma
  breadth     — % of sector ETFs above their 50dma
  commodity   — oil + gold momentum + gold/oil ratio (inflation + risk-off detection)
  crypto      — BTC/ETH momentum + BTC/SPY beta (risk appetite proxy)

Output: dict ready for classify_regime.py to consume.

Score convention: POSITIVE = pro-risk / bullish, NEGATIVE = bearish.
"""
import csv
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import (
    DATA, log, load_history, load_macro_series, last_value,
    returns, sma, clamp, squash, zscore
)

SECTOR_ETFS = ["XLK", "XLF", "XLV", "XLE", "XLI", "XLP", "XLY", "XLU", "XLB", "XLRE", "XLC"]
# Additional non-equity data points the signal engine consumes.
# Crypto: BTC and ETH spot prices (yahoo "BTC-USD", "ETH-USD")
# Futures: WTI crude, gold, copper, S&P E-mini (yahoo "CL=F", "GC=F", "HG=F", "ES=F")
# FX: dollar index proxy (yahoo "DX-Y.NYB" or UUP ETF as fallback)
COMMODITY_TICKERS = ["CL=F", "GC=F", "HG=F"]
CRYPTO_TICKERS = ["BTC-USD", "ETH-USD"]
FX_TICKERS = ["UUP"]  # dollar index ETF proxy
REFERENCE_TICKERS = (
    ["SPY", "^VIX", "^VIX9D", "HYG", "LQD"]
    + SECTOR_ETFS
    + COMMODITY_TICKERS
    + CRYPTO_TICKERS
    + FX_TICKERS
)


def _spy_pe_from_master():
    """Pull SPY P/E from master.csv (written by fetch_data.py)."""
    f = DATA / "master.csv"
    if not f.exists():
        return None
    try:
        with f.open("r", newline="", encoding="utf-8") as fp:
            for row in csv.DictReader(fp):
                if row.get("ticker", "").upper() == "SPY":
                    pe = (row.get("pe") or "").strip()
                    return float(pe) if pe else None
    except (OSError, ValueError):
        pass
    return None


# ── Individual signals ──────────────────────────────────────────────
# Each returns: (score in [-1, +1], confidence in [0, 1], notes dict)

def signal_macro(hist, macro):
    """Macro momentum: equal-weighted 1M return of sector ETFs.
    Pure price-based; FRED INDPRO trend layered on if available.
    """
    rets_1m = []
    for t in SECTOR_ETFS:
        r = returns(hist.get(t, []), 30)
        if r is not None:
            rets_1m.append(r)
    if len(rets_1m) < 5:
        return None, 0.0, {"reason": "insufficient sector data"}

    avg = statistics.mean(rets_1m)
    # ±5% monthly sector return = ±1.0 score
    price_score = clamp(avg / 0.05)
    notes = {"avg_1m_sector_return": round(avg, 4), "n_sectors": len(rets_1m)}

    # Layer in INDPRO YoY if available (industrial production)
    indpro = macro.get("INDPRO", [])
    if len(indpro) >= 13:
        last = indpro[-1][1]
        yr_ago = indpro[-13][1] if yr_ago_idx_valid(indpro, 13) else None
        if yr_ago and yr_ago > 0:
            yoy = (last / yr_ago) - 1
            # +5% YoY industrial = strong growth → +1.0 from this component
            macro_component = clamp(yoy / 0.05)
            notes["indpro_yoy"] = round(yoy, 4)
            score = 0.6 * price_score + 0.4 * macro_component
            return clamp(score), min(1.0, len(rets_1m) / len(SECTOR_ETFS)), notes

    return price_score, min(1.0, len(rets_1m) / len(SECTOR_ETFS)), notes


def signal_liquidity(hist, macro):
    """Liquidity: curve slope + HY credit spread + HYG/LQD relative momentum.
    Steep curve + tight spreads + HYG outperforming LQD → POSITIVE.
    """
    components = []
    notes = {}

    # Component 1: FRED 10Y-2Y spread
    t10y2y = macro.get("T10Y2Y", [])
    if t10y2y:
        slope = last_value(t10y2y)
        if slope is not None:
            # -1% inversion → -1.0, +2% steep → +1.0
            components.append(clamp(slope / 1.5))
            notes["t10y2y"] = round(slope, 3)

    # Component 2: FRED high yield OAS (spread). Wider = bearish. Z-score it.
    hy = macro.get("BAMLH0A0HYM2", [])
    if len(hy) >= 252:
        last_hy = hy[-1][1]
        recent_values = [v for _, v in hy[-1260:]]  # ~5y
        z = zscore(last_hy, recent_values)
        if z is not None:
            # High Z (spread blowing out) = bearish. Invert sign.
            components.append(squash(-z, k=0.5))
            notes["hy_spread"] = round(last_hy, 3)
            notes["hy_spread_z"] = round(z, 2) if z is not None else None

    # Component 3: HYG vs LQD 30d return diff
    r_hyg = returns(hist.get("HYG", []), 30)
    r_lqd = returns(hist.get("LQD", []), 30)
    if r_hyg is not None and r_lqd is not None:
        diff = r_hyg - r_lqd
        components.append(clamp(diff / 0.02))  # 2% diff = ±1.0
        notes["hyg_minus_lqd_1m"] = round(diff, 4)

    if not components:
        return None, 0.0, {"reason": "no liquidity inputs"}
    return statistics.mean(components), min(1.0, len(components) / 3), notes


def signal_volatility(hist, macro):
    """Volatility: VIX level + VIX9D term structure.
    LOW VIX + contango (VIX9D < VIX) → POSITIVE (calm).
    HIGH VIX + backwardation (VIX9D > VIX) → NEGATIVE (panic).
    Score is INVERTED — high vol = bearish.
    """
    components = []
    notes = {}

    # Prefer FRED VIXCLS (cleaner) but fall back to yfinance ^VIX
    vix_series = macro.get("VIXCLS", []) or hist.get("^VIX", [])
    vix = last_value(vix_series)
    if vix is not None:
        if vix < 20:
            level_score = (20 - vix) / 8.0  # ~+1 at VIX 12
        else:
            level_score = -(vix - 20) / 15.0  # -1 at VIX 35
        components.append(clamp(level_score))
        notes["vix_level"] = round(vix, 2)

    vix9d = last_value(hist.get("^VIX9D", []))
    if vix9d is not None and vix and vix > 0:
        term = vix9d / vix
        # 0.95 (deep contango) → +1, 1.0 flat → 0, 1.15 (backwardation) → -1
        components.append(clamp(-(term - 1.0) / 0.10))
        notes["vix9d_over_vix"] = round(term, 4)

    if not components:
        return None, 0.0, notes
    return statistics.mean(components), min(1.0, len(components) / 2), notes


def signal_valuation(hist, macro):
    """Valuation: SPY P/E vs long-run baseline (~17).
    Layered with Fed model (earnings yield vs real 10y) when available.
    Rich valuations → NEGATIVE.
    """
    pe = _spy_pe_from_master()
    notes = {}
    if pe is None or pe <= 0:
        return None, 0.0, {"reason": "no SPY P/E in master.csv"}

    # PE 12 → +1, PE 17 → 0, PE 25 → -1 (8 PE points = 1 unit)
    pe_score = clamp(-(pe - 17.0) / 8.0)
    notes["spy_pe"] = round(pe, 2)

    # Fed model bonus: earnings yield − real 10y. Higher = cheaper.
    dgs10 = last_value(macro.get("DGS10", []))
    cpi = macro.get("CPIAUCSL", [])
    if dgs10 is not None and len(cpi) >= 13:
        last_cpi = cpi[-1][1]
        yr_ago = cpi[-13][1]
        if yr_ago and yr_ago > 0:
            inflation_yoy = (last_cpi / yr_ago) - 1
            real_10y = (dgs10 / 100.0) - inflation_yoy
            earnings_yield = 1.0 / pe
            spread = earnings_yield - real_10y
            # +5% spread (very cheap) → +1, -2% (rich) → -1
            fed_score = clamp(spread / 0.05)
            notes["earnings_yield"] = round(earnings_yield, 4)
            notes["real_10y"] = round(real_10y, 4)
            notes["fed_model_spread"] = round(spread, 4)
            score = 0.6 * pe_score + 0.4 * fed_score
            return clamp(score), 0.85, notes

    return pe_score, 0.7, notes


def signal_trend(hist, macro):
    """Trend: SPY vs 50dma vs 200dma posture."""
    spy = hist.get("SPY", [])
    if len(spy) < 200:
        return None, 0.0, {"reason": "insufficient SPY history"}
    price = last_value(spy)
    sma50 = sma(spy, 50)
    sma200 = sma(spy, 200)
    if not all([price, sma50, sma200]):
        return None, 0.0, {}

    # Distance from 200dma, max ±10%
    dist_200 = (price - sma200) / sma200
    s1 = clamp(dist_200 / 0.10)
    # 50/200 crossover posture, max ±5%
    s2 = clamp(((sma50 - sma200) / sma200) / 0.05)
    score = (s1 + s2) / 2
    notes = {
        "spy_price": round(price, 2),
        "sma50": round(sma50, 2),
        "sma200": round(sma200, 2),
        "pct_above_200dma": round(dist_200, 4),
    }
    return score, 1.0, notes


def signal_commodity(hist, macro):
    """
    Commodity signal: oil + gold + copper.
    Combines:
      (a) Industrial-demand pulse: copper momentum (HG=F) — positive = expansion
      (b) Inflation-shock proxy: oil momentum (CL=F) — high positive feeds inflation_shock regime
      (c) Risk-off detector: gold/oil ratio — rising = flight to safety (negative score)
    Net: positive when copper/oil rally together (cyclical demand);
         negative when gold dominates (defensive flight).
    """
    oil = hist.get("CL=F", [])
    gold = hist.get("GC=F", [])
    copper = hist.get("HG=F", [])
    components = []
    notes = {}

    # (a) Copper 60-day return — industrial bellwether
    if copper and len(copper) >= 60:
        ret60 = returns(copper, 60)
        if ret60 is not None:
            # ±15% over 60d is roughly ±1
            components.append(("copper_60d", squash(ret60 / 0.15)))
            notes["copper_60d_return"] = round(ret60, 4)

    # (b) Oil 60-day return
    if oil and len(oil) >= 60:
        ret60 = returns(oil, 60)
        if ret60 is not None:
            components.append(("oil_60d", squash(ret60 / 0.20)))
            notes["oil_60d_return"] = round(ret60, 4)

    # (c) Gold/Oil ratio Z-score: HIGH ratio = risk-off (flight to gold)
    # We invert it: positive component = ratio is LOW = risk-on
    # NOTE: load_history returns list of (date, price) TUPLES, so index with [1] for price.
    if gold and oil and len(gold) >= 252 and len(oil) >= 252:
        try:
            # Compute ratio series for the last 252 days
            n = min(len(gold), len(oil), 252)
            gold_recent = gold[-n:]
            oil_recent = oil[-n:]
            # align by index (both should be daily; assume same trading calendar)
            ratios = [g[1] / o[1] for g, o in zip(gold_recent, oil_recent) if o[1] > 0]
            if len(ratios) > 60:
                current = ratios[-1]
                mean = statistics.mean(ratios)
                stdev = statistics.stdev(ratios) if len(ratios) > 1 else 1.0
                z = (current - mean) / (stdev or 1.0)
                # Z > 0 = elevated risk-off → negative score
                components.append(("gold_oil_ratio_z_inv", squash(-z / 2.0)))
                notes["gold_oil_ratio"] = round(current, 3)
                notes["gold_oil_zscore"] = round(z, 3)
        except (ZeroDivisionError, IndexError, statistics.StatisticsError):
            pass

    if not components:
        return None, 0.0, {"reason": "no commodity data"}

    score = clamp(sum(c[1] for c in components) / len(components))
    confidence = min(1.0, len(components) / 3.0)
    notes["n_components"] = len(components)
    return score, confidence, notes


def signal_crypto(hist, macro):
    """
    Crypto signal: BTC + ETH momentum as a retail risk-appetite proxy.
    BTC/ETH tend to lead equity risk-on phases (high beta to liquidity).
    Negative when crypto is selling off independent of equities = stress signal.
    """
    btc = hist.get("BTC-USD", [])
    eth = hist.get("ETH-USD", [])
    components = []
    notes = {}

    # BTC 30-day return
    if btc and len(btc) >= 30:
        r30 = returns(btc, 30)
        if r30 is not None:
            # ±25% in 30d ~ ±1 (crypto vol is high)
            components.append(("btc_30d", squash(r30 / 0.25)))
            notes["btc_30d_return"] = round(r30, 4)

    # BTC 90-day trend (price vs 50dma proxy)
    if btc and len(btc) >= 50:
        s50 = sma(btc, 50)
        p = last_value(btc)
        if s50 and p:
            dev = (p - s50) / s50
            components.append(("btc_vs_50dma", squash(dev / 0.15)))
            notes["btc_pct_from_50dma"] = round(dev, 4)

    # ETH 30-day return
    if eth and len(eth) >= 30:
        r30 = returns(eth, 30)
        if r30 is not None:
            components.append(("eth_30d", squash(r30 / 0.30)))
            notes["eth_30d_return"] = round(r30, 4)

    if not components:
        return None, 0.0, {"reason": "no crypto data"}

    score = clamp(sum(c[1] for c in components) / len(components))
    confidence = min(1.0, len(components) / 3.0)
    notes["n_components"] = len(components)
    return score, confidence, notes


def signal_breadth(hist, macro):
    """Breadth: % of sector ETFs above their 50dma."""
    above, total = 0, 0
    for t in SECTOR_ETFS:
        h = hist.get(t, [])
        s50 = sma(h, 50)
        p = last_value(h)
        if s50 and p:
            total += 1
            if p > s50:
                above += 1
    if total < 5:
        return None, 0.0, {"reason": "insufficient sector data"}
    pct = above / total
    # 50% = neutral, 100% = +1, 0% = -1
    score = clamp((pct - 0.5) * 2)
    return score, min(1.0, total / len(SECTOR_ETFS)), {
        "pct_sectors_above_50dma": round(pct, 3),
        "n_above": above,
        "n_total": total,
    }


def yr_ago_idx_valid(series, lookback):
    """True if series has at least `lookback` entries."""
    return len(series) >= lookback


# ── Orchestrator ─────────────────────────────────────────────────────

def compute_all():
    """Run all 8 signals. Returns dict ready for classify_regime."""
    hist = {t: load_history(t) for t in REFERENCE_TICKERS}
    log(f"Loaded {sum(1 for v in hist.values() if v)} of {len(REFERENCE_TICKERS)} price histories")

    macro = {}
    for f in (DATA / "macro").glob("*.json"):
        macro[f.stem] = load_macro_series(f.stem)
    log(f"Loaded {len(macro)} FRED series")

    raw = {
        "macro":      signal_macro(hist, macro),
        "liquidity":  signal_liquidity(hist, macro),
        "volatility": signal_volatility(hist, macro),
        "valuation":  signal_valuation(hist, macro),
        "trend":      signal_trend(hist, macro),
        "breadth":    signal_breadth(hist, macro),
        "commodity":  signal_commodity(hist, macro),
        "crypto":     signal_crypto(hist, macro),
    }
    scores = {k: t[0] for k, t in raw.items()}
    confidences = {k: t[1] for k, t in raw.items()}
    notes = {k: t[2] for k, t in raw.items()}

    return scores, confidences, notes


if __name__ == "__main__":
    scores, confs, notes = compute_all()
    log("")
    log("=== SIGNAL SCORES ===")
    for k, v in scores.items():
        c = confs[k]
        s = "—" if v is None else f"{v:+.3f}"
        log(f"  {k:12s} {s:>8s}  conf={c:.2f}")
    log("")
    log("=== NOTES ===")
    import json as _json
    log(_json.dumps(notes, indent=2))

    # Write data/signals.json so merge_signals.py (which fetches each repo's
    # signals.json and builds the consensus the app reads) has a source. Without
    # this the signal-consensus chain has nothing to merge. Shape is the
    # "aggregate" form merge_signals auto-detects: { signals: { key: {raw, tier} } }.
    import datetime as _dt
    out_signals = {}
    for k, v in scores.items():
        out_signals[k] = {
            "raw": v,
            "confidence": confs.get(k),
            "tier": "LIVE" if (confs.get(k) or 0) >= 0.66 else "CACHED" if (confs.get(k) or 0) >= 0.33 else "STALE",
            "note": notes.get(k),
        }
    out_doc = {
        "_schema": "valuatio-signals-v1",
        "updatedAt": _dt.datetime.utcnow().isoformat() + "Z",
        "tickerCount": len(REFERENCE_TICKERS),
        "signals": out_signals,
    }
    try:
        out_path = DATA / "signals.json"
        out_path.write_text(_json.dumps(out_doc, indent=2))
        log(f"\nWrote {len(out_signals)} signals -> {out_path}")
    except Exception as e:
        log(f"\nFailed to write signals.json: {e}")
