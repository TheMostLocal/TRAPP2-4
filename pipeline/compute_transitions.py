#!/usr/bin/env python3
"""
TRAPP2 — Phase 4: Markov Transition Layer.

Reads data/regime_history.json (accumulated daily snapshots from classify_regime).
Estimates the maximum-likelihood transition matrix P using Laplace smoothing,
then computes:
  - 1-step, 5-step, 20-step transition probabilities (via P^n)
  - stationary distribution (long-run regime proportions)
  - expected persistence in current regime

Attaches results to data/regime_current.json so the dashboard can render them.

Requires no numpy — uses pure-Python list-of-lists math. (Matrix is only 6×6
so this is plenty fast.)

Walk-forward safe: only uses past observations to estimate P. No lookahead.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from lib import DATA, log, read_json, write_json, utc_now_iso

# Must match the order used in classify_regime.REGIME_DEFINITIONS
REGIME_NAMES = ["expansion", "risk_on_melt_up", "inflation_shock",
                "recession_fear", "risk_off", "neutral"]
N = len(REGIME_NAMES)
IDX = {name: i for i, name in enumerate(REGIME_NAMES)}

# Minimum number of transitions before we trust the empirical matrix.
# Until then, regime_current still gets a P matrix but with a low-confidence flag.
MIN_OBSERVATIONS = 60

# Laplace smoothing constant — adds this much "virtual" count to every cell.
# Prevents zero-probability transitions when a particular i→j has never happened yet.
LAPLACE_ALPHA = 1.0


def estimate_transition_matrix(snapshots):
    """Build P[i][j] = P(next_regime=j | current_regime=i) via MLE + Laplace smoothing.
    snapshots: list of {date, regime, ...} sorted ascending by date.
    Returns (P matrix as list-of-lists, raw count matrix, total transitions observed).
    """
    # Count transitions
    counts = [[0] * N for _ in range(N)]
    for k in range(len(snapshots) - 1):
        cur = snapshots[k].get("regime")
        nxt = snapshots[k + 1].get("regime")
        if cur not in IDX or nxt not in IDX:
            continue
        counts[IDX[cur]][IDX[nxt]] += 1

    total_obs = sum(sum(row) for row in counts)

    # Convert to probabilities with Laplace smoothing
    P = [[0.0] * N for _ in range(N)]
    for i in range(N):
        row_sum = sum(counts[i]) + LAPLACE_ALPHA * N
        for j in range(N):
            P[i][j] = (counts[i][j] + LAPLACE_ALPHA) / row_sum

    return P, counts, total_obs


def mat_mul(A, B):
    """Multiply two N×N matrices (pure Python)."""
    out = [[0.0] * N for _ in range(N)]
    for i in range(N):
        for j in range(N):
            out[i][j] = sum(A[i][k] * B[k][j] for k in range(N))
    return out


def mat_power(P, n):
    """Raise P to the n-th power via repeated squaring."""
    if n == 0:
        # Identity matrix
        I = [[1.0 if i == j else 0.0 for j in range(N)] for i in range(N)]
        return I
    if n == 1:
        return [row[:] for row in P]
    half = mat_power(P, n // 2)
    full = mat_mul(half, half)
    if n % 2 == 1:
        full = mat_mul(full, P)
    return full


def stationary_distribution(P, max_iter=500, tol=1e-10):
    """Solve πP = π, Σπ=1 via Gauss-Jordan on (P^T - I) | replace last row with 1s.
    Returns list of N probabilities.
    """
    # Build augmented matrix: A = (P^T - I), with last row = [1,1,...,1] | RHS = [0,0,...,1]
    A = [[(P[j][i] - (1.0 if i == j else 0.0)) for j in range(N)] for i in range(N)]
    # Replace last equation with normalization Σπ = 1
    A[-1] = [1.0] * N
    b = [0.0] * N
    b[-1] = 1.0

    # Solve A π = b via Gauss-Jordan
    M = [A[i] + [b[i]] for i in range(N)]
    for col in range(N):
        # Partial pivot
        pivot = col
        for row in range(col + 1, N):
            if abs(M[row][col]) > abs(M[pivot][col]):
                pivot = row
        M[col], M[pivot] = M[pivot], M[col]
        if abs(M[col][col]) < 1e-15:
            # Degenerate — fallback to uniform
            return [1.0 / N] * N
        # Normalize pivot row
        pv = M[col][col]
        M[col] = [x / pv for x in M[col]]
        # Eliminate other rows
        for row in range(N):
            if row != col and abs(M[row][col]) > 1e-15:
                factor = M[row][col]
                M[row] = [M[row][k] - factor * M[col][k] for k in range(N + 1)]

    pi = [max(0.0, M[i][N]) for i in range(N)]
    s = sum(pi)
    if s > 0:
        pi = [p / s for p in pi]
    return pi


def expected_persistence(P, i):
    """Expected number of steps the chain stays in state i once it enters.
    For an absorbing/sticky state with self-loop p = P[i][i],
    expected persistence is 1 / (1 - p). Capped at 365 for display sanity.
    """
    p = P[i][i]
    if p >= 0.9999:
        return 365
    return min(365, round(1.0 / (1.0 - p), 1))


def main():
    history = read_json(DATA / "regime_history.json", default=[])
    if not isinstance(history, list) or len(history) < 2:
        log(f"⚠ regime_history.json has {len(history) if isinstance(history, list) else 0} rows.")
        log("  Need ≥2 snapshots before transition matrix is meaningful.")
        log("  This script will still run but emit a low-confidence stub.")
        history = history if isinstance(history, list) else []

    P, counts, n_obs = estimate_transition_matrix(history)
    log(f"Estimated transition matrix from {n_obs} observed transitions across {len(history)} snapshots")

    # Multi-step forecasts: 1, 5, 20 day ahead
    P1 = P
    P5 = mat_power(P, 5)
    P20 = mat_power(P, 20)

    # Current regime → forecast distributions
    current = read_json(DATA / "regime_current.json", default={})
    cur_regime = current.get("regime", "neutral")
    if cur_regime not in IDX:
        cur_regime = "neutral"
    i = IDX[cur_regime]

    def row_to_dict(row):
        return {REGIME_NAMES[j]: round(row[j], 4) for j in range(N)}

    transitions = {
        "1_step":  row_to_dict(P1[i]),
        "5_step":  row_to_dict(P5[i]),
        "20_step": row_to_dict(P20[i]),
    }

    # Stationary distribution
    pi = stationary_distribution(P)
    stationary = {REGIME_NAMES[j]: round(pi[j], 4) for j in range(N)}

    persistence = expected_persistence(P, i)

    # Attach to regime_current.json
    current["transitions"] = transitions
    current["stationary"] = stationary
    current["expected_persistence_days"] = persistence
    current["transition_matrix"] = [[round(P[i][j], 4) for j in range(N)] for i in range(N)]
    current["transition_observation_count"] = n_obs
    current["transition_warning"] = (
        None if n_obs >= MIN_OBSERVATIONS
        else f"Only {n_obs} transitions observed — need ≥{MIN_OBSERVATIONS} for high confidence"
    )
    current["transitions_computed_at"] = utc_now_iso()

    write_json(DATA / "regime_current.json", current)

    # Pretty-print
    log("")
    log("=" * 60)
    log(f"CURRENT REGIME: {cur_regime}")
    log(f"Expected persistence: {persistence} steps")
    log("")
    log("1-step transition probabilities:")
    for name in sorted(transitions["1_step"], key=lambda k: -transitions["1_step"][k]):
        p = transitions["1_step"][name]
        bar = "█" * int(p * 30)
        log(f"  {name:18s} {p:.3f}  {bar}")
    log("")
    log("Stationary distribution (long-run):")
    for name in sorted(stationary, key=lambda k: -stationary[k]):
        p = stationary[name]
        bar = "▒" * int(p * 30)
        log(f"  {name:18s} {p:.3f}  {bar}")

    if current["transition_warning"]:
        log("")
        log(f"⚠ {current['transition_warning']}")

    log("")
    log("✓ Updated data/regime_current.json with transitions + stationary + persistence")
    return 0


if __name__ == "__main__":
    sys.exit(main())
