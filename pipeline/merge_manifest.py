#!/usr/bin/env python3
"""
TRAPP2 — merge per-batch 20y history manifest fragments into the authoritative
data/history_manifest.json.

Each batch of fetch_history.py writes data/history_manifest.b<N>.json with the
spans of the tickers IT handled. This script unions all those fragments (plus
whatever is already in history_manifest.json) into one manifest, so the final
file lists every ticker that has history and its first/last/rows coverage.

It is intentionally tolerant: if NO fragments exist (e.g. a run where every
ticker was fresh-skipped, or the fragments were already folded in), it leaves
the existing manifest untouched and exits 0 rather than failing the job.
"""
import json
import glob
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
MANIFEST = DATA / "history_manifest.json"


def log(*a):
    print("[merge_manifest]", *a, flush=True)


def load_json(p):
    try:
        return json.loads(Path(p).read_text())
    except Exception:
        return None


def main():
    by_ticker = {}

    # Seed with the existing manifest so we never lose tickers already recorded.
    existing = load_json(MANIFEST) if MANIFEST.exists() else None
    if isinstance(existing, dict):
        bt = existing.get("by_ticker")
        if isinstance(bt, dict):
            by_ticker.update(bt)
        elif isinstance(existing.get("tickers"), list):
            for t in existing["tickers"]:
                by_ticker.setdefault(t, {})

    # Fold in every batch fragment.
    frags = sorted(glob.glob(str(DATA / "history_manifest.b*.json")))
    log(f"found {len(frags)} fragment(s)")
    for fp in frags:
        frag = load_json(fp)
        if not isinstance(frag, dict):
            continue
        spans = frag.get("by_ticker", {})
        if isinstance(spans, dict):
            for t, span in spans.items():
                prev = by_ticker.get(t, {})
                if isinstance(prev, dict) and isinstance(span, dict):
                    prev.update(span)
                    by_ticker[t] = prev
                else:
                    by_ticker[t] = span

    tickers = sorted(by_ticker.keys())
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "count": len(tickers),
        "tickers": tickers,            # backward-compat list
        "by_ticker": by_ticker,        # per-ticker {first, last, rows}
    }
    MANIFEST.write_text(json.dumps(manifest, separators=(",", ":")))
    log(f"✓ merged manifest covers {len(tickers)} tickers")

    # Clean up the fragments now that they're folded in, so the next run starts
    # clean and the git glob never goes stale. Best-effort.
    for fp in frags:
        try:
            Path(fp).unlink()
            log(f"  removed {Path(fp).name}")
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
