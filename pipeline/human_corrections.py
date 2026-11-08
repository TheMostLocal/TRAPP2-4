#!/usr/bin/env python3
"""
human_corrections.py — apply XTRAPP human corrections to backend data.

XTRAPP is the human-in-the-loop store. Its corrections OUTRANK pulled data, so
every pipeline that feeds the bot, research grades or Supabase applies them
right after the pull. The app already does the same on the frontend
(resolveFieldValue: verified override -> active override -> fresh value).

    python pipeline/human_corrections.py master   # equity repos: data/master.json
    python pipeline/human_corrections.py grades   # TRAPP2-ANALYTICS: data/research_grades.json

Source: <owner>/XTRAPP/main/data/xtrapp_data.json (raw, public).
    - overrides:   { TICKER: { field: {value, setAt, expiresAt, verified, source} } }
                   (legacy flat values are accepted and treated as permanent)
    - humanGrades: { TICKER: {ticker, grade, status, note, updatedAt} }
Optional local fallback: data/overrides.json (the Editor's "Export for Backend"),
merged per field with XTRAPP, newer setAt wins.

Rules (same as the app):
    * An override is ACTIVE until its expiresAt (ms); null = permanent.
      Expired -> the pulled value shows through again.
    * Only tickers already present in this repo's file are touched - a
      correction never creates a row (4 repos share one XTRAPP).
    * The pulled value is preserved in row["_pulled"][column] so provenance is
      never lost; row["_overridden"] lists corrected columns.
    * Idempotent: re-running keeps the ORIGINAL pulled value, and a correction
      that has since expired / been removed is rolled back to its pulled value.
    * A human grade is active when it has a grade letter, status is
      graded/confirmed, and it is younger than 92 days (the app's TTL).
    * Never blocks a pipeline: if XTRAPP can't be fetched, nothing changes and
      the step exits 0 with a warning.

master.csv is deliberately NOT patched: the app reads master.csv and layers the
same corrections itself, which keeps its "pulled X -> you set Y" display honest.
"""
import json
import math
import os
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

_GH_OWNER = (os.environ.get("VALUATIO_OWNER")
             or os.environ.get("GITHUB_REPOSITORY_OWNER")
             or "TheMostLocal").strip()
XTRAPP_URL = os.environ.get(
    "XTRAPP_URL",
    f"https://raw.githubusercontent.com/{_GH_OWNER}/XTRAPP/main/data/xtrapp_data.json")
DATA = Path(__file__).resolve().parent.parent / "data"
MASTER_JSON = DATA / "master.json"
GRADES_JSON = DATA / "research_grades.json"
LOCAL_OVERRIDES = DATA / "overrides.json"

GRADE_TTL_MS = 92 * 24 * 60 * 60 * 1000

# App override key -> (master.json column, value transform). Keys not listed map
# to the same-named column. Columns the master doesn't have are still written
# (JSON tolerates extra keys; downstream readers that know them can use them).
FIELD_MAP = {
    "website":       ("web_url", None),
    # App stores dividendYield as a DECIMAL (0.0254); master.json carries the
    # pipeline's PERCENT (2.54) in dividend_yield.
    "dividendYield": ("dividend_yield", lambda v: round(v * 100, 6) if isinstance(v, (int, float)) else v),
    "marketCap":     ("marketcap", None),
    "changePct":     ("changepct", None),
    "priorClose":    ("closeyest", None),
}
# Bookkeeping keys never treated as data columns.
_META = {"_pulled", "_overridden", "_correctedBy", "_correctedAt"}

# Exact mirror of compute_research.grade_letter() bands -> a human letter maps to
# the MIDPOINT of its band, so gradeScore consumers (bot, ticker_snapshot) agree.
_BANDS = [(93, 100, "A+"), (85, 93, "A"), (78, 85, "A-"), (70, 78, "B+"), (62, 70, "B"),
          (54, 62, "B-"), (46, 54, "C+"), (38, 46, "C"), (30, 38, "C-"), (22, 30, "D+"),
          (14, 22, "D"), (0, 14, "F")]
GRADE_SCORE = {g: round((lo + hi) / 2, 2) for lo, hi, g in _BANDS}


def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _ts(v):
    """ms timestamp from a number (ms) or ISO string; 0 if unknown."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v) if math.isfinite(v) else 0
    try:
        return int(datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def _finite(o):
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {k: _finite(v) for k, v in o.items()}
    if isinstance(o, list):
        return [_finite(v) for v in o]
    return o


def load_xtrapp():
    try:
        req = urllib.request.Request(XTRAPP_URL, headers={"User-Agent": "ValuatioHumanCorrections",
                                                          "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"::warning::human corrections: could not read XTRAPP ({e}) - pulled data left as-is")
        return None


def _norm_entry(e):
    if isinstance(e, dict) and "value" in e:
        return e
    return {"value": e, "setAt": 0, "expiresAt": None, "source": "legacy"}


def active_overrides(*docs, now=None):
    """Merge override maps (newer setAt wins per ticker+field) and keep only
    active, non-empty values. -> { TICKER: { field: value } }"""
    now = now_ms() if now is None else now
    merged = {}
    for doc in docs:
        for tk, fields in (doc or {}).items():
            if not isinstance(fields, dict):
                continue
            slot = merged.setdefault(str(tk).upper(), {})
            for f, e in fields.items():
                e = _norm_entry(e)
                cur = slot.get(f)
                if cur is None or _ts(e.get("setAt")) >= _ts(cur.get("setAt")):
                    slot[f] = e
    out = {}
    for tk, fields in merged.items():
        for f, e in fields.items():
            exp = e.get("expiresAt")
            if exp is not None and _ts(exp) and now > _ts(exp):
                continue
            v = e.get("value")
            if v is None or v == "":
                continue
            out.setdefault(tk, {})[f] = v
    return out


def apply_to_rows(rows, overrides):
    """Patch rows in place. Returns (applied, rolled_back, touched_rows)."""
    applied = rolled = touched = 0
    for r in rows:
        if not isinstance(r, dict):
            continue
        tk = str(r.get("ticker") or "").upper()
        want = {}
        for f, v in (overrides.get(tk) or {}).items():
            col, tf = FIELD_MAP.get(f, (f, None))
            if col in _META or col == "ticker":
                continue
            want[col] = tf(v) if tf else v
        pulled = r.get("_pulled") if isinstance(r.get("_pulled"), dict) else {}
        changed = False
        # Roll back corrections that are no longer active.
        for col in list(pulled):
            if col not in want:
                r[col] = pulled.pop(col)
                rolled += 1
                changed = True
        # Apply active corrections, preserving the ORIGINAL pulled value once.
        for col, v in want.items():
            if col not in pulled:
                pulled[col] = r.get(col)
            if r.get(col) != v:
                r[col] = v
                changed = True
            applied += 1
        if pulled:
            r["_pulled"] = pulled
            r["_overridden"] = sorted(pulled)
            r["_correctedBy"] = "xtrapp"
        else:
            for k in ("_pulled", "_overridden", "_correctedBy"):
                r.pop(k, None)
        if changed or pulled:
            touched += 1
    return applied, rolled, touched


def active_human_grades(hg, now=None):
    now = now_ms() if now is None else now
    out = {}
    for tk, g in (hg or {}).items():
        if not isinstance(g, dict):
            continue
        letter = str(g.get("grade") or "").strip().upper()
        if letter not in GRADE_SCORE:
            continue
        if (g.get("status") or "graded") not in ("graded", "confirmed"):
            continue
        at = _ts(g.get("updatedAt"))
        if at and now - at > GRADE_TTL_MS:
            continue
        out[str(tk).upper()] = {"grade": letter, "updatedAt": g.get("updatedAt"), "note": g.get("note") or ""}
    return out


def apply_to_grades(doc, human):
    """Patch research_grades.json byTicker in place. -> (applied, rolled_back)"""
    by = doc.get("byTicker") if isinstance(doc, dict) else None
    if not isinstance(by, dict):
        return 0, 0
    applied = rolled = 0
    for tk, rec in by.items():
        if not isinstance(rec, dict):
            continue
        h = human.get(str(tk).upper())
        machine = rec.get("_machine")
        if h:
            if not isinstance(machine, dict):
                rec["_machine"] = {"grade": rec.get("grade"), "gradeScore": rec.get("gradeScore")}
            rec["grade"] = h["grade"]
            rec["gradeScore"] = GRADE_SCORE[h["grade"]]
            rec["gradeSource"] = "human"
            rec["humanGradeAt"] = h["updatedAt"]
            applied += 1
        elif isinstance(machine, dict):
            rec["grade"] = machine.get("grade")
            rec["gradeScore"] = machine.get("gradeScore")
            for k in ("_machine", "gradeSource", "humanGradeAt"):
                rec.pop(k, None)
            rolled += 1
    return applied, rolled


# ---------------------------------------------------------------- leadership ----
# XTRAPP leadershipOverrides (written by the app's CEO / Management editors):
#   people:  {slug: {name, assignedTickers, assignedRoleByTicker{T: role},
#                    sourceFieldByTicker{T: executives|board|founders}, updatedAt}}
#   removed: {TICKER: [names removed by the editor]}
# The human edit is the system override (z99): removed names are dropped from the
# pulled company facts, assigned people are added / their role corrected, and the
# record is stamped so it's clear a human changed it. Applied to every company file
# each run - company facts are only re-fetched every 30 days, so applying only at
# fetch time would delay a correction by up to a month.
LEAD_FIELDS = ("executives", "board", "founders")


def leadership_overrides(xt=None):
    xt = load_xtrapp() if xt is None else xt
    lo = (xt or {}).get("leadershipOverrides") or {}
    return lo if isinstance(lo, dict) else {}


def _pname(p):
    return (p.get("name") if isinstance(p, dict) else str(p or "")).strip()


def apply_leadership(ticker, rec, lo):
    """Mutates rec (a data/company/<T>.json record). -> number of changes."""
    if not isinstance(rec, dict) or not lo:
        return 0
    T = str(ticker).upper()
    n = 0
    removed = {str(x).strip().lower() for x in ((lo.get("removed") or {}).get(T) or [])}
    assigned = {_pname(p).lower() for p in (lo.get("people") or {}).values()
                if isinstance(p, dict) and T in [str(x).upper() for x in (p.get("assignedTickers") or [])]}
    # undo what a PREVIOUS run did for a human edit that has since been withdrawn:
    # people it inserted are removed; pulled people whose role it corrected get
    # their pulled role back
    for f in LEAD_FIELDS:
        lst = rec.get(f) or []
        keep = []
        for p in lst:
            if isinstance(p, dict) and _pname(p).lower() not in assigned:
                if p.get("_human"):
                    n += 1
                    continue
                if "_pulledRole" in p:
                    p["role"] = p.pop("_pulledRole")
                    if p["role"] is None:
                        p.pop("role")
                    n += 1
            keep.append(p)
        rec[f] = keep
    if removed:
        for f in LEAD_FIELDS:
            lst = rec.get(f) or []
            keep = [p for p in lst if _pname(p).lower() not in removed]
            n += len(lst) - len(keep)
            rec[f] = keep
    for person in (lo.get("people") or {}).values():
        if not isinstance(person, dict) or T not in [str(x).upper() for x in (person.get("assignedTickers") or [])]:
            continue
        name = _pname(person)
        if not name or name.lower() in removed:
            continue
        field = ((person.get("sourceFieldByTicker") or {}).get(T) or "executives")
        field = field if field in LEAD_FIELDS else "executives"
        role = (person.get("assignedRoleByTicker") or {}).get(T) or person.get("role") or None
        lst = rec.setdefault(field, [])
        hit = next((p for p in lst if isinstance(p, dict) and _pname(p).lower() == name.lower()), None)
        if hit is None:
            lst.insert(0, {k: v for k, v in {"name": name, "role": role, "_human": True}.items() if v is not None})
            n += 1
        elif role and hit.get("role") != role:
            if "_pulledRole" not in hit:
                hit["_pulledRole"] = hit.get("role")      # remember the pulled value so it can be restored
            hit["role"] = role
            n += 1
    if n:
        rec["_humanLeadership"] = {"changes": n, "appliedAt": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    return n


def apply_leadership_to_dir(company_dir, tickers=None, xt=None):
    """Apply XTRAPP leadership overrides to every affected company file on disk.
    -> {ticker: changes}. Only tickers mentioned by an override are touched."""
    lo = leadership_overrides(xt)
    if not lo:
        return {}
    touched = {str(t).upper() for t in (lo.get("removed") or {})}
    for person in (lo.get("people") or {}).values():
        if isinstance(person, dict):
            touched |= {str(t).upper() for t in (person.get("assignedTickers") or [])}
    if tickers is not None:
        touched &= {str(t).upper() for t in tickers}
    out = {}
    for T in sorted(touched):
        p = Path(company_dir) / f"{T}.json"
        if not p.exists():
            continue
        try:
            rec = json.loads(p.read_text())
        except Exception:
            continue
        rec.pop("_humanLeadership", None)
        n = apply_leadership(T, rec, lo)
        if n:
            p.write_text(json.dumps(rec, indent=2))
            out[T] = n
    return out


def main(argv):
    mode = (argv[1] if len(argv) > 1 else "master").lower()
    x = load_xtrapp()
    if x is None:
        return 0
    if mode == "master":
        if not MASTER_JSON.exists():
            print("human corrections: no data/master.json - nothing to do")
            return 0
        local = {}
        if LOCAL_OVERRIDES.exists():
            try:
                local = (json.loads(LOCAL_OVERRIDES.read_text()) or {}).get("overrides") or {}
            except Exception:
                local = {}
        ov = active_overrides(x.get("overrides") or {}, local)
        rows = json.loads(MASTER_JSON.read_text())
        if not isinstance(rows, list):
            print("::warning::human corrections: master.json is not a row list - skipped")
            return 0
        applied, rolled, touched = apply_to_rows(rows, ov)
        if applied or rolled:
            MASTER_JSON.write_text(json.dumps(_finite(rows), separators=(",", ":"), allow_nan=False))
        print(f"human corrections -> master.json: {len(ov)} ticker(s) corrected in XTRAPP · "
              f"{applied} field(s) applied · {rolled} rolled back · {touched} row(s) touched")
        return 0
    if mode == "grades":
        if not GRADES_JSON.exists():
            print("human corrections: no data/research_grades.json - nothing to do")
            return 0
        doc = json.loads(GRADES_JSON.read_text())
        human = active_human_grades(x.get("humanGrades") or {})
        applied, rolled = apply_to_grades(doc, human)
        if applied or rolled:
            GRADES_JSON.write_text(json.dumps(_finite(doc), separators=(",", ":"), allow_nan=False))
        print(f"human corrections -> research_grades.json: {len(human)} active human grade(s) · "
              f"{applied} applied · {rolled} rolled back")
        return 0
    print(f"usage: {argv[0]} master|grades")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
