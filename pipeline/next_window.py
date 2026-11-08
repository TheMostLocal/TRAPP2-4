#!/usr/bin/env python3
# ============================================================
#  >>> DESTINATION: TheMostLocal/TRAPP2-CORE
#  >>> FILE PATH:   pipeline/next_window.py
#  z168 - seconds until the intraday loop's window next opens (LOOP_START_UTC, HHMM UTC) on a NYSE trading day,
#  using the tape writer's own calendar. Prints nothing if that's more than LOOP_BRIDGE_MAX_H hours away (a
#  weekend or a holiday): the loop then stops and the heartbeat / GitHub's schedule starts the next session.
# ============================================================
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from append_intraday_snapshot import _et_now, session_for   # noqa: E402


def seconds_to_next(now=None, start_hhmm=None, max_h=None):
    now = now or datetime.now(timezone.utc)
    start = int(start_hhmm if start_hhmm is not None else os.environ.get("LOOP_START_UTC", "1130"))
    max_h = float(max_h if max_h is not None else os.environ.get("LOOP_BRIDGE_MAX_H", "12"))
    for d in range(0, 6):
        day = (now + timedelta(days=d)).date()
        s = datetime(day.year, day.month, day.day, start // 100, start % 100, tzinfo=timezone.utc)
        if s <= now:
            continue
        trading, _close, _label = session_for(_et_now(s))
        if trading:
            secs = int((s - now).total_seconds())
            return secs if secs <= max_h * 3600 else None
    return None


if __name__ == "__main__":
    v = seconds_to_next()
    print("" if v is None else v)
