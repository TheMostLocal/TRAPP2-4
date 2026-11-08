#!/usr/bin/env python3
"""intraday_session.py - prints 'trading' on a US trading day (ET), else the
reason ('weekend' / 'holiday'). Used by intraday_loop.sh (z115). Reuses the
tape writer's own calendar so the two can never disagree."""
import sys, os
from datetime import datetime, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from append_intraday_snapshot import _et_now, session_for

et = _et_now(datetime.now(timezone.utc))
trading, _close, label = session_for(et)
print("trading" if trading else label)
