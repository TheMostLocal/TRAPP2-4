#!/usr/bin/env bash
# intraday_loop.sh - ONE Actions job writes the whole session's 15-minute tape (z115).
# Shared, identical in TRAPP2 / TRAPP2-1 / TRAPP2-2 / TRAPP2-3.
#
# Why: GitHub's scheduler delays and drops most "every 15 minutes" crons (the
# live tape had 4 points on 2026-10-15: 13:09, 17:36, 20:33, 23:19 UTC). So a
# run now LOOPS: quotes -> corrections -> signals/regime -> tape point -> commit,
# then sleeps to the next 15-minute mark, until the session window ends or the
# job's time budget runs out. GitHub only has to start the job, not keep time.
#
# z117:
#  * START FROM THE LATEST MAIN. A queued run checks out the commit it was
#    TRIGGERED on. 2026-10-16: TRAPP2 run 35 was triggered 19:44 UTC, waited
#    behind the running loop until 21:30, and its first push conflict's "ours
#    wins" merge overwrote the newer tape - 19:45-21:15 UTC lost (the 21:30:16
#    commit still had them). The loop now fast-forwards before its first pass.
#  * SESSION WINDOW. A start outside LOOP_START_UTC..LOOP_STOP_UTC does ONE pass
#    and exits (a 03:20 UTC start looped 5h25m overnight on 10-16).
#
# Env: LOOP_INTERVAL_SEC (900) · LOOP_BUDGET_SEC (20400 = 5h40m; job limit 6h)
#      LOOP_OFFSET_SEC (stagger between repos) · LOOP_START_UTC (HHMM, default 1130)
#      LOOP_STOP_UTC (HHMM, default 2230) · LOOP_MAX_ITERS (1000; 1 = single pass)
#      INTRADAY_FORCE (write a tape point outside hours)
set -u
INTERVAL=${LOOP_INTERVAL_SEC:-900}
BUDGET=${LOOP_BUDGET_SEC:-20400}
OFFSET=${LOOP_OFFSET_SEC:-0}
START=${LOOP_START_UTC:-1130}
STOP=${LOOP_STOP_UTC:-2230}
MAX=${LOOP_MAX_ITERS:-1000}
BRANCH=${GITHUB_REF_NAME:-main}
start=$(date +%s)
hhmm() { echo $((10#$(date -u +%H%M))); }
in_window() { local n; n=$(hhmm); [ "$n" -ge $((10#$START)) ] && [ "$n" -lt $((10#$STOP)) ]; }

# z158: the shared scripts come from TRAPP2-CORE (checked out at $CORE_DIR by the workflow). A sync can put
# this repo's own copies back (a reset, a pull), so they are re-applied after every sync - all but this
# running script itself, which bash is still reading.
core_overlay() {
  [ -n "${CORE_DIR:-}" ] && [ -d "$CORE_DIR/pipeline" ] || return 0
  for f in "$CORE_DIR"/pipeline/*; do
    [ "$(basename "$f")" = "intraday_loop.sh" ] && continue
    cp -r "$f" pipeline/
  done
}

# nothing is local yet, so this is a plain fast-forward (reset to the fetched tip as a fallback)
if git fetch -q origin "$BRANCH" 2>/dev/null; then
  git merge -q --ff-only FETCH_HEAD 2>/dev/null || git reset -q --hard FETCH_HEAD
  echo "synced to origin/$BRANCH @ $(git rev-parse --short HEAD)"
else
  echo "::warning::could not fetch origin/$BRANCH - continuing from the checked-out commit"
fi
core_overlay
[ -n "${CORE_DIR:-}" ] && echo "shared pipeline: TRAPP2-CORE${ALPACA_KEY_ID:+ · Alpaca on}"
# z168: say whether quotes go to Supabase - a silent skip looked like a failure on the health card
if [ -n "${SUPABASE_LIVE_URL:-}" ] && [ -n "${SUPABASE_LIVE_KEY:-}" ]; then echo "Supabase live: ON → $(echo "$SUPABASE_LIVE_URL" | sed -E 's#https?://([^.]+)\..*#\1#')"
else echo "::notice::Supabase live: OFF - add SUPABASE_LIVE_URL and SUPABASE_LIVE_KEY (the PORT+ANALYTICS project's URL and service key) to this repo's Actions secrets"; fi
DAY=$(python pipeline/intraday_session.py 2>/dev/null || echo trading)
SINGLE=""
in_window || SINGLE="started outside the session window (${START}Z-${STOP}Z)"
echo "intraday loop: day=$DAY interval=${INTERVAL}s budget=${BUDGET}s window=${START}Z-${STOP}Z max=$MAX${SINGLE:+ · $SINGLE}"

# z135: the official OPEN (0930 ET) and CLOSE (1600 ET) are captured the moment Yahoo has them, not at the
# next 15-minute mark: event passes at 0930:40 / 0932:30 and 1600:40 / 1602:30 ET (+ a few seconds'
# stagger per book). ET from the tz database (DST-correct), never a fixed UTC offset.
EVENT_STAGGER=$(( OFFSET / 20 ))                # 0 / 9 / 15 / 30 s across the four books
et_epoch() { date -d "TZ=\"America/New_York\" $(TZ=America/New_York date +%F) $1" +%s 2>/dev/null; }
next_event() {   # seconds until the next open / close capture, or nothing
  local now=$1 best="" e d
  for hm in 09:30:40 09:32:30 16:00:40 16:02:30; do
    e=$(et_epoch "$hm"); [ -z "$e" ] && continue
    e=$(( e + EVENT_STAGGER )); d=$(( e - now ))
    if [ "$d" -gt 5 ] && { [ -z "$best" ] || [ "$d" -lt "$best" ]; }; then best=$d; fi
  done
  echo "$best"
}

# event pass: only what the open / close needs (quotes → tape), committed at once (~40 s)
event_pass() {
  python pipeline/fetch_quotes_fast.py || echo "::warning::quote fetch failed (event pass)"
  python pipeline/human_corrections.py master || true
  python pipeline/append_intraday_snapshot.py || true
  commit_push "Official open / close $(TZ=America/New_York date +%H%M) ET [skip ci]"
  live_push
}

one_pass() {
  python pipeline/fetch_quotes_fast.py || python pipeline/fetch_data.py || echo "::warning::quote fetch failed this pass"
  python pipeline/human_corrections.py master || true
  python pipeline/compute_signals.py || true
  python pipeline/classify_regime.py || true
  python pipeline/append_intraday_snapshot.py || true
  python pipeline/fetch_intraday_bars.py || true          # z131: 5-min OHLCV incl. pre-market / after hours
  commit_push "Quotes + intraday tape $(date -u +%Y-%m-%dT%H:%M:%SZ) [skip ci]"
  live_push
}

# z159: after GitHub has the pass, the same quotes and bars go to Supabase (when SUPABASE_LIVE_* are set) -
# best-effort, never blocks the loop; the first pass of the job also prunes old bar days
LIVE_FIRST=1
live_push() {
  [ -n "${SUPABASE_LIVE_URL:-}" ] || return 0
  if [ -n "$LIVE_FIRST" ]; then python pipeline/supabase_live.py --prune || true; LIVE_FIRST=""
  else python pipeline/supabase_live.py || true; fi
}

# z161: when this job stops for its TIME BUDGET while its window is still open, it queues the next run of its own
# workflow - GitHub's scheduler went hours without starting anything (2026-10-26: no intraday run by 0900 ET).
# Needs `actions: write` and GH_TOKEN (the job's own token) in the workflow; the concurrency group queues the new
# run behind this one. GitHub's schedule and the Supabase heartbeat (supabase/heartbeat.sql) stay as backstops.
chain_next() {
  [ -n "${GH_TOKEN:-}" ] || { echo "::notice::GH_TOKEN not set - not chaining"; return 0; }
  local wf="${GITHUB_WORKFLOW_REF:-}"
  wf="${wf%@*}"; wf="${wf##*/}"
  { [ -n "$wf" ] && [ -n "${GITHUB_REPOSITORY:-}" ]; } || return 0
  if gh workflow run "$wf" --ref "${GITHUB_REF_NAME:-main}" -R "$GITHUB_REPOSITORY" >/dev/null 2>&1; then
    echo "chained: the next run of $wf is queued"
  else
    echo "::warning::could not queue the next run of $wf (does the workflow grant actions: write?)"
  fi
}

commit_push() {
  OWN="data/master.csv data/master.json data/intraday data/bars data/signals.json data/regime_current.json data/regime_history.json"
  for f in $OWN; do [ -e "$f" ] && git add -A "$f"; done          # one missing path must not block the rest
  if git diff --staged --quiet; then echo "no quote changes"; return 0; fi
  git commit -q -m "$1"
  for k in 1 2 3 4 5; do
    if git push -q origin "HEAD:$BRANCH"; then echo "pushed (attempt $k)"; return 0; fi
    echo "push rejected - reconciling (ours wins for OUR files)"
    git fetch -q origin "$BRANCH"
    git merge -X ours "origin/$BRANCH" --no-edit -q 2>/dev/null || true
    if [ -f .git/MERGE_HEAD ]; then
      for f in $OWN; do [ -e "$f" ] && git checkout --ours -- "$f" 2>/dev/null; done
      for f in $OWN; do [ -e "$f" ] && git add -A "$f"; done
      git commit -q --no-edit 2>/dev/null || true
    fi
    sleep $((k * 2))
  done
  echo "::warning::push failed after 5 attempts this pass"
}

# z168: between sessions the loop keeps itself alive instead of waiting for GitHub's scheduler (which started only a
# few of the hourly triggers on 10-26..10-30): after the window closes on a weeknight it sleeps until the next one
# opens - or to the end of this job, then queues the next. Gaps longer than LOOP_BRIDGE_MAX_H (12 h: weekends,
# holidays) aren't bridged; the heartbeat / schedule starts those.
bridge() {
  [ "${LOOP_BRIDGE:-1}" = "1" ] || return 1
  local nxt now left
  nxt=$(python pipeline/next_window.py 2>/dev/null)
  if [ -z "$nxt" ]; then echo "next window is more than ${LOOP_BRIDGE_MAX_H:-12} h away (weekend / holiday) - not bridging"; return 1; fi
  now=$(date +%s); left=$(( start + BUDGET - now - 180 ))
  if [ "$nxt" -le "$left" ]; then
    echo "bridging: the next window opens in $(( nxt / 60 )) min - sleeping until then"
    sleep "$nxt"
    DAY=$(python pipeline/intraday_session.py 2>/dev/null || echo trading)
    SINGLE=""
    return 0
  fi
  echo "bridging: the next window opens in $(( nxt / 60 )) min - sleeping to the end of this job, then queueing the next"
  [ "$left" -gt 0 ] && sleep "$left"
  chain_next
  return 2
}

i=0
EVENT=""
while :; do
  i=$((i + 1))
  echo "── pass $i · $(date -u +%H:%M:%S)Z · $(TZ=America/New_York date +%H%M) ET${EVENT:+ · official ${EVENT} capture}"
  if [ -n "$EVENT" ]; then event_pass; else one_pass; fi
  EVENT=""
  # keep the checkout current so the next pass builds on what's on GitHub
  git pull -q --no-rebase -X ours origin "$BRANCH" 2>/dev/null || true
  core_overlay
  now=$(date +%s)
  if [ "$i" -ge "$MAX" ]; then echo "max passes reached"; break; fi
  why=""
  if [ -n "$SINGLE" ]; then why="$SINGLE"
  elif [ "$DAY" != "trading" ] && [ -z "${INTRADAY_FORCE:-}" ]; then why="not a trading day ($DAY)"
  elif ! in_window; then why="past the session window (${STOP}Z)"; fi
  if [ -n "$why" ]; then
    echo "$why"
    bridge; rc=$?
    [ "$rc" -eq 0 ] && continue
    break
  fi
  wait=$(( INTERVAL - (now % INTERVAL) + OFFSET ))
  [ "$wait" -gt "$INTERVAL" ] && wait=$(( wait - INTERVAL ))
  [ "$wait" -lt 30 ] && wait=$(( wait + INTERVAL ))
  ev=$(next_event "$now")                                     # z135: an open / close capture comes first
  if [ -n "$ev" ] && [ "$ev" -lt "$wait" ]; then
    wait=$ev; EVENT=$( [ "$(TZ=America/New_York date -d "@$(( now + ev ))" +%H)" -lt 12 ] && echo open || echo close )
  fi
  if [ $(( now - start + wait + 120 )) -gt "$BUDGET" ]; then echo "time budget reached - queueing the next run"; chain_next; break; fi
  echo "sleeping ${wait}s"
  sleep "$wait"
done
echo "intraday loop done: $i pass(es)"
