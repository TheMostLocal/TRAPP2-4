# TRAPP2-4 — NYSE equities that weren't in the app

Book repo #5 of Valuatio (TRAPP2). Same pipeline as TRAPP2-3; holds the **NYSE-listed equities that weren't in the
app** on 2026-11-07 — common stock, ADRs, REITs, MLP / LP units, royalty trusts, BDCs and SPAC class A shares:
**1,622 tickers** (`data/tickers.txt`). Preferreds, notes / debentures, closed-end funds, ETFs / ETNs, structured
trusts and SPAC units / warrants / rights were split off for a TRAPP2-1-style repo (they need a different kind of
research than a company); tickers already in another book were left where they are.

Symbols may be written as the exchange lists them — `BF.A`, `ABR$D` — the pipeline reads them as Yahoo's `BF-A`,
`ABR-PD`, and each row keeps `symbolListed`.

## What runs (UTC; a few minutes after TRAPP2-3 so the same sources aren't hit twice at once)

| Workflow | When | Does |
|---|---|---|
| Intraday Quotes + 15-min Tape | weekdays, hourly starts 08-23 at :07; loops every 15 min (offset 7 min) | quotes + tape + Supabase live (shared scripts from TRAPP2-CORE), queues its own next run |
| Nightly Full Refresh | daily 01:07 | quotes + fundamentals for every ticker (6 in parallel; profiles every 48-96 h), history top-up |
| Mid-Day Refresh | weekdays 08:14 / 11:39 ET | FRED macro, signals, regime |
| Refresh 20-Year History | daily 07:57 | 20 years of daily bars; afterwards only the new day |
| Refresh Financial Statements | Mondays 09:17 | statements for every ticker |
| Company Facts (Monthly) | 1st, 04:33 | Wikidata company facts |
| Leadership History A-G / H-M / N-Z | Mondays 09:23 / 10:23 / 11:23 | CEO / leadership careers |
| Cleanup History Orphans | by hand | removes history for tickers taken out of tickers.txt |

## Set up (once)

1. **Secrets** (Settings → Secrets and variables → Actions), the same values the other books use:
   `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY`, `SUPABASE_LIVE_URL`, `SUPABASE_LIVE_KEY`, `FRED_API_KEY`.
2. **First fill**, in this order (Actions → … → Run workflow): *Refresh 20-Year History* (≈ 1-2 h the first time),
   *Nightly Full Refresh*, *Company Facts Refresh*, then the three *Leadership History* workflows.
3. **Heartbeat:** add TRAPP2-4 to the GitHub token the Supabase heartbeat uses (repository access), then re-run
   `TRAPP2-ANALYTICS/supabase/heartbeat.sql` so the intraday loop is started from Supabase too.

## Size

About five times TRAPP2-3: ~1,600 history files (≈ 250-400 MB once 20 years are in), a ~4 MB `master.json`.
GitHub's guidance is under 1 GB per repository; if this book grows past ~2,500 tickers, split it.
