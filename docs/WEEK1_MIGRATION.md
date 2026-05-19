# Week 1: Consolidate onto main_v2

Three operator-controlled env-gates shipped in `a39b116`. All default
to **off** so a deploy with no env changes preserves current behavior.
Flip them on the VPS `.env` (or wherever the bot's environment is
sourced — see `CLAUDE.md` for the live env path) when ready.

## What each flag does

### `SEXYBOT_TRADING_DISABLED=1`
Stops `bot.py` from opening new positions. The position monitor
(TP/SL/trailing-stop) still runs, the redeemer still claims wins,
the dashboard endpoints stay live. Use this to quiesce the legacy
service ahead of full consolidation onto `main_v2.py`, without
uninstalling the systemd unit.

Skip reason `service_disabled` will show up in the dashboard
pretrade panel as new signals get filtered.

### `SEXYBOT_SPORTS_ONLY=1`
Filters signals by `classify_market()` — only `sports` category
trades fire. Backtest evidence: sports +$31.97 / 100% win rate
over 30d, all other categories net negative.

Skip reason `sports_only_filter` surfaces the filtered count.

### `SEXYBOT_BIG_WINNER_PCT=<N>`
When set to N > 0, the position monitor exits any DB-tracked
position up N% or more from entry, regardless of the cents-based
TP rule. Added because the +15¢ momentum-TP undershoots big
winners on low-price entries (12¢ → 39.5¢ is +27.5¢ AND +229% —
either should fire). `0` disables (default).

Recommended values:
- `25`: aggressive — will close the current BTC NO 76¢ at +27%
  on the next monitor tick
- `50`: moderate — locks in Rizespor-class outliers but rides
  smaller winners through their full curve
- `100`: only catches genuine 2x outliers

## Recommended rollout

1. **Deploy code** — already done in `a39b116`. No behavior change yet.
2. **Add env vars to VPS .env**:
   ```
   # Week 1 — consolidate prep
   SEXYBOT_SPORTS_ONLY=1
   SEXYBOT_BIG_WINNER_PCT=25

   # Risk tightening — see "Why" below
   DAILY_LOSS_LIMIT=15
   MIN_BUY_PRICE=0.20
   ```
3. **Restart bot.py** (`systemctl restart sexybot` or equivalent).
   Monitor the dashboard's pretrade panel for skip reasons.
4. **Wait 24h**, observe:
   - Sports-only filter rate (how many signals got skipped)
   - Whether the big-winner exit fired on existing positions
   - No new error classes in execution health
5. **If clean**: flip `SEXYBOT_TRADING_DISABLED=1`. bot.py stops
   opening new positions entirely; main_v2 takes over as the sole
   directional trader.

## Why the risk-tightening values

Pulled from the 30-day equity-curve walk on 2026-05-19:

- **Worst single-day loss: −$32.87 on 2026-05-02**, dropping the
  account 21.6% in one session. Current `DAILY_LOSS_LIMIT=50` would
  not have paused trading until the damage was nearly done. Dropping
  to **$15** caps the worst-day damage at ~10% of bankroll. The bot
  then sat underwater for 13 days before a single +$43 day on May 16
  pulled it back to net positive — that's variance, not edge.

- **0-10¢ entry-price bucket: −$27.60 over 11 trades** (avg
  −$2.50/trade). `MIN_YES_BUY_PRICE=0.30` already blocks the YES
  side; **`MIN_BUY_PRICE=0.20`** applies the same logic to NO buys
  and trims the bleeder bucket without touching the 70-80¢ band
  (+$67.37 / 78 trades — the actual engine of profit).

- **Sports vs everything else: sports +$31.97 / 214 trades, crypto
  −$11.35 / 25 trades.** `SEXYBOT_SPORTS_ONLY=1` is the highest-
  leverage single change.

## Rollback

One env edit + `systemctl restart`. The code path defaults match
prior behavior with all flags at 0.
