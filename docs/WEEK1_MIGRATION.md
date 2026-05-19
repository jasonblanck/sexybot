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
   SEXYBOT_SPORTS_ONLY=1
   SEXYBOT_BIG_WINNER_PCT=25
   ```
   These two are the low-risk wins — filter trades to the proven-
   profitable category and lock in existing big winners.
3. **Restart bot.py** (`systemctl restart sexybot` or equivalent).
   Monitor the dashboard's pretrade panel for skip reasons.
4. **Wait 24h**, observe:
   - Sports-only filter rate (how many signals got skipped)
   - Whether the big-winner exit fired on existing positions
   - No new error classes in execution health
5. **If clean**: flip `SEXYBOT_TRADING_DISABLED=1`. bot.py stops
   opening new positions entirely; main_v2 takes over as the sole
   directional trader.

## Rollback

One env edit + `systemctl restart`. The code path defaults match
prior behavior with all three flags at 0.
