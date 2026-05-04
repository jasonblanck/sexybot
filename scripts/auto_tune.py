#!/usr/bin/env python3
"""auto_tune.py — Offline parameter recommendation for SexyBot.

Reads resolved real-money trades from trades.db and runs a grid search
over MIN_CONFIDENCE, KELLY_FRACTION, and MAX_ENTRY_PRICE. Prints the top
5 parameter sets ranked by out-of-sample (test split) score so the
operator can review before applying.

This script does NOT modify .env or the live bot. It only reports
recommendations.

Usage:
    /root/polybot/venv/bin/python3 /root/polybot/scripts/auto_tune.py

Override the DB path or output count:
    AUTO_TUNE_DB=/path/to/trades.db AUTO_TUNE_TOP=10 python3 auto_tune.py

Notes:
- Needs >= 50 resolved real-money trades with attribution columns set
  (ai_probability, ai_confidence) to produce meaningful results.
- Approximation: realized_pnl is scaled linearly by kelly_fraction
  relative to the bot's actual sizing at the time. This is roughly
  correct for outcome-token positions but understates non-linear
  impact (e.g. a trade that didn't fill at all under tighter price
  caps wouldn't have any P&L at all). Treat the output as a
  recommendation to validate, not a directive.
"""

import os
import sqlite3
import itertools
import sys

DB_PATH         = os.getenv("AUTO_TUNE_DB", "/root/polybot/trades.db")
TOP_N           = int(os.getenv("AUTO_TUNE_TOP", "5"))
MIN_TRADES      = int(os.getenv("AUTO_TUNE_MIN_TRADES", "50"))
TRAIN_FRACTION  = 0.7

# Bot's current default kelly_fraction. Used to scale realized_pnl when
# evaluating alternative kelly_fractions. Override via env if your live
# value differs.
BASELINE_KELLY  = float(os.getenv("AUTO_TUNE_BASELINE_KELLY", "0.25"))

CONFIDENCES = [50, 55, 60, 65, 70, 75]
KELLY_FRACS = [0.10, 0.15, 0.20, 0.25, 0.30, 0.35]
MAX_PRICES  = [0.80, 0.85, 0.90, 0.95]


def load_trades(db_path: str) -> list:
    if not os.path.exists(db_path):
        sys.exit(f"DB not found at {db_path}")
    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT id, amount, price, won, realized_pnl, ai_probability, ai_confidence
        FROM trades
        WHERE dry_run=0
          AND resolved=1
          AND ai_probability IS NOT NULL
          AND ai_confidence IS NOT NULL
          AND realized_pnl IS NOT NULL
          AND status IN ('matched','filled')
        ORDER BY time
    """).fetchall()
    conn.close()
    return rows


def evaluate(trades, min_conf: int, kelly_frac: float, max_price: float) -> dict:
    """Replay trades with one parameter set; return aggregate metrics.
    Score is realized_pnl - 0.5 * max_drawdown (penalises bumpy paths)."""
    accepted = wins = 0
    pnl_total = 0.0
    running = peak = 0.0
    max_dd = 0.0
    for (_id, amount, price, won, pnl, ai_p, ai_c) in trades:
        if ai_c is None or ai_c < min_conf:
            continue
        if price is None or price > max_price:
            continue
        # Linear scaling — assumes pnl is proportional to size at the time.
        scaled_pnl = float(pnl) * (kelly_frac / BASELINE_KELLY)
        accepted += 1
        pnl_total += scaled_pnl
        if won == 1:
            wins += 1
        running += scaled_pnl
        peak = max(peak, running)
        max_dd = max(max_dd, peak - running)
    return {
        "trades":   accepted,
        "pnl":      round(pnl_total, 2),
        "win_rate": round(wins / accepted, 3) if accepted else 0.0,
        "max_dd":   round(max_dd, 2),
        "score":    round(pnl_total - 0.5 * max_dd, 2),
    }


def main() -> None:
    rows = load_trades(DB_PATH)
    print(f"Loaded {len(rows)} resolved real-money trades from {DB_PATH}")
    if len(rows) < MIN_TRADES:
        print(f"⚠️  Need at least {MIN_TRADES} resolved trades; have {len(rows)}.")
        print("   Recommendations would be too noisy to act on.")
        return

    split = int(TRAIN_FRACTION * len(rows))
    train, test = rows[:split], rows[split:]
    print(f"Train: {len(train)} trades  |  Test (out-of-sample): {len(test)} trades\n")

    results = []
    for conf, kf, mp in itertools.product(CONFIDENCES, KELLY_FRACS, MAX_PRICES):
        results.append({
            "params": {"min_confidence": conf, "kelly_fraction": kf, "max_entry_price": mp},
            "train":  evaluate(train, conf, kf, mp),
            "test":   evaluate(test,  conf, kf, mp),
        })
    # Rank by out-of-sample score so we don't reward overfitting.
    results.sort(key=lambda r: r["test"]["score"], reverse=True)

    print(f"=== TOP {TOP_N} PARAMETER SETS (ranked by out-of-sample score) ===\n")
    for i, r in enumerate(results[:TOP_N], 1):
        p, tr, ts = r["params"], r["train"], r["test"]
        print(f"#{i}  min_confidence={p['min_confidence']}%  "
              f"kelly_fraction={p['kelly_fraction']:.2f}  "
              f"max_entry_price={p['max_entry_price']}")
        print(f"    TRAIN: n={tr['trades']:3d}  pnl=${tr['pnl']:+.2f}  "
              f"win={tr['win_rate']*100:.1f}%  dd=${tr['max_dd']:.2f}  score={tr['score']:+.2f}")
        print(f"    TEST:  n={ts['trades']:3d}  pnl=${ts['pnl']:+.2f}  "
              f"win={ts['win_rate']*100:.1f}%  dd=${ts['max_dd']:.2f}  score={ts['score']:+.2f}\n")

    print("=== RECOMMENDED ===")
    if results:
        best = results[0]["params"]
        print(f"MIN_CONFIDENCE={best['min_confidence']}")
        print(f"KELLY_FRACTION={best['kelly_fraction']}")
        print(f"MAX_ENTRY_PRICE={best['max_entry_price']}")
        print()
        print("To apply: edit /root/polybot/.env, then `systemctl restart sexybot`.")
        print("Validate via paper-trade or a few small positions before enabling")
        print("on full size — the linear PnL scaling is approximate.")


if __name__ == "__main__":
    main()
