#!/usr/bin/env python3
"""
analyze_performance.py
Walk-forward performance analyzer over trades.db.

Reads resolved trades and brier_scores, stratifies by strategy / category /
entry-regime / confidence bucket / Kelly-size bucket, and reports:

  • overall win rate + realized P&L
  • per-stratum win rate and net P&L  (which cohorts make money?)
  • Brier calibration curve            (are the predicted probs well-calibrated?)
  • suggested parameter changes based on the above

Intended as an operator tool, NOT part of the hot path. Safe to run
concurrently with the live bot — SQLite WAL mode handles reader/writer
concurrency.

Usage
-----
    python3 scripts/analyze_performance.py                   # last 30 d
    python3 scripts/analyze_performance.py --days 90
    python3 scripts/analyze_performance.py --db /root/polybot/trades.db
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from typing import Iterable, Optional


DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "trades.db"
)


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _pct(n: int, d: int) -> str:
    return f"{100 * n / d:5.1f}%" if d else "   ––"


def _money(x: Optional[float]) -> str:
    if x is None:
        return "     ––"
    sign = "+" if x >= 0 else "−"
    return f"{sign}${abs(x):6.2f}"


def _print_table(title: str, rows: list[tuple], header: tuple[str, ...]) -> None:
    print(f"\n── {title} " + "─" * max(1, 78 - len(title) - 4))
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) if rows else len(h)
              for i, h in enumerate(header)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*header))
    print("  ".join("─" * w for w in widths))
    for r in rows:
        print(fmt.format(*[str(x) for x in r]))
    if not rows:
        print("  (no rows)")


# ── Data model ─────────────────────────────────────────────────────────────────

def fetch_resolved_trades(conn: sqlite3.Connection, days: int) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    q = """
    SELECT
        id, market, side, amount, price, shares, status,
        dry_run, time, token_id,
        resolved, won, realized_pnl,
        strategy, category, ai_probability, ai_confidence, ai_risk,
        regime_at_entry
    FROM trades
    WHERE resolved = 1
      AND realized_pnl IS NOT NULL
      AND (dry_run IS NULL OR dry_run = 0)
      AND time >= datetime('now', ?)
    ORDER BY time ASC
    """
    return list(conn.execute(q, (f"-{days} days",)))


def fetch_brier_rows(conn: sqlite3.Connection, days: int) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cols = [r[1] for r in conn.execute("PRAGMA table_info(brier_scores)").fetchall()]
    source_expr = "source" if "source" in cols else "NULL AS source"
    q = f"""
    SELECT predicted_prob, market_price, actual_outcome, {source_expr}
    FROM brier_scores
    WHERE resolved = 1
      AND predicted_prob IS NOT NULL
      AND actual_outcome IS NOT NULL
      AND time >= datetime('now', ?)
    ORDER BY time ASC
    """
    return list(conn.execute(q, (f"-{days} days",)))


# ── Analyzers ──────────────────────────────────────────────────────────────────

def summarize_group(rows: Iterable[sqlite3.Row], key: str) -> list[tuple]:
    """Group resolved trades by `key` column; return table rows."""
    buckets: dict[str, dict] = defaultdict(lambda: {"n": 0, "won": 0, "pnl": 0.0, "notional": 0.0})
    for r in rows:
        k = (r[key] or "(none)")
        b = buckets[str(k)]
        b["n"]        += 1
        b["won"]      += 1 if (r["won"] or 0) else 0
        b["pnl"]      += float(r["realized_pnl"] or 0)
        b["notional"] += abs(float(r["amount"] or 0))

    out = []
    for k, b in sorted(buckets.items(), key=lambda kv: -kv[1]["pnl"]):
        roi = (b["pnl"] / b["notional"] * 100) if b["notional"] else 0.0
        out.append((k, b["n"], _pct(b["won"], b["n"]), _money(b["pnl"]), f"{roi:+5.1f}%"))
    return out


def confidence_buckets(rows: Iterable[sqlite3.Row]) -> list[tuple]:
    """Bucket ai_confidence into 0–20, 20–40, …, 80–100."""
    edges = [0, 20, 40, 60, 80, 101]
    labels = [f"{edges[i]}–{edges[i+1]-1}" for i in range(len(edges) - 1)]
    buckets = defaultdict(lambda: {"n": 0, "won": 0, "pnl": 0.0, "notional": 0.0})
    for r in rows:
        c = r["ai_confidence"]
        if c is None:
            continue
        c = int(c)
        idx = next((i for i in range(len(edges) - 1) if edges[i] <= c < edges[i + 1]), None)
        if idx is None:
            continue
        label = labels[idx]
        b = buckets[label]
        b["n"]        += 1
        b["won"]      += 1 if (r["won"] or 0) else 0
        b["pnl"]      += float(r["realized_pnl"] or 0)
        b["notional"] += abs(float(r["amount"] or 0))

    out = []
    for label in labels:
        b = buckets[label]
        if b["n"] == 0:
            continue
        roi = (b["pnl"] / b["notional"] * 100) if b["notional"] else 0.0
        out.append((label, b["n"], _pct(b["won"], b["n"]), _money(b["pnl"]), f"{roi:+5.1f}%"))
    return out


def calibration_curve(brier_rows: list[sqlite3.Row]) -> list[tuple]:
    """
    Decile-ish calibration: bucket predicted probability, compare
    average predicted to average realized outcome.
    """
    edges = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.001]
    labels = [f"{edges[i]:.1f}–{edges[i+1]:.1f}" for i in range(len(edges) - 1)]
    buckets = defaultdict(lambda: {"n": 0, "pred_sum": 0.0, "actual_sum": 0})
    for r in brier_rows:
        p = float(r["predicted_prob"])
        a = int(r["actual_outcome"])
        idx = next((i for i in range(len(edges) - 1) if edges[i] <= p < edges[i + 1]), None)
        if idx is None:
            continue
        b = buckets[labels[idx]]
        b["n"]          += 1
        b["pred_sum"]   += p
        b["actual_sum"] += a

    out = []
    for label in labels:
        b = buckets[label]
        if b["n"] == 0:
            continue
        mean_pred   = b["pred_sum"]   / b["n"]
        mean_actual = b["actual_sum"] / b["n"]
        bias        = mean_pred - mean_actual
        out.append((
            label, b["n"], f"{mean_pred:.3f}", f"{mean_actual:.3f}",
            f"{bias:+.3f}",
        ))
    return out


def overall_metrics(rows: list[sqlite3.Row]) -> dict:
    n     = len(rows)
    won   = sum(1 for r in rows if (r["won"] or 0))
    pnl   = sum(float(r["realized_pnl"] or 0) for r in rows)
    notional = sum(abs(float(r["amount"] or 0)) for r in rows)
    return {
        "n":        n,
        "won":      won,
        "pnl":      pnl,
        "notional": notional,
        "roi":      (pnl / notional * 100) if notional else 0.0,
        "win_rate": (won / n * 100)        if n        else 0.0,
    }


def brier_overall(brier_rows: list[sqlite3.Row]) -> dict:
    n = len(brier_rows)
    if not n:
        return {"n": 0, "brier": None, "bias": None}
    brier = sum((float(r["predicted_prob"]) - int(r["actual_outcome"])) ** 2
                for r in brier_rows) / n
    bias  = sum(float(r["predicted_prob"]) - int(r["actual_outcome"])
                for r in brier_rows) / n
    return {"n": n, "brier": brier, "bias": bias}


# ── Recommendations ────────────────────────────────────────────────────────────

def recommendations(
    overall:   dict,
    by_source: list[tuple],
    by_conf:   list[tuple],
    brier:     dict,
    cal_curve: list[tuple],
) -> list[str]:
    recs: list[str] = []

    if overall["n"] == 0:
        return ["No resolved trades in window. Let the bot run longer, "
                "or widen --days, before drawing conclusions."]

    if overall["pnl"] < 0:
        recs.append(
            f"Overall PnL is {_money(overall['pnl'])} on {overall['n']} resolved trades "
            f"(win rate {overall['win_rate']:.1f}%) — tighten MIN_EDGE and/or raise "
            "OBI_SOLO_MIN before scaling size."
        )

    # Losing strategies / categories
    losers = [r for r in by_source if r[3].startswith("−")]
    if losers:
        losing_list = ", ".join(f"{r[0]} ({r[3]})" for r in losers[:3])
        recs.append(
            f"Losing cohorts: {losing_list} — add these to EXCLUDE_CATEGORIES or "
            "disable the losing strategy."
        )

    # Confidence sweep — if low-confidence trades lose, raise AI_MIN_CONFIDENCE.
    low_conf_losing = [r for r in by_conf if r[0] in ("0–19", "20–39") and r[3].startswith("−")]
    if low_conf_losing:
        recs.append(
            "Sub-40% confidence bucket is net-negative — raise AI_MIN_CONFIDENCE "
            "to 40 (or disable that tier of signals)."
        )

    # Brier calibration
    if brier["bias"] is not None and abs(brier["bias"]) > 0.03:
        direction = "over-predicting" if brier["bias"] > 0 else "under-predicting"
        recs.append(
            f"Predictions are {direction} by {brier['bias']:+.3f} on average. "
            "Calibrator will correct automatically once there are ≥30 resolved "
            "samples with a non-null source — confirm CALIBRATOR_ENABLED=true."
        )

    # Calibration curve tail check
    tail_rows = [r for r in cal_curve if r[0] in ("0.8–0.9", "0.9–1.0")]
    for label, n, pred, actual, bias in tail_rows:
        if float(bias) > 0.10 and int(n) >= 10:
            recs.append(
                f"High-confidence tail ({label}) mis-calibrates by {bias}: "
                f"predicted {pred} vs realized {actual}. Shrink conf_boost in "
                "estimate_true_probability or reduce AI_MAX_CONFIDENCE cap."
            )

    if not recs:
        recs.append(
            "No immediate red flags. Consider a modest Kelly bump "
            "(KELLY_FRACTION 0.25 → 0.30) for the best-performing stratum."
        )
    return recs


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[2])
    parser.add_argument("--db",   default=DEFAULT_DB, help="path to trades.db")
    parser.add_argument("--days", type=int, default=30, help="lookback in days (default 30)")
    args = parser.parse_args()

    db_path = os.path.abspath(args.db)
    if not os.path.exists(db_path):
        print(f"ERROR: {db_path} does not exist.", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db_path)
    try:
        trades = fetch_resolved_trades(conn, args.days)
        brier  = fetch_brier_rows(conn, args.days)
    finally:
        conn.close()

    print(f"\nWalk-forward analysis | db={db_path} | last {args.days} days")
    print(f"  trades resolved: {len(trades):>4}   brier rows: {len(brier):>4}\n")

    if not trades and not brier:
        print("No resolved rows in this window. Nothing to analyze.")
        return 0

    overall = overall_metrics(trades)
    print(
        f"Overall: {overall['n']} trades | win {overall['win_rate']:.1f}% | "
        f"PnL {_money(overall['pnl'])} | ROI {overall['roi']:+.1f}%"
    )

    by_strategy = summarize_group(trades, "strategy")
    by_category = summarize_group(trades, "category")
    by_regime   = summarize_group(trades, "regime_at_entry")
    by_conf     = confidence_buckets(trades)
    cal_curve   = calibration_curve(brier)

    _print_table("By strategy",          by_strategy,
                 ("strategy", "n", "win", "pnl", "roi"))
    _print_table("By category",          by_category,
                 ("category", "n", "win", "pnl", "roi"))
    _print_table("By regime-at-entry",   by_regime,
                 ("regime", "n", "win", "pnl", "roi"))
    _print_table("By ai_confidence band", by_conf,
                 ("confidence", "n", "win", "pnl", "roi"))

    b = brier_overall(brier)
    if b["n"] > 0:
        print(f"\nBrier score: {b['brier']:.4f} over {b['n']} predictions"
              f"   |  mean bias (pred − actual) = {b['bias']:+.3f}")
        _print_table("Calibration curve (decile)", cal_curve,
                     ("bucket", "n", "mean_pred", "mean_actual", "bias"))
    else:
        print("\nNo resolved brier rows — calibrator has no training data yet.")

    print("\n── Recommendations " + "─" * 60)
    for i, r in enumerate(
        recommendations(overall, by_strategy, by_conf, b, cal_curve), 1
    ):
        # Simple wrap to 78 cols
        print(f"{i:>2}. {r}")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
