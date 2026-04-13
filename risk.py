"""
risk.py
Polymarket V2 — PMUSD Balance Info + Execution Gate

BalanceInfo: canonical dataclass shared across the codebase.
ExecutionGate: two-condition pre-trade check (spread + EV).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from orderbook_ws import BookSnapshot

log = logging.getLogger(__name__)

SPREAD_MAX_CENTS    = 0.50
SLIPPAGE_RATE       = 0.01
MIN_EV              = 0.0
LOW_BALANCE         = 50.0
CRITICAL_BALANCE    = 10.0
PMUSD_SCALAR        = 1_000_000

MAX_DRAWDOWN_USD    = float(os.getenv("MAX_DRAWDOWN_USD", "50.0"))   # halt if peak-to-trough > $50
DRAWDOWN_WINDOW_SEC = int(os.getenv("DRAWDOWN_WINDOW",   "600"))     # rolling 10-minute window


@dataclass
class BalanceInfo:
    """
    PMUSD balance and CTF Exchange V2 allowance in both atomic and human units.
    Single canonical class — do not duplicate in other modules.
    """
    balance_raw:   int    # atomic PMUSD (6 decimals)
    allowance_raw: int    # CTF Exchange V2 approved spend (atomic)

    @property
    def balance(self) -> float:
        return self.balance_raw / PMUSD_SCALAR

    @property
    def allowance(self) -> float:
        return self.allowance_raw / PMUSD_SCALAR

    @property
    def is_low(self) -> bool:
        return self.balance < LOW_BALANCE

    @property
    def is_critical(self) -> bool:
        return self.balance < CRITICAL_BALANCE

    @property
    def allowance_sufficient(self) -> bool:
        """True when on-chain allowance covers at least the available balance."""
        return self.allowance_raw >= self.balance_raw


@dataclass
class GateVerdict:
    passed:        bool
    ev_net:        Optional[float] = None
    spread_cents:  Optional[float] = None
    reject_reason: Optional[str]   = None

    def __bool__(self) -> bool:
        return self.passed


class ExecutionGate:
    """
    Two-condition pre-trade gate.

    Condition 1 — Spread:
        (best_ask - best_bid) × 100  <  spread_max_cents
        Tight spread = low transaction cost + liquid book.

    Condition 2 — EV after 1 % slippage:
        BUY : ev = true_prob − best_ask × (1 + slippage)  > min_ev
        SELL: ev = best_bid  × (1 − slippage) − true_prob > min_ev

    Both must pass. Balance/order-size guards run first.
    """

    def __init__(
        self,
        spread_max_cents: float = SPREAD_MAX_CENTS,
        slippage_rate:    float = SLIPPAGE_RATE,
        min_ev:           float = MIN_EV,
    ):
        self.spread_max_cents = spread_max_cents
        self.slippage_rate    = slippage_rate
        self.min_ev           = min_ev

    def check(
        self,
        book:       BookSnapshot,
        true_prob:  float,
        side:       str,                        # "BUY" or "SELL"
        *,
        balance:    Optional[BalanceInfo] = None,
        order_size: Optional[float]       = None,
    ) -> GateVerdict:

        # ── Guard: stale / empty book ────────────────────────────────────────
        if book.is_stale:
            return GateVerdict(passed=False, reject_reason="order book is stale")

        if book.best_bid is None or book.best_ask is None:
            return GateVerdict(passed=False, reject_reason="empty order book")

        # ── Guard: balance checks ────────────────────────────────────────────
        if balance is not None and balance.is_critical:
            return GateVerdict(
                passed=False,
                reject_reason=f"critical balance ${balance.balance:.2f} PMUSD",
            )

        if balance is not None and order_size is not None:
            spendable = balance.balance - CRITICAL_BALANCE   # never dip below $10 reserve
            if order_size > spendable:
                return GateVerdict(
                    passed=False,
                    reject_reason=(
                        f"order ${order_size:.2f} would breach ${CRITICAL_BALANCE:.0f} reserve "
                        f"(balance=${balance.balance:.2f} spendable=${spendable:.2f})"
                    ),
                )

        # ── Condition 1: spread ──────────────────────────────────────────────
        spread_cents = book.spread_cents   # float or None
        spread_str   = f"{spread_cents:.3f}" if spread_cents is not None else "None"

        if spread_cents is None or spread_cents >= self.spread_max_cents:
            return GateVerdict(
                passed=False,
                spread_cents=spread_cents,
                reject_reason=f"spread {spread_str}c >= max {self.spread_max_cents:.2f}c",
            )

        # ── Condition 2: EV with slippage ────────────────────────────────────
        if side.upper() == "BUY":
            execution_price = book.best_ask
            effective_price = execution_price * (1 + self.slippage_rate)
            ev_net          = true_prob - effective_price
        else:
            execution_price = book.best_bid
            effective_price = execution_price * (1 - self.slippage_rate)
            ev_net          = effective_price - true_prob

        ev_net = round(ev_net, 6)

        if ev_net <= self.min_ev:
            return GateVerdict(
                passed=False,
                ev_net=ev_net,
                spread_cents=spread_cents,
                reject_reason=(
                    f"EV {ev_net:.4f} <= {self.min_ev:.4f} "
                    f"(prob={true_prob:.3f} exec={execution_price:.4f} "
                    f"slip={self.slippage_rate * 100:.0f}%)"
                ),
            )

        log.info(
            "GATE OPEN | side=%s ev=%.4f spread=%.3fc prob=%.3f exec=%.4f",
            side, ev_net, spread_cents, true_prob, execution_price,
        )
        return GateVerdict(passed=True, ev_net=ev_net, spread_cents=spread_cents)


class DrawdownHalt(Exception):
    """
    Raised by DrawdownGuard when the account-level drawdown kill-switch fires.
    Caught by the strategy restart wrapper to stop quoting without restarting.
    """


class DrawdownGuard:
    """
    Account-level max-drawdown kill-switch — the "Golden Rule".

    Records balance snapshots on every scan cycle. If the peak-to-current
    drawdown within a rolling window exceeds `max_drawdown`, raises
    DrawdownHalt (cancels restart loop) and logs a CRITICAL alert.

    Usage
    -----
    guard = DrawdownGuard()
    # In the main loop, after every balance fetch:
    guard.record_and_check(cycle_balance.balance)   # raises DrawdownHalt if triggered
    """

    def __init__(
        self,
        max_drawdown: float = MAX_DRAWDOWN_USD,
        window:       float = DRAWDOWN_WINDOW_SEC,
    ):
        self.max_drawdown = max_drawdown
        self.window       = window
        self._history:    list[tuple[float, float]] = []   # (ts, balance)
        self._triggered   = False

    @property
    def is_triggered(self) -> bool:
        return self._triggered

    def record_and_check(self, balance: float) -> None:
        """
        Record current balance; raise DrawdownHalt if limit is breached.
        Once triggered, every subsequent call also raises — strategy stays halted.
        """
        if self._triggered:
            raise DrawdownHalt("drawdown kill-switch already triggered — manual reset required")

        now = time.time()
        self._history.append((now, balance))
        cutoff = now - self.window
        self._history = [(t, b) for t, b in self._history if t >= cutoff]

        if len(self._history) < 2:
            return

        peak     = max(b for _, b in self._history)
        drawdown = peak - balance
        if drawdown >= self.max_drawdown:
            self._triggered = True
            log.critical(
                "⛔ DRAWDOWN KILL-SWITCH | peak=$%.2f  current=$%.2f  drop=$%.2f >= limit=$%.2f  "
                "window=%ds — all quoting halted, manual restart required",
                peak, balance, drawdown, self.max_drawdown, int(self.window),
            )
            raise DrawdownHalt(
                f"balance dropped ${drawdown:.2f} "
                f"(peak=${peak:.2f} → current=${balance:.2f}) "
                f"within {self.window / 60:.0f}min window"
            )

    def reset(self) -> None:
        """Manually reset after investigating the drawdown. Use with caution."""
        self._triggered = False
        self._history.clear()
        log.warning("DrawdownGuard manually reset — strategy will resume on next restart")


def kelly_size(
    true_prob:  float,
    price:      float,
    balance:    float,
    *,
    kelly_fraction: float = 0.25,
    max_size:       float = 10.0,
    min_size:       float = 1.0,
) -> float:
    """
    Quarter-Kelly position sizing for a binary prediction market.

    Formula (buying YES at `price`):
        f* = (true_prob - price) / (1 - price)
        bet = f* × kelly_fraction × balance

    The Quarter Kelly (kelly_fraction=0.25) caps exposure at 25% of the
    theoretical full-Kelly bet, protecting against model overconfidence and
    undetected oracle risk ("black swan" resolution disputes).

    Returns 0.0 when there is no positive edge.
    """
    if price <= 0 or price >= 1:
        return 0.0
    edge = true_prob - price
    if edge <= 0:
        return 0.0
    full_kelly = edge / (1.0 - price)
    size = full_kelly * kelly_fraction * balance
    return round(max(min_size, min(size, max_size)), 2)

