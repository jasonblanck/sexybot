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
from dataclasses import dataclass
from typing import Optional

from orderbook_ws import BookSnapshot

log = logging.getLogger(__name__)

SPREAD_MAX_CENTS    = 0.50
SLIPPAGE_RATE       = 0.01
MIN_EV              = 0.0
LOW_BALANCE         = 50.0
CRITICAL_BALANCE    = 10.0
PMUSD_SCALAR        = 1_000_000
# Payoff ratio <1 makes sub-0.30 YES BUYs unprofitable unless accuracy is
# near-perfect; block them at the gate.
MIN_YES_BUY_PRICE   = float(os.getenv("MIN_YES_BUY_PRICE", "0.30"))

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
        min_yes_buy_price: float = MIN_YES_BUY_PRICE,
    ):
        self.spread_max_cents  = spread_max_cents
        self.slippage_rate     = slippage_rate
        self.min_ev            = min_ev
        self.min_yes_buy_price = min_yes_buy_price

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
            # 5% cushion so fees / minor slippage can't flip a "just barely
            # fits" order into a "not enough balance" error from the CLOB.
            # Recommendation from the 2026-04-22 backtest: the April 9 storm
            # fired ~460 orders against a $0.86 wallet; every one errored.
            required  = order_size * 1.05
            spendable = balance.balance - CRITICAL_BALANCE   # never dip below $10 reserve
            if required > spendable:
                return GateVerdict(
                    passed=False,
                    reject_reason=(
                        f"order ${order_size:.2f}×1.05=${required:.2f} would breach "
                        f"${CRITICAL_BALANCE:.0f} reserve "
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
            # Underdog-BUY guard — see MIN_YES_BUY_PRICE.
            if execution_price < self.min_yes_buy_price:
                return GateVerdict(
                    passed=False,
                    spread_cents=spread_cents,
                    reject_reason=(
                        f"BUY price {execution_price:.3f} < min "
                        f"{self.min_yes_buy_price:.3f} (underdog guard)"
                    ),
                )
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


BALANCE_ERROR_LIMIT       = int(os.getenv("BALANCE_ERROR_LIMIT", "5"))
BALANCE_ERROR_WINDOW_SEC  = int(os.getenv("BALANCE_ERROR_WINDOW", "600"))   # 10 min
BALANCE_ERROR_PAUSE_SEC   = int(os.getenv("BALANCE_ERROR_PAUSE",  "900"))   # 15 min


class BalanceErrorCircuitBreaker:
    """
    Pause all strategy order placement after N balance-related errors in a
    rolling window. Protects against runaway retry storms like the 2026-04-09
    event where 460 orders were fired in 82 min against a $0.86 wallet.

    Not a kill-switch — just a cooldown. After `pause_sec` without fresh
    errors, the breaker automatically re-arms (history is cleared lazily
    on the next `record_error()` or `is_tripped()` call).

    Usage
    -----
    breaker = BalanceErrorCircuitBreaker()
    # In the main loop, before any order-placement pass:
    if breaker.is_tripped():
        continue        # skip this cycle

    # After each order attempt:
    if "not enough balance" in (result.error or "").lower():
        breaker.record_error()
    """

    BALANCE_ERROR_PATTERNS = (
        "not enough balance",
        "insufficient balance",
        "insufficient funds",
        "balance too low",
    )

    def __init__(
        self,
        limit:     int = BALANCE_ERROR_LIMIT,
        window:    int = BALANCE_ERROR_WINDOW_SEC,
        pause_sec: int = BALANCE_ERROR_PAUSE_SEC,
    ):
        self.limit      = limit
        self.window     = window
        self.pause_sec  = pause_sec
        self._errors:   list[float] = []
        self._tripped_at: Optional[float] = None

    @classmethod
    def is_balance_error(cls, msg: Optional[str]) -> bool:
        if not msg:
            return False
        m = msg.lower()
        return any(p in m for p in cls.BALANCE_ERROR_PATTERNS)

    def record_error(self) -> None:
        now = time.time()
        self._errors.append(now)
        cutoff = now - self.window
        self._errors = [t for t in self._errors if t >= cutoff]
        if len(self._errors) >= self.limit and self._tripped_at is None:
            self._tripped_at = now
            log.critical(
                "⛔ BALANCE CIRCUIT BREAKER TRIPPED | %d balance errors in %ds — "
                "pausing order placement for %ds",
                len(self._errors), self.window, self.pause_sec,
            )

    def is_tripped(self) -> bool:
        if self._tripped_at is None:
            return False
        if time.time() - self._tripped_at >= self.pause_sec:
            log.warning("BALANCE CIRCUIT BREAKER | cooldown elapsed, re-arming")
            self._tripped_at = None
            self._errors.clear()
            return False
        return True


@dataclass
class MarketRegime:
    """
    Lightweight regime descriptor used to scale Kelly fraction and exit bands.

    All fields are optional — missing fields are treated as "benign" (no scaling).
    """
    book_depth_usdc:          Optional[float] = None   # top-5 both sides, USDC
    mid_volatility:           Optional[float] = None   # stddev of recent mids
    spread_cents:             Optional[float] = None
    time_to_resolution_hours: Optional[float] = None

    @property
    def multiplier(self) -> float:
        """
        Scale factor in [0.25, 1.0] applied to kelly_fraction.

        Shrink in adverse conditions:
          • Thin books  (depth < $500)           → 0.50x
          • Thinnish    (depth $500–$1500)       → 0.75x
          • High vol    (mid stddev > 2c)        → 0.50x
          • Medium vol  (mid stddev > 1c)        → 0.75x
          • Wide spread (> 1.0c)                 → 0.75x
          • Near resolution (< 6h)               → 0.50x  — oracle/black-swan risk
        """
        mult = 1.0
        if self.book_depth_usdc is not None:
            if self.book_depth_usdc < 500:
                mult *= 0.50
            elif self.book_depth_usdc < 1500:
                mult *= 0.75
        if self.mid_volatility is not None:
            if self.mid_volatility > 0.02:
                mult *= 0.50
            elif self.mid_volatility > 0.01:
                mult *= 0.75
        if self.spread_cents is not None and self.spread_cents > 1.0:
            mult *= 0.75
        if self.time_to_resolution_hours is not None and self.time_to_resolution_hours < 6:
            mult *= 0.50
        return round(max(0.25, min(1.0, mult)), 3)


def kelly_size(
    true_prob:  float,
    price:      float,
    balance:    float,
    *,
    kelly_fraction: float = 0.25,
    max_size:       float = 10.0,
    min_size:       float = 1.0,
    max_pct_of_balance: float = 0.05,
    signal_strength:    float = 1.0,
    regime:             Optional[MarketRegime] = None,
) -> float:
    """
    Quarter-Kelly position sizing for a binary prediction market.

    Formula (buying YES at `price`):
        f* = (true_prob - price) / (1 - price)
        effective_fraction = kelly_fraction × signal_strength × regime.multiplier
        bet = f* × effective_fraction × balance

    `signal_strength` ∈ [0,1] scales the bet by model conviction — pure-OBI
    signals use ~0.3, spike-only ~0.6, spike+OBI-aligned ~1.0.

    `regime` shrinks the bet in thin / volatile / short-horizon markets where
    execution quality and resolution risk are both worse.

    Returns 0.0 when there is no positive edge.
    """
    if price <= 0 or price >= 1:
        return 0.0
    edge = true_prob - price
    if edge <= 0:
        return 0.0

    regime_mult = regime.multiplier if regime is not None else 1.0
    effective_fraction = kelly_fraction * max(0.0, min(1.0, signal_strength)) * regime_mult

    full_kelly = edge / (1.0 - price)
    size = full_kelly * effective_fraction * balance
    ceiling = min(max_size, max(min_size, balance * max_pct_of_balance))
    size = min(size, ceiling)
    if size < min_size:
        return 0.0
    return round(size, 2)


def dynamic_exit_levels(
    entry_price: float,
    *,
    base_profit: float = 0.08,
    base_stop:   float = 0.05,
    regime:      Optional[MarketRegime] = None,
) -> tuple[float, float]:
    """
    Return (profit_target, stop_loss) as positive fractions of entry_price.

    Scaling rules:
      • Near tails (|entry_price − 0.5| > 0.35, i.e. below 0.15 or above 0.85)
        small absolute price moves produce large *relative* swings, so both
        target and stop are widened to avoid exiting on noise.
      • In high-volatility regimes (mid stddev > 1c) both bands are widened
        proportionally so normal chop doesn't trigger exits.
      • Values are clamped to sensible bounds — profit ≤ 50%, stop ≤ 25%.
    """
    tail_distance = min(entry_price, 1.0 - entry_price)

    if tail_distance < 0.15:
        profit = base_profit * 2.0
        stop   = base_stop   * 1.5
    elif tail_distance < 0.30:
        profit = base_profit * 1.25
        stop   = base_stop   * 1.10
    else:
        profit = base_profit
        stop   = base_stop

    if regime is not None and regime.mid_volatility is not None:
        vol_scale = 1.0 + min(2.0, regime.mid_volatility / 0.01)   # cap at 3×
        profit *= vol_scale
        stop   *= vol_scale

    return (
        round(max(0.02, min(0.50, profit)), 4),
        round(max(0.02, min(0.25, stop)),   4),
    )

