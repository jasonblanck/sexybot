"""
risk.py
Polymarket V2 — PMUSD Balance Monitor + Execution Gate

Task 2a: check_balance()  — monitor PMUSD, alert if low
Task 2b: ExecutionGate    — spread < 0.5c AND EV > 0 at 1% slippage
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import requests

from orderbook_ws import BookSnapshot

log = logging.getLogger(__name__)

CLOB_BASE         = "https://clob.polymarket.com"
PMUSD_SCALAR      = 1_000_000
REQUEST_TIMEOUT   = 8

SPREAD_MAX_CENTS  = 0.50
SLIPPAGE_RATE     = 0.01
MIN_EV            = 0.0
LOW_BALANCE_ALERT = 50.0
CRITICAL_BALANCE  = 10.0


@dataclass
class BalanceInfo:
    balance_raw:   int
    allowance_raw: int

    @property
    def balance(self) -> float:
        return self.balance_raw / PMUSD_SCALAR

    @property
    def allowance(self) -> float:
        return self.allowance_raw / PMUSD_SCALAR

    @property
    def is_low(self) -> bool:
        return self.balance < LOW_BALANCE_ALERT

    @property
    def is_critical(self) -> bool:
        return self.balance < CRITICAL_BALANCE

    @property
    def allowance_sufficient(self) -> bool:
        return self.allowance_raw >= self.balance_raw


def check_balance(
    address:      str,
    auth_headers: dict,
    *,
    alert_fn: Optional[callable] = None,
) -> BalanceInfo:
    params = {"asset_type": "COLLATERAL", "signature_type": "0"}
    try:
        resp = requests.get(
            f"{CLOB_BASE}/balance-allowance",
            params=params,
            headers=auth_headers,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"check_balance request failed: {exc}") from exc

    info = BalanceInfo(
        balance_raw   = int(float(data.get("balance",   "0"))),
        allowance_raw = int(float(data.get("allowance", "0"))),
    )

    if info.is_critical:
        msg = f"CRITICAL: PMUSD balance ${info.balance:.2f} below ${CRITICAL_BALANCE:.2f}"
        log.error(msg)
        if alert_fn:
            alert_fn(msg)
    elif info.is_low:
        msg = f"WARNING: PMUSD balance ${info.balance:.2f} below ${LOW_BALANCE_ALERT:.2f}"
        log.warning(msg)
        if alert_fn:
            alert_fn(msg)

    if not info.allowance_sufficient:
        msg = (
            f"WARNING: on-chain allowance ${info.allowance:.2f} "
            f"< balance ${info.balance:.2f} — approve more PMUSD on CTF Exchange"
        )
        log.warning(msg)
        if alert_fn:
            alert_fn(msg)

    log.info("Balance: $%.2f PMUSD  Allowance: $%.2f PMUSD", info.balance, info.allowance)
    return info


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
    Two-condition execution gate.

    Condition 1 — Spread: (best_ask - best_bid) * 100 < spread_max_cents
    Condition 2 — EV at 1% slippage:
        BUY:  ev = true_prob - best_ask * (1 + slippage)  > min_ev
        SELL: ev = best_bid  * (1 - slippage) - true_prob > min_ev
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
        side:       str,
        *,
        balance:    Optional[BalanceInfo] = None,
        order_size: Optional[float]       = None,
    ) -> GateVerdict:
        if book.is_stale:
            return GateVerdict(passed=False, reject_reason="order book is stale")

        if book.best_bid is None or book.best_ask is None:
            return GateVerdict(passed=False, reject_reason="empty order book")

        if balance is not None and balance.is_critical:
            return GateVerdict(passed=False, reject_reason=f"critical balance ${balance.balance:.2f}")

        if balance is not None and order_size is not None:
            if order_size > balance.balance:
                return GateVerdict(
                    passed=False,
                    reject_reason=f"order size ${order_size:.2f} > balance ${balance.balance:.2f}",
                )

        # Condition 1: spread
        spread_cents = book.spread_cents
        if spread_cents is None or spread_cents >= self.spread_max_cents:
            return GateVerdict(
                passed=False,
                spread_cents=spread_cents,
                reject_reason=f"spread {spread_cents:.3f}c >= max {self.spread_max_cents:.2f}c",
            )

        # Condition 2: EV with slippage
        if side.upper() == "BUY":
            execution_price = book.best_ask
            effective_price = execution_price * (1 + self.slippage_rate)
            ev_net = true_prob - effective_price
        else:
            execution_price = book.best_bid
            effective_price = execution_price * (1 - self.slippage_rate)
            ev_net = effective_price - true_prob

        ev_net = round(ev_net, 6)

        if ev_net <= self.min_ev:
            return GateVerdict(
                passed=False,
                ev_net=ev_net,
                spread_cents=spread_cents,
                reject_reason=(
                    f"EV {ev_net:.4f} <= {self.min_ev:.4f} "
                    f"(true_prob={true_prob:.3f} exec={execution_price:.4f} "
                    f"slip={self.slippage_rate*100:.0f}%)"
                ),
            )

        log.info(
            "GATE OPEN | side=%s ev=%.4f spread=%.3fc true_prob=%.3f exec=%.4f",
            side, ev_net, spread_cents, true_prob, execution_price,
        )
        return GateVerdict(passed=True, ev_net=ev_net, spread_cents=spread_cents)
