"""
executor.py
Polymarket V2 — Limit Order Execution Pipeline

Auth flow:
  1. ClobClient derives API key via L1 ClobAuth EIP-712 on startup
  2. All subsequent calls use L2 HMAC auth (API key + secret)

Order signing:
  - EIP-712 V2 order struct is built by signing.py (raw implementation)
  - ClobClient wraps the signed order for submission

Balance note:
  If balance shows 0, PMUSD allowance for CTF Exchange V2 is likely not
  approved. Approve via Polymarket UI or call approve() on the PMUSD contract.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    AssetType,
    BalanceAllowanceParams,
    OrderArgs,
    OrderType,
)
from py_clob_client.constants import POLYGON
from py_clob_client.order_builder.constants import BUY, SELL

from orderbook_ws import BookManager, BookSnapshot
from risk import BalanceInfo, ExecutionGate, GateVerdict
from signing import OrderSide

log = logging.getLogger(__name__)

CLOB_HOST        = "https://clob.polymarket.com"
PMUSD_SCALAR     = 1_000_000


def _make_client(private_key: str) -> ClobClient:
    """
    Construct a ClobClient and derive API credentials (L1 → L2).
    Called once on startup; derived creds are cached in the client instance.
    """
    pk = private_key if private_key.startswith("0x") else f"0x{private_key}"
    client = ClobClient(host=CLOB_HOST, chain_id=POLYGON, key=pk, signature_type=0)
    creds  = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    log.info("CLOB auth ready | address=%s api_key=%s", client.get_address(), creds.api_key[:8] + "…")
    return client


@dataclass
class BalanceInfo:
    balance_raw:   int      # atomic PMUSD (6 decimals)
    allowance_raw: int      # CTF Exchange V2 allowance (atomic)

    @property
    def balance(self) -> float:
        return self.balance_raw / PMUSD_SCALAR

    @property
    def allowance(self) -> float:
        return self.allowance_raw / PMUSD_SCALAR

    @property
    def is_low(self) -> bool:
        return self.balance < 50.0

    @property
    def is_critical(self) -> bool:
        return self.balance < 10.0


CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"


def _parse_balance(raw: dict) -> BalanceInfo:
    """
    Parse the V2 balance-allowance response.
    Response shape: {'balance': '213800000', 'allowances': {'0x4bFb...': '0', ...}}
    """
    balance_raw   = int(float(raw.get("balance", "0")))
    allowances    = raw.get("allowances", {})
    allowance_raw = int(float(allowances.get(CTF_EXCHANGE, "0")))
    return BalanceInfo(balance_raw=balance_raw, allowance_raw=allowance_raw)


@dataclass
class OrderResult:
    success:  bool
    order_id: Optional[str]  = None
    status:   Optional[str]  = None
    error:    Optional[str]  = None

    def __bool__(self) -> bool:
        return self.success


class ClobExecutor:
    """
    End-to-end limit order execution for Polymarket CLOB v2.

    1. Fetch live BookSnapshot from BookManager
    2. Run ExecutionGate (spread + EV)
    3. Check PMUSD balance + CTF Exchange allowance
    4. Build + EIP-712 sign via ClobClient (uses py_order_utils internally)
    5. Submit via ClobClient.post_order() with L2 HMAC auth
    """

    def __init__(
        self,
        private_key:  str,
        book_manager: BookManager,
        *,
        gate:     Optional[ExecutionGate] = None,
        dry_run:  bool = True,
        neg_risk: bool = False,
    ):
        self._pk          = private_key
        self._client      = _make_client(private_key)
        self._books       = book_manager
        self._gate        = gate or ExecutionGate()
        self._dry_run     = dry_run
        self._neg_risk    = neg_risk

        log.info("ClobExecutor ready | address=%s dry_run=%s",
                 self._client.get_address(), dry_run)

    def get_balance(self) -> BalanceInfo:
        raw = self._client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        info = _parse_balance(raw)

        if info.is_critical:
            log.error("CRITICAL: PMUSD balance $%.2f — trading halted", info.balance)
        elif info.is_low:
            log.warning("WARNING: PMUSD balance $%.2f is low", info.balance)

        if info.balance > 0 and info.allowance_raw == 0:
            log.error(
                "CTF Exchange V2 allowance is 0 — approve PMUSD at "
                "https://polymarket.com or call approve() on the PMUSD contract"
            )

        log.info("Balance: $%.2f PMUSD  CTF-allowance: $%.2f", info.balance, info.allowance)
        return info

    def place_limit_order(
        self,
        token_id:       str | int,
        side:           OrderSide,
        true_prob:      float,
        size_pmusd:     float,
        *,
        price_override: Optional[float] = None,
        expiration:     int             = 0,
    ) -> OrderResult:
        token_id_str = str(token_id)

        # 1. Live book
        book = self._books.get_book(token_id_str)
        if book is None:
            return OrderResult(success=False, error=f"no live book for token {token_id_str[:16]}…")

        # 2. Gate
        side_str = "BUY" if side == OrderSide.BUY else "SELL"
        balance  = self.get_balance()

        # Gate needs a BalanceInfo-like object — pass ours directly
        from risk import BalanceInfo as RiskBalanceInfo
        risk_bal = RiskBalanceInfo(
            balance_raw   = balance.balance_raw,
            allowance_raw = balance.allowance_raw,
        )
        verdict: GateVerdict = self._gate.check(
            book=book, true_prob=true_prob, side=side_str,
            balance=risk_bal, order_size=size_pmusd,
        )
        if not verdict:
            log.info("GATE BLOCKED: %s", verdict.reject_reason)
            return OrderResult(success=False, error=verdict.reject_reason)

        # 3. Price from book
        if price_override is not None:
            price = price_override
        elif side == OrderSide.BUY:
            price = book.best_ask
        else:
            price = book.best_bid
        price = max(0.001, min(0.999, round(price, 4)))

        log.info(
            "ORDER QUEUED | side=%s price=%.4f size=$%.2f ev=%.4f spread=%.3fc",
            side.name, price, size_pmusd, verdict.ev_net, verdict.spread_cents,
        )

        if self._dry_run:
            log.info("DRY RUN — order not submitted | token=%s…", token_id_str[:14])
            return OrderResult(success=True, order_id="DRY_RUN", status="dry_run")

        # 4. Build + sign via ClobClient (EIP-712 internally)
        clob_side = BUY if side == OrderSide.BUY else SELL
        try:
            signed = self._client.create_order(
                OrderArgs(
                    token_id   = token_id_str,
                    price      = price,
                    size       = size_pmusd,
                    side       = clob_side,
                    expiration = expiration,
                )
            )
        except Exception as exc:
            log.error("create_order failed: %s", exc)
            return OrderResult(success=False, error=str(exc))

        # 5. Submit
        try:
            resp = self._client.post_order(signed, OrderType.GTC)
        except Exception as exc:
            log.error("post_order failed: %s", exc)
            return OrderResult(success=False, error=str(exc))

        order_id = resp.get("orderID") or resp.get("order_id") or resp.get("id", "")
        status   = resp.get("status", "unknown")
        log.info("ORDER PLACED | id=%s status=%s token=%s…", order_id, status, token_id_str[:14])
        return OrderResult(success=True, order_id=order_id, status=status)
