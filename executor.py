"""
executor.py
Polymarket V2 — Limit Order Execution Pipeline

Flow: gate check → EIP-712 sign → L1 auth (py_clob_client) → POST /order
Auth: uses py_clob_client's sign_clob_auth_message (ClobAuth EIP-712 struct)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests
from web3 import Web3

# py_clob_client handles the ClobAuth EIP-712 struct for L1 authentication
from py_clob_client.signer import Signer
from py_clob_client.headers.headers import create_level_1_headers

from orderbook_ws import BookManager, BookSnapshot
from risk import BalanceInfo, ExecutionGate, GateVerdict, check_balance
from signing import LimitOrder, OrderSide, OrderSigner, SignedOrderPayload

log = logging.getLogger(__name__)

CLOB_BASE        = "https://clob.polymarket.com"
POLYGON_CHAIN_ID = 137
REQUEST_TIMEOUT  = 10


def build_l1_headers(private_key: str) -> dict:
    """
    Build L1 authentication headers using the correct ClobAuth EIP-712 struct.
    The CLOB verifies these on every authenticated request.
    """
    pk = private_key if private_key.startswith("0x") else f"0x{private_key}"
    signer = Signer(pk, chain_id=POLYGON_CHAIN_ID)
    headers = create_level_1_headers(signer)
    headers["Content-Type"] = "application/json"
    return headers


@dataclass
class OrderResult:
    success:  bool
    order_id: Optional[str]  = None
    status:   Optional[str]  = None
    error:    Optional[str]  = None
    payload:  Optional[dict] = None

    def __bool__(self) -> bool:
        return self.success


class ClobExecutor:
    """
    End-to-end limit order execution for Polymarket CLOB v2.

    1. Fetch live BookSnapshot from BookManager
    2. Run ExecutionGate (spread + EV)
    3. Confirm PMUSD balance
    4. Build + EIP-712 sign LimitOrder (custom signing.py)
    5. POST to CLOB /order with ClobAuth L1 headers
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
        self._pk      = private_key if private_key.startswith("0x") else f"0x{private_key}"
        self._signer  = OrderSigner(private_key, neg_risk=neg_risk)
        self._books   = book_manager
        self._gate    = gate or ExecutionGate()
        self._dry_run = dry_run
        self._address = self._signer.address
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

        log.info("ClobExecutor | address=%s dry_run=%s neg_risk=%s",
                 self._address, dry_run, neg_risk)

    def get_balance(self) -> BalanceInfo:
        headers = build_l1_headers(self._pk)
        return check_balance(self._address, headers, alert_fn=self._alert)

    def place_limit_order(
        self,
        token_id:       str | int,
        side:           OrderSide,
        true_prob:      float,
        size_pmusd:     float,
        *,
        price_override: Optional[float] = None,
        expiration:     int             = 0,
        order_type:     str             = "GTC",
    ) -> OrderResult:
        token_id_str = str(token_id)

        # 1. Book snapshot
        book = self._books.get_book(token_id_str)
        if book is None:
            return OrderResult(success=False, error=f"no live book for token {token_id_str[:16]}…")

        # 2. Execution gate
        balance  = self.get_balance()
        side_str = "BUY" if side == OrderSide.BUY else "SELL"
        verdict: GateVerdict = self._gate.check(
            book=book, true_prob=true_prob, side=side_str,
            balance=balance, order_size=size_pmusd,
        )
        if not verdict:
            log.info("GATE BLOCKED: %s", verdict.reject_reason)
            return OrderResult(success=False, error=verdict.reject_reason)

        # 3. Derive price from live book
        if price_override is not None:
            price = price_override
        elif side == OrderSide.BUY:
            price = book.best_ask
        else:
            price = book.best_bid
        price = max(0.001, min(0.999, round(price, 4)))

        # 4. Build + EIP-712 sign
        signed_order: LimitOrder = self._signer.build_and_sign(
            token_id=token_id_str, side=side, price=price,
            size_pmusd=size_pmusd, expiration=expiration,
        )
        payload: SignedOrderPayload = self._signer.to_api_payload(
            signed_order, order_type=order_type
        )

        log.info(
            "ORDER BUILT | side=%s price=%.4f size=$%.2f ev=%.4f spread=%.3fc token=%s…",
            side.name, price, size_pmusd, verdict.ev_net, verdict.spread_cents,
            token_id_str[:14],
        )

        if self._dry_run:
            log.info("DRY RUN — order not submitted")
            return OrderResult(
                success=True, order_id="DRY_RUN", status="dry_run",
                payload=self._as_dict(payload),
            )

        # 5. Submit to CLOB
        return self._submit(payload)

    def _submit(self, payload: SignedOrderPayload) -> OrderResult:
        headers = build_l1_headers(self._pk)
        body    = self._as_dict(payload)
        try:
            resp = self._session.post(
                f"{CLOB_BASE}/order", json=body, headers=headers, timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError as exc:
            err = f"CLOB HTTP {exc.response.status_code}: {exc.response.text[:200]}"
            log.error("ORDER FAILED: %s", err)
            return OrderResult(success=False, error=err)
        except requests.RequestException as exc:
            log.error("ORDER FAILED (network): %s", exc)
            return OrderResult(success=False, error=str(exc))

        order_id = data.get("orderID") or data.get("order_id") or data.get("id", "")
        status   = data.get("status", "unknown")
        log.info("ORDER PLACED | id=%s status=%s", order_id, status)
        return OrderResult(success=True, order_id=order_id, status=status, payload=body)

    @staticmethod
    def _as_dict(p: SignedOrderPayload) -> dict:
        return {
            "order":     p.order,
            "owner":     p.owner,
            "orderType": p.order_type,
            "signature": p.signature,
        }

    @staticmethod
    def _alert(msg: str) -> None:
        log.warning("[BALANCE ALERT] %s", msg)
