"""
executor.py
Polymarket V2 — Limit Order Execution Pipeline

Flow: gate check → EIP-712 sign → L1 auth headers → POST /order
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests
from eth_account import Account
from eth_account.messages import defunct_hash_message
from web3 import Web3

from orderbook_ws import BookManager, BookSnapshot
from risk import BalanceInfo, ExecutionGate, GateVerdict, check_balance
from signing import LimitOrder, OrderSide, OrderSigner, SignedOrderPayload

log = logging.getLogger(__name__)

CLOB_BASE       = "https://clob.polymarket.com"
REQUEST_TIMEOUT = 10


def build_auth_headers(private_key: str, address: str) -> dict:
    """
    Generate L1 authentication headers for the CLOB v2 API.
    POLY_SIGNATURE = personal_sign(keccak256(timestamp_string))
    """
    ts  = str(int(time.time()))
    pk  = private_key if private_key.startswith("0x") else f"0x{private_key}"
    acc = Account.from_key(pk)

    msg_hash = defunct_hash_message(text=ts)
    signed   = acc.sign_message(msg_hash)
    sig_hex  = signed.signature.hex()
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex

    return {
        "Content-Type":   "application/json",
        "POLY_ADDRESS":   Web3.to_checksum_address(address),
        "POLY_SIGNATURE": sig_hex,
        "POLY_TIMESTAMP": ts,
        "POLY_NONCE":     "0",
    }


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
    4. Build + EIP-712 sign order
    5. POST to CLOB /order with L1 auth
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
        headers = build_auth_headers(self._pk, self._address)
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

        # 1. Book
        book = self._books.get_book(token_id_str)
        if book is None:
            return OrderResult(success=False, error=f"no live book for token {token_id_str[:16]}…")

        # 2. Gate
        balance = self.get_balance()
        side_str = "BUY" if side == OrderSide.BUY else "SELL"
        verdict: GateVerdict = self._gate.check(
            book=book, true_prob=true_prob, side=side_str,
            balance=balance, order_size=size_pmusd,
        )
        if not verdict:
            log.info("GATE BLOCKED: %s", verdict.reject_reason)
            return OrderResult(success=False, error=verdict.reject_reason)

        # 3. Price
        if price_override is not None:
            price = price_override
        elif side == OrderSide.BUY:
            price = book.best_ask
        else:
            price = book.best_bid
        price = max(0.001, min(0.999, round(price, 4)))

        # 4. Sign
        signed_order: LimitOrder = self._signer.build_and_sign(
            token_id=token_id_str, side=side, price=price,
            size_pmusd=size_pmusd, expiration=expiration,
        )
        payload: SignedOrderPayload = self._signer.to_api_payload(signed_order, order_type=order_type)

        log.info(
            "ORDER BUILT | side=%s price=%.4f size=$%.2f ev=%.4f spread=%.3fc token=%s…",
            side.name, price, size_pmusd, verdict.ev_net, verdict.spread_cents, token_id_str[:14],
        )

        if self._dry_run:
            log.info("DRY RUN — order not submitted")
            return OrderResult(
                success=True, order_id="DRY_RUN", status="dry_run",
                payload=self._as_dict(payload),
            )

        # 5. Submit
        return self._submit(payload)

    def _submit(self, payload: SignedOrderPayload) -> OrderResult:
        headers = build_auth_headers(self._pk, self._address)
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
