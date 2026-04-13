"""
executor.py
Polymarket V2 — Limit Order Execution Pipeline

Auth flow:
  1. ClobClient.create_or_derive_api_creds() calls L1 ClobAuth EIP-712 once
  2. All subsequent calls use L2 HMAC (api_key + secret)

Balance: fetched once per scan cycle by the caller; passed into
  place_limit_order() to avoid an HTTP round-trip per signal.
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

from orderbook_ws import BookManager
from risk import BalanceInfo, ExecutionGate, GateVerdict
from signing import OrderSide

log = logging.getLogger(__name__)

# py_clob_client uses a shared httpx.Client with no timeout — patch it to 15 s
# so get_balance_allowance / post_order / create_order never hang indefinitely.
try:
    import httpx as _httpx
    import py_clob_client.http_helpers.helpers as _clob_http
    _clob_http._http_client = _httpx.Client(http2=True, timeout=15.0)
    log.debug("py_clob_client HTTP timeout patched to 15 s")
except Exception as _e:
    log.warning("Could not patch py_clob_client timeout: %s", _e)

CLOB_HOST = "https://clob.polymarket.com"

# CTF Exchange V2 address — key used to look up allowance in API response
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF = "0xC5d563A36AE78145C45a50134d48A1215220f80a"  # used in _parse_balance allowance lookup


def _make_client(private_key: str, funder_address: Optional[str] = None) -> ClobClient:
    """
    Build a ClobClient and derive API credentials via L1 on startup.

    funder_address: proxy/funder wallet that holds USDC.e and has CTF Exchange
                    allowances set. When provided, signature_type=2 (POLY_GNOSIS_SAFE)
                    is used so orders are built with maker=funder_address and
                    balance is read from the funder wallet.
                    When None, signature_type=0 (EOA) and the signing key's
                    wallet is used for both signing and funding.
    """
    pk = private_key if private_key.startswith("0x") else f"0x{private_key}"

    if funder_address:
        sig_type = 2   # POLY_GNOSIS_SAFE / proxy-wallet: signer != funder/maker
        funder   = funder_address
    else:
        sig_type = 0   # EOA: signer == maker
        funder   = None

    client = ClobClient(
        host           = CLOB_HOST,
        chain_id       = POLYGON,
        key            = pk,
        signature_type = sig_type,
        funder         = funder,
    )
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)
    mode = {0: "EOA", 1: "POLY_PROXY", 2: "POLY_GNOSIS_SAFE"}.get(sig_type, str(sig_type))
    log.info(
        "CLOB auth ready | signer=%s funder=%s mode=%s api_key=%s",
        client.get_address(),
        funder_address or "(self)",
        mode,
        creds.api_key[:8] + "…",
    )
    return client


def _parse_balance(raw: dict) -> BalanceInfo:
    """
    Parse the V2 /balance-allowance response.
    Response: {'balance': '213800000', 'allowances': {'0x4bFb...': '0', ...}}
    Returns the maximum allowance across CTF Exchange V2 and NegRisk Exchange
    (both use the same USDC.e collateral; whichever has allowance is usable).
    Lookup is case-insensitive to guard against mixed-case API responses.
    """
    balance_raw = int(float(raw.get("balance", "0") or "0"))
    allowances  = raw.get("allowances", {})
    lc          = {k.lower(): v for k, v in allowances.items()}
    allow_ctf   = int(float(lc.get(CTF_EXCHANGE.lower(),  "0") or "0"))
    allow_nr    = int(float(lc.get(NEG_RISK_CTF.lower(),  "0") or "0"))
    allowance_raw = max(allow_ctf, allow_nr)
    return BalanceInfo(balance_raw=balance_raw, allowance_raw=allowance_raw)


@dataclass
class OrderResult:
    success:    bool
    order_id:   Optional[str]   = None
    status:     Optional[str]   = None
    error:      Optional[str]   = None
    fill_price: Optional[float] = None   # actual execution price (best_ask/bid at order time)
    token_qty:  Optional[float] = None   # outcome tokens purchased

    def __bool__(self) -> bool:
        return self.success


class ClobExecutor:
    """
    End-to-end limit order execution for Polymarket CLOB v2.

    Typical call sequence per scan cycle:
        balance = executor.get_balance()          # one HTTP call per cycle
        result  = executor.place_limit_order(     # uses cached balance
                      ..., cached_balance=balance)
    """

    def __init__(
        self,
        private_key:    str,
        book_manager:   BookManager,
        *,
        gate:           Optional[ExecutionGate] = None,
        dry_run:        bool = True,
        funder_address: Optional[str] = None,
    ):
        self._client  = _make_client(private_key, funder_address=funder_address)
        self._books   = book_manager
        self._gate    = gate or ExecutionGate()
        self._dry_run = dry_run

        log.info("ClobExecutor ready | address=%s dry_run=%s", self._client.get_address(), dry_run)

    # ── Public ─────────────────────────────────────────────────────────────────

    def get_balance(self) -> BalanceInfo:
        """
        Fetch current PMUSD balance and CTF Exchange allowance.
        Call once per scan cycle; pass the result to place_limit_order().
        """
        raw  = self._client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        info = _parse_balance(raw)

        if info.is_critical:
            log.error("CRITICAL: PMUSD balance $%.2f — orders blocked", info.balance)
        elif info.is_low:
            log.warning("WARNING: PMUSD balance $%.2f is low", info.balance)

        if info.balance > 0 and info.allowance_raw == 0:
            log.error(
                "CTF Exchange allowance is 0 — approve PMUSD spending at polymarket.com "
                "or call approve() on the PMUSD contract for %s",
                CTF_EXCHANGE,
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
        cached_balance: Optional[BalanceInfo] = None,
        price_override: Optional[float]       = None,
        expiration:     int                   = 0,
        bypass_gate:    bool                  = False,
    ) -> OrderResult:
        """
        Gate → sign → submit a single limit order.

        Parameters
        ----------
        cached_balance : pass the result of get_balance() to avoid an extra
                         HTTP round-trip; if None, a fresh call is made.
        """
        token_id_str = str(token_id)

        # 1. Live book
        book = self._books.get_book(token_id_str)
        if book is None:
            return OrderResult(success=False, error=f"no live book for {token_id_str[:16]}…")

        # 2. Balance (use cached value when available)
        balance = cached_balance if cached_balance is not None else self.get_balance()

        # 3. Gate check (skipped for market-making orders that bypass it)
        side_str = "BUY" if side == OrderSide.BUY else "SELL"
        if bypass_gate:
            verdict = GateVerdict(passed=True, ev_net=0.0, spread_cents=0.0)
        else:
            verdict = self._gate.check(
                book=book, true_prob=true_prob, side=side_str,
                balance=balance, order_size=size_pmusd,
            )
        if not verdict:
            log.debug("GATE BLOCKED [%s]: %s", token_id_str[:12], verdict.reject_reason)
            return OrderResult(success=False, error=verdict.reject_reason)

        # 4. Execution price from live book
        if price_override is not None:
            price = price_override
        elif side == OrderSide.BUY:
            price = book.best_ask
        else:
            price = book.best_bid

        if price is None:
            return OrderResult(success=False, error="no execution price available (empty book side)")

        price = max(0.001, min(0.999, round(price, 4)))

        # 5. Compute token size (must happen before dry-run so it's available for both paths)
        # py_clob_client OrderArgs.size = outcome tokens (not USDC).
        # Convert: tokens = USDC_amount / price_per_token.
        clob_side  = BUY if side == OrderSide.BUY else SELL
        token_size = round(size_pmusd / price, 4)

        # Guard: Polymarket rejects orders below $1 notional value
        if token_size * price < 1.0:
            return OrderResult(
                success=False,
                error=f"order too small: ${token_size * price:.2f} < $1.00 min",
            )

        log.info(
            "ORDER QUEUED | side=%s price=%.4f size=$%.2f tokens=%.4f ev=%.4f spread=%.3fc",
            side.name, price, size_pmusd, token_size, verdict.ev_net, verdict.spread_cents,
        )

        if self._dry_run:
            log.info("DRY RUN — not submitted | token=%s…", token_id_str[:14])
            return OrderResult(
                success=True, order_id="DRY_RUN", status="dry_run",
                fill_price=price, token_qty=token_size,
            )
        try:
            signed = self._client.create_order(
                OrderArgs(
                    token_id   = token_id_str,
                    price      = price,
                    size       = token_size,
                    side       = clob_side,
                    expiration = expiration,
                )
            )
        except Exception as exc:
            log.error("create_order failed: %s", exc)
            return OrderResult(success=False, error=str(exc))

        # 6. Submit
        try:
            resp = self._client.post_order(signed, OrderType.GTC)
        except Exception as exc:
            log.error("post_order failed: %s", exc)
            return OrderResult(success=False, error=str(exc))

        order_id = resp.get("orderID") or resp.get("order_id") or resp.get("id", "")
        status   = resp.get("status", "unknown")
        log.info("ORDER PLACED | id=%s status=%s token=%s…", order_id, status, token_id_str[:14])
        return OrderResult(
            success=True, order_id=order_id, status=status,
            fill_price=price, token_qty=token_size,
        )

    def close_position(
        self,
        token_id:   str | int,
        token_qty:  float,
        *,
        reason:     str = "exit",
        expiration: int = 0,
    ) -> OrderResult:
        """
        Sell `token_qty` outcome tokens at the current best bid.
        Used for profit-taking and stop-loss exits.

        Bypasses the EV gate (we always want to exit a losing/winning position),
        but still requires a live, non-stale book and a non-zero bid.
        """
        token_id_str = str(token_id)

        book = self._books.get_book(token_id_str)
        if book is None:
            return OrderResult(success=False, error=f"no live book for {token_id_str[:16]}…")

        if book.is_stale:
            return OrderResult(success=False, error="order book is stale — skipping exit")

        price = book.best_bid
        if price is None:
            return OrderResult(success=False, error="no bid on book — cannot exit")

        price = max(0.001, min(0.999, round(price, 4)))

        # Guard: Polymarket rejects orders below $1 notional value
        if round(token_qty, 4) * price < 1.0:
            return OrderResult(
                success=False,
                error=f"close too small: ${token_qty * price:.2f} < $1.00 min — position abandoned",
            )

        log.info(
            "CLOSE QUEUED | reason=%s price=%.4f qty=%.4f token=%s…",
            reason, price, token_qty, token_id_str[:14],
        )

        if self._dry_run:
            log.info("DRY RUN — close not submitted | token=%s…", token_id_str[:14])
            return OrderResult(
                success=True, order_id="DRY_RUN", status="dry_run",
                fill_price=price, token_qty=token_qty,
            )

        try:
            signed = self._client.create_order(
                OrderArgs(
                    token_id   = token_id_str,
                    price      = price,
                    size       = round(token_qty, 4),
                    side       = SELL,
                    expiration = expiration,
                )
            )
        except Exception as exc:
            log.error("close_position create_order failed: %s", exc)
            return OrderResult(success=False, error=str(exc))

        try:
            resp = self._client.post_order(signed, OrderType.GTC)
        except Exception as exc:
            log.error("close_position post_order failed: %s", exc)
            return OrderResult(success=False, error=str(exc))

        order_id = resp.get("orderID") or resp.get("order_id") or resp.get("id", "")
        status   = resp.get("status", "unknown")
        log.info(
            "POSITION CLOSED | reason=%s id=%s status=%s token=%s…",
            reason, order_id, status, token_id_str[:14],
        )
        return OrderResult(
            success=True, order_id=order_id, status=status,
            fill_price=price, token_qty=token_qty,
        )

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a single open order by ID."""
        if self._dry_run:
            log.info("DRY RUN — cancel order %s skipped", order_id)
            return True
        try:
            self._client.cancel(order_id)
            log.info("CANCELLED | order_id=%s", order_id)
            return True
        except Exception as exc:
            log.error("cancel_order %s failed: %s", order_id, exc)
            return False

    def cancel_all_orders(self) -> bool:
        """Cancel all outstanding open orders."""
        if self._dry_run:
            log.info("DRY RUN — cancel_all skipped")
            return True
        try:
            self._client.cancel_all()
            log.info("CANCEL ALL orders executed")
            return True
        except Exception as exc:
            log.error("cancel_all_orders failed: %s", exc)
            return False

