"""
market_maker.py
Polymarket V2 — Two-Sided Market Making Strategy

Strategy overview
-----------------
For each watched market the bot simultaneously quotes:
  • BUY YES at (mid − half_spread)   ← absorbs YES sellers
  • BUY NO  at (1 − mid − half_spread) ← absorbs YES buyers (equivalent ask side)

When both legs fill, the combined YES + NO position is worth $1.00
regardless of outcome; the P&L is the spread captured minus fees.

Inventory skewing
-----------------
If the bot accumulates more YES than NO (or vice versa) it adjusts the
quoted mid downward / upward so the over-held side is more aggressively
offered back into the market.

Circuit breaker
---------------
If the mid-price moves more than CIRCUIT_BREAKER_PCT (5 %) within
CIRCUIT_BREAKER_WINDOW (60 s) all MM orders for that market are cancelled
and quoting is paused for PAUSE_AFTER_CB (120 s).  This prevents the bot
from providing cheap liquidity to an informed trader.

Cancel-and-replace
------------------
When the current mid has drifted more than REQUOTE_TICK (0.01) from the
price at which quotes were placed, the bot cancels both legs and places
fresh quotes at the new mid.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from discovery import PolyMarket
from executor import ClobExecutor, OrderResult
from orderbook_ws import BookManager, BookSnapshot
from risk import BalanceInfo, CRITICAL_BALANCE
from signing import OrderSide

log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

ORDER_SIZE_USDC      = 5.0     # USDC per side per quote
MAX_INVENTORY_USDC   = 50.0    # stop quoting if abs(yes−no) > this in USDC value
SKEW_FACTOR          = 0.5     # fraction of inventory imbalance used to skew mid
CIRCUIT_BREAKER_PCT  = 0.05    # 5 % price move → circuit break
CIRCUIT_BREAKER_WIN  = 60      # seconds to look back for CB
PAUSE_AFTER_CB       = 120     # seconds to stay quiet after a CB fires
REQUOTE_TICK         = 0.01    # min mid movement that triggers a cancel-replace
MIN_SPREAD_TO_QUOTE  = 0.005   # don't quote if market spread is already < 0.5 c
MAX_MID_FOR_MM       = 0.80    # don't quote above 80 % (too close to resolution)
MIN_MID_FOR_MM       = 0.20    # don't quote below 20 %
# Adverse-selection guard: if OBI moves hard against a standing leg, informed
# flow is likely about to hit it — cancel before we become the exit liquidity.
ADVERSE_OBI_THRESHOLD = 0.55   # |OBI| above which a standing opposite-side leg is pulled


@dataclass
class MMState:
    """Per-market state for the market maker."""
    yes_order_id:     Optional[str]   = None   # active BUY YES order id
    no_order_id:      Optional[str]   = None   # active BUY NO  order id
    quote_mid:        Optional[float] = None   # mid when quotes were last placed
    yes_inventory:    float = 0.0              # YES tokens confirmed filled
    no_inventory:     float = 0.0              # NO  tokens confirmed filled
    # Cumulative filled qty seen for the *currently-active* order id — used
    # to compute fresh fills across poll cycles without double-counting.
    yes_seen_filled:  float = 0.0
    no_seen_filled:   float = 0.0
    mid_history:      list  = field(default_factory=list)  # [(ts, mid), …]
    paused_until:     float = 0.0              # epoch time — stay quiet until this


class MarketMaker:
    """
    Two-sided market maker for multiple Polymarket binary markets.

    Usage (inside async strategy):
        mm = MarketMaker(executor, book_manager, half_spread=0.02, order_size=5.0)
        while True:
            actions = await mm.run_once(markets, balance)
            await asyncio.sleep(scan_interval)
    """

    def __init__(
        self,
        executor:     ClobExecutor,
        book_manager: BookManager,
        *,
        half_spread:  float = 0.02,
        order_size:   float = ORDER_SIZE_USDC,
    ):
        self._ex      = executor
        self._books   = book_manager
        self._hs      = half_spread
        self._size    = order_size
        self._states: dict[str, MMState] = {}   # yes_token_id → MMState

    # ── Public ─────────────────────────────────────────────────────────────────

    async def run_once(
        self,
        markets: list[PolyMarket],
        balance: Optional[BalanceInfo],
    ) -> int:
        """
        Process all markets in one pass.
        Returns the number of order actions (place / cancel) taken.
        """
        if balance is not None and balance.is_critical:
            log.warning("MM: critical balance — skipping all quoting")
            return 0

        # Guard: ensure both order legs won't push balance below the $10 reserve.
        # bypass_gate=True skips the executor's reserve check, so we enforce it here.
        # Each cycle places a BUY YES leg and a BUY NO leg, each costing self._size USDC,
        # so the spendable balance must cover 2 × order_size.
        if balance is not None:
            spendable = balance.balance - CRITICAL_BALANCE
            required  = 2 * self._size
            if spendable < required:
                log.warning(
                    "MM: spendable $%.2f < 2 × order_size $%.2f — insufficient margin for both legs",
                    spendable, required,
                )
                return 0

        actions = 0
        for mkt in markets:
            try:
                actions += await self._process_market(mkt, balance)
            except Exception as exc:
                log.error("MM error on %s: %s", mkt.question[:40], exc)
        return actions

    async def cancel_all(self) -> None:
        """Cancel every outstanding MM order (e.g. on shutdown)."""
        ok = await asyncio.to_thread(self._ex.cancel_all_orders)
        if ok:
            for state in self._states.values():
                state.yes_order_id    = None
                state.no_order_id     = None
                state.yes_seen_filled = 0.0
                state.no_seen_filled  = 0.0
        log.info("MM: cancel_all issued")

    def record_fill(self, token_id: str, side: str, qty: float) -> None:
        """
        Manually record a confirmed fill. Normally fills are detected via
        `_poll_fills()` in each cycle; this method is a public hook for
        external fill events (e.g. a user-initiated trade).  side: 'yes' | 'no'
        """
        for yes_tid, state in self._states.items():
            if yes_tid == token_id or token_id.endswith(yes_tid[-8:]):
                if side == "yes":
                    state.yes_inventory += qty
                else:
                    state.no_inventory  += qty
                break

    async def _poll_fills(self, state: MMState) -> None:
        """
        Poll the CLOB for the current state of each live leg and update
        inventory tracking.  Called once per market per cycle, before
        deciding whether to requote.  Silent on lookup errors — the next
        cycle will retry.  Requires executor.get_order() (skipped in dry-run).
        """
        for side, order_attr, inv_attr, seen_attr in (
            ("yes", "yes_order_id", "yes_inventory", "yes_seen_filled"),
            ("no",  "no_order_id",  "no_inventory",  "no_seen_filled"),
        ):
            oid = getattr(state, order_attr)
            if not oid or oid == "DRY_RUN":
                continue
            info = await asyncio.to_thread(self._ex.get_order, oid)
            if not isinstance(info, dict):
                continue
            # Polymarket returns size_matched as a string decimal in outcome tokens.
            try:
                matched = float(info.get("size_matched", 0) or 0)
            except (TypeError, ValueError):
                continue
            seen = getattr(state, seen_attr)
            fresh = matched - seen
            if fresh > 1e-6:
                setattr(state, inv_attr, getattr(state, inv_attr) + fresh)
                setattr(state, seen_attr, matched)
                log.info(
                    "MM FILL | side=%s token=%s… fresh=%.4f total inv(yes=%.4f no=%.4f)",
                    side.upper(), oid[:12], fresh,
                    state.yes_inventory, state.no_inventory,
                )
            # If the order is fully filled / terminal, drop the id so we requote next cycle.
            status = (info.get("status") or "").lower()
            if status in ("matched", "filled", "canceled", "cancelled", "expired"):
                setattr(state, order_attr, None)
                setattr(state, seen_attr, 0.0)

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _process_market(
        self,
        mkt:     PolyMarket,
        balance: Optional[BalanceInfo],
    ) -> int:
        book = self._books.get_book(mkt.yes_token_id)
        if book is None or book.is_stale or book.mid is None:
            return 0

        # VAMP gives a depth-weighted fair value; fall back to simple mid
        mid = book.vamp or book.mid
        state = self._states.setdefault(mkt.yes_token_id, MMState())

        # ── Poll fills so inventory (and therefore skewing) stays accurate.
        # Must happen BEFORE we decide whether to cancel/replace so a freshly
        # filled leg drops its id and we compute the new skewed quote.
        await self._poll_fills(state)

        # Track mid history for circuit breaker
        now = time.time()
        state.mid_history.append((now, mid))
        state.mid_history = [(t, m) for t, m in state.mid_history
                             if now - t <= CIRCUIT_BREAKER_WIN]

        # ── Circuit breaker ───────────────────────────────────────────────────
        if self._circuit_breaker_triggered(state):
            log.warning(
                "MM CIRCUIT BREAK | %s  mid=%.4f  cancelling quotes",
                mkt.question[:45], mid,
            )
            state.paused_until = now + PAUSE_AFTER_CB
            return await self._cancel_quotes(mkt, state)

        if now < state.paused_until:
            log.debug("MM PAUSED | %s  resumes in %.0fs",
                      mkt.question[:40], state.paused_until - now)
            return 0

        # ── Adverse-selection cancel ──────────────────────────────────────────
        # If order-book imbalance has drifted strongly against a standing leg,
        # informed flow is about to hit it — pull the quote before becoming
        # the exit liquidity for someone who knows more than we do.
        actions_cancelled = 0
        if state.yes_order_id and book.obi <= -ADVERSE_OBI_THRESHOLD:
            log.info(
                "MM ADVERSE (YES) | %s  obi=%+.3f — pulling bid",
                mkt.question[:40], book.obi,
            )
            if state.yes_order_id != "DRY_RUN":
                ok = await asyncio.to_thread(self._ex.cancel_order, state.yes_order_id)
                if ok:
                    actions_cancelled += 1
            state.yes_order_id    = None
            state.yes_seen_filled = 0.0
        if state.no_order_id and book.obi >= ADVERSE_OBI_THRESHOLD:
            log.info(
                "MM ADVERSE (NO)  | %s  obi=%+.3f — pulling ask",
                mkt.question[:40], book.obi,
            )
            if state.no_order_id != "DRY_RUN":
                ok = await asyncio.to_thread(self._ex.cancel_order, state.no_order_id)
                if ok:
                    actions_cancelled += 1
            state.no_order_id    = None
            state.no_seen_filled = 0.0

        # ── Mid range guard ───────────────────────────────────────────────────
        if not (MIN_MID_FOR_MM <= mid <= MAX_MID_FOR_MM):
            log.debug("MM SKIP | %s  mid=%.4f outside range", mkt.question[:40], mid)
            return 0

        # Don't quote into an already-tight spread (another MM is better positioned)
        if book.spread_cents is not None and book.spread_cents < MIN_SPREAD_TO_QUOTE * 100:
            log.debug("MM SKIP | %s  spread %.3fc already tight", mkt.question[:40],
                      book.spread_cents)
            return 0

        # ── Inventory guard ───────────────────────────────────────────────────
        # YES and NO tokens are complementary (YES+NO pays $1 at any resolution),
        # so equal yes/no inventory is a perfect hedge — only the unhedged delta
        # carries directional risk. Price that delta at the side we're long.
        net_delta = state.yes_inventory - state.no_inventory
        imbalance_usdc = abs(net_delta) * (mid if net_delta >= 0 else (1 - mid))
        if imbalance_usdc > MAX_INVENTORY_USDC:
            log.info(
                "MM INVENTORY LIMIT | %s  net_delta=%+.1f imbalance=$%.2f — pausing quotes",
                mkt.question[:40], net_delta, imbalance_usdc,
            )
            return await self._cancel_quotes(mkt, state)

        # ── Decide if requote is needed ───────────────────────────────────────
        has_quotes   = state.yes_order_id or state.no_order_id
        mid_drifted  = (state.quote_mid is None or
                        abs(mid - state.quote_mid) >= REQUOTE_TICK)
        need_requote = (not has_quotes) or mid_drifted

        if not need_requote:
            return actions_cancelled

        # ── Cancel stale quotes ───────────────────────────────────────────────
        cancelled = await self._cancel_quotes(mkt, state) + actions_cancelled

        # ── Compute skewed quotes ─────────────────────────────────────────────
        bid_price, no_bid_price = self._compute_quotes(mid, state)

        log.info(
            "MM QUOTE | %s  bid=%.4f  ask=%.4f  inv(yes=%.1f no=%.1f)",
            mkt.question[:45], bid_price, 1 - no_bid_price,
            state.yes_inventory, state.no_inventory,
        )

        actions = cancelled

        # ── Place BUY YES (bid leg) ───────────────────────────────────────────
        yes_result: OrderResult = await asyncio.to_thread(
            self._ex.place_limit_order,
            mkt.yes_token_id,
            OrderSide.BUY,
            bid_price,       # true_prob = our quote price (gate bypassed)
            self._size,
            cached_balance = balance,
            price_override = bid_price,
            bypass_gate    = True,
        )
        if yes_result.success:
            state.yes_order_id = yes_result.order_id
            state.quote_mid    = mid
            actions += 1
        else:
            log.debug("MM bid failed: %s", yes_result.error)

        # ── Place BUY NO (ask leg = 1 − ask_price) ───────────────────────────
        no_result: OrderResult = await asyncio.to_thread(
            self._ex.place_limit_order,
            mkt.no_token_id,
            OrderSide.BUY,
            no_bid_price,    # NO-token buy price
            self._size,
            cached_balance = balance,
            price_override = no_bid_price,
            bypass_gate    = True,
        )
        if no_result.success:
            state.no_order_id = no_result.order_id
            actions += 1
        else:
            log.debug("MM ask failed: %s", no_result.error)

        return actions

    def _compute_quotes(
        self,
        mid:   float,
        state: MMState,
    ) -> tuple[float, float]:
        """
        Return (yes_bid_price, no_bid_price) after applying inventory skew.

        Inventory skew: if we hold excess YES, shift quoted mid downward so
        we buy less YES and more NO (rebalancing the book).
        skew = (yes_inventory − no_inventory) / target_inventory × SKEW_FACTOR × mid
        """
        yes_imbalance = state.yes_inventory - state.no_inventory
        # Positive imbalance → long YES → shift mid down to reduce YES buying
        skew = yes_imbalance / max(1.0, state.yes_inventory + state.no_inventory + 1) \
               * SKEW_FACTOR * mid
        skewed_mid = max(0.05, min(0.95, mid - skew))

        yes_bid  = round(max(0.01, skewed_mid - self._hs), 4)
        no_bid   = round(max(0.01, (1 - skewed_mid) - self._hs), 4)
        return yes_bid, no_bid

    def _circuit_breaker_triggered(self, state: MMState) -> bool:
        """True if mid has moved more than CIRCUIT_BREAKER_PCT in the window."""
        if len(state.mid_history) < 2:
            return False
        oldest_mid = state.mid_history[0][1]
        newest_mid = state.mid_history[-1][1]
        if oldest_mid == 0:
            return False
        move = abs(newest_mid - oldest_mid) / oldest_mid
        return move >= CIRCUIT_BREAKER_PCT

    async def _cancel_quotes(self, mkt: PolyMarket, state: MMState) -> int:
        """Cancel both legs for a market. Returns number of cancels issued."""
        cancelled = 0
        for order_id in (state.yes_order_id, state.no_order_id):
            if order_id and order_id != "DRY_RUN":
                ok = await asyncio.to_thread(self._ex.cancel_order, order_id)
                if ok:
                    cancelled += 1
        # Reset both the active id and the per-order matched-qty baseline.
        # The next order placed against this state will be a fresh order
        # whose size_matched starts at 0; if seen_filled stayed at the old
        # value, _poll_fills would compute fresh = matched_new - seen_old
        # and silently drop fills until matched climbed past the stale
        # baseline.
        state.yes_order_id    = None
        state.no_order_id     = None
        state.yes_seen_filled = 0.0
        state.no_seen_filled  = 0.0
        return cancelled
