"""
main_v2.py
SexyBot V2 — Full integration: Discovery → WebSocket → Gate → Execute

Run:  python3 main_v2.py
Env:  PRIVATE_KEY, DRY_RUN, MAX_ORDER_SIZE, MIN_LIQUIDITY,
      MIN_VOLUME, SPREAD_MAX_CENTS, SCAN_INTERVAL
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import requests

from discovery import MarketFilter, PolyMarket, fetch_markets
from executor import ClobExecutor
from orderbook_ws import BookManager, BookSnapshot
from redeemer import PositionRedeemer
from risk import BalanceInfo, ExecutionGate
from signing import OrderSide

log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

PRIVATE_KEY      = os.environ["PRIVATE_KEY"]
FUNDER_ADDRESS   = os.getenv("POLYMARKET_FUNDER")   # proxy wallet holding USDC.e; None = EOA mode
MIN_LIQUIDITY    = float(os.getenv("MIN_LIQUIDITY",    "10000"))
MIN_VOLUME_24H   = float(os.getenv("MIN_VOLUME",       "1000"))
MAX_ORDER_SIZE   = float(os.getenv("MAX_ORDER_SIZE",   "10"))
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() == "true"
SPREAD_MAX_CENTS = float(os.getenv("SPREAD_MAX_CENTS", "0.5"))
SCAN_INTERVAL    = float(os.getenv("SCAN_INTERVAL",    "30"))

CLOB_BASE       = "https://clob.polymarket.com"
REQUEST_TIMEOUT = 8

# Momentum thresholds
SPIKE_WINDOW_SEC = 60     # look-back window for recent trades
SPIKE_RATIO_MIN  = 2.5    # recent rate must be 2.5× the baseline
OBI_CONFIRM_MIN  = 0.20   # OBI must agree with signal direction
MIN_EDGE         = 0.03   # minimum probability edge to place a trade

# Position management
TRADE_COOLDOWN_SEC    = 300   # seconds before re-buying the same token
MARKET_REFRESH_CYCLES = 20   # re-discover markets every N scan cycles
PROFIT_TARGET         = float(os.getenv("PROFIT_TARGET", "0.08"))   # 8% gain → close
STOP_LOSS             = float(os.getenv("STOP_LOSS",     "0.05"))   # 5% loss → close


@dataclass
class Position:
    token_id:   str
    entry_price: float   # price per outcome token at fill time
    token_qty:   float   # outcome tokens held
    entry_time:  float   # unix timestamp


# ── Momentum signal ────────────────────────────────────────────────────────────

def _get_recent_trades_sync(token_id: str) -> list[dict]:
    """Synchronous trade fetch — always call via asyncio.to_thread()."""
    try:
        resp = requests.get(
            f"{CLOB_BASE}/trades",
            params={"asset_id": token_id, "limit": 100},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])
    except Exception:
        return []


async def _get_recent_trades(token_id: str) -> list[dict]:
    """
    Fetch recent CLOB trades without blocking the event loop.
    Uses asyncio.to_thread so WebSocket processing continues while waiting.
    """
    return await asyncio.to_thread(_get_recent_trades_sync, token_id)


def _detect_volume_spike(trades: list[dict]) -> dict:
    """
    Detect a directional volume spike in recent CLOB trades.
    Returns: {has_spike, dominant_side, confidence, spike_ratio}
    """
    if len(trades) < 5:
        return {"has_spike": False, "dominant_side": None, "confidence": 0.0, "spike_ratio": 0.0}

    now = time.time()
    recent: list[dict] = []
    baseline: list[dict] = []

    for t in trades:
        try:
            ts = float(t.get("timestamp", t.get("created_at", 0)))
            if ts > 1e12:
                ts /= 1000      # milliseconds → seconds
            age = now - ts
            if age <= SPIKE_WINDOW_SEC:
                recent.append(t)
            elif age <= SPIKE_WINDOW_SEC * 5:
                baseline.append(t)
        except (ValueError, TypeError):
            continue

    if not recent:
        return {"has_spike": False, "dominant_side": None, "confidence": 0.0, "spike_ratio": 0.0}

    baseline_rate = max(len(baseline) / (SPIKE_WINDOW_SEC * 4), 0.1)
    recent_rate   = len(recent) / SPIKE_WINDOW_SEC
    spike_ratio   = recent_rate / baseline_rate
    has_spike     = spike_ratio >= SPIKE_RATIO_MIN

    yes_vol = sum(float(t.get("size", 0)) for t in recent
                  if t.get("side", "").upper() in ("BUY", "YES"))
    no_vol  = sum(float(t.get("size", 0)) for t in recent
                  if t.get("side", "").upper() in ("SELL", "NO"))
    total   = (yes_vol + no_vol) or 1.0

    yes_pct = yes_vol / total
    if yes_pct >= 0.60:
        dominant_side = "YES"
        side_conf     = yes_pct * 100
    elif yes_pct <= 0.40:
        dominant_side = "NO"
        side_conf     = (1 - yes_pct) * 100
    else:
        dominant_side = None
        side_conf     = 50.0

    confidence = round(min(side_conf * (spike_ratio / 3.0), 85.0), 1) if has_spike else 0.0

    return {
        "has_spike":     has_spike,
        "dominant_side": dominant_side,
        "confidence":    confidence,
        "spike_ratio":   round(spike_ratio, 2),
    }


async def estimate_true_probability(
    market: PolyMarket,
    book:   BookSnapshot,
) -> Optional[tuple[float, OrderSide]]:
    """
    Momentum + OBI signal model. Returns (true_prob, side) or None.

    Logic:
      1. Fetch recent CLOB trades asynchronously
      2. Detect volume spike; fall back to pure OBI if no spike
      3. OBI must confirm (not oppose) the trade direction
      4. Edge = |estimated_prob − execution_price| must exceed MIN_EDGE
    """
    if book.mid is None:
        return None

    yes_price = book.mid

    # 1. Volume spike (non-blocking)
    trades = await _get_recent_trades(book.token_id)
    spike  = _detect_volume_spike(trades)

    if not spike["has_spike"] or spike["dominant_side"] is None:
        # Fall back to OBI-only signal
        obi = book.obi
        if abs(obi) < 0.35:
            return None
        dominant_side = "YES" if obi > 0 else "NO"
        confidence    = abs(obi) * 50    # maps ±0.35–1.0 → 17.5–50%
    else:
        dominant_side = spike["dominant_side"]
        confidence    = spike["confidence"]

    # 2. OBI must not oppose the signal direction
    obi = book.obi
    if dominant_side == "YES" and obi < -OBI_CONFIRM_MIN:
        log.debug("OBI opposes YES trade (obi=%.3f) — skipping %s", obi, market.question[:40])
        return None
    if dominant_side == "NO" and obi > OBI_CONFIRM_MIN:
        log.debug("OBI opposes NO trade (obi=%.3f) — skipping %s", obi, market.question[:40])
        return None

    # 3. Estimate true probability
    conf_boost = (confidence / 100.0) * 0.12

    if dominant_side == "YES":
        true_prob = min(yes_price + conf_boost, 0.97)
        side      = OrderSide.BUY
        edge      = true_prob - (book.best_ask or yes_price)
    else:
        true_prob = max(yes_price - conf_boost, 0.03)
        side      = OrderSide.SELL
        edge      = (book.best_bid or yes_price) - true_prob

    if edge < MIN_EDGE:
        return None

    log.info(
        "SIGNAL | %s  side=%s  prob=%.3f  mid=%.3f  edge=%.3f  obi=%+.3f  conf=%.1f%%",
        market.question[:55], dominant_side, true_prob, yes_price, edge, obi, confidence,
    )
    return true_prob, side


# ── Strategy loop ──────────────────────────────────────────────────────────────

async def strategy_loop(
    executor:       ClobExecutor,
    book_manager:   BookManager,
    markets:        list[PolyMarket],
    last_traded:    dict[str, float],      # persisted across restarts by caller
    cycle_count:    list[int],             # [0] mutable int, persisted across restarts
    open_positions: dict[str, "Position"], # token_id → Position; persisted across restarts
    redeemer:       Optional[PositionRedeemer] = None,
) -> None:
    # Give WebSocket connections time to receive initial snapshots for all tokens
    log.info("Waiting 10s for WebSocket order books to populate…")
    await asyncio.sleep(10)

    # Build the set of token IDs that have active WS subscriptions (fixed at startup).
    # Used to filter out new-market results from periodic refreshes that have no WS data.
    subscribed_yes_ids: set[str] = {mkt.yes_token_id for mkt in markets}

    while True:
        cycle_count[0] += 1
        log.info("── Scanning %d markets (cycle %d) ──", len(markets), cycle_count[0])

        # Periodically re-discover markets so closed/resolved ones drop off.
        # Only keep markets whose tokens are already subscribed — new tokens can't
        # receive WS data since BookManager subscriptions are fixed at startup.
        if cycle_count[0] % MARKET_REFRESH_CYCLES == 0:
            try:
                fresh_raw = fetch_markets(
                    min_liquidity=MIN_LIQUIDITY, min_volume=MIN_VOLUME_24H, max_pages=3
                )
                fresh = (
                    MarketFilter(fresh_raw)
                    .max_spread_cents(3.0)
                    .price_range(0.05, 0.95)
                    .top(20, key="volume_24h")
                    .results()
                )
                # Drop any new markets — only keep already-subscribed ones
                still_active = [m for m in fresh if m.yes_token_id in subscribed_yes_ids]
                if still_active:
                    markets = still_active
                log.info(
                    "Markets refreshed — %d/%d still active",
                    len(markets), len(subscribed_yes_ids),
                )
            except Exception as exc:
                log.warning("Market refresh failed (keeping old list): %s", exc)

        # Fetch balance once per cycle via thread so the event loop stays free
        try:
            cycle_balance: Optional[BalanceInfo] = await asyncio.to_thread(executor.get_balance)
        except Exception as exc:
            log.error("Balance fetch failed: %s — skipping cycle", exc)
            await asyncio.sleep(SCAN_INTERVAL)
            continue

        # ── Claim resolved positions / merge back-to-back positions ────────────
        if redeemer is not None:
            try:
                claimed = await asyncio.to_thread(redeemer.run_once)
                if claimed:
                    log.info("REDEEMER: %d on-chain transaction(s) sent", claimed)
            except Exception as exc:
                log.warning("REDEEMER error (non-fatal): %s", exc)

        # ── Exit open positions that hit profit target or stop-loss ─────────────
        for token_id, pos in list(open_positions.items()):
            book = book_manager.get_book(token_id)
            if book is None or book.is_stale or book.best_bid is None:
                continue
            gain = (book.best_bid - pos.entry_price) / pos.entry_price
            if gain >= PROFIT_TARGET:
                reason = f"profit {gain * 100:.1f}%"
            elif gain <= -STOP_LOSS:
                reason = f"stop-loss {gain * 100:.1f}%"
            else:
                continue

            log.info(
                "EXIT | %s  reason=%s  entry=%.4f  bid=%.4f  qty=%.4f",
                token_id[:14], reason, pos.entry_price, book.best_bid, pos.token_qty,
            )
            try:
                exit_result = await asyncio.to_thread(
                    executor.close_position, token_id, pos.token_qty, reason=reason
                )
            except Exception as exc:
                log.error("close_position error for %s: %s", token_id[:14], exc)
                continue

            if exit_result:
                del open_positions[token_id]
                last_traded[token_id] = time.time()   # cooldown after exit
                log.info(
                    "EXITED | %s  reason=%s  id=%s  status=%s",
                    token_id[:14], reason, exit_result.order_id, exit_result.status,
                )

        # Collect live books — skip markets with an open position (no stacking)
        live: list[tuple[PolyMarket, BookSnapshot]] = []
        for mkt in markets:
            if mkt.yes_token_id in open_positions or mkt.no_token_id in open_positions:
                log.debug("SKIP (position held): %s", mkt.question[:40])
                continue
            yes_book = book_manager.get_book(mkt.yes_token_id)
            if yes_book is None or yes_book.is_stale:
                log.debug("No live book for %s", mkt.question[:40])
                continue
            live.append((mkt, yes_book))

        # Run all signal estimates concurrently (each fires one HTTP request)
        signal_tasks = [estimate_true_probability(mkt, book) for mkt, book in live]
        signal_results = await asyncio.gather(*signal_tasks, return_exceptions=True)

        for (mkt, _yes_book), result_pair in zip(live, signal_results):
            if isinstance(result_pair, Exception):
                log.error("Signal error for %s: %s", mkt.question[:40], result_pair)
                continue
            if result_pair is None:
                continue

            true_prob, side = result_pair

            # On Polymarket you can only BUY tokens you don't yet own.
            # A bearish (NO) signal means BUY NO tokens, not SELL YES tokens.
            # Map: SELL YES → BUY NO using the complementary token_id.
            if side == OrderSide.SELL:
                trade_token_id = mkt.no_token_id
                trade_side     = OrderSide.BUY
                trade_prob     = 1.0 - true_prob   # flip to NO perspective
            else:
                trade_token_id = mkt.yes_token_id
                trade_side     = side
                trade_prob     = true_prob

            # Cooldown: skip if this token was traded recently
            now = time.time()
            last = last_traded.get(trade_token_id, 0.0)
            if now - last < TRADE_COOLDOWN_SEC:
                log.debug(
                    "COOLDOWN %ds remaining for %s",
                    int(TRADE_COOLDOWN_SEC - (now - last)), mkt.question[:40],
                )
                continue

            try:
                result = await asyncio.to_thread(
                    executor.place_limit_order,
                    trade_token_id,
                    trade_side,
                    trade_prob,
                    MAX_ORDER_SIZE,
                    cached_balance=cycle_balance,
                )
            except Exception as exc:
                log.error("Order error for %s: %s", mkt.question[:40], exc)
                continue

            if result:
                last_traded[trade_token_id] = time.time()
                # Deduct spend from cached balance so the next order this cycle
                # sees the reduced figure and cannot breach the $10 reserve.
                if cycle_balance is not None and result.fill_price and result.token_qty:
                    spent_raw = int(result.fill_price * result.token_qty * 1_000_000)
                    cycle_balance = BalanceInfo(
                        balance_raw   = max(0, cycle_balance.balance_raw - spent_raw),
                        allowance_raw = cycle_balance.allowance_raw,
                    )
                # Track position for profit-taking / stop-loss
                if result.fill_price is not None and result.token_qty is not None:
                    open_positions[trade_token_id] = Position(
                        token_id    = trade_token_id,
                        entry_price = result.fill_price,
                        token_qty   = result.token_qty,
                        entry_time  = time.time(),
                    )
                log.info(
                    "EXECUTED | %s  id=%s  status=%s  entry=%.4f  qty=%.4f",
                    mkt.question[:55], result.order_id, result.status,
                    result.fill_price or 0, result.token_qty or 0,
                )

        await asyncio.sleep(SCAN_INTERVAL)


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info(
        "SexyBot V2 starting | DRY_RUN=%s MAX_ORDER=$%.2f FUNDER=%s",
        DRY_RUN, MAX_ORDER_SIZE, FUNDER_ADDRESS or "(EOA/self)",
    )

    # 1. Discover markets
    log.info("Fetching markets from Gamma API…")
    raw = fetch_markets(min_liquidity=MIN_LIQUIDITY, min_volume=MIN_VOLUME_24H, max_pages=3)
    markets = (
        MarketFilter(raw)
        .max_spread_cents(3.0)
        .price_range(0.05, 0.95)
        .top(20, key="volume_24h")
        .results()
    )
    log.info("Watching %d markets", len(markets))

    if not markets:
        log.error("No tradeable markets found — exiting")
        return

    # 2. WebSocket — all tokens batched into minimal connections
    book_manager = BookManager()
    for mkt in markets:
        book_manager.add_market(mkt.yes_token_id, mkt.no_token_id)

    # 3. Executor
    executor = ClobExecutor(
        private_key    = PRIVATE_KEY,
        book_manager   = book_manager,
        gate           = ExecutionGate(spread_max_cents=SPREAD_MAX_CENTS),
        dry_run        = DRY_RUN,
        funder_address = FUNDER_ADDRESS,
    )

    # 3b. Position redeemer (only when a proxy/funder wallet is configured)
    redeemer: Optional[PositionRedeemer] = None
    if FUNDER_ADDRESS:
        redeemer = PositionRedeemer(
            private_key    = PRIVATE_KEY,
            safe_address   = FUNDER_ADDRESS,
            signer_address = executor._client.get_address(),
            dry_run        = DRY_RUN,
        )
        log.info("PositionRedeemer ready | safe=%s", FUNDER_ADDRESS)

    # 4. Run WebSocket + strategy concurrently
    # strategy_loop is isolated — an exception there won't cancel book_manager.
    # last_traded and cycle_count live here so they survive strategy_loop restarts.
    async def _strategy_with_restart() -> None:
        last_traded:    dict[str, float]    = {}
        open_positions: dict[str, Position] = {}
        cycle_count:    list[int]           = [0]   # mutable box
        while True:
            try:
                await strategy_loop(
                    executor, book_manager, markets,
                    last_traded, cycle_count, open_positions,
                    redeemer=redeemer,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("strategy_loop crashed, restarting in 5s: %s", exc)
                await asyncio.sleep(5)

    await asyncio.gather(
        book_manager.run(),
        _strategy_with_restart(),
    )


if __name__ == "__main__":
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s %(levelname)-8s %(message)s",
    )
    asyncio.run(main())
