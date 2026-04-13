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
from typing import Optional

import requests

from discovery import MarketFilter, PolyMarket, fetch_markets
from executor import ClobExecutor
from orderbook_ws import BookManager, BookSnapshot
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
    executor:     ClobExecutor,
    book_manager: BookManager,
    markets:      list[PolyMarket],
) -> None:
    # Give WebSocket connections time to receive initial snapshots for all tokens
    log.info("Waiting 10s for WebSocket order books to populate…")
    await asyncio.sleep(10)

    while True:
        log.info("── Scanning %d markets ──", len(markets))

        # Fetch balance once per cycle — not once per signal
        try:
            cycle_balance: Optional[BalanceInfo] = executor.get_balance()
        except Exception as exc:
            log.error("Balance fetch failed: %s — skipping cycle", exc)
            await asyncio.sleep(SCAN_INTERVAL)
            continue

        for mkt in markets:
            yes_book = book_manager.get_book(mkt.yes_token_id)
            if yes_book is None or yes_book.is_stale:
                log.debug("No live book for %s", mkt.question[:40])
                continue

            # estimate_true_probability is now async (trades fetched non-blocking)
            try:
                result_pair = await estimate_true_probability(mkt, yes_book)
            except Exception as exc:
                log.error("Signal error for %s: %s", mkt.question[:40], exc)
                continue

            if result_pair is None:
                continue

            true_prob, side = result_pair

            try:
                result = executor.place_limit_order(
                    token_id       = mkt.yes_token_id,
                    side           = side,
                    true_prob      = true_prob,
                    size_pmusd     = MAX_ORDER_SIZE,
                    cached_balance = cycle_balance,
                )
            except Exception as exc:
                log.error("Order error for %s: %s", mkt.question[:40], exc)
                continue

            if result:
                log.info(
                    "EXECUTED | %s  id=%s  status=%s",
                    mkt.question[:55], result.order_id, result.status,
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

    # 4. Run WebSocket + strategy concurrently
    # strategy_loop is isolated — an exception there won't cancel book_manager
    async def _strategy_with_restart() -> None:
        while True:
            try:
                await strategy_loop(executor, book_manager, markets)
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
