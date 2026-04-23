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
from datetime import datetime, timezone
from typing import Optional

import requests
from pydantic import BaseModel, field_validator

from calibrator import Calibrator, RegimeReader, RegimeState, record_prediction
from discovery import MarketFilter, PolyMarket, fetch_markets
from executor import ClobExecutor
from market_maker import MarketMaker
from orderbook_ws import BookManager, BookSnapshot
from redeemer import PositionRedeemer
from risk import (
    BalanceErrorCircuitBreaker,
    BalanceInfo,
    DrawdownGuard,
    DrawdownHalt,
    ExecutionGate,
    MarketRegime,
    dynamic_exit_levels,
    kelly_size,
)
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
# OBI-only fallback is statistically weaker than a trade-volume spike, so
# require a MUCH larger imbalance before trading on it alone. This blocks
# the quiet-market false positives the 0.35 threshold used to allow.
OBI_SOLO_MIN     = float(os.getenv("OBI_SOLO_MIN", "0.55"))
MIN_EDGE         = 0.03   # minimum probability edge to place a trade
MIN_BOOK_DEPTH_USDC = float(os.getenv("MIN_BOOK_DEPTH_USDC", "200"))  # skip threadbare books

# Position management
TRADE_COOLDOWN_SEC    = 300   # seconds before re-buying the same token
MARKET_REFRESH_CYCLES = 20   # re-discover markets every N scan cycles
PROFIT_TARGET         = float(os.getenv("PROFIT_TARGET", "0.08"))   # 8% gain → close (base; dynamic)
STOP_LOSS             = float(os.getenv("STOP_LOSS",     "0.05"))   # 5% loss → close (base; dynamic)
KELLY_FRACTION        = float(os.getenv("KELLY_FRACTION", "0.25"))  # Quarter Kelly
# Time-based exit: if a position doesn't hit profit/stop within this window,
# close at current bid rather than continuing to hold dead inventory. Helps
# recycle capital into fresher signals and caps "slow bleed" losses that
# never quite trigger a stop.
MAX_HOLD_SEC          = int(os.getenv("MAX_HOLD_SEC", "3600"))   # 1 h default
# Below this absolute (not relative) loss, the time-stop will exit even if
# the standard percentage stop hasn't triggered. Prevents being stuck in a
# chronically drifting-lower position.
TIME_STOP_MIN_GAIN    = float(os.getenv("TIME_STOP_MIN_GAIN", "-0.01"))
# Comma-separated list of category substrings to skip at discovery. Defaults
# to 'crypto' per the 2026-04-22 backtest recommendation (short-horizon crypto
# price markets; 0% resolved win rate and structurally efficient). Override
# with empty string to re-enable.
EXCLUDE_CATEGORIES    = [
    c.strip() for c in os.getenv("EXCLUDE_CATEGORIES", "crypto").split(",") if c.strip()
]

# Strategy selection
# STRATEGY=momentum  (default) — directional momentum + OBI signals
# STRATEGY=market_making       — two-sided quotes, spread capture, liquidity rewards
STRATEGY      = os.getenv("STRATEGY", "momentum")
MM_HALF_SPREAD = float(os.getenv("MM_HALF_SPREAD", "0.02"))   # half-spread in prob units
MM_ORDER_SIZE  = float(os.getenv("MM_ORDER_SIZE",  "5.0"))    # USDC per side per quote

# Drawdown kill-switch — inherited from risk.py env vars but logged here for clarity
# MAX_DRAWDOWN_USD (default $50): halt if balance drops this much from recent peak
# DRAWDOWN_WINDOW  (default 600s): rolling window for peak measurement

# Brier-score calibration and regime wiring
# CALIBRATOR_ENABLED    — apply learned bias correction to true_prob (default on)
# CALIBRATION_DB_PATH   — overridable in calibrator.py; default = ./trades.db
# REGIME_RESPECT        — if true, main_v2 respects bot.py's regime_log decisions:
#                          normal   → no change
#                          cautious → Kelly × 0.5, skip OBI-only tier
#                          hostile  → skip momentum entirely this cycle
CALIBRATOR_ENABLED    = os.getenv("CALIBRATOR_ENABLED", "true").lower() == "true"
CALIBRATION_SOURCE    = os.getenv("CALIBRATION_SOURCE", "momentum_v2")
REGIME_RESPECT        = os.getenv("REGIME_RESPECT", "true").lower() == "true"


# ── Signal schema ──────────────────────────────────────────────────────────────

class VolumeSpike(BaseModel):
    """
    Strictly-typed momentum signal from CLOB trade data.
    Replaces the plain-dict return so callers get type safety and
    pydantic's runtime validation catches malformed data early.
    """
    has_spike:     bool
    dominant_side: Optional[str]   = None   # "YES" | "NO" | None
    confidence:    float           = 0.0    # 0–85 %
    spike_ratio:   float           = 0.0    # recent_rate / baseline_rate

    @field_validator("dominant_side")
    @classmethod
    def _validate_side(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in ("YES", "NO"):
            raise ValueError(f"dominant_side must be 'YES', 'NO', or None, got {v!r}")
        return v

    @field_validator("confidence", "spike_ratio")
    @classmethod
    def _non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("confidence and spike_ratio must be >= 0")
        return v


@dataclass
class Position:
    token_id:       str
    entry_price:    float   # price per outcome token at fill time
    token_qty:      float   # outcome tokens held
    entry_time:     float   # unix timestamp
    # Exit bands computed once at entry using entry regime + price. Held as
    # positive fractions of entry_price (e.g. 0.12 = 12% profit target).
    profit_target:  float = PROFIT_TARGET
    stop_loss:      float = STOP_LOSS


@dataclass
class Signal:
    """
    Tiered momentum signal — richer than a bare (prob, side) tuple.

    `strength` ∈ [0, 1] is a conviction scalar used to shrink Kelly on weaker
    signals (pure-OBI plays get ~0.3; a spike + OBI-aligned play gets ~1.0).
    """
    true_prob: float
    side:      OrderSide
    strength:  float
    source:    str   # "spike+obi" | "spike" | "obi"


def _hours_to_resolution(end_date: str) -> Optional[float]:
    """Parse Gamma API end_date → hours from now. Returns None on malformed input."""
    if not end_date:
        return None
    try:
        iso = end_date.replace("Z", "+00:00")
        dt  = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(tz=timezone.utc)).total_seconds() / 3600.0
        return round(delta, 2)
    except (ValueError, TypeError):
        return None


def _build_regime(market: PolyMarket, book: BookSnapshot) -> MarketRegime:
    return MarketRegime(
        book_depth_usdc          = book.book_depth_usdc,
        mid_volatility           = book.mid_volatility,
        spread_cents             = book.spread_cents,
        time_to_resolution_hours = _hours_to_resolution(market.end_date),
    )


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
    except Exception as exc:
        log.warning("CLOB trade API error for %s: %s", token_id[:14], exc)
        return []


async def _get_recent_trades(token_id: str) -> list[dict]:
    """
    Fetch recent CLOB trades without blocking the event loop.
    Uses asyncio.to_thread so WebSocket processing continues while waiting.
    """
    return await asyncio.to_thread(_get_recent_trades_sync, token_id)


def _detect_volume_spike(trades: list[dict]) -> VolumeSpike:
    """
    Detect a directional volume spike in recent CLOB trades.
    Returns a validated VolumeSpike model (replaces plain dict).
    """
    _null = VolumeSpike(has_spike=False)

    if len(trades) < 5:
        return _null

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
        return _null

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
        dominant_side: Optional[str] = "YES"
        side_conf     = yes_pct * 100
    elif yes_pct <= 0.40:
        dominant_side = "NO"
        side_conf     = (1 - yes_pct) * 100
    else:
        dominant_side = None
        side_conf     = 50.0

    confidence = round(min(side_conf * (spike_ratio / 3.0), 85.0), 1) if has_spike else 0.0

    return VolumeSpike(
        has_spike     = has_spike,
        dominant_side = dominant_side,
        confidence    = confidence,
        spike_ratio   = round(spike_ratio, 2),
    )


async def estimate_true_probability(
    market:     PolyMarket,
    book:       BookSnapshot,
    calibrator: Optional[Calibrator] = None,
) -> Optional[Signal]:
    """
    Tiered momentum + OBI signal model.

    Pipeline:
      1. Guard: require a live VAMP (so both book sides have real volume) and
         a minimum top-5 depth — thin / phantom books produce unreliable signals.
      2. Fetch recent CLOB trades asynchronously.
      3. Classify into tier:
           spike+obi — highest conviction (strength → 1.0)
           spike     — volume-spike with weak/non-aligned OBI (strength → 0.6)
           obi       — pure imbalance fallback, requires |OBI| ≥ OBI_SOLO_MIN (strength → 0.3)
      4. OBI must not actively oppose the trade direction.
      5. Edge = |estimated_prob − execution_price| must exceed MIN_EDGE.
    """
    # 1. Fair value — VAMP only; don't fall back to a bare mid from a phantom book
    yes_price = book.vamp
    if yes_price is None:
        return None

    # Thin books → skip. Liquidity-starved markets produce erratic signals and
    # can't absorb even a $1 Kelly order without slippage blowing the edge.
    if book.book_depth_usdc is None or book.book_depth_usdc < MIN_BOOK_DEPTH_USDC:
        log.debug("DEPTH SKIP | %s  depth=%s < $%.0f",
                  market.question[:40],
                  f"${book.book_depth_usdc:.0f}" if book.book_depth_usdc else "None",
                  MIN_BOOK_DEPTH_USDC)
        return None

    # 2. Volume spike (non-blocking)
    trades = await _get_recent_trades(book.token_id)
    spike  = _detect_volume_spike(trades)

    obi = book.obi

    # 3. Classify tier
    if spike.has_spike and spike.dominant_side is not None:
        dominant_side = spike.dominant_side
        # OBI alignment: positive OBI supports YES, negative supports NO.
        obi_aligned = (
            (dominant_side == "YES" and obi >=  OBI_CONFIRM_MIN) or
            (dominant_side == "NO"  and obi <= -OBI_CONFIRM_MIN)
        )
        if obi_aligned:
            source   = "spike+obi"
            strength = min(1.0, spike.confidence / 85.0)   # 85% conf → strength 1.0
        else:
            source   = "spike"
            strength = min(0.6, spike.confidence / 85.0 * 0.6)
        confidence = spike.confidence
    else:
        # Pure-OBI fallback — tightest threshold, smallest sizing.
        if abs(obi) < OBI_SOLO_MIN:
            return None
        dominant_side = "YES" if obi > 0 else "NO"
        source        = "obi"
        # Map |obi| from [OBI_SOLO_MIN, 1.0] → strength [0.15, 0.30].
        strength      = 0.15 + 0.15 * (abs(obi) - OBI_SOLO_MIN) / max(1e-6, 1.0 - OBI_SOLO_MIN)
        confidence    = abs(obi) * 100

    # 4. OBI must not actively oppose the signal
    if dominant_side == "YES" and obi < -OBI_CONFIRM_MIN:
        log.debug("OBI opposes YES trade (obi=%+.3f) — skip %s", obi, market.question[:40])
        return None
    if dominant_side == "NO" and obi > OBI_CONFIRM_MIN:
        log.debug("OBI opposes NO trade (obi=%+.3f) — skip %s", obi, market.question[:40])
        return None

    # 5. Estimate true probability + edge check
    conf_boost = (confidence / 100.0) * 0.12

    if dominant_side == "YES":
        raw_true_prob = min(yes_price + conf_boost, 0.97)
        side          = OrderSide.BUY
    else:
        raw_true_prob = max(yes_price - conf_boost, 0.03)
        side          = OrderSide.SELL

    # Apply calibration (if enabled and we have enough resolved-prediction
    # history to have learned the model's bias). The calibrator shrinks
    # overconfident tails toward 0.5; under-confident middle predictions
    # are nudged outward. No-op when the DB / calibration data is missing.
    true_prob = calibrator.adjust(raw_true_prob) if calibrator is not None else raw_true_prob

    if side == OrderSide.BUY:
        edge = true_prob - (book.best_ask or yes_price)
    else:
        edge = (book.best_bid or yes_price) - true_prob

    if edge < MIN_EDGE:
        return None

    calibration_note = (
        f" cal={raw_true_prob:.3f}→{true_prob:.3f}"
        if calibrator is not None and abs(raw_true_prob - true_prob) > 1e-4
        else ""
    )
    log.info(
        "SIGNAL | %s  side=%s  prob=%.3f  vamp=%.3f  edge=%.3f  obi=%+.3f  src=%s  str=%.2f%s",
        market.question[:55], dominant_side, true_prob, yes_price,
        edge, obi, source, strength, calibration_note,
    )
    return Signal(true_prob=true_prob, side=side, strength=strength, source=source)


# ── Strategy loop ──────────────────────────────────────────────────────────────

async def strategy_loop(
    executor:       ClobExecutor,
    book_manager:   BookManager,
    markets_box:    list[list[PolyMarket]], # [0] mutable box — persisted across restarts
    last_traded:    dict[str, float],       # persisted across restarts by caller
    cycle_count:    list[int],              # [0] mutable int, persisted across restarts
    open_positions: dict[str, "Position"],  # token_id → Position; persisted across restarts
    redeemer:       Optional[PositionRedeemer]            = None,
    market_maker:   Optional[MarketMaker]                 = None,
    drawdown_guard: Optional[DrawdownGuard]               = None,
    balance_breaker: Optional[BalanceErrorCircuitBreaker] = None,
    calibrator:     Optional[Calibrator]                  = None,
    regime_reader:  Optional[RegimeReader]                = None,
) -> None:
    # Give WebSocket connections time to receive initial snapshots for all tokens
    log.info("Waiting 10s for WebSocket order books to populate…")
    await asyncio.sleep(10)

    # Build the set of token IDs that have active WS subscriptions (fixed at startup).
    # Used to filter out new-market results from periodic refreshes that have no WS data.
    subscribed_yes_ids: set[str] = {mkt.yes_token_id for mkt in markets_box[0]}

    while True:
        cycle_count[0] += 1
        markets = markets_box[0]
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
                    .price_range(0.08, 0.92)
                    .exclude_categories(EXCLUDE_CATEGORIES)
                    .top(20, key="volume_24h")
                    .results()
                )
                # Drop any new markets — only keep already-subscribed ones
                still_active = [m for m in fresh if m.yes_token_id in subscribed_yes_ids]
                if still_active:
                    markets_box[0] = still_active
                    markets = markets_box[0]
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

        # ── Drawdown kill-switch check ────────────────────────────────────────
        # DrawdownHalt propagates up — _strategy_with_restart will NOT restart
        # after a drawdown halt (unlike normal crashes).
        if drawdown_guard is not None and cycle_balance is not None:
            drawdown_guard.record_and_check(cycle_balance.balance)

        # ── Claim resolved positions / merge back-to-back positions ────────────
        if redeemer is not None:
            try:
                claimed = await asyncio.to_thread(redeemer.run_once)
                if claimed:
                    log.info("REDEEMER: %d on-chain transaction(s) sent", claimed)
            except Exception as exc:
                log.warning("REDEEMER error (non-fatal): %s", exc)

        # ── Regime gate ─────────────────────────────────────────────────────────
        # bot.py's Claude regime detector writes to regime_log. We read the latest
        # entry here and use it to throttle or halt risk-taking.  No-op when the
        # detector is disabled or the DB is missing — behaviour then matches the
        # pre-integration baseline.
        regime_state: Optional[RegimeState] = (
            regime_reader.current() if (regime_reader is not None and REGIME_RESPECT) else None
        )
        regime_kelly_scale = 1.0
        skip_obi_only     = False
        if regime_state is not None:
            if regime_state.is_hostile:
                log.warning(
                    "REGIME HOSTILE (age=%.0fs) — skipping momentum trades this cycle: %s",
                    regime_state.age_sec, (regime_state.reasoning or "")[:120],
                )
                regime_kelly_scale = 0.0    # disables momentum entries; exits still run
            elif regime_state.is_cautious:
                regime_kelly_scale = 0.5
                skip_obi_only     = True
                log.info(
                    "REGIME CAUTIOUS — Kelly × 0.5, OBI-only signals skipped: %s",
                    (regime_state.reasoning or "")[:120],
                )
            # If bot.py supplied an explicit kelly_mult, honour it (it may be
            # tighter than our rule-based scaling).
            if regime_state.kelly_mult is not None:
                regime_kelly_scale = min(regime_kelly_scale, float(regime_state.kelly_mult))

        # ── Market Making: two-sided quoting + circuit breaker ──────────────────
        if market_maker is not None:
            try:
                mm_actions = await market_maker.run_once(markets, cycle_balance)
                if mm_actions:
                    log.info("MM: %d order action(s) this cycle", mm_actions)
            except Exception as exc:
                log.warning("MM error (non-fatal): %s", exc)
            # MM handles its own order management — skip momentum signal loop
            await asyncio.sleep(SCAN_INTERVAL)
            continue

        # ── Exit open positions that hit profit target, stop-loss, or time-stop ─
        for token_id, pos in list(open_positions.items()):
            book = book_manager.get_book(token_id)
            if book is None or book.is_stale or book.best_bid is None:
                continue
            if pos.entry_price <= 0:
                log.warning("skip exit check: %s has invalid entry_price=%s",
                            token_id[:14], pos.entry_price)
                continue
            gain    = (book.best_bid - pos.entry_price) / pos.entry_price
            held_s  = time.time() - pos.entry_time
            # Per-position exit bands (computed at entry with the entry regime).
            # Fall back to module constants for positions opened before this upgrade.
            profit_band = pos.profit_target or PROFIT_TARGET
            stop_band   = pos.stop_loss     or STOP_LOSS
            if gain >= profit_band:
                reason = f"profit {gain * 100:.1f}% (target {profit_band * 100:.1f}%)"
            elif gain <= -stop_band:
                reason = f"stop-loss {gain * 100:.1f}% (band {stop_band * 100:.1f}%)"
            elif held_s >= MAX_HOLD_SEC and gain <= TIME_STOP_MIN_GAIN:
                # Trade stalled past MAX_HOLD_SEC and isn't meaningfully winning →
                # recycle capital rather than holding dead inventory indefinitely.
                reason = f"time-stop {held_s / 60:.0f}m gain={gain * 100:+.1f}%"
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
            elif exit_result.error and "too small" in (exit_result.error or ""):
                # Position value < $1 — Polymarket won't accept the order.
                # Abandon it rather than retrying every cycle forever.
                log.warning(
                    "ABANDON | %s  value<$1  entry=%.4f  qty=%.4f  bid=%.4f",
                    token_id[:14], pos.entry_price, pos.token_qty, book.best_bid,
                )
                del open_positions[token_id]

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

        # Hostile regime — run exits above, but don't open new positions.
        if regime_kelly_scale == 0.0:
            await asyncio.sleep(SCAN_INTERVAL)
            continue

        # Run all signal estimates concurrently (each fires one HTTP request)
        signal_tasks = [
            estimate_true_probability(mkt, book, calibrator=calibrator)
            for mkt, book in live
        ]
        signal_results = await asyncio.gather(*signal_tasks, return_exceptions=True)

        for (mkt, _yes_book), result in zip(live, signal_results):
            if isinstance(result, Exception):
                log.error("Signal error for %s: %s", mkt.question[:40], result)
                continue
            if result is None:
                continue

            signal: Signal = result   # type: ignore[assignment]
            true_prob = signal.true_prob
            side      = signal.side

            # In cautious regime, drop the weakest (pure-OBI) tier entirely.
            if skip_obi_only and signal.source == "obi":
                log.debug("CAUTIOUS REGIME skipping OBI-only signal on %s",
                          mkt.question[:40])
                continue

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

            # Build regime descriptor from the book we're about to trade on.
            trade_book = book_manager.get_book(trade_token_id)
            regime     = _build_regime(mkt, trade_book) if trade_book else _build_regime(mkt, _yes_book)

            # Kelly sizing now scales by signal strength × regime multiplier, so
            # weaker / noisier signals in thinner books take smaller bets.
            trade_price = (trade_book.best_ask if trade_book and trade_book.best_ask
                           else trade_prob)
            kelly_dollars = kelly_size(
                trade_prob, trade_price,
                cycle_balance.balance if cycle_balance else MAX_ORDER_SIZE,
                # Regime scale is applied on top of the static KELLY_FRACTION so
                # a macro-cautious call from the detector further throttles sizing
                # even when book-level regime multiplier would allow more.
                kelly_fraction  = KELLY_FRACTION * regime_kelly_scale,
                max_size        = MAX_ORDER_SIZE,
                signal_strength = signal.strength,
                regime          = regime,
            )
            if kelly_dollars < 1.0:
                log.debug(
                    "KELLY SKIP | %s  kelly=$%.2f strength=%.2f regime_mult=%.2f",
                    mkt.question[:40], kelly_dollars, signal.strength, regime.multiplier,
                )
                continue

            log.info(
                "SIZING | %s  kelly=$%.2f strength=%.2f regime_mult=%.2f src=%s",
                mkt.question[:45], kelly_dollars, signal.strength,
                regime.multiplier, signal.source,
            )

            # Balance-error circuit breaker: after N failed orders on
            # "insufficient balance" errors, stop trying to place anything
            # for a cooldown. Prevents retry-storm behavior like 2026-04-09.
            if balance_breaker is not None and balance_breaker.is_tripped():
                log.warning("CIRCUIT BREAKER ACTIVE — skipping order placement this cycle")
                break

            try:
                result = await asyncio.to_thread(
                    executor.place_limit_order,
                    trade_token_id,
                    trade_side,
                    trade_prob,
                    kelly_dollars,
                    cached_balance=cycle_balance,
                )
            except Exception as exc:
                log.error("Order error for %s: %s", mkt.question[:40], exc)
                # Apply cooldown on exception too so the same market isn't
                # hammered next cycle with the same failing call.
                last_traded[trade_token_id] = time.time()
                continue

            # Cooldown on EVERY attempt (success or failure) — without this,
            # a failing order can be retried every cycle. Pre-fix this is the
            # mechanism that let April 9 fire 460 orders in 82 min.
            last_traded[trade_token_id] = time.time()

            # Feed balance errors to the circuit breaker
            if balance_breaker is not None and not result.success:
                if BalanceErrorCircuitBreaker.is_balance_error(result.error):
                    balance_breaker.record_error()

            if result:
                # Deduct spend from cached balance so the next order this cycle
                # sees the reduced figure and cannot breach the $10 reserve.
                if cycle_balance is not None and result.fill_price and result.token_qty:
                    spent_raw = int(result.fill_price * result.token_qty * 1_000_000)
                    cycle_balance = BalanceInfo(
                        balance_raw   = max(0, cycle_balance.balance_raw - spent_raw),
                        allowance_raw = cycle_balance.allowance_raw,
                    )
                # Track position for profit-taking / stop-loss.
                # Exit bands are computed at entry using the entry regime so they
                # scale with the book's volatility at the time the position was taken.
                if result.fill_price is not None and result.token_qty is not None:
                    pt, sl = dynamic_exit_levels(
                        result.fill_price,
                        base_profit = PROFIT_TARGET,
                        base_stop   = STOP_LOSS,
                        regime      = regime,
                    )
                    open_positions[trade_token_id] = Position(
                        token_id      = trade_token_id,
                        entry_price   = result.fill_price,
                        token_qty     = result.token_qty,
                        entry_time    = time.time(),
                        profit_target = pt,
                        stop_loss     = sl,
                    )
                    log.info(
                        "EXIT BANDS | %s  entry=%.4f profit=+%.1f%% stop=-%.1f%%",
                        mkt.question[:45], result.fill_price, pt * 100, sl * 100,
                    )
                    # Persist the prediction to brier_scores so the Calibrator
                    # has fresh training data for future cycles. Best-effort —
                    # a DB error here never blocks trading.
                    try:
                        await asyncio.to_thread(
                            record_prediction,
                            source         = CALIBRATION_SOURCE,
                            market         = mkt.question,
                            token_id       = trade_token_id,
                            side           = "BUY",
                            predicted_prob = trade_prob,
                            market_price   = result.fill_price,
                            kelly_fraction = KELLY_FRACTION * regime_kelly_scale,
                            kelly_size     = kelly_dollars,
                            ai_reasoning   = f"src={signal.source} strength={signal.strength:.2f}",
                        )
                    except Exception as exc:
                        log.debug("record_prediction failed: %s", exc)
                log.info(
                    "EXECUTED | %s  id=%s  status=%s  entry=%.4f  qty=%.4f",
                    mkt.question[:55], result.order_id, result.status,
                    result.fill_price or 0, result.token_qty or 0,
                )

        await asyncio.sleep(SCAN_INTERVAL)


# ── Entry point ────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info(
        "SexyBot V2 starting | strategy=%s DRY_RUN=%s MAX_ORDER=$%.2f KELLY=%.0f%% FUNDER=%s",
        STRATEGY, DRY_RUN, MAX_ORDER_SIZE, KELLY_FRACTION * 100, FUNDER_ADDRESS or "(EOA/self)",
    )
    log.info(
        "Signal gates | min_edge=%.3f obi_solo=%.2f min_depth=$%.0f  "
        "Exits | base_profit=%.1f%% base_stop=%.1f%% max_hold=%dm",
        MIN_EDGE, OBI_SOLO_MIN, MIN_BOOK_DEPTH_USDC,
        PROFIT_TARGET * 100, STOP_LOSS * 100, MAX_HOLD_SEC // 60,
    )
    log.info(
        "Learning loop | calibrator=%s source=%s regime_respect=%s",
        "on" if CALIBRATOR_ENABLED else "off",
        CALIBRATION_SOURCE, REGIME_RESPECT,
    )

    # 1. Discover markets
    log.info("Fetching markets from Gamma API…")
    raw = fetch_markets(min_liquidity=MIN_LIQUIDITY, min_volume=MIN_VOLUME_24H, max_pages=3)
    markets = (
        MarketFilter(raw)
        .max_spread_cents(3.0)
        .price_range(0.08, 0.92)
        .exclude_categories(EXCLUDE_CATEGORIES)
        .top(20, key="volume_24h")
        .results()
    )
    log.info(
        "Watching %d markets (excluding categories: %s)",
        len(markets), EXCLUDE_CATEGORIES or "none",
    )

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

    # 3c. Market Maker (when STRATEGY=market_making)
    market_maker: Optional[MarketMaker] = None
    if STRATEGY == "market_making":
        market_maker = MarketMaker(
            executor,
            book_manager,
            half_spread = MM_HALF_SPREAD,
            order_size  = MM_ORDER_SIZE,
        )
        log.info(
            "MarketMaker ready | half_spread=%.3f order_size=$%.2f",
            MM_HALF_SPREAD, MM_ORDER_SIZE,
        )

    # 4. Run WebSocket + strategy concurrently
    # strategy_loop is isolated — an exception there won't cancel book_manager.
    # last_traded and cycle_count live here so they survive strategy_loop restarts.
    # Calibrator + regime reader are lazy-safe: they silently no-op when
    # trades.db is missing, which is the expected state on a fresh install
    # or a local dev machine.
    calibrator    = Calibrator(source=CALIBRATION_SOURCE) if CALIBRATOR_ENABLED else None
    regime_reader = RegimeReader() if REGIME_RESPECT else None

    async def _strategy_with_restart() -> None:
        last_traded:    dict[str, float]    = {}
        open_positions: dict[str, Position] = {}
        cycle_count:    list[int]           = [0]            # mutable box
        markets_box:    list[list[PolyMarket]] = [markets]   # mutable box — survives restarts
        drawdown_guard  = DrawdownGuard()                     # account-level kill-switch
        balance_breaker = BalanceErrorCircuitBreaker()        # retry-storm guard
        while True:
            try:
                await strategy_loop(
                    executor, book_manager, markets_box,
                    last_traded, cycle_count, open_positions,
                    redeemer        = redeemer,
                    market_maker    = market_maker,
                    drawdown_guard  = drawdown_guard,
                    balance_breaker = balance_breaker,
                    calibrator      = calibrator,
                    regime_reader   = regime_reader,
                )
            except DrawdownHalt as exc:
                # Cancel all outstanding orders before halting.
                # NOTE: explicit if/else required — `await X if cond else Y` parses
                # as `(await X) if cond else Y`, so the else branch would be unawaited.
                try:
                    if market_maker:
                        await market_maker.cancel_all()
                    else:
                        await asyncio.to_thread(executor.cancel_all_orders)
                except Exception:
                    pass
                log.critical(
                    "STRATEGY HALTED by drawdown kill-switch: %s\n"
                    "  Fix: investigate balance drop, then restart the service manually.",
                    exc,
                )
                return   # exit restart wrapper — bot stops quoting, dashboard stays up
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
    # Safety guard: main_v2.py and bot.py both load the same codebase,
    # same wallet, and same allowance. Running them in parallel (as
    # happened once when a stray main_v2.py was left running on the
    # VPS while the sexybot systemd service also ran bot.py) causes
    # them to race on orders and drain allowance. Refuse to start if
    # the sexybot service is already active — the operator must either
    # stop sexybot first or set SEXYBOT_ALLOW_DUAL=1 to override.
    import subprocess as _sp, sys as _sys
    if os.getenv("SEXYBOT_ALLOW_DUAL") != "1":
        try:
            _r = _sp.run(
                ["systemctl", "is-active", "--quiet", "sexybot"],
                timeout=3,
            )
            if _r.returncode == 0:
                print(
                    "REFUSING TO START: sexybot service is active — "
                    "bot.py is already running and shares this wallet. "
                    "Either `systemctl stop sexybot` first, or set "
                    "SEXYBOT_ALLOW_DUAL=1 to override.",
                    file=_sys.stderr,
                )
                _sys.exit(2)
        except (FileNotFoundError, _sp.TimeoutExpired):
            pass  # systemctl unavailable (e.g. local dev on macOS) — allow

    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s %(levelname)-8s %(message)s",
    )
    asyncio.run(main())
