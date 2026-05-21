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
from dotenv import load_dotenv
load_dotenv()
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

import requests
from pydantic import BaseModel, field_validator

from calibrator import Calibrator, RegimeReader, RegimeState, record_prediction
from discovery import MarketFilter, PolyMarket, classify_internal_category, fetch_markets
from executor import ClobExecutor
from market_maker import MarketMaker
from observability import (
    record_discovery_batch,
    record_postmortem,
    record_shadow_batch,
    record_trade,
)
from orderbook_ws import BookManager, BookSnapshot, Level
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
# Polymarket splits order placement (CLOB, L2-auth required) from public
# market-data reads (data-api, no auth). The /trades endpoint moved/stayed
# on data-api; the CLOB /trades route 401s for unauthenticated GETs and
# silently broke volume-spike detection from 2026-04-13 until 2026-05-18.
DATA_API_BASE   = "https://data-api.polymarket.com"
REQUEST_TIMEOUT = 8

# Momentum thresholds
SPIKE_WINDOW_SEC = 60     # look-back window for recent trades
SPIKE_RATIO_MIN  = 2.5    # recent rate must be 2.5× the baseline
OBI_CONFIRM_MIN  = 0.20   # OBI must agree with signal direction
# OBI-only fallback is statistically weaker than a trade-volume spike, so
# require a MUCH larger imbalance before trading on it alone. This blocks
# the quiet-market false positives the 0.35 threshold used to allow.
OBI_SOLO_MIN     = float(os.getenv("OBI_SOLO_MIN", "0.55"))
# Minimum |estimated_prob − fill_price| edge to take a trade. Lower → more
# trades but more noise; higher → cleaner signals but fewer fires. 0.03 (3%)
# is conservative; the calibrator-corrected probabilities should be more
# trustworthy than this raw bar suggests, so 0.025 may be reasonable once
# calibration is active.
MIN_EDGE         = float(os.getenv("MIN_EDGE", "0.03"))
MIN_EDGE_YES     = float(os.getenv("MIN_EDGE_YES", "0.07"))
MIN_BOOK_DEPTH_USDC = float(os.getenv("MIN_BOOK_DEPTH_USDC", "200"))  # skip threadbare books
# Hard ceiling on the *side we're buying*. At fills ≥ MAX_ENTRY_PRICE the
# remaining ask side of the book is typically empty or dust, so FOK BUYs
# reject ("no_match") and pile up as status="error" trades. There is also
# almost no upside to buying at 0.99 — best case ~1c, worst case −99c.
# Default lowered from 0.80 to 0.70 to prevent asymmetric, negative-EV entries.
# 80-100c and 70-80c regions were assessed, and the lower ceiling caps capital bleedout.
MAX_ENTRY_PRICE  = float(os.getenv("MAX_ENTRY_PRICE", "0.70"))

# Position management
TRADE_COOLDOWN_SEC    = 300   # seconds before re-buying the same token
MARKET_REFRESH_CYCLES = 20   # re-discover markets every N scan cycles
PROFIT_TARGET         = float(os.getenv("PROFIT_TARGET", "0.08"))   # 8% gain → close (base; dynamic)
STOP_LOSS             = float(os.getenv("STOP_LOSS",     "0.05"))   # 5% loss → close (base; dynamic)
KELLY_FRACTION        = float(os.getenv("KELLY_FRACTION", "0.25"))  # Quarter Kelly
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "3"))  # Max open positions to limit correlated risk
MAX_POSITION_COST_PCT    = float(os.getenv("MAX_POSITION_COST_PCT", "0.15"))  # Max 15% of wallet balance per position
# Time-based exit: if a position doesn't hit profit/stop within this window,
# close at current bid rather than continuing to hold dead inventory. Helps
# recycle capital into fresher signals and caps "slow bleed" losses that
# never quite trigger a stop.
MAX_HOLD_SEC          = int(os.getenv("MAX_HOLD_SEC", "3600"))   # 1 h default
# Below this absolute (not relative) loss, the time-stop will exit even if
# the standard percentage stop hasn't triggered. Prevents being stuck in a
# chronically drifting-lower position.
TIME_STOP_MIN_GAIN    = float(os.getenv("TIME_STOP_MIN_GAIN", "-0.01"))
MIN_EXIT_PRICE        = float(os.getenv("MIN_EXIT_PRICE", "0.05"))
# Comma-separated list of category substrings to skip at discovery. Defaults
# to 'crypto' per the 2026-04-22 backtest recommendation (short-horizon crypto
# price markets; 0% resolved win rate and structurally efficient). Override
# with empty string to re-enable.
EXCLUDE_CATEGORIES    = [
    c.strip() for c in os.getenv("EXCLUDE_CATEGORIES", "crypto").split(",") if c.strip()
]
# Comma-separated substrings to skip on the market QUESTION (not category),
# because Polymarket's category field puts most political markets under
# "other". Empirically the bot's worst losses (Iran diplomatic-meeting
# −87%, uranium −78%) all came from this bucket.
EXCLUDE_KEYWORDS      = [
    k.strip() for k in os.getenv(
        "EXCLUDE_KEYWORDS",
        "iran,uranium,ukraine,russia,taiwan,diplomatic,nuclear,sanctions,"
        "trump,newsom,desantis,election,impeach,supreme court,fed chair,"
        "hormuz,houthi,tehran,kremlin,gaza,israel,hamas,hezbollah"
    ).split(",") if k.strip()
]
# Comma-separated list of *internal* coarse-category buckets to skip at
# discovery (politics, crypto, sports, macro, legal, weather, other). This
# uses bot.classify_market's rule-based classifier so the blocklist matches
# the same buckets the dashboard's category-attribution P&L is keyed on.
# Distinct from EXCLUDE_CATEGORIES, which filters Polymarket's raw category
# field. The 2026-05-07 backtest flagged "other" as a chronic loser
# (-$50.63 over 137 trades, 73.7% win rate but $5 losses dominate); set
# BLOCK_INTERNAL_CATEGORIES=other to act on that finding.
BLOCK_INTERNAL_CATEGORIES = [
    c.strip().lower() for c in os.getenv("BLOCK_INTERNAL_CATEGORIES", "").split(",")
    if c.strip()
]
# Sports-only mandate active for exactly 10 days.
# Started: 2026-05-21T15:05:48-04:00 (Unix: 1779390348.0)
# Expires: 2026-05-31T15:05:48-04:00 (Unix: 1780254348.0)
_SEXYBOT_SPORTS_ONLY_ENV = os.getenv("SEXYBOT_SPORTS_ONLY", "0") == "1"
SPORTS_ONLY_CUTOFF_TS = 1780254348.0

def is_sports_only_active() -> bool:
    if not _SEXYBOT_SPORTS_ONLY_ENV:
        return False
    if time.time() > SPORTS_ONLY_CUTOFF_TS:
        return False
    return True

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
    # Observability — peak/trough best_bid seen while holding, plus the entry
    # signal source/strength. None of these affect exit decisions; they only
    # feed the position_postmortem table on close.
    peak_bid:           Optional[float] = None
    trough_bid:         Optional[float] = None
    market_question:    Optional[str]   = None
    entry_signal_source: Optional[str]  = None
    entry_signal_strength: Optional[float] = None
    # Sweep-to-fill exit order tracking
    exit_order_id:      Optional[str]   = None
    exit_order_time:    Optional[float] = None
    exit_qty_filled:    float           = 0.0
    exit_reason:        Optional[str]   = None
    # Entry order tracking
    entry_order_id:     Optional[str]   = None
    entry_order_time:   Optional[float] = None


def save_open_positions(open_positions: dict[str, Position]) -> None:
    import json
    data = {}
    for tid, pos in open_positions.items():
        data[tid] = {
            "token_id": pos.token_id,
            "entry_price": pos.entry_price,
            "token_qty": pos.token_qty,
            "entry_time": pos.entry_time,
            "profit_target": pos.profit_target,
            "stop_loss": pos.stop_loss,
            "peak_bid": pos.peak_bid,
            "trough_bid": pos.trough_bid,
            "market_question": pos.market_question,
            "entry_signal_source": pos.entry_signal_source,
            "entry_signal_strength": pos.entry_signal_strength,
            "exit_order_id": pos.exit_order_id,
            "exit_order_time": pos.exit_order_time,
            "exit_qty_filled": pos.exit_qty_filled,
            "exit_reason": pos.exit_reason,
            "entry_order_id": pos.entry_order_id,
            "entry_order_time": pos.entry_order_time,
        }
    try:
        with open("open_positions.json", "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.error("Failed to save open_positions.json: %s", e)


def load_open_positions() -> dict[str, Position]:
    import json
    if not os.path.exists("open_positions.json"):
        return {}
    try:
        with open("open_positions.json", "r") as f:
            data = json.load(f)
        positions = {}
        for tid, d in data.items():
            positions[tid] = Position(
                token_id=d["token_id"],
                entry_price=d["entry_price"],
                token_qty=d["token_qty"],
                entry_time=d["entry_time"],
                profit_target=d.get("profit_target", PROFIT_TARGET),
                stop_loss=d.get("stop_loss", STOP_LOSS),
                peak_bid=d.get("peak_bid"),
                trough_bid=d.get("trough_bid"),
                market_question=d.get("market_question"),
                entry_signal_source=d.get("entry_signal_source"),
                entry_signal_strength=d.get("entry_signal_strength"),
                exit_order_id=d.get("exit_order_id"),
                exit_order_time=d.get("exit_order_time"),
                exit_qty_filled=d.get("exit_qty_filled", 0.0),
                exit_reason=d.get("exit_reason"),
                entry_order_id=d.get("entry_order_id"),
                entry_order_time=d.get("entry_order_time"),
            )
        return positions
    except Exception as e:
        log.error("Failed to load open_positions.json: %s", e)
        return {}


def check_correlation_and_category_gates(mkt: PolyMarket, open_positions: dict[str, Position]) -> tuple[bool, str]:
    # 1. Coarse Category Check
    mkt_category = classify_internal_category(mkt.question or "")
    category_count = 0
    for pos in open_positions.values():
        if pos.market_question:
            pos_category = classify_internal_category(pos.market_question)
            if pos_category == mkt_category:
                category_count += 1
                
    MAX_POSITIONS_PER_CATEGORY = 2
    if category_count >= MAX_POSITIONS_PER_CATEGORY:
        return False, f"Category concentration breach: already have {category_count} positions in category '{mkt_category}' (max={MAX_POSITIONS_PER_CATEGORY})"

    # 2. Correlation Check (Event-level keyword matching)
    STOP_WORDS = {
        "will", "over", "under", "game", "points", "team", "vs", "the", "and", "or", 
        "to", "in", "of", "for", "on", "at", "by", "with", "a", "an", "is", "be", 
        "who", "what", "which", "how", "many", "than", "more", "less", "yes", "no",
        "play", "player", "players", "score", "scores", "match", "win", "lose", "first",
        "second", "third", "fourth", "quarter", "half", "total", "greater", "fewer", "between",
        "season", "league", "championship", "finals", "playoffs", "series", "stage", "round",
        # Expanded common comparison and sports transition verbs/adjectives
        "beat", "beats", "beaten", "defeat", "defeats", "defeated", "against", "each", "other",
        "either", "any", "all", "only", "both", "from", "most", "best", "worst", "next", "last",
        "some", "time", "date", "month", "year", "week", "day", "night", "today", "tomorrow",
        "tonight", "this", "that", "these", "those", "versus", "matchup", "matchups", "win",
        "wins", "won", "lose", "loses", "lost", "game", "games", "playoff", "final"
    }
    
    def extract_keywords(text: str) -> set[str]:
        if not text:
            return set()
        import re
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
        return {w for w in words if w not in STOP_WORDS}

    mkt_keywords = extract_keywords(mkt.question)
    if not mkt_keywords:
        return True, ""

    for pos in open_positions.values():
        if pos.market_question:
            pos_keywords = extract_keywords(pos.market_question)
            overlap = mkt_keywords.intersection(pos_keywords)
            if overlap:
                return False, f"Event correlation breach: overlapping keywords {overlap} with active position '{pos.market_question}'"

    return True, ""


def get_last_buy_trade(token_id: str) -> Optional[dict]:
    import sqlite3
    db_path = os.getenv("CALIBRATION_DB_PATH", "trades.db")
    try:
        conn = sqlite3.connect(db_path, timeout=3.0)
        conn.row_factory = sqlite3.Row
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trades'")
        if not cur.fetchone():
            return None
        cur = conn.execute(
            "SELECT market, price, shares, time, strategy FROM trades "
            "WHERE token_id = ? AND side = 'BUY' "
            "ORDER BY time DESC LIMIT 1",
            (token_id,)
        )
        row = cur.fetchone()
        if row:
            return dict(row)
        return None
    except Exception as e:
        log.warning("get_last_buy_trade error for %s: %s", token_id[:14], e)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


async def reconcile_positions_on_startup(executor: ClobExecutor, open_positions: dict[str, Position]) -> None:
    log.info("Starting on-chain position reconciliation...")
    loaded = load_open_positions()
    open_positions.clear()
    open_positions.update(loaded)

    user_addr = FUNDER_ADDRESS or (executor._client.get_address() if executor else None)
    if not user_addr:
        log.warning("No user/funder wallet address available. Skipping on-chain position reconciliation.")
        return

    try:
        import json as _j
        import urllib.request as _ureq

        url = f"https://data-api.polymarket.com/positions?user={user_addr}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Origin": "https://polymarket.com",
            "Referer": "https://polymarket.com/",
        }

        req = _ureq.Request(url, headers=headers)

        def fetch_url():
            with _ureq.urlopen(req, timeout=8) as r:
                return _j.loads(r.read())

        live_positions = await asyncio.get_event_loop().run_in_executor(None, fetch_url)

        if not isinstance(live_positions, list):
            log.warning("Polymarket positions data-api returned invalid format. Skipping on-chain reconciliation.")
            return

        onchain_tids = {}
        for p in live_positions:
            asset = p.get("asset")
            size = float(p.get("size", 0) or 0)
            avg_price = float(p.get("avgPrice", 0) or 0)
            title = p.get("title", "")
            if asset and size > 0:
                onchain_tids[asset] = {
                    "size": size,
                    "avg_price": avg_price,
                    "title": title
                }

        log.info("Found %d active position(s) on-chain.", len(onchain_tids))

        # Reconcile on-chain assets into open_positions
        for tid, o_info in onchain_tids.items():
            size = o_info["size"]
            avg_price = o_info["avg_price"]
            title = o_info["title"]

            if tid in open_positions:
                pos = open_positions[tid]
                if abs(pos.token_qty - size) > 1e-4:
                    log.info("Reconciliation: updating %s qty from %.4f to %.4f",
                             tid[:14], pos.token_qty, size)
                    pos.token_qty = size
            else:
                log.warning("Reconciliation: untracked active on-chain position found for %s (%s). Reconstructing...",
                            tid[:14], title[:40])

                db_trade = get_last_buy_trade(tid)
                if db_trade:
                    entry_price = float(db_trade.get("price", avg_price) or avg_price)
                    try:
                        entry_time = datetime.fromisoformat(db_trade["time"].replace("Z", "+00:00")).timestamp()
                    except Exception:
                        entry_time = time.time()
                    strategy = db_trade.get("strategy", "reconstructed")
                    market_question = db_trade.get("market", title)
                else:
                    entry_price = avg_price if avg_price > 0 else 0.50
                    entry_time = time.time()
                    strategy = "reconstructed"
                    market_question = title

                pt, sl = dynamic_exit_levels(
                    entry_price,
                    base_profit = PROFIT_TARGET,
                    base_stop   = STOP_LOSS,
                )
                open_positions[tid] = Position(
                    token_id              = tid,
                    entry_price           = entry_price,
                    token_qty             = size,
                    entry_time            = entry_time,
                    profit_target         = pt,
                    stop_loss             = sl,
                    peak_bid              = entry_price,
                    trough_bid            = entry_price,
                    market_question       = market_question,
                    entry_signal_source   = strategy,
                )
                log.info("Reconstructed position: %s, entry=%.4f, qty=%.4f",
                         tid[:14], entry_price, size)

        # Prune tracking for positions no longer held on-chain (excluding very recent ones < 5 min old)
        now_ts = time.time()
        to_prune = []
        for tid, pos in list(open_positions.items()):
            if tid not in onchain_tids:
                if now_ts - pos.entry_time < 300:
                    continue
                if getattr(pos, "exit_order_id", None) is not None:
                    continue
                if getattr(pos, "entry_order_id", None) is not None:
                    continue
                to_prune.append(tid)

        if to_prune:
            for tid in to_prune:
                log.warning("Reconciliation: pruning inactive/phantom position from local tracking: %s (%s)",
                            tid[:14], open_positions[tid].market_question or "")
                del open_positions[tid]

        save_open_positions(open_positions)
        log.info("Position reconciliation complete. Currently tracking %d position(s).", len(open_positions))

    except Exception as e:
        log.error("Error during position reconciliation: %s", e)


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


def _audit_discovery(raw_markets: list[PolyMarket]) -> list[dict]:
    """
    Mirror the MarketFilter chain used in main() / strategy_loop and tag each
    candidate with the FIRST filter that would exclude it ('kept' otherwise).
    Pure observation — never mutates anything, never raises. Output is a
    list of dicts ready for record_discovery_batch.

    Filter order must match the chain at line ~1024 of main(); if that order
    changes, this should change too (otherwise the dashboard's reason
    attribution will mis-blame the wrong gate).
    """
    rows: list[dict] = []
    # Pre-compute the top-N-by-volume cutoff so we can identify markets that
    # passed every filter but lost the volume cap.
    survivors = []
    excluded_kw_lower    = [k.lower() for k in EXCLUDE_KEYWORDS]
    excluded_cat_lower   = [c.lower() for c in EXCLUDE_CATEGORIES]
    blocked_internal_set = {c.lower() for c in BLOCK_INTERNAL_CATEGORIES}

    for m in raw_markets:
        question_lower = (m.question or "").lower()
        category_lower = (m.category or "").lower()
        internal_cat   = classify_internal_category(m.question or "")
        base = {
            "token_id":          m.yes_token_id,
            "question":          m.question,
            "category":          m.category,
            "internal_category": internal_cat,
            "yes_price":         m.yes_price,
            "volume_24h":        m.volume_24h,
            "liquidity":         m.liquidity,
            "spread_cents":      m.spread,
        }
        if m.spread > 3.0:
            rows.append({**base, "excluded_by": "spread"})
            continue
        if m.yes_price < 0.08 or m.yes_price > 0.92:
            rows.append({**base, "excluded_by": "price_range"})
            continue
        if any(c in category_lower for c in excluded_cat_lower):
            rows.append({**base, "excluded_by": "category"})
            continue
        if any(k in question_lower for k in excluded_kw_lower):
            rows.append({**base, "excluded_by": "keyword"})
            continue
        if internal_cat.lower() in blocked_internal_set:
            rows.append({**base, "excluded_by": "internal_category"})
            continue
        survivors.append((m, base))

    # Top-20 by volume_24h — the only ones that actually make the watchlist.
    survivors.sort(key=lambda pair: pair[0].volume_24h, reverse=True)
    for idx, (_m, base) in enumerate(survivors):
        rows.append({**base, "excluded_by": "kept" if idx < 20 else "top_n"})
    return rows


# ── Momentum signal ────────────────────────────────────────────────────────────

def _get_recent_trades_sync(token_id: str) -> list[dict]:
    """Synchronous trade fetch — always call via asyncio.to_thread().
    Uses the public data-api host; the CLOB host requires L2 auth and was
    silently returning 401 for over a month, disabling _detect_volume_spike."""
    try:
        resp = requests.get(
            f"{DATA_API_BASE}/trades",
            params={"asset_id": token_id, "limit": 100},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("data", [])
    except Exception as exc:
        log.warning("data-api trade fetch error for %s: %s", token_id[:14], exc)
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
    shadow_recorder: Optional[Callable[..., None]] = None,
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

    `shadow_recorder` is an optional best-effort callback invoked at every
    return point with the gate outcome and the observable state at that
    moment. It exists purely for the shadow_signals table — trading
    behaviour is identical whether or not it's passed in.
    """
    def _shadow(outcome: str, **extra) -> None:
        if shadow_recorder is None:
            return
        try:
            shadow_recorder(
                token_id=book.token_id,
                market=market.question,
                outcome=outcome,
                yes_price=book.vamp,
                best_bid=book.best_bid,
                best_ask=book.best_ask,
                book_depth_usdc=book.book_depth_usdc,
                obi=book.obi,
                **extra,
            )
        except Exception as exc:   # never block trading on observability errors
            log.debug("shadow_recorder error (non-fatal): %s", exc)

    # 1. Fair value — VAMP only; don't fall back to a bare mid from a phantom book
    yes_price = book.vamp
    if yes_price is None:
        _shadow("no_vamp")
        return None

    # Thin books → skip. Liquidity-starved markets produce erratic signals and
    # can't absorb even a $1 Kelly order without slippage blowing the edge.
    if book.book_depth_usdc is None or book.book_depth_usdc < MIN_BOOK_DEPTH_USDC:
        log.debug("DEPTH SKIP | %s  depth=%s < $%.0f",
                  market.question[:40],
                  f"${book.book_depth_usdc:.0f}" if book.book_depth_usdc else "None",
                  MIN_BOOK_DEPTH_USDC)
        _shadow("depth_skip")
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
            _shadow(
                "obi_solo_below",
                spike_has=spike.has_spike,
                spike_dominant_side=spike.dominant_side,
                spike_confidence=spike.confidence,
                spike_ratio=spike.spike_ratio,
            )
            return None
        dominant_side = "YES" if obi > 0 else "NO"
        source        = "obi"
        # Map |obi| from [OBI_SOLO_MIN, 1.0] → strength [0.15, 0.30].
        strength      = 0.15 + 0.15 * (abs(obi) - OBI_SOLO_MIN) / max(1e-6, 1.0 - OBI_SOLO_MIN)
        confidence    = abs(obi) * 100

    # 4. OBI must not actively oppose the signal
    if dominant_side == "YES" and obi < -OBI_CONFIRM_MIN:
        log.debug("OBI opposes YES trade (obi=%+.3f) — skip %s", obi, market.question[:40])
        _shadow(
            "obi_opposes",
            spike_has=spike.has_spike,
            spike_dominant_side=spike.dominant_side,
            spike_confidence=spike.confidence,
            spike_ratio=spike.spike_ratio,
            signal_source=source,
            signal_strength=strength,
        )
        return None
    if dominant_side == "NO" and obi > OBI_CONFIRM_MIN:
        log.debug("OBI opposes NO trade (obi=%+.3f) — skip %s", obi, market.question[:40])
        _shadow(
            "obi_opposes",
            spike_has=spike.has_spike,
            spike_dominant_side=spike.dominant_side,
            spike_confidence=spike.confidence,
            spike_ratio=spike.spike_ratio,
            signal_source=source,
            signal_strength=strength,
        )
        return None

    # 4b. Price ceiling. For BUY YES the fill price ≈ best_ask; for BUY NO the
    # fill price on the NO side ≈ 1 - best_bid. Either way, refuse to enter
    # near 1.0 — fills there reject as "no_match" (empty ask side) and even
    # when they fill the upside is sub-cent.
    side_fill_price = (
        (book.best_ask if book.best_ask is not None else yes_price)
        if dominant_side == "YES"
        else (1.0 - book.best_bid if book.best_bid is not None else 1.0 - yes_price)
    )
    if side_fill_price >= MAX_ENTRY_PRICE:
        log.info("PRICE CEILING [signal] | %s  side=%s  fill=%.4f >= %.2f — skip",
                 market.question[:40], dominant_side, side_fill_price, MAX_ENTRY_PRICE)
        _shadow(
            "price_ceiling",
            spike_has=spike.has_spike,
            spike_dominant_side=spike.dominant_side,
            spike_confidence=spike.confidence,
            spike_ratio=spike.spike_ratio,
            signal_source=source,
            signal_strength=strength,
            fill_price=side_fill_price,
        )
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

    required_edge = MIN_EDGE_YES if dominant_side == "YES" else MIN_EDGE
    if edge < required_edge:
        _shadow(
            "edge_below",
            spike_has=spike.has_spike,
            spike_dominant_side=spike.dominant_side,
            spike_confidence=spike.confidence,
            spike_ratio=spike.spike_ratio,
            signal_source=source,
            signal_strength=strength,
            estimated_prob=true_prob,
            edge=edge,
            fill_price=side_fill_price,
        )
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
    _shadow(
        "accepted",
        spike_has=spike.has_spike,
        spike_dominant_side=spike.dominant_side,
        spike_confidence=spike.confidence,
        spike_ratio=spike.spike_ratio,
        signal_source=source,
        signal_strength=strength,
        estimated_prob=true_prob,
        edge=edge,
        fill_price=side_fill_price,
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
                    .exclude_keywords(EXCLUDE_KEYWORDS)
                    .exclude_internal_categories(BLOCK_INTERNAL_CATEGORIES)
                    .top(20, key="volume_24h")
                    .results()
                )
                if is_sports_only_active():
                    fresh = [m for m in fresh if classify_internal_category(m.question) == "sports"]
                # Drop any new markets — only keep already-subscribed ones
                still_active = [m for m in fresh if m.yes_token_id in subscribed_yes_ids]
                if still_active:
                    markets_box[0] = still_active
                    markets = markets_box[0]
                log.info(
                    "Markets refreshed — %d/%d still active",
                    len(markets), len(subscribed_yes_ids),
                )
                # Audit the refresh: same chain rules, same recorder. Lets the
                # dashboard see which markets are newly excluded or newly
                # available over time without us having to rerun discovery.
                try:
                    refresh_rows = _audit_discovery(fresh_raw)
                    await asyncio.to_thread(record_discovery_batch, refresh_rows)
                except Exception as exc:
                    log.debug("discovery audit (refresh) failed: %s", exc)
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

        # ── Active Entry Order Sweeper ────────────────────────────────────────
        for token_id, pos in list(open_positions.items()):
            entry_id = getattr(pos, "entry_order_id", None)
            if entry_id is None:
                continue

            if entry_id == "DRY_RUN":
                pos.entry_order_id = None
                pos.entry_order_time = None
                save_open_positions(open_positions)
                continue

            # Live order polling
            info = await asyncio.to_thread(executor.get_order, entry_id)
            if not isinstance(info, dict):
                log.warning("Entry order sweeper: get_order for %s failed. Retrying next cycle.", entry_id)
                continue

            status = (info.get("status") or "").lower()
            try:
                size_matched = float(info.get("size_matched", 0) or 0)
            except (TypeError, ValueError):
                size_matched = 0.0

            original_qty = pos.token_qty
            remaining_qty = max(0.0, original_qty - size_matched)

            # If matched/filled or negligible remaining
            if status in ("matched", "filled") or remaining_qty <= 1e-4:
                log.info("ENTRY ORDER FILLED | %s fully filled %.4f shares.", token_id[:14], size_matched)
                pos.token_qty = size_matched
                pos.entry_order_id = None
                pos.entry_order_time = None
                save_open_positions(open_positions)
                continue

            # If terminated (canceled/expired)
            if status in ("canceled", "cancelled", "expired"):
                log.warning("ENTRY ORDER TERMINATED (%s) | %s had size_matched=%.4f/%.4f.",
                            status, token_id[:14], size_matched, original_qty)
                value = size_matched * pos.entry_price
                if value >= 1.0:
                    log.info("Keeping partially filled entry position for %s: qty=%.4f (value=$%.2f)",
                             token_id[:14], size_matched, value)
                    pos.token_qty = size_matched
                    pos.entry_order_id = None
                    pos.entry_order_time = None
                else:
                    log.warning("Partially filled value $%.2f < $1.00 min notional, pruning position %s",
                                value, token_id[:14])
                    del open_positions[token_id]
                save_open_positions(open_positions)
                continue

            # If open and stale (>= 60 seconds)
            elapsed = time.time() - (pos.entry_order_time or time.time())
            if elapsed >= 60.0:
                log.info("ENTRY ORDER STALLED (%.0fs) | Cancelling entry order %s...", elapsed, token_id[:14])
                await asyncio.to_thread(executor.cancel_order, entry_id)

                # Fetch final state post-cancel to see what filled
                info_post = await asyncio.to_thread(executor.get_order, entry_id)
                if isinstance(info_post, dict):
                    try:
                        size_matched = float(info_post.get("size_matched", size_matched) or size_matched)
                    except (TypeError, ValueError):
                        pass

                value = size_matched * pos.entry_price
                if value >= 1.0:
                    log.info("Keeping partially filled entry position post-cancel for %s: qty=%.4f (value=$%.2f)",
                             token_id[:14], size_matched, value)
                    pos.token_qty = size_matched
                    pos.entry_order_id = None
                    pos.entry_order_time = None
                else:
                    log.warning("Unfilled or tiny filled value $%.2f < $1.00 post-cancel, pruning position %s",
                                value, token_id[:14])
                    del open_positions[token_id]
                save_open_positions(open_positions)
                continue

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
            # If position is still in pending entry phase, skip exit evaluations
            if getattr(pos, "entry_order_id", None) is not None:
                continue

            # 1. Process pending exit order if any (exiting state)
            if getattr(pos, "exit_order_id", None) is not None:
                if pos.exit_order_id == "DRY_RUN":
                    log.info("EXIT DRY RUN COMPLETED | %s was in exiting state.", token_id[:14])
                    try:
                        await asyncio.to_thread(
                            record_postmortem,
                            token_id              = token_id,
                            market                = pos.market_question,
                            entry_price           = pos.entry_price,
                            entry_time            = datetime.fromtimestamp(pos.entry_time, tz=timezone.utc).isoformat(),
                            exit_price            = pos.entry_price,
                            exit_time             = datetime.now(timezone.utc).isoformat(),
                            held_seconds          = int(time.time() - pos.entry_time),
                            peak_bid              = pos.peak_bid or pos.entry_price,
                            trough_bid            = pos.trough_bid or pos.entry_price,
                            max_gain_pct          = 0.0,
                            min_gain_pct          = 0.0,
                            realized_gain_pct     = 0.0,
                            exit_reason           = pos.exit_reason or "dry_run_exit",
                            entry_signal_source   = pos.entry_signal_source,
                            entry_signal_strength = pos.entry_signal_strength,
                            profit_target         = pos.profit_target,
                            stop_loss             = pos.stop_loss,
                            exit_order_id         = "DRY_RUN",
                            exit_status           = "dry_run",
                        )
                    except Exception as exc:
                        log.debug("record_postmortem dry_run failed: %s", exc)
                    try:
                        await asyncio.to_thread(
                            record_trade,
                            market       = pos.market_question or "",
                            side         = "SELL",
                            amount_usdc  = pos.entry_price * pos.token_qty,
                            price        = pos.entry_price,
                            shares       = pos.token_qty,
                            token_id     = token_id,
                            order_id     = "DRY_RUN",
                            order_type   = "limit",
                            strategy     = pos.entry_signal_source,
                            category     = classify_internal_category(pos.market_question or ""),
                            status_detail = pos.exit_reason or "dry_run_exit",
                            dry_run      = True,
                        )
                    except Exception as exc:
                        log.debug("record_trade dry_run failed: %s", exc)
                    del open_positions[token_id]
                    save_open_positions(open_positions)
                    last_traded[token_id] = time.time()
                    continue

                info = await asyncio.to_thread(executor.get_order, pos.exit_order_id)
                if not isinstance(info, dict):
                    log.warning("Exit order monitoring: get_order for %s failed. Retrying next cycle.", pos.exit_order_id)
                    continue

                status = (info.get("status") or "").lower()
                try:
                    size_matched = float(info.get("size_matched", 0) or 0)
                except (TypeError, ValueError):
                    size_matched = 0.0

                original_qty = pos.token_qty
                remaining_qty = max(0.0, original_qty - size_matched)

                if status in ("matched", "filled") or remaining_qty <= 1e-4:
                    log.info("EXIT ORDER FILLED | %s fully filled %.4f shares.", token_id[:14], original_qty)
                    try:
                        fill_price = float(info.get("price", 0) or pos.entry_price)
                        peak = pos.peak_bid if pos.peak_bid is not None else fill_price
                        trough = pos.trough_bid if pos.trough_bid is not None else fill_price
                        max_gain = (peak - pos.entry_price) / pos.entry_price
                        min_gain = (trough - pos.entry_price) / pos.entry_price
                        realized = (fill_price - pos.entry_price) / pos.entry_price

                        await asyncio.to_thread(
                            record_postmortem,
                            token_id              = token_id,
                            market                = pos.market_question,
                            entry_price           = pos.entry_price,
                            entry_time            = datetime.fromtimestamp(pos.entry_time, tz=timezone.utc).isoformat(),
                            exit_price            = fill_price,
                            exit_time             = datetime.now(timezone.utc).isoformat(),
                            held_seconds          = int(time.time() - pos.entry_time),
                            peak_bid              = peak,
                            trough_bid            = trough,
                            max_gain_pct          = max_gain * 100,
                            min_gain_pct          = min_gain * 100,
                            realized_gain_pct     = realized * 100,
                            exit_reason           = pos.exit_reason or "exit",
                            entry_signal_source   = pos.entry_signal_source,
                            entry_signal_strength = pos.entry_signal_strength,
                            profit_target         = pos.profit_target,
                            stop_loss             = pos.stop_loss,
                            exit_order_id         = pos.exit_order_id,
                            exit_status           = status,
                        )
                    except Exception as exc:
                        log.debug("record_postmortem in monitoring failed: %s", exc)

                    try:
                        fill_price = float(info.get("price", 0) or pos.entry_price)
                        await asyncio.to_thread(
                            record_trade,
                            market       = pos.market_question or "",
                            side         = "SELL",
                            amount_usdc  = fill_price * original_qty,
                            price        = fill_price,
                            shares       = original_qty,
                            token_id     = token_id,
                            order_id     = pos.exit_order_id,
                            order_type   = "limit",
                            strategy     = pos.entry_signal_source,
                            category     = classify_internal_category(pos.market_question or ""),
                            status_detail = pos.exit_reason or "exit",
                            dry_run      = DRY_RUN,
                        )
                    except Exception as exc:
                        log.debug("record_trade in monitoring failed: %s", exc)

                    del open_positions[token_id]
                    save_open_positions(open_positions)
                    last_traded[token_id] = time.time()
                    continue

                if status in ("canceled", "cancelled", "expired"):
                    log.warning("EXIT ORDER TERMINATED (%s) | %s had size_matched=%.4f/%.4f. Re-queueing remainder...",
                                status, token_id[:14], size_matched, original_qty)
                    pos.token_qty = remaining_qty
                    pos.exit_order_id = None
                    pos.exit_order_time = None
                    book = book_manager.get_book(token_id)
                    bid_price = book.best_bid if (book and book.best_bid) else 0.5
                    if pos.token_qty * bid_price < 1.0:
                        log.warning("Remaining exit quantity too small (<$1), abandoning: %s (qty=%.4f)", token_id[:14], pos.token_qty)
                        del open_positions[token_id]
                    save_open_positions(open_positions)
                    continue

                elapsed = time.time() - (pos.exit_order_time or time.time())
                if elapsed >= 30:
                    log.info("EXIT ORDER STALLED (%.0fs) | Cancelling and re-quoting %s...", elapsed, token_id[:14])
                    try:
                        await asyncio.to_thread(executor.cancel_order, pos.exit_order_id)
                    except Exception as exc:
                        log.error("Failed to cancel stalled exit order %s: %s", pos.exit_order_id, exc)

                    pos.token_qty = remaining_qty
                    pos.exit_order_id = None
                    pos.exit_order_time = None
                    book = book_manager.get_book(token_id)
                    bid_price = book.best_bid if (book and book.best_bid) else 0.5
                    if pos.token_qty * bid_price < 1.0:
                        log.warning("Remaining exit quantity too small (<$1) after stall cancel, abandoning: %s (qty=%.4f)", token_id[:14], pos.token_qty)
                        del open_positions[token_id]
                    save_open_positions(open_positions)
                    continue

                continue

            book = book_manager.get_book(token_id)
            if book is None or book.is_stale or book.best_bid is None:
                try:
                    log.warning("Exit check: live WS book for %s is %s. Attempting REST fallback fetch...",
                                token_id[:14], "missing" if book is None else ("stale" if book.is_stale else "lacks best bid"))
                    rest_book = await asyncio.to_thread(executor._client.get_order_book, token_id)
                    if rest_book:
                        bids = [Level(price=float(b["price"]), size=float(b["size"])) for b in rest_book.get("bids", [])]
                        asks = [Level(price=float(a["price"]), size=float(a["size"])) for a in rest_book.get("asks", [])]
                        book = BookSnapshot(
                            token_id=token_id,
                            bids=bids,
                            asks=asks,
                            timestamp=time.time(),
                            sequence=0,
                            mid_history=[]
                        )
                except Exception as e:
                    log.error("REST fallback orderbook fetch failed in exit loop: %s", e)

            if book is None or book.is_stale or book.best_bid is None:
                continue
            if pos.entry_price <= 0:
                log.warning("skip exit check: %s has invalid entry_price=%s",
                            token_id[:14], pos.entry_price)
                continue

            if pos.peak_bid is None or book.best_bid > pos.peak_bid:
                pos.peak_bid = book.best_bid
            if pos.trough_bid is None or book.best_bid < pos.trough_bid:
                pos.trough_bid = book.best_bid

            gain    = (book.best_bid - pos.entry_price) / pos.entry_price
            held_s  = time.time() - pos.entry_time
            profit_band = pos.profit_target or PROFIT_TARGET
            stop_band   = pos.stop_loss     or STOP_LOSS
            if gain >= profit_band:
                reason = f"profit {gain * 100:.1f}% (target {profit_band * 100:.1f}%)"
            elif gain <= -stop_band:
                reason = f"stop-loss {gain * 100:.1f}% (band {stop_band * 100:.1f}%)"
            elif held_s >= MAX_HOLD_SEC and gain <= TIME_STOP_MIN_GAIN:
                reason = f"time-stop {held_s / 60:.0f}m gain={gain * 100:+.1f}%"
            else:
                continue

            if book.best_bid < MIN_EXIT_PRICE:
                log.warning(
                    "EXIT BLOCKED | %s best bid %.4f < MIN_EXIT_PRICE %.4f (liquidity floor guard) — exit aborted!",
                    token_id[:14], book.best_bid, MIN_EXIT_PRICE
                )
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

            if exit_result and exit_result.success:
                if drawdown_guard is not None:
                    drawdown_guard.record_trade()
                pos.exit_order_id = exit_result.order_id
                pos.exit_order_time = time.time()
                pos.exit_reason = reason
                save_open_positions(open_positions)
                log.info(
                    "EXIT INITIATED | %s  reason=%s  order_id=%s",
                    token_id[:14], reason, exit_result.order_id,
                )
            elif exit_result and exit_result.error and "too small" in (exit_result.error or ""):
                log.warning(
                    "ABANDON | %s  value<$1  entry=%.4f  qty=%.4f  bid=%.4f",
                    token_id[:14], pos.entry_price, pos.token_qty, book.best_bid,
                )
                try:
                    await asyncio.to_thread(
                        record_postmortem,
                        token_id              = token_id,
                        market                = pos.market_question,
                        entry_price           = pos.entry_price,
                        entry_time            = datetime.fromtimestamp(
                            pos.entry_time, tz=timezone.utc
                        ).isoformat(),
                        exit_price            = book.best_bid,
                        exit_time             = datetime.now(timezone.utc).isoformat(),
                        held_seconds          = int(held_s),
                        peak_bid              = pos.peak_bid  or book.best_bid,
                        trough_bid            = pos.trough_bid or book.best_bid,
                        exit_reason           = "abandoned_below_min_notional",
                        entry_signal_source   = pos.entry_signal_source,
                        entry_signal_strength = pos.entry_signal_strength,
                        profit_target         = pos.profit_target,
                        stop_loss             = pos.stop_loss,
                    )
                except Exception as exc:
                    log.debug("record_postmortem (abandon) failed: %s", exc)
                del open_positions[token_id]
                save_open_positions(open_positions)

        # Collect live books — skip markets with an open position (no stacking)
        live: list[tuple[PolyMarket, BookSnapshot]] = []
        if len(open_positions) >= MAX_CONCURRENT_POSITIONS:
            log.info("MAX OPEN POSITIONS REACHED (%d/%d) — skipping momentum entries", len(open_positions), MAX_CONCURRENT_POSITIONS)
        else:
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

        # Shadow recorder — collects one row per (market, cycle) into a local
        # list. Flushed to SQLite at end-of-cycle in a thread so DB writes
        # never sit in the event loop. Trading behaviour is unchanged whether
        # this list is populated or not.
        shadow_rows: list[dict] = []
        def _shadow_record(**kw) -> None:
            shadow_rows.append(kw)

        # Run all signal estimates concurrently (each fires one HTTP request)
        signal_tasks = [
            estimate_true_probability(
                mkt, book, calibrator=calibrator,
                shadow_recorder=_shadow_record,
            )
            for mkt, book in live
        ]
        signal_results = await asyncio.gather(*signal_tasks, return_exceptions=True)

        for (mkt, _yes_book), result in zip(live, signal_results):
            if isinstance(result, Exception):
                log.error("Signal error for %s: %s", mkt.question[:40], result)
                continue
            if result is None:
                continue

            passed, gate_msg = check_correlation_and_category_gates(mkt, open_positions)
            if not passed:
                log.info("CONCENTRATION GATE BLOCKED | %s: %s", mkt.question[:40], gate_msg)
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
            if cycle_balance is not None:
                max_pos_dollars = cycle_balance.balance * MAX_POSITION_COST_PCT
                if kelly_dollars > max_pos_dollars:
                    log.info("SIZING GATE | %s: capped Kelly size $%.2f to $%.2f (max %d%% of balance)",
                             mkt.question[:40], kelly_dollars, max_pos_dollars, int(MAX_POSITION_COST_PCT * 100))
                    kelly_dollars = max_pos_dollars
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
                    if drawdown_guard is not None:
                        drawdown_guard.record_trade()
                    pt, sl = dynamic_exit_levels(
                        result.fill_price,
                        base_profit = PROFIT_TARGET,
                        base_stop   = STOP_LOSS,
                        regime      = regime,
                    )
                    open_positions[trade_token_id] = Position(
                        token_id              = trade_token_id,
                        entry_price           = result.fill_price,
                        token_qty             = result.token_qty,
                        entry_time            = time.time(),
                        profit_target         = pt,
                        stop_loss             = sl,
                        peak_bid              = result.fill_price,
                        trough_bid            = result.fill_price,
                        market_question       = mkt.question,
                        entry_signal_source   = signal.source,
                        entry_signal_strength = signal.strength,
                        entry_order_id        = result.order_id,
                        entry_order_time      = time.time(),
                    )
                    save_open_positions(open_positions)
                    log.info(
                        "EXIT BANDS | %s  entry=%.4f profit=+%.1f%% stop=-%.1f%%",
                        mkt.question[:45], result.fill_price, pt * 100, sl * 100,
                    )
                    # Mirror the BUY into the shared trades table so /pnl/cuts,
                    # /pnl/realized, and /activity see sexybot-v2's activity
                    # (closes the accounting blind spot identified 2026-05-18).
                    # Best-effort; a DB error must never block trading.
                    try:
                        await asyncio.to_thread(
                            record_trade,
                            market       = mkt.question,
                            side         = "BUY",
                            amount_usdc  = float(result.fill_price) * float(result.token_qty),
                            price        = float(result.fill_price),
                            shares       = float(result.token_qty),
                            token_id     = trade_token_id,
                            order_id     = result.order_id,
                            order_type   = "limit",
                            strategy     = signal.source,
                            category     = classify_internal_category(mkt.question),
                            dry_run      = DRY_RUN,
                        )
                    except Exception as exc:
                        log.debug("record_trade (BUY) failed: %s", exc)
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

        # Flush this cycle's shadow rows in a single batch insert so DB I/O
        # is amortised and doesn't run on the event loop. Best-effort — a
        # busy lock or missing trades.db is a log-and-move-on.
        if shadow_rows:
            try:
                written = await asyncio.to_thread(record_shadow_batch, shadow_rows)
                log.debug("shadow_signals: wrote %d/%d rows", written, len(shadow_rows))
            except Exception as exc:
                log.debug("shadow_signals flush failed (non-fatal): %s", exc)

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
        .exclude_keywords(EXCLUDE_KEYWORDS)
        .exclude_internal_categories(BLOCK_INTERNAL_CATEGORIES)
        .top(20, key="volume_24h")
        .results()
    )
    if is_sports_only_active():
        markets = [m for m in markets if classify_internal_category(m.question) == "sports"]
        log.info("SEXYBOT_SPORTS_ONLY is active (expires %s): filtered watch list down to %d sports markets",
                 datetime.fromtimestamp(SPORTS_ONLY_CUTOFF_TS, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                 len(markets))
    log.info(
        "Watching %d markets (excluding categories: %s; keywords: %s; internal: %s)",
        len(markets), EXCLUDE_CATEGORIES or "none", EXCLUDE_KEYWORDS or "none",
        BLOCK_INTERNAL_CATEGORIES or "none",
    )

    # Discovery audit — log every candidate from fetch_markets with the filter
    # that dropped it (or 'kept'). Sync write at startup is fine; the periodic
    # refresh inside strategy_loop uses asyncio.to_thread.
    try:
        audit_rows = _audit_discovery(raw)
        written = record_discovery_batch(audit_rows)
        log.info(
            "Discovery audit: %d candidates evaluated, %d written",
            len(audit_rows), written,
        )
    except Exception as exc:
        log.debug("discovery audit (startup) failed: %s", exc)

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
        
        # Cancel any active/stray orders before we reconcile and trade
        log.info("Startup safety sweep: cancelling all active orders...")
        try:
            await asyncio.to_thread(executor.cancel_all_orders)
        except Exception as exc:
            log.warning("Startup safety sweep failed to cancel all orders (non-fatal): %s", exc)
            
        # Reconcile local state with on-chain holdings on startup/restart
        await reconcile_positions_on_startup(executor, open_positions)
        
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
