"""
observability.py
Pure-observation sinks for the trading bot. Records what the bot *saw* and
*decided* every cycle, without ever influencing order placement or exits.

Three tables, all in the same trades.db that bot.py / calibrator.py share:

  shadow_signals     — one row per (market, cycle) the signal pipeline evaluated,
                       with the gate outcome (accepted | depth_skip | obi_solo_below
                       | obi_opposes | price_ceiling | edge_below). Lets the
                       nightly review answer "what would we have traded under
                       looser thresholds?" without rerunning the loop.

  position_postmortem — one row per closed position with entry/exit/peak/trough,
                       exit reason, and the entry signal source. Quantifies how
                       much was left on the table by the exit timing and which
                       signal sources actually drive winners.

  discovery_audit    — one row per (market, discovery cycle) showing which
                       filter ('kept' | 'category' | 'keyword' | 'internal_category'
                       | 'top_n' | 'spread' | 'price_range') dropped each
                       candidate from the watchlist. Without this we can't see
                       which sports/leagues exist but never reach the scanner.

All writes are best-effort: a closed DB or busy lock returns False, never raises.
"""
from __future__ import annotations

import logging
import os
import sqlite3
from typing import Optional

log = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.getenv(
    "CALIBRATION_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.db"),
)


# ── Schema (lazy CREATE IF NOT EXISTS on every open — cheap, decouples startup) ─

_SCHEMA_SHADOW = """
CREATE TABLE IF NOT EXISTS shadow_signals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id            TEXT,
    market              TEXT,
    yes_price           REAL,
    best_bid            REAL,
    best_ask            REAL,
    book_depth_usdc     REAL,
    obi                 REAL,
    spike_has           INTEGER,
    spike_dominant_side TEXT,
    spike_confidence    REAL,
    spike_ratio         REAL,
    signal_source       TEXT,
    signal_strength     REAL,
    estimated_prob      REAL,
    edge                REAL,
    fill_price          REAL,
    outcome             TEXT NOT NULL,   -- 'accepted' | 'depth_skip' | 'no_vamp'
                                         -- | 'obi_solo_below' | 'obi_opposes'
                                         -- | 'price_ceiling' | 'edge_below'
                                         -- | 'cautious_obi_skip' | 'kelly_skip'
                                         -- | 'cooldown' | 'cb_active' | 'order_fail'
    created_at          TEXT NOT NULL
)
"""

_SCHEMA_POSTMORTEM = """
CREATE TABLE IF NOT EXISTS position_postmortem (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id              TEXT NOT NULL,
    market                TEXT,
    entry_price           REAL,
    entry_time            TEXT,
    exit_price            REAL,
    exit_time             TEXT,
    held_seconds          INTEGER,
    peak_bid              REAL,
    trough_bid            REAL,
    max_gain_pct          REAL,
    min_gain_pct          REAL,
    realized_gain_pct     REAL,
    exit_reason           TEXT,
    entry_signal_source   TEXT,
    entry_signal_strength REAL,
    profit_target         REAL,
    stop_loss             REAL,
    exit_order_id         TEXT,
    exit_status           TEXT,
    created_at            TEXT NOT NULL
)
"""

_SCHEMA_DISCOVERY = """
CREATE TABLE IF NOT EXISTS discovery_audit (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id          TEXT,
    question          TEXT,
    category          TEXT,
    internal_category TEXT,
    yes_price         REAL,
    volume_24h        REAL,
    liquidity         REAL,
    spread_cents      REAL,
    excluded_by       TEXT NOT NULL,    -- 'kept' | 'category' | 'keyword'
                                        -- | 'internal_category' | 'top_n'
                                        -- | 'spread' | 'price_range'
    created_at        TEXT NOT NULL
)
"""

_SCHEMA_EXTERNAL_FEEDS = """
CREATE TABLE IF NOT EXISTS external_feeds (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,        -- 'metaculus' | 'hn_top' | 'reddit_sportsbook'
                                        -- | 'gdelt' | 'news_bbc' | 'news_nyt'
                                        -- | 'news_reuters' | 'news_politico'
                                        -- | 'news_google' | 'nws_alerts'
                                        -- | 'usgs_quakes' | 'nhc_storms'
                                        -- | 'espn_nfl' | 'espn_nba' | 'espn_mlb'
                                        -- | 'espn_nhl' | 'espn_soccer' | 'mlb_schedule'
                                        -- | 'coingecko' | 'binance_ticker'
                                        -- | 'defillama_tvl' | 'mempool_fees'
    category      TEXT,                 -- 'news' | 'prediction_market' | 'social'
                                        -- | 'sports' | 'weather' | 'disaster'
                                        -- | 'crypto' | 'macro'
    external_id   TEXT NOT NULL,        -- per-source dedup key (article URL,
                                        -- market slug, event id, etc.)
    title         TEXT,
    url           TEXT,
    numeric_value REAL,                 -- price / probability / magnitude /
                                        -- score / vol — source-defined meaning
    metadata      TEXT,                 -- JSON blob with anything else worth
                                        -- keeping (raw event payload subset)
    published_at  TEXT,                 -- ISO8601 if source provides it
    fetched_at    TEXT NOT NULL,
    UNIQUE(source, external_id)
)
"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_shadow_time ON shadow_signals(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_shadow_outcome ON shadow_signals(outcome)",
    "CREATE INDEX IF NOT EXISTS idx_shadow_token ON shadow_signals(token_id)",
    "CREATE INDEX IF NOT EXISTS idx_postmortem_time ON position_postmortem(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_postmortem_reason ON position_postmortem(exit_reason)",
    "CREATE INDEX IF NOT EXISTS idx_discovery_time ON discovery_audit(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_discovery_excluded ON discovery_audit(excluded_by)",
    "CREATE INDEX IF NOT EXISTS idx_feeds_time ON external_feeds(fetched_at)",
    "CREATE INDEX IF NOT EXISTS idx_feeds_source ON external_feeds(source)",
    "CREATE INDEX IF NOT EXISTS idx_feeds_category ON external_feeds(category)",
)


def _connect(db_path: str = DEFAULT_DB_PATH) -> Optional[sqlite3.Connection]:
    """Open a write-capable connection with WAL mode + short timeout."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=3.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn
    except sqlite3.Error as exc:
        log.debug("observability _connect(%s) failed: %s", db_path, exc)
        return None


_SCHEMA_INITIALISED = False


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Idempotent CREATE TABLE IF NOT EXISTS for our three tables + indexes.
    Cheap to call repeatedly because SQLite no-ops the CREATE on existing tables.
    The module-level flag elides repeated work after the first successful run."""
    global _SCHEMA_INITIALISED
    if _SCHEMA_INITIALISED:
        return
    try:
        with conn:
            conn.execute(_SCHEMA_SHADOW)
            conn.execute(_SCHEMA_POSTMORTEM)
            conn.execute(_SCHEMA_DISCOVERY)
            conn.execute(_SCHEMA_EXTERNAL_FEEDS)
            for stmt in _INDEXES:
                conn.execute(stmt)
        _SCHEMA_INITIALISED = True
    except sqlite3.Error as exc:
        log.debug("observability _ensure_schema failed: %s", exc)


# ── Public writers ─────────────────────────────────────────────────────────────

def record_shadow_signal(
    *,
    token_id:            str,
    market:              str,
    outcome:             str,
    yes_price:           Optional[float] = None,
    best_bid:            Optional[float] = None,
    best_ask:            Optional[float] = None,
    book_depth_usdc:     Optional[float] = None,
    obi:                 Optional[float] = None,
    spike_has:           Optional[bool]  = None,
    spike_dominant_side: Optional[str]   = None,
    spike_confidence:    Optional[float] = None,
    spike_ratio:         Optional[float] = None,
    signal_source:       Optional[str]   = None,
    signal_strength:     Optional[float] = None,
    estimated_prob:      Optional[float] = None,
    edge:                Optional[float] = None,
    fill_price:          Optional[float] = None,
    db_path:             str             = DEFAULT_DB_PATH,
) -> bool:
    conn = _connect(db_path)
    if conn is None:
        return False
    try:
        _ensure_schema(conn)
        with conn:
            conn.execute(
                """
                INSERT INTO shadow_signals (
                    token_id, market, yes_price, best_bid, best_ask,
                    book_depth_usdc, obi, spike_has, spike_dominant_side,
                    spike_confidence, spike_ratio, signal_source,
                    signal_strength, estimated_prob, edge, fill_price,
                    outcome, created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now')
                )
                """,
                (
                    token_id, market, yes_price, best_bid, best_ask,
                    book_depth_usdc, obi,
                    int(spike_has) if spike_has is not None else None,
                    spike_dominant_side, spike_confidence, spike_ratio,
                    signal_source, signal_strength, estimated_prob, edge,
                    fill_price, outcome,
                ),
            )
        return True
    except sqlite3.Error as exc:
        log.debug("record_shadow_signal failed: %s", exc)
        return False
    finally:
        conn.close()


def record_shadow_batch(rows: list[dict], db_path: str = DEFAULT_DB_PATH) -> int:
    """Batch insert — single transaction. Returns number of rows written."""
    if not rows:
        return 0
    conn = _connect(db_path)
    if conn is None:
        return 0
    try:
        _ensure_schema(conn)
        params = [
            (
                r.get("token_id"), r.get("market"), r.get("yes_price"),
                r.get("best_bid"), r.get("best_ask"), r.get("book_depth_usdc"),
                r.get("obi"),
                int(r["spike_has"]) if r.get("spike_has") is not None else None,
                r.get("spike_dominant_side"), r.get("spike_confidence"),
                r.get("spike_ratio"), r.get("signal_source"),
                r.get("signal_strength"), r.get("estimated_prob"),
                r.get("edge"), r.get("fill_price"), r.get("outcome", "unknown"),
            )
            for r in rows
        ]
        with conn:
            conn.executemany(
                """
                INSERT INTO shadow_signals (
                    token_id, market, yes_price, best_bid, best_ask,
                    book_depth_usdc, obi, spike_has, spike_dominant_side,
                    spike_confidence, spike_ratio, signal_source,
                    signal_strength, estimated_prob, edge, fill_price,
                    outcome, created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now')
                )
                """,
                params,
            )
        return len(params)
    except sqlite3.Error as exc:
        log.debug("record_shadow_batch failed: %s", exc)
        return 0
    finally:
        conn.close()


def record_postmortem(
    *,
    token_id:              str,
    market:                Optional[str]   = None,
    entry_price:           Optional[float] = None,
    entry_time:            Optional[str]   = None,
    exit_price:            Optional[float] = None,
    exit_time:             Optional[str]   = None,
    held_seconds:          Optional[int]   = None,
    peak_bid:              Optional[float] = None,
    trough_bid:            Optional[float] = None,
    max_gain_pct:          Optional[float] = None,
    min_gain_pct:          Optional[float] = None,
    realized_gain_pct:     Optional[float] = None,
    exit_reason:           Optional[str]   = None,
    entry_signal_source:   Optional[str]   = None,
    entry_signal_strength: Optional[float] = None,
    profit_target:         Optional[float] = None,
    stop_loss:             Optional[float] = None,
    exit_order_id:         Optional[str]   = None,
    exit_status:           Optional[str]   = None,
    db_path:               str             = DEFAULT_DB_PATH,
) -> bool:
    conn = _connect(db_path)
    if conn is None:
        return False
    try:
        _ensure_schema(conn)
        with conn:
            conn.execute(
                """
                INSERT INTO position_postmortem (
                    token_id, market, entry_price, entry_time,
                    exit_price, exit_time, held_seconds,
                    peak_bid, trough_bid,
                    max_gain_pct, min_gain_pct, realized_gain_pct,
                    exit_reason, entry_signal_source, entry_signal_strength,
                    profit_target, stop_loss, exit_order_id, exit_status,
                    created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    datetime('now')
                )
                """,
                (
                    token_id, market, entry_price, entry_time,
                    exit_price, exit_time, held_seconds,
                    peak_bid, trough_bid,
                    max_gain_pct, min_gain_pct, realized_gain_pct,
                    exit_reason, entry_signal_source, entry_signal_strength,
                    profit_target, stop_loss, exit_order_id, exit_status,
                ),
            )
        return True
    except sqlite3.Error as exc:
        log.debug("record_postmortem failed: %s", exc)
        return False
    finally:
        conn.close()


def record_discovery_batch(rows: list[dict], db_path: str = DEFAULT_DB_PATH) -> int:
    """Batch insert for discovery audit. Returns number of rows written."""
    if not rows:
        return 0
    conn = _connect(db_path)
    if conn is None:
        return 0
    try:
        _ensure_schema(conn)
        params = [
            (
                r.get("token_id"), r.get("question"), r.get("category"),
                r.get("internal_category"), r.get("yes_price"),
                r.get("volume_24h"), r.get("liquidity"), r.get("spread_cents"),
                r.get("excluded_by", "unknown"),
            )
            for r in rows
        ]
        with conn:
            conn.executemany(
                """
                INSERT INTO discovery_audit (
                    token_id, question, category, internal_category,
                    yes_price, volume_24h, liquidity, spread_cents,
                    excluded_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                params,
            )
        return len(params)
    except sqlite3.Error as exc:
        log.debug("record_discovery_batch failed: %s", exc)
        return 0
    finally:
        conn.close()


def record_external_feeds(rows: list[dict], db_path: str = DEFAULT_DB_PATH) -> int:
    """Batch upsert into external_feeds.

    Dedupes on (source, external_id) via the UNIQUE index — rerunning a feed
    poller after a partial fetch is idempotent. Returns the number of NEW
    rows persisted (existing rows are silently skipped by INSERT OR IGNORE).
    """
    if not rows:
        return 0
    conn = _connect(db_path)
    if conn is None:
        return 0
    try:
        _ensure_schema(conn)
        params = [
            (
                r.get("source", "unknown"),
                r.get("category"),
                r.get("external_id", ""),
                r.get("title"),
                r.get("url"),
                r.get("numeric_value"),
                r.get("metadata"),
                r.get("published_at"),
            )
            for r in rows
            if r.get("external_id")    # rows without a dedup key are dropped
        ]
        if not params:
            return 0
        with conn:
            cur = conn.executemany(
                """
                INSERT OR IGNORE INTO external_feeds (
                    source, category, external_id, title, url,
                    numeric_value, metadata, published_at, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                params,
            )
            return cur.rowcount if cur.rowcount is not None else 0
    except sqlite3.Error as exc:
        log.debug("record_external_feeds failed: %s", exc)
        return 0
    finally:
        conn.close()
