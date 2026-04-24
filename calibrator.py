"""
calibrator.py
Self-learning probability calibration + regime reader for the trading loop.

Two pieces that both read from the shared trades.db (same schema that
bot.py creates):

  Calibrator    — reads resolved rows from `brier_scores`, learns the
                  systematic bias in our predicted_prob, and exposes
                  `.adjust(prob)` so the signal path can shrink over-
                  confident estimates toward the realized base rate.
                  Filters by a `source` column so different prediction
                  models (bot.py's Claude signals vs main_v2.py's
                  momentum signals) calibrate independently.

  RegimeReader  — reads the latest row from `regime_log` (written by
                  bot.py's Claude regime detector) and exposes the
                  current regime label + kelly/edge overrides. main_v2.py
                  uses these to throttle or halt risk-taking when the
                  detector has flagged a hostile macro regime.

  record_prediction — best-effort append to brier_scores when a new
                      trade is placed, so the Calibrator has fresh
                      training data next cycle.

All three no-op silently when the DB / table doesn't exist so local-dev
and fresh installs keep working.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

DEFAULT_DB_PATH = os.getenv(
    "CALIBRATION_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "trades.db"),
)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _connect(db_path: str) -> Optional[sqlite3.Connection]:
    """Open a read-capable connection, or return None if the file is missing."""
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False, timeout=3.0)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn
    except sqlite3.Error as exc:
        log.debug("calibrator _connect(%s) failed: %s", db_path, exc)
        return None


def _ensure_source_column(conn: sqlite3.Connection) -> None:
    """
    Add `source` column to brier_scores if it doesn't exist.
    Lets multiple prediction models share the same table without their
    biases contaminating each other's calibration.
    """
    try:
        conn.execute("ALTER TABLE brier_scores ADD COLUMN source TEXT")
        conn.commit()
        log.info("calibrator: added source column to brier_scores")
    except sqlite3.OperationalError:
        pass   # already present, or table doesn't exist yet


# ── Calibrator ─────────────────────────────────────────────────────────────────

@dataclass
class CalibrationStats:
    samples:     int
    mean_bias:   float   # mean(predicted - actual)  — positive = we overpredict
    mae:         float   # mean absolute error
    high_conf_bias: float   # bias restricted to |pred - 0.5| > 0.2
    generated_at:   float


class Calibrator:
    """
    Mean-bias probability calibrator.

    Principle
    ---------
    If our recent resolved predictions have a systematic bias
    (mean(predicted − actual) > 0), we are overpredicting — shrink future
    predictions toward 0.5 by the measured bias, clipped to [−0.15, +0.15]
    so a few noisy resolutions can't catastrophically flip the signal.

    Edge cases
    ----------
    • <30 resolved samples → no correction (stats too noisy).
    • Missing DB / table   → no-op, `.adjust(p)` returns `p` unchanged.
    • Stale stats          → reloaded every `reload_every_sec`.
    • Prediction is always clamped to [0.02, 0.98] after adjustment.

    Usage
    -----
        cal = Calibrator(source="momentum_v2")
        true_prob = cal.adjust(raw_true_prob)
    """

    MAX_SHRINK      = 0.15
    DEFAULT_RELOAD  = 900         # 15 min
    MIN_SAMPLES     = 30
    LOOKBACK_DAYS   = 30

    def __init__(
        self,
        *,
        db_path:          str  = DEFAULT_DB_PATH,
        source:           str  = "momentum_v2",
        reload_every_sec: float = DEFAULT_RELOAD,
        min_samples:      int  = MIN_SAMPLES,
    ):
        self.db_path          = db_path
        self.source           = source
        self.reload_every_sec = reload_every_sec
        self.min_samples      = min_samples
        self._stats: Optional[CalibrationStats] = None
        self._last_load = 0.0

    # ── Public API ─────────────────────────────────────────────────────────────

    def adjust(self, prob: float) -> float:
        """Return the calibrated probability, clamped to [0.02, 0.98]."""
        self._maybe_reload()
        if self._stats is None:
            return self._clamp(prob)
        # Shrink toward 0.5 by the measured bias, but only in proportion to
        # how far from 0.5 the prediction is — bias is magnified at the tails.
        bias      = self._stats.mean_bias
        tailness  = abs(prob - 0.5) * 2         # 0 at 0.5, 1 at the tails
        delta     = max(-self.MAX_SHRINK, min(self.MAX_SHRINK, bias)) * tailness
        adjusted  = prob - delta
        return round(self._clamp(adjusted), 4)

    def stats(self) -> Optional[CalibrationStats]:
        self._maybe_reload()
        return self._stats

    # ── Internals ──────────────────────────────────────────────────────────────

    @staticmethod
    def _clamp(p: float) -> float:
        return max(0.02, min(0.98, p))

    def _maybe_reload(self) -> None:
        if time.time() - self._last_load < self.reload_every_sec:
            return
        self._last_load = time.time()

        conn = _connect(self.db_path)
        if conn is None:
            return
        try:
            _ensure_source_column(conn)
            # Only rows for this prediction source, resolved, within lookback.
            # A source='NULL'-tolerant query so early rows (before source was
            # populated) don't poison calibration.
            rows = conn.execute(
                """
                SELECT predicted_prob, actual_outcome
                FROM brier_scores
                WHERE resolved = 1
                  AND predicted_prob IS NOT NULL
                  AND actual_outcome IS NOT NULL
                  AND (source = ? OR (? = 'default'))
                  AND time >= datetime('now', ?)
                """,
                (self.source, self.source, f"-{self.LOOKBACK_DAYS} days"),
            ).fetchall()
        except sqlite3.Error as exc:
            log.debug("calibrator reload failed: %s", exc)
            return
        finally:
            conn.close()

        if len(rows) < self.min_samples:
            log.debug("calibrator: %d samples < %d min; keeping no-op", len(rows), self.min_samples)
            self._stats = None
            return

        n               = len(rows)
        signed_errors   = [float(p) - float(a) for p, a in rows]
        abs_errors      = [abs(e) for e in signed_errors]
        mean_bias       = sum(signed_errors) / n
        mae             = sum(abs_errors)    / n
        hi              = [e for e, (p, _) in zip(signed_errors, rows) if abs(float(p) - 0.5) > 0.2]
        hi_bias         = (sum(hi) / len(hi)) if hi else 0.0
        self._stats = CalibrationStats(
            samples        = n,
            mean_bias      = round(mean_bias, 4),
            mae            = round(mae, 4),
            high_conf_bias = round(hi_bias, 4),
            generated_at   = time.time(),
        )
        log.info(
            "Calibrator loaded | source=%s n=%d mean_bias=%+.3f mae=%.3f hi_conf_bias=%+.3f",
            self.source, n, mean_bias, mae, hi_bias,
        )


# ── Regime reader ──────────────────────────────────────────────────────────────

@dataclass
class RegimeState:
    """Snapshot of the latest bot.py regime-detector decision."""
    regime:     str                # "normal" | "cautious" | "hostile"
    kelly_mult: Optional[float]    # multiplier applied to Kelly sizing, None = use local
    min_edge:   Optional[float]    # min edge override, None = use local
    reasoning:  str
    created_at: str
    age_sec:    float              # how old the decision is relative to read time

    @property
    def is_hostile(self) -> bool:
        return (self.regime or "").lower() == "hostile"

    @property
    def is_cautious(self) -> bool:
        return (self.regime or "").lower() == "cautious"


class RegimeReader:
    """
    Polls the shared regime_log table every `reload_every_sec`.

    • `current()` → most recent RegimeState, or None if the table / DB is
      missing, or if the latest entry is older than `max_age_sec`.
    • Cheap: one SELECT per reload, cached in memory otherwise.
    • Silent on every error so the trading loop never crashes on a stale DB.
    """

    DEFAULT_RELOAD  = 120           # 2 min
    DEFAULT_MAX_AGE = 4 * 3600      # 4 h — regime decisions older than this are ignored

    def __init__(
        self,
        *,
        db_path:          str   = DEFAULT_DB_PATH,
        reload_every_sec: float = DEFAULT_RELOAD,
        max_age_sec:      float = DEFAULT_MAX_AGE,
    ):
        self.db_path          = db_path
        self.reload_every_sec = reload_every_sec
        self.max_age_sec      = max_age_sec
        self._state: Optional[RegimeState] = None
        self._last_load = 0.0

    def current(self) -> Optional[RegimeState]:
        self._maybe_reload()
        if self._state is None:
            return None
        if self._state.age_sec > self.max_age_sec:
            return None
        return self._state

    def _maybe_reload(self) -> None:
        if time.time() - self._last_load < self.reload_every_sec:
            return
        self._last_load = time.time()

        conn = _connect(self.db_path)
        if conn is None:
            self._state = None
            return
        try:
            row = conn.execute(
                """
                SELECT regime, kelly_mult, min_edge, reasoning, created_at
                FROM regime_log
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        except sqlite3.Error as exc:
            log.debug("RegimeReader reload failed: %s", exc)
            self._state = None
            return
        finally:
            conn.close()

        if not row:
            self._state = None
            return

        regime, kelly_mult, min_edge, reasoning, created_at = row
        age = _age_seconds(created_at)
        self._state = RegimeState(
            regime     = str(regime or "normal"),
            kelly_mult = float(kelly_mult) if kelly_mult is not None else None,
            min_edge   = float(min_edge)   if min_edge   is not None else None,
            reasoning  = str(reasoning or ""),
            created_at = str(created_at or ""),
            age_sec    = age,
        )


def _age_seconds(created_at: Optional[str]) -> float:
    """Parse a `YYYY-MM-DD HH:MM:SS[±TZ]` string to seconds-since-now. ∞ on parse failure."""
    if not created_at:
        return float("inf")
    s = created_at.replace("Z", "+00:00")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S.%f",   "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (datetime.now(tz=timezone.utc) - dt).total_seconds())
        except ValueError:
            continue
    return float("inf")


# ── Prediction logger ──────────────────────────────────────────────────────────

def record_prediction(
    *,
    db_path:        str = DEFAULT_DB_PATH,
    source:         str,
    market:         str,
    token_id:       str,
    side:           str,
    predicted_prob: float,
    market_price:   float,
    kelly_fraction: Optional[float] = None,
    kelly_size:     Optional[float] = None,
    ai_reasoning:   Optional[str]   = None,
) -> bool:
    """
    Best-effort insert into brier_scores. Returns True on success.

    Safe to call from the trading hot path — the whole body is wrapped in
    try/except and the sqlite3 connection uses a short timeout so a busy
    writer can never stall the trading loop.
    """
    conn = _connect(db_path)
    if conn is None:
        return False
    try:
        _ensure_source_column(conn)
        conn.execute(
            """
            INSERT INTO brier_scores
                (market, token_id, side, predicted_prob, market_price,
                 kelly_fraction, kelly_size, ai_reasoning, time, source, resolved)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, 0)
            """,
            (market, token_id, side, float(predicted_prob), float(market_price),
             kelly_fraction, kelly_size, ai_reasoning, source),
        )
        conn.commit()
        return True
    except sqlite3.Error as exc:
        log.debug("record_prediction failed: %s", exc)
        return False
    finally:
        conn.close()
