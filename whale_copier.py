"""
whale_copier.py
Polymarket V2 — Whale Copy-Trading & Smart Money Sentiment Engine
Queries Polymarket Data API (/trades) to detect real-time institutional whale consensus
and generates momentum probability boost signals.
"""

import time
import logging
from dataclasses import dataclass
from typing import Dict, Optional
import requests

log = logging.getLogger(__name__)

TRADES_API_URL = "https://data-api.polymarket.com/trades"


@dataclass
class WhaleConsensusSignal:
    token_id: str
    whale_count: int
    total_volume_usd: float
    latest_price: float
    boost: float   # e.g. +0.08 (8% probability boost)


class WhaleCopierEngine:
    def __init__(self, min_trade_usd: float = 200.0, window_sec: float = 1800.0, cache_ttl_sec: float = 60.0):
        self.min_trade_usd = min_trade_usd
        self.window_sec     = window_sec
        self.cache_ttl_sec  = cache_ttl_sec
        self._last_fetch_ts = 0.0
        self._whale_signals: Dict[str, WhaleConsensusSignal] = {}

    def fetch_whale_signals(self) -> Dict[str, WhaleConsensusSignal]:
        now = time.time()
        if now - self._last_fetch_ts < self.cache_ttl_sec:
            return self._whale_signals

        try:
            resp = requests.get(f"{TRADES_API_URL}?limit=100", headers={"User-Agent": "SexyBot/2.0"}, timeout=5)
            if resp.status_code != 200:
                log.warning("Polymarket trades API returned status %d", resp.status_code)
                return self._whale_signals

            trades = resp.json()
            whale_trades_by_token: Dict[str, list] = {}

            for t in trades:
                side = t.get("side", "")
                if side != "BUY":
                    continue

                size = float(t.get("size", 0) or 0)
                price = float(t.get("price", 0) or 0)
                usd_val = size * price
                ts = float(t.get("timestamp", 0) or 0)

                # Filter for trades within time window and above USD threshold
                if (now - ts <= self.window_sec) and (usd_val >= self.min_trade_usd):
                    token_id = str(t.get("asset", ""))
                    if token_id:
                        if token_id not in whale_trades_by_token:
                            whale_trades_by_token[token_id] = []
                        whale_trades_by_token[token_id].append((usd_val, price, ts))

            new_signals: Dict[str, WhaleConsensusSignal] = {}
            for token_id, trades_list in whale_trades_by_token.items():
                total_usd = sum(usd for usd, p, t in trades_list)
                trade_count = len(trades_list)
                latest_price = trades_list[0][1]

                # Trigger boost if >= 2 large whale trades or total > $500
                if trade_count >= 2 or total_usd >= 500.0:
                    boost = 0.08 if total_usd >= 1000.0 else 0.05
                    sig = WhaleConsensusSignal(
                        token_id=token_id,
                        whale_count=trade_count,
                        total_volume_usd=total_usd,
                        latest_price=latest_price,
                        boost=boost,
                    )
                    new_signals[token_id] = sig
                    log.info("WHALE CONSENSUS DETECTED 🐋 | token=%s... count=%d total=$%.2f boost=+%.2f",
                             token_id[:14], trade_count, total_usd, boost)

            self._whale_signals = new_signals
            self._last_fetch_ts = now
            return self._whale_signals

        except Exception as exc:
            log.error("Failed to fetch whale signals: %s", exc)
            return self._whale_signals

    def get_whale_boost(self, token_id: str) -> float:
        signals = self.fetch_whale_signals()
        sig = signals.get(token_id)
        return sig.boost if sig else 0.0
