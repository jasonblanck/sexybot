"""
orderbook_ws.py
Polymarket V2 — Real-time CLOB WebSocket Order Book
Endpoint : wss://ws-subscriptions-clob.polymarket.com/ws/market
Collateral: PMUSD | Sig Type: EOA (0) | Chain: Polygon (137)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Optional, Tuple

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

log = logging.getLogger(__name__)

WS_URL           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RECONNECT_DELAY  = 2.0
RECONNECT_MAX    = 60.0
PING_INTERVAL    = 20.0
PING_TIMEOUT     = 10.0
OBI_DEPTH        = 5
STALE_THRESHOLD  = 30.0
MID_HISTORY_LEN  = 60      # last 60 mid observations for volatility estimation
MIN_VAMP_VOL     = 0.5     # min outcome-token volume on each side before VAMP is trusted


@dataclass
class Level:
    price: float
    size:  float


@dataclass
class BookSnapshot:
    token_id:    str
    bids:        list[Level] = field(default_factory=list)
    asks:        list[Level] = field(default_factory=list)
    timestamp:   float       = field(default_factory=time.time)
    sequence:    int         = 0
    # (ts, mid) observations used for volatility / regime estimation.
    # Populated by OrderBook.snapshot(); default empty for hand-built snapshots in tests.
    mid_history: list[Tuple[float, float]] = field(default_factory=list)

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return round((self.best_bid + self.best_ask) / 2, 6)

    @property
    def vamp(self) -> Optional[float]:
        """
        Volume-Adjusted Mid Price — depth-weighted average across both sides.

        Returns None (not mid) when either side lacks meaningful volume.
        This prevents a stale/phantom book — one with zero-size top levels
        or only one side populated — from silently producing a mid-price
        signal that callers would treat as reliable. Call sites that still
        want a best-effort fallback use the `book.vamp or book.mid` idiom.
        """
        bid_lvs = self.bids[:5]
        ask_lvs = self.asks[:5]
        if not bid_lvs or not ask_lvs:
            return None
        bid_total = sum(lv.size for lv in bid_lvs)
        ask_total = sum(lv.size for lv in ask_lvs)
        if bid_total < MIN_VAMP_VOL or ask_total < MIN_VAMP_VOL:
            return None
        bid_vwap = sum(lv.price * lv.size for lv in bid_lvs) / bid_total
        ask_vwap = sum(lv.price * lv.size for lv in ask_lvs) / ask_total
        return round((bid_vwap + ask_vwap) / 2, 6)

    @property
    def spread_cents(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return round((self.best_ask - self.best_bid) * 100, 4)

    @property
    def obi(self) -> float:
        bid_vol = sum(lv.size for lv in self.bids[:OBI_DEPTH])
        ask_vol = sum(lv.size for lv in self.asks[:OBI_DEPTH])
        total   = bid_vol + ask_vol
        if total == 0:
            return 0.0
        return round((bid_vol - ask_vol) / total, 4)

    @property
    def is_stale(self) -> bool:
        return (time.time() - self.timestamp) > STALE_THRESHOLD

    def bid_depth(self, levels: int = 5) -> float:
        return sum(lv.price * lv.size for lv in self.bids[:levels])

    def ask_depth(self, levels: int = 5) -> float:
        return sum(lv.price * lv.size for lv in self.asks[:levels])

    @property
    def book_depth_usdc(self) -> Optional[float]:
        """Combined USDC notional on top-5 levels both sides. None if a side is empty."""
        if not self.bids or not self.asks:
            return None
        return round(self.bid_depth(5) + self.ask_depth(5), 4)

    @property
    def mid_volatility(self) -> Optional[float]:
        """
        Rolling stddev of recent mid observations (fraction units, e.g. 0.012 = 1.2c).
        Returns None until we have enough history (≥8 points).
        Used to shrink Kelly / widen exit bands in choppy markets.
        """
        if len(self.mid_history) < 8:
            return None
        mids = [m for _, m in self.mid_history]
        mean = sum(mids) / len(mids)
        var  = sum((m - mean) ** 2 for m in mids) / len(mids)
        return round(var ** 0.5, 6)

    def recent_obi_trend(self, window_sec: float = 30.0) -> Optional[float]:
        """
        Approximate recent OBI direction as a simple signed momentum:
        (newest_mid - oldest_mid_in_window) / oldest_mid.
        Positive → bid-side pressure; negative → ask-side pressure.
        Returns None if history too short.
        """
        if len(self.mid_history) < 4:
            return None
        now = time.time()
        windowed = [(t, m) for t, m in self.mid_history if now - t <= window_sec]
        if len(windowed) < 4:
            return None
        oldest = windowed[0][1]
        newest = windowed[-1][1]
        if oldest <= 0:
            return None
        return round((newest - oldest) / oldest, 6)


class OrderBook:
    def __init__(self, token_id: str):
        self.token_id  = token_id
        self._bids:    dict[float, float] = {}
        self._asks:    dict[float, float] = {}
        self._seq:     int   = 0
        self._updated: float = 0.0
        # Bounded mid-price history for volatility estimation.
        self._mid_history: Deque[Tuple[float, float]] = deque(maxlen=MID_HISTORY_LEN)

    def _record_mid(self) -> None:
        """Append current (ts, mid) if both sides are populated with real volume."""
        bb = next((p for p, s in sorted(self._bids.items(), reverse=True) if s > 0), None)
        ba = next((p for p, s in sorted(self._asks.items())               if s > 0), None)
        if bb is not None and ba is not None:
            self._mid_history.append((time.time(), (bb + ba) / 2))

    def _apply_snapshot(self, bids: list[dict], asks: list[dict]) -> None:
        self._bids = {float(b["price"]): float(b["size"]) for b in bids}
        self._asks = {float(a["price"]): float(a["size"]) for a in asks}
        self._updated = time.time()
        self._record_mid()

    def _apply_delta(self, changes: list[dict]) -> None:
        for ch in changes:
            price = float(ch["price"])
            size  = float(ch["size"])
            book  = self._bids if ch["side"] == "BUY" else self._asks
            if size == 0.0:
                book.pop(price, None)
            else:
                book[price] = size
        self._updated = time.time()
        self._record_mid()

    def snapshot(self) -> BookSnapshot:
        bids = [Level(p, s) for p, s in sorted(self._bids.items(), reverse=True) if s > 0]
        asks = [Level(p, s) for p, s in sorted(self._asks.items())               if s > 0]
        return BookSnapshot(
            token_id    = self.token_id,
            bids        = bids,
            asks        = asks,
            timestamp   = self._updated,
            sequence    = self._seq,
            mid_history = list(self._mid_history),
        )


BookCallback = Callable[[BookSnapshot], None]


class ClobWebSocket:
    def __init__(self, token_ids: list[str], on_book_update: Optional[BookCallback] = None):
        if not token_ids:
            raise ValueError("token_ids must not be empty")
        self.token_ids   = token_ids
        self._callback   = on_book_update
        self._books:     dict[str, OrderBook] = {tid: OrderBook(tid) for tid in token_ids}
        self._running    = False
        self._ws         = None
        self._reconnects = 0

    def get_book(self, token_id: str) -> Optional[BookSnapshot]:
        ob = self._books.get(token_id)
        return ob.snapshot() if ob else None

    def get_all_books(self) -> dict[str, BookSnapshot]:
        return {tid: ob.snapshot() for tid, ob in self._books.items()}

    async def stop(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def run(self) -> None:
        self._running = True
        self._backoff = RECONNECT_DELAY
        while self._running:
            try:
                await self._connect_and_consume()
                self._backoff = RECONNECT_DELAY
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                if not self._running:
                    break
                self._reconnects += 1
                log.warning("WS disconnected (reconnect #%d, retry in %.1fs): %s", self._reconnects, self._backoff, exc)
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, RECONNECT_MAX)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("Unexpected WS error: %s", exc)
                await asyncio.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, RECONNECT_MAX)

    async def _connect_and_consume(self) -> None:
        log.info("WS connecting → %s", WS_URL)
        async with websockets.connect(
            WS_URL,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
            max_size=2**23,
            open_timeout=15,
        ) as ws:
            self._ws = ws
            log.info("WS connected — subscribing to %d token(s)", len(self.token_ids))
            await self._subscribe(ws)
            # Subscribe succeeded — reset backoff so a later drop doesn't
            # inherit a 60 s retry delay from a previous short outage.
            self._backoff = RECONNECT_DELAY
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                await self._dispatch(msg)

    async def _subscribe(self, ws) -> None:
        payload = json.dumps({"assets_ids": self.token_ids, "type": "market"})
        await ws.send(payload)
        log.debug("WS subscribed: %s", payload[:120])

    async def _dispatch(self, msg: dict | list) -> None:
        if isinstance(msg, list):
            for item in msg:
                await self._dispatch(item)
            return

        event = msg.get("event_type", "")
        token = msg.get("asset_id", "")

        if not token or token not in self._books:
            return

        ob = self._books[token]

        if event == "book":
            ob._seq += 1
            ob._apply_snapshot(bids=msg.get("bids", []), asks=msg.get("asks", []))
            log.debug("SNAP  %s… bids=%d asks=%d mid=%.4f obi=%+.3f",
                      token[:12], len(ob._bids), len(ob._asks),
                      ob.snapshot().mid or 0, ob.snapshot().obi)
        elif event == "price_change":
            ob._seq += 1
            ob._apply_delta(msg.get("changes", []))
        elif event in ("tick_size_change", "last_trade_price"):
            pass
        else:
            log.debug("WS unknown event_type=%s", event)
            return

        if self._callback is not None:
            snap = ob.snapshot()
            if asyncio.iscoroutinefunction(self._callback):
                await self._callback(snap)
            else:
                self._callback(snap)


class BookManager:
    """
    Manages live order books for multiple markets over a minimal number of
    WebSocket connections.

    All token IDs are batched into a single ClobWebSocket (up to TOKENS_PER_CONN
    per connection). This replaces the previous design that created one connection
    per market pair, which opened 20+ connections for 20 markets.

    Usage
    -----
    mgr = BookManager()
    mgr.add_market(yes_token_id, no_token_id)   # call before run()
    await mgr.run()                              # blocks; use create_task()
    snap = mgr.get_book(token_id)
    """

    TOKENS_PER_CONN = 80   # Polymarket WS handles ~100 tokens; stay conservative

    def __init__(self, on_update: Optional[BookCallback] = None):
        self._on_update   = on_update
        self._pending:    list[str]       = []   # all token IDs to subscribe
        self._ws_tasks:   list[asyncio.Task]     = []
        self._clients:    list[ClobWebSocket]    = []

    def add_market(self, yes_token_id: str, no_token_id: str) -> None:
        """Register a market pair. Must be called before run()."""
        for tid in (yes_token_id, no_token_id):
            if tid not in self._pending:
                self._pending.append(tid)

    def get_book(self, token_id: str) -> Optional[BookSnapshot]:
        """Return the latest snapshot for a token, or None if not yet received."""
        for client in self._clients:
            snap = client.get_book(token_id)
            if snap is not None:
                return snap
        return None

    async def run(self) -> None:
        """
        Batch all registered tokens into connections of TOKENS_PER_CONN and
        launch them concurrently. Blocks until all connections stop.
        """
        if not self._pending:
            log.warning("BookManager.run() called with no tokens registered")
            return

        # Chunk tokens into batches
        batches = [
            self._pending[i : i + self.TOKENS_PER_CONN]
            for i in range(0, len(self._pending), self.TOKENS_PER_CONN)
        ]

        self._clients = [
            ClobWebSocket(token_ids=batch, on_book_update=self._on_update)
            for batch in batches
        ]

        log.info(
            "BookManager: %d token(s) across %d WS connection(s)",
            len(self._pending), len(self._clients),
        )

        self._ws_tasks = [
            asyncio.create_task(client.run(), name=f"ws-{i}")
            for i, client in enumerate(self._clients)
        ]
        await asyncio.gather(*self._ws_tasks, return_exceptions=True)

    async def stop(self) -> None:
        for client in self._clients:
            await client.stop()
        for task in self._ws_tasks:
            task.cancel()
        # Await the cancellations so stop() doesn't return while WS coroutines
        # are still finalising — otherwise callers that immediately tear down
        # the loop can leak half-cancelled tasks.
        if self._ws_tasks:
            await asyncio.gather(*self._ws_tasks, return_exceptions=True)
