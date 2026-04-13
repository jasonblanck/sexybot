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
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Optional

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


@dataclass
class Level:
    price: float
    size:  float


@dataclass
class BookSnapshot:
    token_id:  str
    bids:      list[Level] = field(default_factory=list)
    asks:      list[Level] = field(default_factory=list)
    timestamp: float       = field(default_factory=time.time)
    sequence:  int         = 0

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

        Uses the VWAP of the top-5 bid levels and the top-5 ask levels, then
        averages them. More accurate than simple mid when one side is thicker:
        e.g. if bids are concentrated near the ask, VAMP > mid, correctly
        signalling a higher fair value.

        Falls back to simple mid if either side is empty.
        """
        bid_lvs = self.bids[:5]
        ask_lvs = self.asks[:5]
        if not bid_lvs or not ask_lvs:
            return self.mid
        bid_total = sum(lv.size for lv in bid_lvs)
        ask_total = sum(lv.size for lv in ask_lvs)
        if bid_total == 0 or ask_total == 0:
            return self.mid
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


class OrderBook:
    def __init__(self, token_id: str):
        self.token_id  = token_id
        self._bids:    dict[float, float] = {}
        self._asks:    dict[float, float] = {}
        self._seq:     int   = 0
        self._updated: float = 0.0

    def _apply_snapshot(self, bids: list[dict], asks: list[dict]) -> None:
        self._bids = {float(b["price"]): float(b["size"]) for b in bids}
        self._asks = {float(a["price"]): float(a["size"]) for a in asks}
        self._updated = time.time()

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

    def snapshot(self) -> BookSnapshot:
        bids = [Level(p, s) for p, s in sorted(self._bids.items(), reverse=True) if s > 0]
        asks = [Level(p, s) for p, s in sorted(self._asks.items())               if s > 0]
        return BookSnapshot(
            token_id  = self.token_id,
            bids      = bids,
            asks      = asks,
            timestamp = self._updated,
            sequence  = self._seq,
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
        delay = RECONNECT_DELAY
        while self._running:
            try:
                await self._connect_and_consume()
                delay = RECONNECT_DELAY
            except (ConnectionClosed, WebSocketException, OSError) as exc:
                if not self._running:
                    break
                self._reconnects += 1
                log.warning("WS disconnected (reconnect #%d, retry in %.1fs): %s", self._reconnects, delay, exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("Unexpected WS error: %s", exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)

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
