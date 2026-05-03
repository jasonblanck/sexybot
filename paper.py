"""
PolymarketPaperHandler — paper trading middleware for Sexybot.

Toggle via PAPER_MODE=true in .env.
All log lines prefixed [PAPER] to prevent confusion with live trading.

Architecture:
  - Mirrors place_market_order / place_limit_order signatures from PolymarketBot
  - Persists state in paper_* tables inside the shared trades.db (WAL mode)
  - Fetches live CLOB order book data for realistic fill simulation
  - Simulates price impact, gas fees, and random tx failures
  - Background oracle settles positions when markets resolve
"""
import sqlite3, json, logging, time, random, asyncio
from datetime import datetime, timezone
import urllib.request as _ureq

log = logging.getLogger("polybot")


def _utcnow() -> datetime:
    # Naive UTC datetime — replacement for the deprecated stdlib utcnow().
    # Kept naive (tzinfo=None) so existing isoformat() strings in the DB
    # continue to compare correctly with SQLite's datetime('now'), which
    # is also a naive UTC string.
    return datetime.now(timezone.utc).replace(tzinfo=None)

# ── Constants ─────────────────────────────────────────────────────────────────
PAPER_GAS_FEE          = 0.02    # USDC deducted per transaction (Polygon gas proxy)
PRICE_IMPACT_THRESHOLD = 0.10    # fraction of top-of-book depth that triggers slippage
FAILURE_RATE           = 0.005   # 0.5% simulated RPC/revert failure rate
CLOB_HOST              = "https://clob.polymarket.com"

_HEADERS = {
    "User-Agent": "sexybot-paper/1.0",
    "Accept":     "application/json",
    "Origin":     "https://polymarket.com",
    "Referer":    "https://polymarket.com/",
}


class PolymarketPaperHandler:
    """
    Drop-in paper trading middleware.

    Usage in bot.py:
        if PAPER_MODE:
            result = self.paper.execute_market_order(token_id, side, amount, market)
        else:
            result = self.place_market_order(token_id, side, amount, market)
    """

    def __init__(self, db: sqlite3.Connection, starting_balance: float = 1000.0):
        self.db = db
        self._init_tables(starting_balance)

    # ── DB init ───────────────────────────────────────────────────────────────

    def _init_tables(self, starting_balance: float):
        with self.db:
            self.db.executescript("""
                CREATE TABLE IF NOT EXISTS paper_settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_positions (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_id   TEXT UNIQUE NOT NULL,
                    market     TEXT,
                    side       TEXT,
                    shares     REAL,
                    avg_price  REAL,
                    cost       REAL,
                    opened_at  TEXT
                );
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_id     TEXT,
                    market       TEXT,
                    side         TEXT,
                    shares       REAL,
                    fill_price   REAL,
                    amount_usdc  REAL,
                    gas_fee      REAL,
                    price_impact REAL,
                    status       TEXT,
                    settled_pnl  REAL,
                    time         TEXT
                );
            """)
            # Seed balance only on first run — never overwrite an existing paper portfolio
            if not self.db.execute(
                "SELECT 1 FROM paper_settings WHERE key='balance'"
            ).fetchone():
                self.db.execute(
                    "INSERT INTO paper_settings (key, value) VALUES ('balance',?),('starting_balance',?)",
                    (str(starting_balance), str(starting_balance))
                )
        log.info(f"[PAPER] Paper trading ready — balance: ${self.get_balance():.2f}")

    # ── Balance ───────────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        row = self.db.execute(
            "SELECT value FROM paper_settings WHERE key='balance'"
        ).fetchone()
        return float(row[0]) if row else 0.0

    def _adjust_balance(self, delta: float):
        # Single atomic SQL UPDATE — avoids read-modify-write race if called concurrently
        with self.db:
            self.db.execute(
                "UPDATE paper_settings SET value = CAST(ROUND(CAST(value AS REAL) + ?, 6) AS TEXT) WHERE key='balance'",
                (delta,)
            )

    # ── CLOB data ─────────────────────────────────────────────────────────────

    def _fetch_orderbook(self, token_id: str) -> dict:
        """Fetch live order book — same data the live bot trades against."""
        try:
            url = f"{CLOB_HOST}/book?token_id={token_id}"
            req = _ureq.Request(url, headers=_HEADERS)
            with _ureq.urlopen(req, timeout=5) as r:
                return json.loads(r.read())
        except Exception as e:
            log.debug(f"[PAPER] orderbook fetch failed ({token_id[:16]}…): {e}")
            return {}

    def _simulate_fill(self, token_id: str, side: str, usdc_amount: float) -> dict:
        """
        Walk the live order book to simulate a market order fill.

        BUY (YES/BUY) → walks asks from lowest to highest
        SELL (NO)     → walks bids from highest to lowest

        If order size > PRICE_IMPACT_THRESHOLD of top-3 depth:
          apply 1–3 tick (1–3%) slippage in the adverse direction.

        Returns fill result dict.
        """
        book = self._fetch_orderbook(token_id)
        bids  = book.get("bids", [])
        asks  = book.get("asks", [])

        best_bid = float(bids[0]["price"]) if bids else 0.45
        best_ask = float(asks[0]["price"]) if asks else 0.55
        mid = round((best_bid + best_ask) / 2, 6)

        buying = side in ("BUY", "YES")
        levels = asks if buying else bids

        if not levels:
            # No book data → fill at mid price
            return {
                "price": mid,
                "shares": round(usdc_amount / mid, 4) if mid > 0 else 0,
                "price_impact": 0.0,
                "note": "mid-price fallback (empty book)",
            }

        # Walk levels
        remaining    = usdc_amount
        total_shares = 0.0
        total_cost   = 0.0
        top3_depth_usdc = sum(float(l.get("size", 0)) * float(l["price"]) for l in levels[:3])

        for level in levels:
            lp     = float(level["price"])
            l_usdc = float(level.get("size", 0)) * lp
            fill   = min(remaining, l_usdc)
            shares = fill / lp if lp > 0 else 0
            total_shares += shares
            total_cost   += fill
            remaining    -= fill
            if remaining <= 0.001:
                break

        if total_shares == 0:
            total_shares = usdc_amount / mid if mid > 0 else 0
            total_cost   = usdc_amount

        fill_price   = round(total_cost / total_shares, 6) if total_shares > 0 else mid
        price_impact = 0.0

        # Price impact for large orders
        if top3_depth_usdc > 0 and (usdc_amount / top3_depth_usdc) > PRICE_IMPACT_THRESHOLD:
            ticks = round(random.uniform(0.01, 0.03), 3)
            price_impact = ticks
            if buying:
                fill_price = min(0.999, fill_price + ticks)
            else:
                fill_price = max(0.001, fill_price - ticks)
            shares_adj = round(usdc_amount / fill_price, 4) if fill_price > 0 else total_shares
            log.info(
                f"[PAPER] Price impact +{ticks:.3f} ticks "
                f"(${usdc_amount:.2f} order > 10% of ${top3_depth_usdc:.1f} book depth)"
            )
            return {"price": round(fill_price, 4), "shares": shares_adj,
                    "price_impact": price_impact, "note": "price impact applied"}

        return {
            "price":        round(fill_price, 4),
            "shares":       round(total_shares, 4),
            "price_impact": 0.0,
            "note":         f"walked {min(len(levels), 5)} book levels",
        }

    # ── Order execution ───────────────────────────────────────────────────────

    def execute_market_order(self, token_id: str, side: str, amount_usdc: float, market: str = "") -> dict:
        """
        Simulate a market order.
        Return value is compatible with PolymarketBot.place_market_order() so the
        rest of the bot (DB saves, dashboard) works unchanged.
        """
        ts = _utcnow().isoformat()
        order_id = f"PAPER-{int(time.time() * 1000)}"
        result = {
            "token_id":   token_id,
            "side":       side,
            "market":     market,
            "amount_usdc": amount_usdc,
            "type":       "market",
            "dry_run":    False,
            "paper":      True,
            "time":       ts,
            "order_id":   order_id,
            "status":     None,
        }

        balance    = self.get_balance()
        total_cost = amount_usdc + PAPER_GAS_FEE

        # Insufficient paper balance
        if balance < total_cost:
            result["status"] = "error: insufficient paper balance"
            log.warning(f"[PAPER] SKIP: insufficient paper balance (${balance:.2f} < ${total_cost:.2f})")
            return result

        # Simulated RPC / GSN failure (0.5%)
        if random.random() < FAILURE_RATE:
            self._adjust_balance(-PAPER_GAS_FEE)   # gas burned even on revert
            result.update({"status": "error: simulated tx failure", "gas_fee": PAPER_GAS_FEE, "price": 0, "shares": 0})
            self._save_trade(result, 0, 0, 0, PAPER_GAS_FEE, amount_usdc, "error: simulated tx failure")
            log.warning(f"[PAPER] Simulated transaction failure (0.5% rate) — gas ${PAPER_GAS_FEE} deducted")
            return result

        fill = self._simulate_fill(token_id, side, amount_usdc)
        fill_price   = fill["price"]
        shares       = fill["shares"]
        price_impact = fill["price_impact"]

        self._adjust_balance(-total_cost)
        self._upsert_position(token_id, market, side, shares, fill_price, amount_usdc, ts)

        result.update({
            "price":        fill_price,
            "shares":       shares,
            "price_impact": price_impact,
            "gas_fee":      PAPER_GAS_FEE,
            "status":       "paper_filled",
        })
        self._save_trade(result, shares, fill_price, price_impact, PAPER_GAS_FEE, amount_usdc, "paper_filled")
        log.info(
            f"[PAPER] ORDER: {side} ${amount_usdc:.2f} @ {fill_price:.4f} → {shares:.4f} shares "
            f"| impact={price_impact:.4f} | gas=${PAPER_GAS_FEE} | balance=${self.get_balance():.2f}"
        )
        return result

    def execute_limit_order(self, token_id: str, side: str, price: float, size: float, market: str = "") -> dict:
        """
        Simulate a limit order (fills immediately at the requested price — optimistic).
        """
        ts = _utcnow().isoformat()
        amount_usdc = round(price * size, 4)
        total_cost  = amount_usdc + PAPER_GAS_FEE
        result = {
            "token_id":   token_id,
            "side":       side,
            "market":     market,
            "amount_usdc": amount_usdc,
            "price":      price,
            "shares":     size,
            "type":       "limit",
            "dry_run":    False,
            "paper":      True,
            "time":       ts,
            "order_id":   f"PAPER-LMT-{int(time.time()*1000)}",
        }

        if self.get_balance() < total_cost:
            result["status"] = "error: insufficient paper balance"
            return result

        if random.random() < FAILURE_RATE:
            self._adjust_balance(-PAPER_GAS_FEE)
            result.update({"status": "error: simulated tx failure", "gas_fee": PAPER_GAS_FEE})
            return result

        self._adjust_balance(-total_cost)
        self._upsert_position(token_id, market, side, size, price, amount_usdc, ts)
        result.update({"status": "paper_filled", "gas_fee": PAPER_GAS_FEE, "price_impact": 0.0})
        self._save_trade(result, size, price, 0, PAPER_GAS_FEE, amount_usdc, "paper_filled")
        log.info(f"[PAPER] LIMIT: {side} {size} @ {price:.4f} → ${amount_usdc:.2f} | balance=${self.get_balance():.2f}")
        return result

    # ── Positions ─────────────────────────────────────────────────────────────

    def _upsert_position(self, token_id, market, side, new_shares, fill_price, cost, ts):
        existing = self.db.execute(
            "SELECT shares, avg_price, cost FROM paper_positions WHERE token_id=?",
            (token_id,)
        ).fetchone()
        with self.db:
            if existing:
                old_shares, old_avg, old_cost = existing
                total_shares = old_shares + new_shares
                new_avg = (old_cost + cost) / total_shares if total_shares > 0 else fill_price
                self.db.execute(
                    "UPDATE paper_positions SET shares=?, avg_price=?, cost=? WHERE token_id=?",
                    (round(total_shares, 6), round(new_avg, 6), round(old_cost + cost, 4), token_id)
                )
            else:
                self.db.execute(
                    """INSERT INTO paper_positions
                       (token_id, market, side, shares, avg_price, cost, opened_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (token_id, market[:100], side, round(new_shares, 6),
                     round(fill_price, 6), round(cost, 4), ts)
                )

    def _save_trade(self, trade: dict, shares, price, impact, gas, amount, status):
        try:
            with self.db:
                self.db.execute(
                    """INSERT INTO paper_trades
                       (token_id,market,side,shares,fill_price,amount_usdc,gas_fee,price_impact,status,settled_pnl,time)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        trade.get("token_id", ""),
                        trade.get("market", "")[:100],
                        trade.get("side", ""),
                        round(shares, 6), round(price, 6), round(amount, 4),
                        round(gas, 4), round(impact, 4), status, None,
                        trade.get("time", ""),
                    )
                )
        except Exception as e:
            log.warning(f"[PAPER] DB save failed: {e}")

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_positions(self) -> list:
        rows = self.db.execute(
            "SELECT token_id,market,side,shares,avg_price,cost,opened_at FROM paper_positions"
        ).fetchall()
        return [
            {"token_id": r[0], "market": r[1], "side": r[2], "shares": r[3],
             "avg_price": r[4], "cost": r[5], "opened_at": r[6]}
            for r in rows
        ]

    def get_trades(self, limit: int = 50) -> list:
        rows = self.db.execute(
            """SELECT token_id,market,side,shares,fill_price,amount_usdc,
                      gas_fee,price_impact,status,settled_pnl,time
               FROM paper_trades ORDER BY id DESC LIMIT ?""",
            (limit,)
        ).fetchall()
        return [
            {"token_id": r[0], "market": r[1], "side": r[2], "shares": r[3],
             "fill_price": r[4], "amount_usdc": r[5], "gas_fee": r[6],
             "price_impact": r[7], "status": r[8], "settled_pnl": r[9], "time": r[10]}
            for r in rows
        ]

    def get_pnl(self) -> dict:
        """
        Mark-to-market P&L using live CLOB mid prices.
        Uses mid = (best_bid + best_ask) / 2 per CLOB best practice —
        never the last sale, which can be minutes stale.
        """
        positions = self.get_positions()
        balance   = self.get_balance()

        current_value = 0.0
        enriched = []
        for pos in positions:
            try:
                book = self._fetch_orderbook(pos["token_id"])
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                if bids and asks:
                    mid = round((float(bids[0]["price"]) + float(asks[0]["price"])) / 2, 4)
                else:
                    mid = pos["avg_price"]
                cur_val = round(mid * pos["shares"], 4)
                current_value += cur_val
                enriched.append({
                    **pos,
                    "current_price":  mid,
                    "current_value":  cur_val,
                    "unrealized_pnl": round(cur_val - pos["cost"], 4),
                })
            except Exception:
                current_value += pos["avg_price"] * pos["shares"]
                enriched.append({**pos, "current_price": pos["avg_price"],
                                  "current_value": round(pos["avg_price"] * pos["shares"], 4),
                                  "unrealized_pnl": 0.0})

        row = self.db.execute(
            "SELECT SUM(settled_pnl) FROM paper_trades WHERE settled_pnl IS NOT NULL"
        ).fetchone()
        realized_pnl   = round(float(row[0]) if row and row[0] else 0.0, 4)
        unrealized_pnl = round(current_value - sum(p["cost"] for p in positions), 4)
        total_pnl      = round(realized_pnl + unrealized_pnl, 4)

        start_row = self.db.execute(
            "SELECT value FROM paper_settings WHERE key='starting_balance'"
        ).fetchone()
        starting = float(start_row[0]) if start_row else 1000.0

        return {
            "balance":          round(balance, 2),
            "portfolio_value":  round(balance + current_value, 2),
            "positions_value":  round(current_value, 2),
            "realized_pnl":     realized_pnl,
            "unrealized_pnl":   unrealized_pnl,
            "total_pnl":        total_pnl,
            "pct_pnl":          round(total_pnl / starting * 100, 2) if starting else 0,
            "starting_balance": starting,
            "positions":        enriched,
            "trades":           self.get_trades(25),
        }

    # ── Resolution oracle ─────────────────────────────────────────────────────

    def check_and_settle(self):
        """
        Oracle: inspect live CLOB prices for each open paper position.

        Heuristic: if mid price >= 0.99 → market resolving YES
                   if mid price <= 0.01 → market resolving NO
        Winning shares pay out at $1.00; losing shares pay $0.00.
        """
        positions = self.get_positions()
        if not positions:
            return
        settled = 0
        for pos in positions:
            try:
                book = self._fetch_orderbook(pos["token_id"])
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                if not bids and not asks:
                    continue
                best_bid = float(bids[0]["price"]) if bids else 0.5
                best_ask = float(asks[0]["price"]) if asks else 0.5
                mid = (best_bid + best_ask) / 2

                if mid >= 0.99:
                    # Resolved YES: YES holders win ($1/share), NO holders lose
                    winnings = pos["shares"] if pos["side"] in ("YES", "BUY") else 0.0
                    self._settle_position(pos, winnings, resolved_yes=True)
                    settled += 1
                elif mid <= 0.01:
                    # Resolved NO: NO holders win ($1/share), YES holders lose
                    winnings = pos["shares"] if pos["side"] == "NO" else 0.0
                    self._settle_position(pos, winnings, resolved_yes=False)
                    settled += 1
            except Exception as e:
                log.debug(f"[PAPER] Oracle check failed ({pos['token_id'][:16]}…): {e}")
        if settled:
            log.info(f"[PAPER] Oracle settled {settled} position(s) — balance: ${self.get_balance():.2f}")

    def _settle_position(self, pos: dict, winnings: float, resolved_yes: bool):
        pnl = round(winnings - pos["cost"], 4)
        self._adjust_balance(winnings)
        with self.db:
            self.db.execute("DELETE FROM paper_positions WHERE token_id=?", (pos["token_id"],))
            self.db.execute(
                "UPDATE paper_trades SET status='settled', settled_pnl=? "
                "WHERE token_id=? AND status='paper_filled'",
                (pnl, pos["token_id"])
            )
        outcome = "YES" if resolved_yes else "NO"
        status  = "WON" if pnl >= 0 else "LOST"
        log.info(
            f"[PAPER] SETTLE [{status}] resolved={outcome} | "
            f"{pos['market'][:45]} | {pos['shares']:.4f} shares | "
            f"P&L ${pnl:+.2f} | balance=${self.get_balance():.2f}"
        )

    def reset(self, new_balance: float = 1000.0):
        """Hard-reset the paper portfolio — wipes all positions and trade history."""
        with self.db:
            self.db.execute("DELETE FROM paper_positions")
            self.db.execute("DELETE FROM paper_trades")
            self.db.execute(
                "INSERT OR REPLACE INTO paper_settings (key, value) VALUES ('balance',?),('starting_balance',?)",
                (str(new_balance), str(new_balance))
            )
        log.info(f"[PAPER] Portfolio reset — starting fresh at ${new_balance:.2f}")


# ── Background oracle task ────────────────────────────────────────────────────

async def paper_resolution_oracle(paper: PolymarketPaperHandler):
    """
    Asyncio background task that checks for resolved markets every 5 minutes.
    Start with: asyncio.create_task(paper_resolution_oracle(bot.paper))
    """
    while True:
        await asyncio.sleep(300)
        try:
            await asyncio.to_thread(paper.check_and_settle)
        except Exception as e:
            log.warning(f"[PAPER] Oracle task error: {e}")
