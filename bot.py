"""
Polymarket Trading Bot — fixed for py_clob_client v0.34+
"""
import asyncio, json, logging, os, time, sqlite3
from datetime import datetime, date
from typing import Optional
from dotenv import load_dotenv
import urllib.request as _ureq

load_dotenv()

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        ApiCreds, OrderArgs, OrderType,
        MarketOrderArgs, BookParams,
    )
    from py_clob_client.constants import POLYGON
except ImportError as e:
    raise SystemExit(f"Missing dependency: {e}\nRun: pip install py-clob-client")

try:
    import uvicorn
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
except ImportError as e:
    raise SystemExit(f"Missing dependency: {e}\nRun: pip install fastapi uvicorn")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("polybot")

CLOB_HOST      = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
CHAIN_ID       = int(os.getenv("CHAIN_ID", "137"))
PRIVATE_KEY    = os.getenv("PRIVATE_KEY", "")
API_KEY        = os.getenv("POLYMARKET_API_KEY", "")
API_SECRET     = os.getenv("POLYMARKET_API_SECRET", "")
API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE", "")
STRATEGY       = os.getenv("STRATEGY", "momentum")
MAX_ORDER_SIZE = float(os.getenv("MAX_ORDER_SIZE", "10"))
DRY_RUN        = os.getenv("DRY_RUN", "true").lower() != "false"
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "500"))
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
API_SECRET_KEY    = os.getenv("API_SECRET_KEY", "sexybot2024")
FRED_API_KEY       = os.getenv("FRED_API_KEY", "")
OPEN_METEO_API_KEY = os.getenv("OPEN_METEO_API_KEY", "")
FMP_API_KEY        = os.getenv("FMP_API_KEY", "")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
TAVILY_API_KEY     = os.getenv("TAVILY_API_KEY", "")
NEWS_API_KEY       = os.getenv("NEWS_API_KEY", "")
ALCHEMY_API_KEY    = os.getenv("ALCHEMY_API_KEY", "")
COINGECKO_API_KEY  = os.getenv("COINGECKO_API_KEY", "")


class PolymarketBot:
    def __init__(self):
        self.client: Optional[ClobClient] = None
        self.running = False
        self.trades: list = []
        self.signals: list = []
        self.log_lines: list = []
        self.ws_clients: list = []

    # ── Auth ──────────────────────────────────────────────────────────────────

    def send_telegram(self, msg: str):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": f"🤖 SEXYBOT\n{msg}"}).encode()
            req = _ureq.Request(url, data=data, headers={"Content-Type": "application/json"})
            _ureq.urlopen(req, timeout=5)
        except Exception as e:
            log.warning(f"Telegram failed: {e}")

    def init_db(self):
        self.db = sqlite3.connect("/root/polybot/trades.db", check_same_thread=False)
        self.db.execute("""CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT, side TEXT, amount REAL, price REAL,
            shares REAL, order_type TEXT, status TEXT,
            order_id TEXT, dry_run INTEGER, time TEXT
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS positions (
            token_id TEXT PRIMARY KEY, market TEXT,
            side TEXT, shares REAL, cost REAL, time TEXT
        )""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS brier_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT, token_id TEXT, side TEXT,
            predicted_prob REAL, market_price REAL,
            kelly_fraction REAL, kelly_size REAL,
            ai_reasoning TEXT, time TEXT,
            resolved INTEGER DEFAULT 0,
            actual_outcome INTEGER DEFAULT NULL,
            brier_score REAL DEFAULT NULL
        )""")
        self.db.commit()

    def save_trade(self, trade: dict):
        try:
            self.db.execute("""INSERT INTO trades
                (market,side,amount,price,shares,order_type,status,order_id,dry_run,time)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (trade.get("market",""), trade.get("side",""), trade.get("amount_usdc",trade.get("amount",0)),
                 trade.get("price",0), trade.get("shares",0), trade.get("type","market"),
                 trade.get("status",""), trade.get("order_id",""),
                 1 if trade.get("dry_run") else 0, trade.get("time","")))
            self.db.commit()
        except Exception as e:
            log.warning(f"DB save failed: {e}")

    def get_daily_loss(self) -> float:
        try:
            today = date.today().isoformat()
            cur = self.db.execute(
                "SELECT SUM(amount) FROM trades WHERE time LIKE ? AND dry_run=0 AND status NOT LIKE '%error%' AND status NOT LIKE '%match%' AND status NOT LIKE '%balance%'",
                (f"{today}%",))
            total = cur.fetchone()[0] or 0
            return float(total)
        except:
            return 0.0

    def has_position(self, token_id: str) -> bool:
        try:
            cur = self.db.execute("SELECT 1 FROM positions WHERE token_id=?", (token_id,))
            return cur.fetchone() is not None
        except:
            return False

    def _sync_positions(self, active_token_ids: set):
        """Remove positions for markets no longer active (resolved/expired)."""
        try:
            cur = self.db.execute("SELECT token_id FROM positions")
            stored = [row[0] for row in cur.fetchall()]
            removed = 0
            for tid in stored:
                if tid not in active_token_ids:
                    self.db.execute("DELETE FROM positions WHERE token_id=?", (tid,))
                    removed += 1
            if removed:
                self.db.commit()
                self._log(f"Cleared {removed} resolved position(s) from DB")
        except Exception as e:
            log.debug(f"sync_positions error: {e}")

    def add_position(self, token_id: str, market: str, side: str, shares: float, cost: float):
        try:
            self.db.execute("""INSERT OR REPLACE INTO positions
                (token_id, market, side, shares, cost, time) VALUES (?,?,?,?,?,?)""",
                (token_id, market, side, shares, cost, datetime.utcnow().isoformat()))
            self.db.commit()
        except Exception as e:
            log.warning(f"Position save failed: {e}")

    def connect(self) -> bool:
        if not PRIVATE_KEY:
            self._log("ERROR: PRIVATE_KEY not set in .env", "error")
            return False
        try:
            creds = None
            if API_KEY and API_SECRET and API_PASSPHRASE:
                creds = ApiCreds(
                    api_key=API_KEY,
                    api_secret=API_SECRET,
                    api_passphrase=API_PASSPHRASE,
                )
            import os
            funder = os.getenv("POLYMARKET_FUNDER", "")
            # signature_type=2 (builder/proxy wallet) required when using API creds
            sig_type = 2 if creds else 0
            self.client = ClobClient(
                host=CLOB_HOST,
                chain_id=CHAIN_ID,
                key=PRIVATE_KEY,
                creds=creds,
                funder=funder if funder else None,
                signature_type=sig_type,
            )
            if not creds:
                self._log("Deriving L2 API credentials from wallet…")
                derived = self.client.create_or_derive_api_creds()
                self.client.set_api_creds(derived)
                # Re-init with sig_type=2 now that we have creds
                self.client = ClobClient(
                    host=CLOB_HOST,
                    chain_id=CHAIN_ID,
                    key=PRIVATE_KEY,
                    creds=derived,
                    funder=funder if funder else None,
                    signature_type=2,
                )
                self._log(f"L2 key: {derived.api_key[:8]}…")
            ok = self.client.get_ok()
            self._log(f"Connected to Polymarket CLOB — {ok}")
            self.init_db()
            self.send_telegram("Bot connected and starting up")
            return True
        except Exception as e:
            self._log(f"Connection failed: {e}", "error")
            return False

    # ── Market data ───────────────────────────────────────────────────────────

    def get_markets(self, limit: int = 20) -> list:
        try:
            import urllib.request, json as _j
            url = f"https://gamma-api.polymarket.com/markets?active=true&closed=false&limit={limit}&order=volume24hr&ascending=false"
            req = urllib.request.Request(url, headers={"User-Agent": "polybot/1.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                return _j.loads(r.read())
        except Exception as e:
            self._log(f"get_markets error: {e}", "error")
            return []

    def get_midpoint(self, token_id: str) -> Optional[float]:
        try:
            mp = self.client.get_midpoint(token_id)
            return float(mp.get("mid", 0))
        except Exception:
            return None

    def get_spread(self, token_id: str) -> Optional[float]:
        try:
            sp = self.client.get_spread(token_id)
            return float(sp.get("spread", 0))
        except Exception:
            return None

    def get_orderbook(self, token_id: str) -> Optional[dict]:
        try:
            return self.client.get_order_book(token_id)
        except Exception as e:
            self._log(f"get_orderbook error: {e}", "error")
            return None

    # ── Orders ────────────────────────────────────────────────────────────────

    def place_market_order(self, token_id: str, side: str, amount_usdc: float) -> dict:
        price = self.get_midpoint(token_id) or 0.5
        result = {
            "token_id": token_id,
            "side": side,
            "amount_usdc": amount_usdc,
            "price": price,
            "shares": round(amount_usdc / price, 4) if price else 0,
            "type": "market",
            "dry_run": DRY_RUN,
            "time": datetime.utcnow().isoformat(),
            "status": None,
            "order_id": None,
        }
        if DRY_RUN:
            result["status"] = "simulated"
            self._log(f"[DRY RUN] {side} ${amount_usdc:.2f} @ {price:.4f} — token {token_id[:16]}…")
        else:
            try:
                order_args = MarketOrderArgs(token_id=token_id, amount=amount_usdc, side=side)
                signed = self.client.create_market_order(order_args)
                resp = self.client.post_order(signed, OrderType.FOK)
                result["status"] = resp.get("status", "unknown")
                result["order_id"] = resp.get("orderID")
                self._log(f"ORDER: {side} ${amount_usdc:.2f} → {result['order_id']} {result['status']}")
            except Exception as e:
                result["status"] = f"error: {e}"
                self._log(f"Order failed: {e}", "error")
        self.trades.append(result)
        self.save_trade(result)
        status_str = str(result.get("status", "")).lower()
        if result.get("status") and "error" not in status_str and status_str not in ("unmatched", "canceled", "cancelled", "no_match"):
            self.add_position(token_id, result.get("market",""), side, result.get("shares",0), amount_usdc)
        return result

    def place_limit_order(self, token_id: str, side: str, price: float, size: float) -> dict:
        result = {
            "token_id": token_id,
            "side": side,
            "price": price,
            "size": size,
            "type": "limit",
            "dry_run": DRY_RUN,
            "time": datetime.utcnow().isoformat(),
            "status": None,
            "order_id": None,
        }
        if DRY_RUN:
            result["status"] = "simulated"
            self._log(f"[DRY RUN] LIMIT {side} {size}@{price:.4f} — token {token_id[:16]}…")
        else:
            try:
                order_args = OrderArgs(token_id=token_id, price=price, size=size, side=side)
                signed = self.client.create_order(order_args)
                resp = self.client.post_order(signed, OrderType.GTC)
                result["status"] = resp.get("status", "unknown")
                result["order_id"] = resp.get("orderID")
                self._log(f"LIMIT: {side} {size}@{price:.4f} → {result['order_id']} {result['status']}")
            except Exception as e:
                result["status"] = f"error: {e}"
                self._log(f"Limit order failed: {e}", "error")
        self.trades.append(result)
        return result

    def cancel_all_orders(self):
        if DRY_RUN:
            self._log("[DRY RUN] Would cancel all open orders")
            return
        try:
            resp = self.client.cancel_all()
            self._log(f"Cancelled all: {resp}")
        except Exception as e:
            self._log(f"Cancel failed: {e}", "error")

    # ── Strategies ────────────────────────────────────────────────────────────

    _macro_cache = {}
    _macro_cache_time = 0
    _weather_cache = {}
    _weather_cache_time = 0
    _fmp_cache = {}
    _fmp_cache_time = 0

    # Key tickers: SPY (S&P proxy), plus individual mega-caps relevant to predictions
    FMP_TICKERS = ["SPY", "AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN"]

    def get_fmp_market(self) -> dict:
        """Fetch stock quotes from FMP and compute market sentiment score."""
        import json as _j
        if not FMP_API_KEY:
            return {}
        if time.time() - self._fmp_cache_time < 300 and self._fmp_cache:
            return self._fmp_cache
        results = {}
        for sym in self.FMP_TICKERS:
            try:
                url = f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={FMP_API_KEY}"
                with _ureq.urlopen(url, timeout=5) as r:
                    data = _j.loads(r.read())
                if data:
                    q = data[0]
                    results[sym] = {
                        "price": q.get("price"),
                        "change": round(q.get("change", 0), 2),
                        "change_pct": round(q.get("changePercentage", 0), 2),
                        "volume": q.get("volume"),
                        "day_high": q.get("dayHigh"),
                        "day_low": q.get("dayLow"),
                        "prev_close": q.get("previousClose"),
                        "mktcap": q.get("marketCap"),
                    }
            except Exception as e:
                log.debug(f"FMP quote {sym} error: {e}")
        if results:
            # Market sentiment: % of tickers up, weighted by magnitude
            changes = [v["change_pct"] for v in results.values()]
            up = sum(1 for c in changes if c > 0)
            avg_chg = round(sum(changes) / len(changes), 2)
            spy_chg = results.get("SPY", {}).get("change_pct", 0)
            results["_sentiment"] = {
                "up_count": up,
                "total": len(changes),
                "avg_change_pct": avg_chg,
                "spy_change_pct": spy_chg,
                "bullish": spy_chg > 0.5,
                "bearish": spy_chg < -0.5,
            }
            self._fmp_cache = results
            self._fmp_cache_time = time.time()
        return results

    def get_macro_context(self) -> dict:
        import urllib.request, json as _j
        if time.time() - self._macro_cache_time < 3600 and self._macro_cache:
            return self._macro_cache
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id=FEDFUNDS&api_key={FRED_API_KEY}&sort_order=desc&limit=1&file_type=json"
            with _ureq.urlopen(url, timeout=5) as r:
                d = _j.loads(r.read())
                rate = float(d["observations"][0]["value"])
        except:
            rate = 4.5
        try:
            # Fetch 13 months to compute YoY % change
            url2 = f"https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL&api_key={FRED_API_KEY}&sort_order=desc&limit=13&file_type=json"
            with urllib.request.urlopen(url2, timeout=5) as r:
                d2 = _j.loads(r.read())
                obs = d2["observations"]
                cpi_now = float(obs[0]["value"])
                cpi_yr  = float(obs[12]["value"])
                cpi = round((cpi_now - cpi_yr) / cpi_yr * 100, 2)  # YoY %
        except:
            cpi = 3.0
        self._macro_cache = {"fed_rate": rate, "cpi": cpi}
        self._macro_cache_time = time.time()
        return self._macro_cache

    def get_weather_context(self) -> dict:
        import json as _j
        if time.time() - self._weather_cache_time < 3600 and self._weather_cache:
            return self._weather_cache
        try:
            base = "https://customer-api.open-meteo.com" if OPEN_METEO_API_KEY else "https://api.open-meteo.com"
            key_param = f"&apikey={OPEN_METEO_API_KEY}" if OPEN_METEO_API_KEY else ""
            url = (
                f"{base}/v1/forecast?latitude=40.7128&longitude=-74.0060"
                f"&current=temperature_2m,apparent_temperature,relative_humidity_2m,"
                f"weathercode,windspeed_10m,precipitation,uv_index,is_day"
                f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
                f"&temperature_unit=fahrenheit&windspeed_unit=mph&forecast_days=3"
                f"{key_param}"
            )
            with _ureq.urlopen(url, timeout=8) as r:
                d = _j.loads(r.read())
            cur = d.get("current", {})
            code = cur.get("weathercode", 0)
            wind = cur.get("windspeed_10m", 0)
            bad_weather = code in [51,53,55,61,63,65,71,73,75,77,80,81,82,85,86,95,96,99] or wind > 20
            self._weather_cache = {
                "bad_weather": bad_weather,
                "temp": cur.get("temperature_2m"),
                "feels_like": cur.get("apparent_temperature"),
                "humidity": cur.get("relative_humidity_2m"),
                "weathercode": code,
                "wind": wind,
                "precipitation": cur.get("precipitation", 0),
                "uv_index": cur.get("uv_index", 0),
                "is_day": cur.get("is_day", 1),
                "daily": d.get("daily", {}),
            }
        except Exception as e:
            log.debug(f"weather fetch error: {e}")
            self._weather_cache = {"bad_weather": False}
        self._weather_cache_time = time.time()
        return self._weather_cache

    # ── Intelligence Layer ────────────────────────────────────────────────────

    _crypto_cache: dict = {}
    _crypto_cache_time: float = 0

    def get_crypto_prices(self) -> dict:
        """CoinGecko free API — BTC, ETH, MATIC prices for crypto markets."""
        if time.time() - self._crypto_cache_time < 120 and self._crypto_cache:
            return self._crypto_cache
        try:
            import json as _j
            headers = {"accept": "application/json"}
            if COINGECKO_API_KEY:
                headers["x-cg-pro-api-key"] = COINGECKO_API_KEY
            url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,matic-network,solana&vs_currencies=usd&include_24hr_change=true"
            req = _ureq.Request(url, headers=headers)
            with _ureq.urlopen(req, timeout=5) as r:
                data = _j.loads(r.read())
            result = {}
            mapping = {"bitcoin": "BTC", "ethereum": "ETH", "matic-network": "MATIC", "solana": "SOL"}
            for cg_id, sym in mapping.items():
                if cg_id in data:
                    result[sym] = {
                        "price": data[cg_id].get("usd"),
                        "change_24h": round(data[cg_id].get("usd_24h_change", 0), 2),
                    }
            self._crypto_cache = result
            self._crypto_cache_time = time.time()
            return result
        except Exception as e:
            log.debug(f"CoinGecko error: {e}")
            return {}

    def get_onchain_balance(self) -> float:
        """Check USDC balance on Polygon via Alchemy RPC. Falls back to CLOB balance."""
        if not ALCHEMY_API_KEY or not PRIVATE_KEY:
            return self.get_balance()
        try:
            from web3 import Web3
            from eth_account import Account
            rpc = f"https://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
            w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 8}))
            acct = Account.from_key(PRIVATE_KEY)
            # USDC on Polygon: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
            usdc = w3.eth.contract(
                address=Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"),
                abi=[{"inputs":[{"name":"account","type":"address"}],"name":"balanceOf",
                      "outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}]
            )
            raw = usdc.functions.balanceOf(acct.address).call()
            return round(raw / 1_000_000, 2)
        except Exception as e:
            log.debug(f"Alchemy RPC error: {e}")
            return self.get_balance()

    def get_news_headlines(self, query: str, limit: int = 5) -> list:
        """Fetch relevant news headlines via NewsAPI."""
        if not NEWS_API_KEY:
            return []
        try:
            import json as _j, urllib.parse
            q = urllib.parse.quote(query[:80])
            url = f"https://newsapi.org/v2/everything?q={q}&sortBy=publishedAt&pageSize={limit}&apiKey={NEWS_API_KEY}"
            with _ureq.urlopen(url, timeout=6) as r:
                data = _j.loads(r.read())
            return [
                {"title": a.get("title",""), "source": a.get("source",{}).get("name",""),
                 "published": a.get("publishedAt","")[:10]}
                for a in data.get("articles", [])[:limit]
            ]
        except Exception as e:
            log.debug(f"NewsAPI error: {e}")
            return []

    def get_tavily_research(self, query: str) -> str:
        """Deep AI-optimized web research via Tavily."""
        if not TAVILY_API_KEY:
            return ""
        try:
            from tavily import TavilyClient
            client = TavilyClient(api_key=TAVILY_API_KEY)
            result = client.search(query=query, search_depth="basic", max_results=3)
            snippets = [r.get("content","")[:300] for r in result.get("results", [])]
            return " | ".join(snippets)
        except Exception as e:
            log.debug(f"Tavily error: {e}")
            return ""

    def get_orderbook_depth(self, token_id: str) -> dict:
        """Fetch order book depth, spread, liquidity, and OBI."""
        try:
            book = self.get_orderbook(token_id)
            if not book:
                return {}
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            best_bid = float(bids[0]["price"]) if bids else 0
            best_ask = float(asks[0]["price"]) if asks else 1
            spread = round(best_ask - best_bid, 4)
            bid_depth = sum(float(b.get("size", 0)) for b in bids[:5])
            ask_depth = sum(float(a.get("size", 0)) for a in asks[:5])
            total_depth = bid_depth + ask_depth
            obi = round((bid_depth - ask_depth) / total_depth, 3) if total_depth > 0 else 0
            return {
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "bid_depth": round(bid_depth, 2),
                "ask_depth": round(ask_depth, 2),
                "liquid": total_depth > 50,
                "obi": obi,  # >0 = buy pressure, <0 = sell pressure
            }
        except Exception as e:
            log.debug(f"Orderbook depth error: {e}")
            return {}

    def kelly_size(self, prob: float, price: float, bankroll: float, fraction: float = 0.25) -> float:
        """
        Fractional Kelly Criterion position sizing.
        prob: true probability of winning (0-1)
        price: cost per share (0-1)
        bankroll: available cash
        fraction: Kelly multiplier (0.25 = quarter Kelly, protects vs LLM overconfidence)
        Returns dollar amount to wager, capped at MAX_ORDER_SIZE.
        """
        if price <= 0 or price >= 1 or prob <= 0:
            return 0.0
        b = (1 - price) / price   # profit ratio per dollar wagered
        q = 1 - prob
        kelly_f = prob - (q / b)  # full Kelly fraction
        if kelly_f <= 0.01:       # no meaningful edge
            return 0.0
        size = bankroll * kelly_f * fraction
        return round(min(size, MAX_ORDER_SIZE, bankroll * 0.20), 2)  # never risk >20% of bankroll

    def log_brier(self, market: str, token_id: str, side: str,
                  predicted_prob: float, market_price: float,
                  kelly_f: float, kelly_size: float, reasoning: str):
        """Log a prediction for Brier score calibration tracking."""
        try:
            self.db.execute("""INSERT INTO brier_scores
                (market, token_id, side, predicted_prob, market_price,
                 kelly_fraction, kelly_size, ai_reasoning, time)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (market[:100], token_id, side, predicted_prob, market_price,
                 kelly_f, kelly_size, reasoning[:200],
                 datetime.utcnow().isoformat()))
            self.db.commit()
        except Exception as e:
            log.debug(f"brier log error: {e}")

    def get_brier_stats(self) -> dict:
        """Return calibration stats from resolved predictions."""
        try:
            cur = self.db.execute(
                "SELECT COUNT(*), AVG(brier_score) FROM brier_scores WHERE resolved=1 AND brier_score IS NOT NULL")
            row = cur.fetchone()
            total_cur = self.db.execute("SELECT COUNT(*) FROM brier_scores").fetchone()
            return {
                "total_predictions": total_cur[0] if total_cur else 0,
                "resolved": row[0] if row else 0,
                "avg_brier_score": round(row[1], 4) if row and row[1] else None,
            }
        except:
            return {}

    def get_courtlistener_data(self, query: str) -> str:
        """Search CourtListener for relevant court dockets/opinions (free API)."""
        try:
            import json as _j, urllib.parse
            q = urllib.parse.quote(query[:60])
            url = f"https://www.courtlistener.com/api/rest/v4/search/?q={q}&type=o&order_by=score+desc&stat_Precedential=on"
            req = _ureq.Request(url, headers={"User-Agent": "polybot/1.0", "Accept": "application/json"})
            with _ureq.urlopen(req, timeout=6) as r:
                data = _j.loads(r.read())
            results = data.get("results", [])[:3]
            if not results:
                return ""
            snippets = []
            for res in results:
                court = res.get("court_id", "")
                date = res.get("dateFiled", "")[:10]
                case = res.get("caseName", "")[:60]
                snippet = res.get("snippet", "")[:150].replace("<mark>","").replace("</mark>","")
                snippets.append(f"[{date}] {case} ({court}): {snippet}")
            return " | ".join(snippets)
        except Exception as e:
            log.debug(f"CourtListener error: {e}")
            return ""

    def get_govtrack_data(self, query: str) -> str:
        """Search GovTrack for relevant bills/legislation (free API)."""
        try:
            import json as _j, urllib.parse
            q = urllib.parse.quote(query[:60])
            url = f"https://www.govtrack.us/api/v2/bill?q={q}&limit=3&order=relevant"
            req = _ureq.Request(url, headers={"User-Agent": "polybot/1.0", "Accept": "application/json"})
            with _ureq.urlopen(req, timeout=6) as r:
                data = _j.loads(r.read())
            bills = data.get("objects", [])[:3]
            if not bills:
                return ""
            snippets = []
            for b in bills:
                title = b.get("title_without_number", b.get("title",""))[:80]
                status = b.get("current_status_description", "")[:60]
                congress = b.get("congress", "")
                snippets.append(f"[{congress}th Congress] {title} — {status}")
            return " | ".join(snippets)
        except Exception as e:
            log.debug(f"GovTrack error: {e}")
            return ""

    async def analyze_with_claude(self, market: dict, yes_p: float, research: dict) -> Optional[dict]:
        """
        Use Claude claude-haiku-4-5 to intelligently score a market using all available context.
        Returns enhanced signal dict or None to skip.
        """
        if not ANTHROPIC_API_KEY:
            return None
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            question = market.get("question", "")
            no_p = round(1 - yes_p, 4)

            # Build context block
            ctx_parts = [
                f"MARKET: {question}",
                f"CURRENT PRICES: YES={yes_p:.3f} ({yes_p*100:.1f}¢)  NO={no_p:.3f} ({no_p*100:.1f}¢)",
                f"VOLUME 24H: ${float(market.get('volume24hr',0)):,.0f}",
            ]
            macro = self._macro_cache
            if macro:
                ctx_parts.append(f"MACRO: Fed Rate={macro.get('fed_rate')}%  CPI YoY={macro.get('cpi')}%")
            fmp = self._fmp_cache.get("_sentiment", {})
            if fmp:
                ctx_parts.append(f"MARKET SENTIMENT: SPY {fmp.get('spy_change_pct',0):+.2f}%  {fmp.get('up_count',0)}/{fmp.get('total',0)} stocks up")
            ob = research.get("orderbook", {})
            if ob:
                ctx_parts.append(f"ORDER BOOK: bid={ob.get('best_bid')} ask={ob.get('best_ask')} spread={ob.get('spread')} liquid={ob.get('liquid')} obi={ob.get('obi',0)} (>0=buy pressure <0=sell pressure)")
            crypto = research.get("crypto", {})
            if crypto and any(k in question.lower() for k in ["bitcoin","btc","ethereum","eth","crypto","blockchain","polygon","matic","solana","sol"]):
                ctx_parts.append(f"CRYPTO: BTC={crypto.get('BTC',{}).get('price')} ({crypto.get('BTC',{}).get('change_24h',0):+.1f}%)  ETH={crypto.get('ETH',{}).get('price')} ({crypto.get('ETH',{}).get('change_24h',0):+.1f}%)")
            news = research.get("news", [])
            if news:
                ctx_parts.append("RECENT NEWS:")
                for n in news[:3]:
                    ctx_parts.append(f"  - [{n.get('published','')}] {n.get('title','')}")
            tavily = research.get("tavily", "")
            if tavily:
                ctx_parts.append(f"WEB RESEARCH: {tavily[:500]}")
            court = research.get("court", "")
            if court:
                ctx_parts.append(f"COURT DOCKETS: {court}")
            govtrack = research.get("govtrack", "")
            if govtrack:
                ctx_parts.append(f"LEGISLATION: {govtrack}")

            context = "\n".join(ctx_parts)

            prompt = f"""You are a Polymarket prediction market trader using Kelly Criterion sizing. Analyze this market and decide whether to trade.

{context}

Respond in JSON only with this exact structure:
{{
  "action": "BUY" or "SKIP",
  "side": "YES" or "NO",
  "probability": <integer 0-100>,
  "confidence": <integer 0-100>,
  "reasoning": "<one sentence>",
  "risk": "low" or "medium" or "high"
}}

Definitions:
- "probability": YOUR estimated true probability (0-100) that the chosen side resolves correctly. This is used for Kelly Criterion sizing. Be calibrated — if the market price already reflects fair value, say so.
- "confidence": how certain you are in your probability estimate (0-100). Lower if limited data.
- "action": BUY only if your probability gives positive Kelly edge over the market price. SKIP if no edge.

Rules:
- Only BUY if your estimated probability meaningfully exceeds the market's implied probability
- SKIP if the market price already reflects fair value (your probability ≈ market price)
- Consider court/legal data, legislation status, and news for political/legal markets
- Consider order book imbalance — if OBI strongly opposes your side, SKIP
- risk = "high" if outcome is binary/volatile, "low" if near-certain resolution"""

            msg = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )
            import json as _j
            text = msg.content[0].text.strip()
            # Extract JSON even if wrapped in markdown
            if "```" in text:
                text = text.split("```")[1].replace("json","").strip()
            decision = _j.loads(text)
            return decision
        except Exception as e:
            log.debug(f"Claude analysis error: {e}")
            return None

    def analyze(self, market: dict) -> Optional[dict]:
        import json as _j
        raw_prices = market.get("outcomePrices", "[0.5,0.5]")
        prices = _j.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
        yes_p = float(prices[0]) if prices else 0.5
        no_p = float(prices[1]) if len(prices) > 1 else 1 - yes_p
        raw_ids = market.get("clobTokenIds", "[]")
        ids = _j.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        yes_id = ids[0] if ids else ""
        no_id = ids[1] if len(ids) > 1 else ""
        question = market.get("question", market.get("slug", "unknown"))
        if not yes_id:
            return None

        if STRATEGY == "momentum":
            dev = abs(yes_p - 0.5)
            if dev > 0.08:
                macro = self.get_macro_context()
                weather = self.get_weather_context()
                fmp = self.get_fmp_market()
                sentiment = fmp.get("_sentiment", {})
                confidence = round(dev * 200, 1)
                amount = min(MAX_ORDER_SIZE, MAX_ORDER_SIZE * dev * 2)
                q_lower = question.lower()
                is_sports = any(x in q_lower for x in ["win","game","match","league","cup","fc","nba","nfl","mlb","nhl"])
                is_political = any(x in q_lower for x in ["president","election","senate","congress","party","nominee"])
                is_economic = any(x in q_lower for x in ["fed","rate","inflation","gdp","economy","recession","cpi","stock","market"])
                is_tech = any(x in q_lower for x in ["tech","ai","apple","microsoft","nvidia","tesla","amazon","meta","google"])
                if weather["bad_weather"] and is_sports:
                    confidence *= 0.75
                    self._log(f"WEATHER ADJ: reducing sports confidence (bad weather)")
                if macro["fed_rate"] > 4.5 and is_economic:
                    confidence *= 1.15
                    self._log(f"MACRO ADJ: boosting economic market confidence (rate={macro['fed_rate']})")
                if macro["cpi"] > 4.0 and is_political:
                    amount *= 0.8
                    self._log(f"MACRO ADJ: reducing political exposure (high CPI={macro['cpi']})")
                # FMP market sentiment adjustments
                if sentiment.get("bullish") and (is_economic or is_tech):
                    confidence *= 1.10
                    self._log(f"FMP ADJ: market bullish ({sentiment.get('spy_change_pct',0):+.2f}%), boosting confidence")
                elif sentiment.get("bearish") and (is_economic or is_tech):
                    confidence *= 0.85
                    amount *= 0.8
                    self._log(f"FMP ADJ: market bearish ({sentiment.get('spy_change_pct',0):+.2f}%), reducing exposure")
                side_label = "YES" if yes_p > 0.5 else "NO"
                tid = yes_id if yes_p > 0.5 else no_id
                return {"strategy": "momentum", "signal": f"BUY {side_label}",
                        "token_id": tid, "price": yes_p,
                        "confidence": round(confidence, 1),
                        "market": question,
                        "amount": amount}

        elif STRATEGY == "meanReversion":
            dev = abs(yes_p - 0.5)
            if dev > 0.35:
                side_label = "NO" if yes_p > 0.5 else "YES"
                tid = no_id if yes_p > 0.5 else yes_id
                return {"strategy": "meanReversion", "signal": f"BUY {side_label} (fade)",
                        "token_id": tid, "price": 1 - yes_p,
                        "confidence": round(dev * 200, 1),
                        "market": question,
                        "amount": min(MAX_ORDER_SIZE, MAX_ORDER_SIZE * (dev - 0.35) * 5)}

        elif STRATEGY == "arbitrage":
            total = yes_p + no_p
            gap = abs(1 - total)
            if gap > 0.03:
                direction = "BUY BOTH" if total < 1 else "SELL BOTH"
                return {"strategy": "arbitrage", "signal": direction,
                        "token_id": yes_id, "no_token_id": no_id,
                        "yes_price": yes_p, "no_price": no_p,
                        "gap": round(gap * 100, 2),
                        "confidence": round(gap * 1000, 1),
                        "market": question,
                        "amount": MAX_ORDER_SIZE}

        elif STRATEGY == "marketMaking":
            spread = self.get_spread(yes_id)
            liq = float(market.get("liquidity", 0))
            if spread and spread > 0.03 and liq < 5000:
                half = spread / 2
                return {"strategy": "marketMaking", "signal": "POST QUOTES",
                        "token_id": yes_id,
                        "bid": round(yes_p - half, 4),
                        "ask": round(yes_p + half, 4),
                        "spread": round(spread * 100, 2),
                        "confidence": round(spread * 500, 1),
                        "market": question,
                        "amount": MAX_ORDER_SIZE / 2}
        return None

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run_loop(self, interval: float = 30.0):
        self.running = True
        ai_enabled = bool(ANTHROPIC_API_KEY)
        self._log(f"Bot started — strategy={STRATEGY} dry_run={DRY_RUN} interval={interval}s AI={'ON' if ai_enabled else 'OFF'}")
        # Pre-warm shared caches in thread executor (non-blocking)
        await asyncio.to_thread(self.get_macro_context)
        await asyncio.to_thread(self.get_fmp_market)
        if ai_enabled:
            await asyncio.to_thread(self.get_crypto_prices)

        while self.running:
            try:
                markets = await asyncio.to_thread(self.get_markets, 30)
                self._log(f"Scanning {len(markets)} markets…")

                # Sync positions: remove resolved/expired markets from DB
                import json as _j_sync
                active_tids = set()
                for _m in markets:
                    _raw = _m.get("clobTokenIds", "[]")
                    _ids = _j_sync.loads(_raw) if isinstance(_raw, str) else _raw
                    active_tids.update(_ids)
                self._sync_positions(active_tids)

                # Fetch balance once per cycle (non-blocking)
                cycle_cash = await asyncio.to_thread(self.get_balance, True)
                self._log(f"Cash: ${cycle_cash:.2f}")

                for mkt in markets:
                    if not self.running:
                        break

                    # ── Stage 1: fast rule-based filter ──────────────────────
                    signal = self.analyze(mkt)
                    if not signal:
                        continue

                    question = mkt.get("question", "")
                    import json as _j
                    raw_prices = mkt.get("outcomePrices", "[0.5,0.5]")
                    prices = _j.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
                    yes_p = float(prices[0]) if prices else 0.5

                    # ── Stage 2: AI deep analysis (if available & confidence ≥ 30) ──
                    token_id = signal.get("token_id", "")
                    ob_data = self.get_orderbook_depth(token_id) if token_id else {}
                    predicted_prob = None  # will be set by AI or estimated below

                    if ai_enabled and float(signal.get("confidence", 0)) >= 30:
                        q_lower = question.lower()
                        is_legal = any(x in q_lower for x in ["indicted","trial","court","lawsuit","ruling","judge","convicted","charged","plea","verdict","sentenced"])
                        is_legislative = any(x in q_lower for x in ["bill","act","legislation","congress","senate","pass","signed","law","vote","amendment"])
                        research = {
                            "orderbook": ob_data,
                            "crypto":    self.get_crypto_prices(),
                            "news":      self.get_news_headlines(question[:60]),
                            "tavily":    self.get_tavily_research(question[:80]) if TAVILY_API_KEY else "",
                            "court":     self.get_courtlistener_data(question[:60]) if is_legal else "",
                            "govtrack":  self.get_govtrack_data(question[:60]) if is_legislative else "",
                        }
                        ai = await self.analyze_with_claude(mkt, yes_p, research)
                        if ai:
                            if ai.get("action") == "SKIP":
                                self._log(f"AI SKIP: {question[:50]} — {ai.get('reasoning','')}")
                                continue
                            signal["confidence"] = ai.get("confidence", signal["confidence"])
                            signal["ai_reasoning"] = ai.get("reasoning", "")
                            signal["ai_risk"] = ai.get("risk", "medium")
                            predicted_prob = ai.get("probability")  # explicit probability for Kelly
                            # AI can flip the side if it disagrees
                            if ai.get("side") == "YES" and "NO" in signal.get("signal",""):
                                raw_ids = mkt.get("clobTokenIds", "[]")
                                ids = _j.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
                                signal["token_id"] = ids[0] if ids else token_id
                                signal["signal"] = "BUY YES"
                            elif ai.get("side") == "NO" and "YES" in signal.get("signal",""):
                                raw_ids = mkt.get("clobTokenIds", "[]")
                                ids = _j.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
                                signal["token_id"] = ids[1] if len(ids) > 1 else token_id
                                signal["signal"] = "BUY NO"
                            self._log(f"AI [{ai.get('risk','?').upper()}] prob={predicted_prob}% conf={ai.get('confidence')}% — {ai.get('reasoning','')[:70]}")

                    # ── OBI check: skip if order book strongly opposes our direction ──
                    obi = ob_data.get("obi", 0)
                    buying_yes = "YES" in signal.get("signal", "")
                    if (buying_yes and obi < -0.4) or (not buying_yes and obi > 0.4):
                        self._log(f"OBI SKIP: book pressure opposes trade (obi={obi:.2f}) {question[:40]}")
                        continue

                    self.signals.append({**signal, "time": datetime.utcnow().isoformat()})
                    self._log(f"SIGNAL [{signal['strategy']}] {signal['signal']} | conf={signal.get('confidence','?')}% | {signal['market'][:50]}")

                    # ── Daily loss guard ──────────────────────────────────────
                    daily_loss = self.get_daily_loss()
                    if daily_loss >= DAILY_LOSS_LIMIT:
                        self._log(f"DAILY LOSS LIMIT HIT: ${daily_loss:.2f} — stopping trading", "error")
                        self.send_telegram(f"⚠️ Daily loss limit hit: ${daily_loss:.2f}. Bot stopped trading.")
                        self.running = False
                        break

                    if signal.get("token_id") and self.has_position(signal["token_id"]):
                        self._log(f"SKIP: already have position in {signal['market'][:40]}")
                        continue

                    # ── Kelly Criterion sizing ────────────────────────────────
                    cash = cycle_cash
                    if cash < 1.0:
                        self._log(f"SKIP: insufficient cash (${cash:.2f})")
                        continue

                    # Determine entry price for Kelly (price of the side we're buying)
                    entry_price = yes_p if "YES" in signal.get("signal","") else (1 - yes_p)
                    # Use AI probability if available, else estimate from confidence
                    if predicted_prob is not None:
                        p_true = predicted_prob / 100.0
                    else:
                        # Conservative estimate: blend market price with confidence signal
                        conf_boost = float(signal.get("confidence", 50)) / 100.0 * 0.10
                        p_true = min(entry_price + conf_boost, 0.99)

                    kelly_f_full = p_true - ((1 - p_true) / ((1 - entry_price) / entry_price)) if entry_price < 1 else 0
                    amt = self.kelly_size(p_true, entry_price, cash)
                    if amt < 0.50:
                        self._log(f"KELLY SKIP: no meaningful edge (p={p_true:.3f} price={entry_price:.3f} kelly_f={kelly_f_full:.3f})")
                        continue

                    # Log prediction for Brier score calibration
                    self.log_brier(
                        market=question, token_id=signal.get("token_id",""),
                        side="YES" if "YES" in signal.get("signal","") else "NO",
                        predicted_prob=p_true, market_price=entry_price,
                        kelly_f=kelly_f_full, kelly_size=amt,
                        reasoning=signal.get("ai_reasoning","")
                    )
                    self._log(f"KELLY: p={p_true:.3f} price={entry_price:.3f} f={kelly_f_full:.3f} → ${amt:.2f}")

                    # ── Execute ───────────────────────────────────────────────
                    min_conf = 55 if ai_enabled else 40
                    if float(signal.get("confidence", 0)) >= min_conf and signal.get("token_id"):
                        if signal["strategy"] == "arbitrage" and "BUY" in signal["signal"]:
                            self.place_market_order(signal["token_id"], "BUY", amt / 2)
                            if signal.get("no_token_id"):
                                self.place_market_order(signal["no_token_id"], "BUY", amt / 2)
                        elif signal["strategy"] == "marketMaking":
                            if signal.get("bid"):
                                self.place_limit_order(signal["token_id"], "BUY", signal["bid"], amt)
                            if signal.get("ask"):
                                self.place_limit_order(signal["token_id"], "SELL", signal["ask"], amt)
                        elif "BUY" in signal.get("signal", ""):
                            self.place_market_order(signal["token_id"], "BUY", amt)

                    try:
                        await bot._broadcast_state()
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)

            except Exception as e:
                self._log(f"Loop error: {e}", "error")
                import traceback
                self._log(traceback.format_exc(), "error")

            self._log(f"Cycle complete. Next scan in {interval}s…")
            try:
                await asyncio.sleep(interval)
            except Exception:
                pass
        self._log("Bot stopped.")
        self.send_telegram("Bot stopped.")

    def stop(self):
        self.running = False
        self.cancel_all_orders()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _log(self, msg: str, level: str = "info"):
        ts = datetime.utcnow().strftime("%H:%M:%S")
        self.log_lines.append(f"[{ts}] {msg}")
        if len(self.log_lines) > 500:
            self.log_lines = self.log_lines[-500:]
        getattr(log, level)(msg)

    async def _broadcast_state(self):
        if not self.ws_clients:
            return
        payload = json.dumps(self.get_state())
        dead = []
        for ws in self.ws_clients:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.ws_clients.remove(d)

    _proxy_wallet: str = ""

    def get_proxy_wallet(self) -> str:
        """Derive proxy wallet address from trade maker_address."""
        if self._proxy_wallet:
            return self._proxy_wallet
        try:
            trades = self.client.get_trades()
            if trades:
                addr = trades[0].get("maker_address", "")
                if addr:
                    self._proxy_wallet = addr
                    return addr
        except Exception:
            pass
        return ""

    _balance_cache: float = 0.0
    _balance_cache_time: float = 0.0
    _positions_cache: float = 0.0
    _positions_cache_time: float = 0.0

    def get_balance(self, force: bool = False) -> float:
        """Fetch USDC cash balance from Polymarket (sig_type=2 proxy wallet). Cached 30s."""
        if not force and time.time() - self._balance_cache_time < 30:
            return self._balance_cache
        if not self.client:
            return self._balance_cache
        try:
            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=2)
            result = self.client.get_balance_allowance(params)
            raw = float(result.get("balance", 0))
            self._balance_cache = round(raw / 1_000_000, 2)
            self._balance_cache_time = time.time()
            return self._balance_cache
        except Exception as e:
            log.debug(f"get_balance error: {e}")
            return self._balance_cache

    _PM_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Origin": "https://polymarket.com",
        "Referer": "https://polymarket.com/",
    }

    def get_positions_value(self, force: bool = False) -> float:
        """Fetch open positions value from Polymarket data API. Cached 60s."""
        if not force and time.time() - self._positions_cache_time < 60:
            return self._positions_cache
        try:
            proxy = os.getenv("POLYMARKET_FUNDER", "") or self.get_proxy_wallet()
            if not proxy:
                return self._positions_cache
            import json as _j
            url = f"https://data-api.polymarket.com/value?user={proxy}"
            req = _ureq.Request(url, headers=self._PM_HEADERS)
            with _ureq.urlopen(req, timeout=5) as r:
                data = _j.loads(r.read())
                if data and isinstance(data, list):
                    self._positions_cache = round(float(data[0].get("value", 0)), 2)
                    self._positions_cache_time = time.time()
                    return self._positions_cache
        except Exception as e:
            log.debug(f"get_positions_value error: {e}")
        return self._positions_cache

    def get_pnl_data(self) -> dict:
        """Fetch positions with P&L breakdown from Polymarket data API."""
        try:
            proxy = os.getenv("POLYMARKET_FUNDER", "") or self.get_proxy_wallet()
            if not proxy:
                return {}
            import json as _j
            url = f"https://data-api.polymarket.com/positions?user={proxy}"
            req = _ureq.Request(url, headers=self._PM_HEADERS)
            with _ureq.urlopen(req, timeout=8) as r:
                positions = _j.loads(r.read())
            total_pnl = round(sum(float(p.get("cashPnl", 0)) for p in positions), 2)
            total_invested = round(sum(float(p.get("initialValue", 0)) for p in positions), 2)
            pct = round(total_pnl / total_invested * 100, 2) if total_invested else 0
            return {
                "total_pnl": total_pnl,
                "pct_pnl": pct,
                "positions": [
                    {
                        "title": p.get("title", "")[:50],
                        "outcome": p.get("outcome", ""),
                        "size": round(float(p.get("size", 0)), 4),
                        "avg_price": round(float(p.get("avgPrice", 0)), 4),
                        "cur_price": round(float(p.get("curPrice", 0)), 4),
                        "current_value": round(float(p.get("currentValue", 0)), 2),
                        "cash_pnl": round(float(p.get("cashPnl", 0)), 2),
                        "pct_pnl": round(float(p.get("percentPnl", 0)), 2),
                    }
                    for p in positions
                ],
            }
        except Exception as e:
            log.debug(f"get_pnl_data error: {e}")
        return {}

    def get_state(self) -> dict:
        cash = self.get_balance()
        positions = self.get_positions_value()
        return {
            "running": self.running,
            "strategy": STRATEGY,
            "dry_run": DRY_RUN,
            "balance": cash,
            "positions_value": positions,
            "portfolio_value": round(cash + positions, 2),
            "trades": self.trades[-50:],
            "signals": self.signals[-50:],
            "log": self.log_lines[-100:],
            "brier": self.get_brier_stats(),
        }


# ── FastAPI ───────────────────────────────────────────────────────────────────

bot = PolymarketBot()
app = FastAPI(title="Polymarket Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
_bot_task: Optional[asyncio.Task] = None


@app.on_event("startup")
async def startup():
    if bot.connect():
        global _bot_task
        _bot_task = asyncio.create_task(bot.run_loop(interval=30.0))
        log.info("Bot auto-started on startup")
    else:
        log.warning("Could not connect — check .env credentials")


@app.get("/status")
def status():
    return JSONResponse(bot.get_state())


@app.get("/portfolio")
def portfolio():
    cash = bot.get_balance()
    positions = bot.get_positions_value()
    pnl = bot.get_pnl_data()
    return JSONResponse({
        "cash": cash,
        "positions_value": positions,
        "portfolio_value": round(cash + positions, 2),
        "total_pnl": pnl.get("total_pnl", 0),
        "pct_pnl": pnl.get("pct_pnl", 0),
        "positions": pnl.get("positions", []),
    })


@app.post("/start")
async def start_bot(interval: float = 30.0):
    global _bot_task
    if bot.running:
        return {"ok": False, "message": "Already running"}
    _bot_task = asyncio.create_task(bot.run_loop(interval=interval))
    return {"ok": True, "message": "Bot started"}


@app.post("/stop")
def stop_bot():
    bot.stop()
    return {"ok": True, "message": "Bot stopped"}


@app.get("/markets")
def markets():
    return JSONResponse(bot.get_markets(limit=30))


@app.get("/orderbook/{token_id}")
def orderbook(token_id: str):
    return JSONResponse(bot.get_orderbook(token_id) or {})


@app.post("/trade")
async def manual_trade(token_id: str, side: str, amount: float, order_type: str = "market", price: float = 0.0):
    if order_type == "limit" and price > 0:
        result = bot.place_limit_order(token_id, side, price, amount)
    else:
        result = bot.place_market_order(token_id, side, amount)
    await bot._broadcast_state()
    return JSONResponse(result)


@app.post("/cancel")
def cancel_all():
    bot.cancel_all_orders()
    return {"ok": True}


_market_cache: dict = {}
_market_cache_ts: float = 0

@app.get("/market-data")
def market_data():
    global _market_cache, _market_cache_ts
    if time.time() - _market_cache_ts < 300 and _market_cache:
        return JSONResponse(_market_cache)
    import urllib.request as _ur, json as _j
    result = {}
    # VIX, Oil (WTI), 10Y via FRED
    fred_series = [
        ("VIXCLS",     "vix",  "price"),
        ("DCOILWTICO", "oil",  "price"),
        ("DGS10",      "t10y", "price"),
    ]
    for sid, key, _ in fred_series:
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_API_KEY}&sort_order=desc&limit=2&file_type=json"
            with _ur.urlopen(url, timeout=8) as r:
                d = _j.loads(r.read())
            obs = [o for o in d["observations"] if o["value"] != "."]
            curr = float(obs[0]["value"])
            prev = float(obs[1]["value"]) if len(obs) > 1 else curr
            change = round(curr - prev, 3) if key == "t10y" else round((curr - prev) / prev * 100, 2)
            result[key] = {"price": curr, "change": change, "date": obs[0]["date"]}
        except Exception as e:
            log.warning(f"market-data {key} error: {e}")
    # CNN Fear & Greed
    try:
        _h = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Origin": "https://www.cnn.com",
            "Referer": "https://www.cnn.com/markets/fear-and-greed",
        }
        req = _ur.Request("https://production.dataviz.cnn.io/index/fearandgreed/graphdata", headers=_h)
        with _ur.urlopen(req, timeout=8) as r:
            d = _j.loads(r.read())
        fg = d.get("fear_and_greed", {})
        result["fear_greed"] = {
            "score": round(fg.get("score", 0)),
            "rating": fg.get("rating", "").replace("_", " ").title(),
        }
    except Exception as e:
        log.warning(f"market-data fear/greed error: {e}")
    _market_cache = result
    _market_cache_ts = time.time()
    return JSONResponse(result)


@app.get("/weather")
def weather_data():
    import json as _j
    try:
        base = "https://customer-api.open-meteo.com" if OPEN_METEO_API_KEY else "https://api.open-meteo.com"
        key_param = f"&apikey={OPEN_METEO_API_KEY}" if OPEN_METEO_API_KEY else ""
        url = (
            f"{base}/v1/forecast?latitude=40.7128&longitude=-74.0060"
            f"&current=temperature_2m,apparent_temperature,relative_humidity_2m,"
            f"weathercode,windspeed_10m,precipitation,uv_index,is_day,surface_pressure"
            f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,"
            f"windspeed_10m_max,weathercode,sunrise,sunset"
            f"&temperature_unit=fahrenheit&windspeed_unit=mph&forecast_days=3"
            f"{key_param}"
        )
        with _ureq.urlopen(url, timeout=8) as r:
            return JSONResponse(_j.loads(r.read()))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/fmp")
def fmp_data():
    return JSONResponse(bot.get_fmp_market())


@app.get("/research")
async def research_market(q: str = ""):
    """Debug endpoint: shows all research data + AI decision for a market question."""
    if not q:
        return JSONResponse({"error": "Pass ?q=market+question"})
    import json as _j
    # Find matching market
    markets = bot.get_markets(limit=30)
    mkt = next((m for m in markets if q.lower() in m.get("question","").lower()), None)
    yes_p = 0.5
    if mkt:
        raw = mkt.get("outcomePrices","[0.5,0.5]")
        prices = _j.loads(raw) if isinstance(raw, str) else raw
        yes_p = float(prices[0]) if prices else 0.5
    research = {
        "news":   bot.get_news_headlines(q[:60]),
        "tavily": bot.get_tavily_research(q[:80]) if TAVILY_API_KEY else "no Tavily key",
        "crypto": bot.get_crypto_prices(),
        "fmp":    bot.get_fmp_market().get("_sentiment"),
        "macro":  bot.get_macro_context(),
    }
    ai = await bot.analyze_with_claude(mkt or {"question": q}, yes_p, research) if ANTHROPIC_API_KEY else None
    return JSONResponse({"question": q, "yes_p": yes_p, "research": research, "ai_decision": ai})


@app.get("/macro")
def macro_data():
    import urllib.request as _ur, json as _j
    key = FRED_API_KEY
    series = [
        ("FEDFUNDS", "Fed Funds Rate", "%"),
        ("CPIAUCSL", "CPI YoY",        "%"),
        ("UNRATE",   "Unemployment",   "%"),
        ("GDP",      "GDP",            "B"),
    ]
    result = []
    for sid, label, unit in series:
        try:
            limit = 13 if sid == "CPIAUCSL" else 1
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={key}&sort_order=desc&limit={limit}&file_type=json"
            with _ur.urlopen(url, timeout=8) as r:
                d = _j.loads(r.read())
            obs = d["observations"]
            if sid == "CPIAUCSL" and len(obs) >= 13:
                cpi_now = float(obs[0]["value"])
                cpi_yr  = float(obs[12]["value"])
                value = str(round((cpi_now - cpi_yr) / cpi_yr * 100, 2))
                date  = obs[0]["date"]
            else:
                value = obs[0]["value"]
                date  = obs[0]["date"]
            result.append({"id": sid, "label": label, "value": value, "date": date, "unit": unit})
        except Exception as e:
            result.append({"id": sid, "label": label, "value": None, "date": None, "unit": unit})
    return JSONResponse(result)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    bot.ws_clients.append(websocket)
    try:
        await websocket.send_text(json.dumps(bot.get_state()))
        while True:
            msg = await websocket.receive_text()
            data = json.loads(msg)
            cmd = data.get("cmd")
            if cmd == "start":
                global _bot_task
                if not bot.running:
                    _bot_task = asyncio.create_task(bot.run_loop())
            elif cmd == "stop":
                bot.stop()
            elif cmd == "trade":
                bot.place_market_order(data["token_id"], data["side"], data["amount"])
    except WebSocketDisconnect:
        if websocket in bot.ws_clients:
            bot.ws_clients.remove(websocket)


if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=8000, reload=False)
