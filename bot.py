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
FRED_API_KEY      = os.getenv("FRED_API_KEY", "")
OPEN_METEO_API_KEY = os.getenv("OPEN_METEO_API_KEY", "")


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
            self.client = ClobClient(
                host=CLOB_HOST,
                chain_id=CHAIN_ID,
                key=PRIVATE_KEY,
                creds=creds,
                funder=funder if funder else None,
                signature_type=2 if funder else 0,
            )
            if not creds:
                self._log("Deriving L2 API credentials from wallet…")
                derived = self.client.create_or_derive_api_creds()
                self.client.set_api_creds(derived)
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
        if result.get("status") and "error" not in str(result.get("status","")).lower():
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
        import urllib.request, json as _j
        if time.time() - self._weather_cache_time < 3600 and self._weather_cache:
            return self._weather_cache
        try:
            url = f"https://api.open-meteo.com/v1/forecast?latitude=40.7128&longitude=-74.0060&current=weathercode,windspeed_10m{f'&apikey={OPEN_METEO_API_KEY}' if OPEN_METEO_API_KEY else ''}"
            with _ureq.urlopen(url, timeout=5) as r:
                d = _j.loads(r.read())
                code = d["current"]["weathercode"]
                wind = d["current"]["windspeed_10m"]
                bad_weather = code in [61,63,65,71,73,75,80,81,82,95,96,99] or wind > 20
        except:
            bad_weather = False
        self._weather_cache = {"bad_weather": bad_weather}
        self._weather_cache_time = time.time()
        return self._weather_cache

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
            if dev > 0.05:
                macro = self.get_macro_context()
                weather = self.get_weather_context()
                confidence = round(dev * 200, 1)
                amount = min(MAX_ORDER_SIZE, MAX_ORDER_SIZE * dev * 2)
                q_lower = question.lower()
                is_sports = any(x in q_lower for x in ["win","game","match","league","cup","fc","nba","nfl","mlb","nhl"])
                is_political = any(x in q_lower for x in ["president","election","senate","congress","party","nominee"])
                is_economic = any(x in q_lower for x in ["fed","rate","inflation","gdp","economy","recession","cpi"])
                if weather["bad_weather"] and is_sports:
                    confidence *= 0.75
                    self._log(f"WEATHER ADJ: reducing sports confidence (bad weather)")
                if macro["fed_rate"] > 5.0 and is_economic:
                    confidence *= 1.15
                    self._log(f"MACRO ADJ: boosting economic market confidence (rate={macro['fed_rate']})")
                if macro["cpi"] > 4.0 and is_political:
                    amount *= 0.8
                    self._log(f"MACRO ADJ: reducing political exposure (high CPI={macro['cpi']})")
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
        self._log(f"Bot started — strategy={STRATEGY} dry_run={DRY_RUN} interval={interval}s")
        while self.running:
            try:
                markets = self.get_markets(limit=20)
                self._log(f"Scanning {len(markets)} markets…")
                for mkt in markets:
                    if not self.running:
                        break
                    signal = self.analyze(mkt)
                    if not signal:
                        continue
                    self.signals.append({**signal, "time": datetime.utcnow().isoformat()})
                    self._log(f"SIGNAL [{signal['strategy']}] {signal['signal']} | conf={signal.get('confidence','?')}% | {signal['market'][:50]}")
                    daily_loss = self.get_daily_loss()
                    if daily_loss >= DAILY_LOSS_LIMIT:
                        self._log(f"DAILY LOSS LIMIT HIT: ${daily_loss:.2f} — stopping trading", "error")
                        self.send_telegram(f"⚠️ Daily loss limit hit: ${daily_loss:.2f}. Bot stopped trading.")
                        self.running = False
                        break
                    if signal.get("token_id") and self.has_position(signal["token_id"]):
                        self._log(f"SKIP: already have position in {signal['market'][:40]}")
                        continue
                    if float(signal.get("confidence", 0)) >= 50 and signal.get("token_id"):
                        if signal["strategy"] == "arbitrage" and "BUY" in signal["signal"]:
                            self.place_market_order(signal["token_id"], "BUY", signal["amount"] / 2)
                            if signal.get("no_token_id"):
                                self.place_market_order(signal["no_token_id"], "BUY", signal["amount"] / 2)
                        elif signal["strategy"] == "marketMaking":
                            if signal.get("bid"):
                                self.place_limit_order(signal["token_id"], "BUY", signal["bid"], signal["amount"])
                            if signal.get("ask"):
                                self.place_limit_order(signal["token_id"], "SELL", signal["ask"], signal["amount"])
                        elif "BUY" in signal.get("signal", ""):
                            self.place_market_order(signal["token_id"], "BUY", signal["amount"])
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

    def get_state(self) -> dict:
        return {
            "running": self.running,
            "strategy": STRATEGY,
            "dry_run": DRY_RUN,
            "trades": self.trades[-50:],
            "signals": self.signals[-50:],
            "log": self.log_lines[-100:],
        }


# ── FastAPI ───────────────────────────────────────────────────────────────────

bot = PolymarketBot()
app = FastAPI(title="Polymarket Bot")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
_bot_task: Optional[asyncio.Task] = None


@app.on_event("startup")
async def startup():
    if not bot.connect():
        log.warning("Could not connect — check .env credentials")


@app.get("/status")
def status():
    return JSONResponse(bot.get_state())


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


@app.get("/macro")
def macro_data():
    import urllib.request as _ur, json as _j
    key = FRED_API_KEY
    series = [
        ("FEDFUNDS", "Fed Funds Rate", "%"),
        ("CPIAUCSL", "CPI", ""),
        ("UNRATE",   "Unemployment", "%"),
        ("GDP",      "GDP", "B"),
    ]
    result = []
    for sid, label, unit in series:
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={key}&sort_order=desc&limit=1&file_type=json"
            with _ur.urlopen(url, timeout=8) as r:
                d = _j.loads(r.read())
            obs = d["observations"][0]
            result.append({"id": sid, "label": label, "value": obs["value"], "date": obs["date"], "unit": unit})
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
