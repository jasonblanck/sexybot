"""
Polymarket Trading Bot — hardened for production security
"""
import asyncio, json, logging, os, re, secrets, time
from collections import defaultdict
from datetime import datetime
from enum import Enum
from typing import Optional

from dotenv import load_dotenv

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
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Request
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    from fastapi.security import APIKeyHeader
    from pydantic import BaseModel, field_validator
    from starlette.middleware.base import BaseHTTPMiddleware
except ImportError as e:
    raise SystemExit(f"Missing dependency: {e}\nRun: pip install fastapi uvicorn pydantic")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("polybot")

# ── Configuration ─────────────────────────────────────────────────────────────

CLOB_HOST      = os.getenv("CLOB_HOST", "https://clob.polymarket.com")
CHAIN_ID       = int(os.getenv("CHAIN_ID", "137"))
PRIVATE_KEY    = os.getenv("PRIVATE_KEY", "")
API_KEY        = os.getenv("POLYMARKET_API_KEY", "")
API_SECRET     = os.getenv("POLYMARKET_API_SECRET", "")
API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE", "")
STRATEGY       = os.getenv("STRATEGY", "momentum")
MAX_ORDER_SIZE = float(os.getenv("MAX_ORDER_SIZE", "10"))
DRY_RUN        = os.getenv("DRY_RUN", "true").lower() != "false"

# ── Security configuration ────────────────────────────────────────────────────

# Secret key that callers must supply in the X-Api-Key header
BOT_API_KEY        = os.getenv("BOT_API_KEY", "")
# Third-party API keys — never hardcode these
FRED_API_KEY       = os.getenv("FRED_API_KEY", "")
OPEN_METEO_API_KEY = os.getenv("OPEN_METEO_API_KEY", "")
# Comma-separated list of allowed CORS origins, e.g. "https://myapp.com,http://localhost:3000"
_raw_origins   = os.getenv("ALLOWED_ORIGINS", "")
ALLOWED_ORIGINS = [o.strip() for o in _raw_origins.split(",") if o.strip()] or ["http://localhost:3000"]
# Expose Swagger/ReDoc only when explicitly enabled (keep off in production)
DOCS_ENABLED   = os.getenv("DOCS_ENABLED", "false").lower() == "true"

if not BOT_API_KEY:
    log.warning("BOT_API_KEY is not set — all API endpoints are UNPROTECTED.")
    log.warning("  Generate one: python -c \"import secrets; print(secrets.token_hex(32))\"")

# ── Security helpers ──────────────────────────────────────────────────────────

_api_key_scheme = APIKeyHeader(name="X-Api-Key", auto_error=False)

# Regex for safe Polymarket token IDs
_TOKEN_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]{1,128}$')


def require_auth(api_key: Optional[str] = Depends(_api_key_scheme)) -> None:
    """Enforce API key authentication on every protected endpoint."""
    if not BOT_API_KEY:
        raise HTTPException(status_code=503, detail="Server authentication not configured")
    if not api_key:
        raise HTTPException(status_code=401, detail="API key required (X-Api-Key header)")
    try:
        if not secrets.compare_digest(api_key.encode("utf-8"), BOT_API_KEY.encode("utf-8")):
            raise HTTPException(status_code=401, detail="Invalid API key")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid API key")


def validate_token_id(token_id: str) -> str:
    """Validate a Polymarket token ID to prevent injection attacks."""
    if not _TOKEN_ID_RE.match(token_id):
        raise HTTPException(status_code=400, detail="Invalid token_id format")
    return token_id


class _RateLimiter:
    """Simple in-memory sliding-window rate limiter (per client IP)."""

    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._timestamps: dict = defaultdict(list)

    def is_allowed(self, key: str) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_seconds
        self._timestamps[key] = [t for t in self._timestamps[key] if t > cutoff]
        if len(self._timestamps[key]) >= self.max_requests:
            return False
        self._timestamps[key].append(now)
        return True


_trade_limiter   = _RateLimiter(max_requests=20, window_seconds=60)
_general_limiter = _RateLimiter(max_requests=60, window_seconds=60)


def check_trade_rate_limit(request: Request) -> None:
    client_ip = request.client.host if request.client else "unknown"
    if not _trade_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Trade rate limit exceeded (20 req/min)")


def check_general_rate_limit(request: Request) -> None:
    client_ip = request.client.host if request.client else "unknown"
    if not _general_limiter.is_allowed(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded (60 req/min)")


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Adds secure HTTP headers to every response."""

    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
        return response


# ── Input models ──────────────────────────────────────────────────────────────

class TradeSide(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class TradeOrderType(str, Enum):
    MARKET = "market"
    LIMIT  = "limit"


class TradeRequest(BaseModel):
    token_id:   str
    side:       TradeSide
    amount:     float
    order_type: TradeOrderType = TradeOrderType.MARKET
    price:      float = 0.0

    @field_validator("token_id")
    @classmethod
    def validate_token_id_field(cls, v: str) -> str:
        if not _TOKEN_ID_RE.match(v):
            raise ValueError("Invalid token_id format")
        return v

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Amount must be positive")
        if v > 10_000:
            raise ValueError("Amount exceeds maximum (10,000)")
        return round(v, 6)

    @field_validator("price")
    @classmethod
    def validate_price(cls, v: float) -> float:
        if v < 0 or v > 1:
            raise ValueError("Price must be between 0 and 1")
        return v


# ── Bot class ─────────────────────────────────────────────────────────────────

class PolymarketBot:
    def __init__(self):
        self.client: Optional[ClobClient] = None
        self.running = False
        self.trades:    list = []
        self.signals:   list = []
        self.log_lines: list = []
        self.ws_clients: list = []

    # ── Auth ──────────────────────────────────────────────────────────────────

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
                # Never log API keys, even partially
                self._log("L2 API credentials derived successfully.")
            ok = self.client.get_ok()
            self._log(f"Connected to Polymarket CLOB — {ok}")
            return True
        except Exception as e:
            self._log(f"Connection failed: {e}", "error")
            return False

    # ── Market data ───────────────────────────────────────────────────────────

    def get_markets(self, limit: int = 20) -> list:
        try:
            import urllib.request, json as _j
            url = (
                "https://gamma-api.polymarket.com/markets"
                f"?active=true&closed=false&limit={limit}&order=volume24hr&ascending=false"
            )
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
            "token_id":   token_id,
            "side":       side,
            "amount_usdc": amount_usdc,
            "price":      price,
            "shares":     round(amount_usdc / price, 4) if price else 0,
            "type":       "market",
            "dry_run":    DRY_RUN,
            "time":       datetime.utcnow().isoformat(),
            "status":     None,
            "order_id":   None,
        }
        if DRY_RUN:
            result["status"] = "simulated"
            self._log(f"[DRY RUN] {side} ${amount_usdc:.2f} @ {price:.4f}")
        else:
            try:
                order_args = MarketOrderArgs(token_id=token_id, amount=amount_usdc, side="BUY")
                signed = self.client.create_market_order(order_args)
                resp = self.client.post_order(signed, OrderType.FOK)
                result["status"]   = resp.get("status", "unknown")
                result["order_id"] = resp.get("orderID")
                self._log(f"ORDER placed: {side} ${amount_usdc:.2f} → status={result['status']}")
            except Exception as e:
                result["status"] = f"error: {e}"
                self._log(f"Order failed: {e}", "error")
        self.trades.append(result)
        return result

    def place_limit_order(self, token_id: str, side: str, price: float, size: float) -> dict:
        result = {
            "token_id": token_id,
            "side":     side,
            "price":    price,
            "size":     size,
            "type":     "limit",
            "dry_run":  DRY_RUN,
            "time":     datetime.utcnow().isoformat(),
            "status":   None,
            "order_id": None,
        }
        if DRY_RUN:
            result["status"] = "simulated"
            self._log(f"[DRY RUN] LIMIT {side} {size}@{price:.4f}")
        else:
            try:
                order_args = OrderArgs(token_id=token_id, price=price, size=size, side=side)
                signed = self.client.create_order(order_args)
                resp = self.client.post_order(signed, OrderType.GTC)
                result["status"]   = resp.get("status", "unknown")
                result["order_id"] = resp.get("orderID")
                self._log(f"LIMIT placed: {side} {size}@{price:.4f} → status={result['status']}")
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
            self._log(f"Cancelled all orders: {resp}")
        except Exception as e:
            self._log(f"Cancel failed: {e}", "error")

    # ── Strategies ────────────────────────────────────────────────────────────

    def get_macro_context(self) -> dict:
        import urllib.request, json as _j
        rate, cpi = 4.5, 3.0
        if not FRED_API_KEY:
            return {"fed_rate": rate, "cpi": cpi}
        try:
            url = (
                "https://api.stlouisfed.org/fred/series/observations"
                f"?series_id=FEDFUNDS&api_key={FRED_API_KEY}&sort_order=desc&limit=1&file_type=json"
            )
            with urllib.request.urlopen(url, timeout=5) as r:
                d = _j.loads(r.read())
                rate = float(d["observations"][0]["value"])
        except Exception as e:
            self._log(f"FRED rate fetch failed: {e}", "warning")
        try:
            url2 = (
                "https://api.stlouisfed.org/fred/series/observations"
                f"?series_id=CPIAUCSL&api_key={FRED_API_KEY}&sort_order=desc&limit=1&file_type=json"
            )
            with urllib.request.urlopen(url2, timeout=5) as r:
                d2 = _j.loads(r.read())
                cpi = float(d2["observations"][0]["value"])
        except Exception as e:
            self._log(f"FRED CPI fetch failed: {e}", "warning")
        return {"fed_rate": rate, "cpi": cpi}

    def get_weather_context(self) -> dict:
        import urllib.request, json as _j
        bad_weather = False
        try:
            base_url = (
                "https://api.open-meteo.com/v1/forecast"
                "?latitude=40.7128&longitude=-74.0060&current=weathercode,windspeed_10m"
            )
            if OPEN_METEO_API_KEY:
                base_url += f"&apikey={OPEN_METEO_API_KEY}"
            with urllib.request.urlopen(base_url, timeout=5) as r:
                d = _j.loads(r.read())
                code = d["current"]["weathercode"]
                wind = d["current"]["windspeed_10m"]
                bad_weather = code in [61,63,65,71,73,75,80,81,82,95,96,99] or wind > 20
        except Exception as e:
            self._log(f"Weather fetch failed: {e}", "warning")
        return {"bad_weather": bad_weather}

    def analyze(self, market: dict) -> Optional[dict]:
        import json as _j
        raw_prices = market.get("outcomePrices", "[0.5,0.5]")
        prices = _j.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
        yes_p = float(prices[0]) if prices else 0.5
        no_p  = float(prices[1]) if len(prices) > 1 else 1 - yes_p
        raw_ids = market.get("clobTokenIds", "[]")
        ids = _j.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        yes_id = ids[0] if ids else ""
        no_id  = ids[1] if len(ids) > 1 else ""
        question = market.get("question", market.get("slug", "unknown"))
        if not yes_id:
            return None

        if STRATEGY == "momentum":
            dev = abs(yes_p - 0.5)
            if dev > 0.05:
                macro   = self.get_macro_context()
                weather = self.get_weather_context()
                confidence = round(dev * 200, 1)
                amount = min(MAX_ORDER_SIZE, MAX_ORDER_SIZE * dev * 2)
                q_lower = question.lower()
                is_sports    = any(x in q_lower for x in ["win","game","match","league","cup","fc","nba","nfl","mlb","nhl"])
                is_political = any(x in q_lower for x in ["president","election","senate","congress","party","nominee"])
                is_economic  = any(x in q_lower for x in ["fed","rate","inflation","gdp","economy","recession","cpi"])
                if weather["bad_weather"] and is_sports:
                    confidence *= 0.75
                    self._log("WEATHER ADJ: reducing sports confidence (bad weather)")
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
                        "market": question, "amount": amount}

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
            gap   = abs(1 - total)
            if gap > 0.03:
                direction = "BUY BOTH" if total < 1 else "SELL BOTH"
                return {"strategy": "arbitrage", "signal": direction,
                        "token_id": yes_id, "no_token_id": no_id,
                        "yes_price": yes_p, "no_price": no_p,
                        "gap": round(gap * 100, 2),
                        "confidence": round(gap * 1000, 1),
                        "market": question, "amount": MAX_ORDER_SIZE}

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
                        "market": question, "amount": MAX_ORDER_SIZE / 2}
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
                    self._log(
                        f"SIGNAL [{signal['strategy']}] {signal['signal']} "
                        f"| conf={signal.get('confidence','?')}% | {signal['market'][:50]}"
                    )
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
            "running":  self.running,
            "strategy": STRATEGY,
            "dry_run":  DRY_RUN,
            "trades":   self.trades[-50:],
            "signals":  self.signals[-50:],
            "log":      self.log_lines[-100:],
        }


# ── FastAPI app ───────────────────────────────────────────────────────────────

bot = PolymarketBot()
app = FastAPI(
    title="Polymarket Bot",
    docs_url="/docs" if DOCS_ENABLED else None,
    redoc_url="/redoc" if DOCS_ENABLED else None,
    openapi_url="/openapi.json" if DOCS_ENABLED else None,
)

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["X-Api-Key", "Content-Type"],
)

_bot_task: Optional[asyncio.Task] = None


@app.on_event("startup")
async def startup():
    if not BOT_API_KEY:
        log.critical(
            "BOT_API_KEY not set — API is completely open! "
            "Set it in .env immediately."
        )
    if not bot.connect():
        log.warning("Could not connect to Polymarket — check .env credentials")


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/status", dependencies=[Depends(require_auth), Depends(check_general_rate_limit)])
def status():
    return JSONResponse(bot.get_state())


@app.post("/start", dependencies=[Depends(require_auth), Depends(check_trade_rate_limit)])
async def start_bot(interval: float = 30.0):
    global _bot_task
    if interval < 5 or interval > 3600:
        raise HTTPException(status_code=400, detail="interval must be 5–3600 seconds")
    if bot.running:
        return {"ok": False, "message": "Already running"}
    _bot_task = asyncio.create_task(bot.run_loop(interval=interval))
    return {"ok": True, "message": "Bot started"}


@app.post("/stop", dependencies=[Depends(require_auth), Depends(check_trade_rate_limit)])
def stop_bot():
    bot.stop()
    return {"ok": True, "message": "Bot stopped"}


@app.get("/markets", dependencies=[Depends(require_auth), Depends(check_general_rate_limit)])
def markets():
    return JSONResponse(bot.get_markets(limit=30))


@app.get("/orderbook/{token_id}", dependencies=[Depends(require_auth), Depends(check_general_rate_limit)])
def orderbook(token_id: str):
    safe_id = validate_token_id(token_id)
    return JSONResponse(bot.get_orderbook(safe_id) or {})


@app.post("/trade", dependencies=[Depends(require_auth), Depends(check_trade_rate_limit)])
async def manual_trade(trade: TradeRequest):
    if trade.order_type == TradeOrderType.LIMIT and trade.price > 0:
        result = bot.place_limit_order(trade.token_id, trade.side.value, trade.price, trade.amount)
    else:
        result = bot.place_market_order(trade.token_id, trade.side.value, trade.amount)
    await bot._broadcast_state()
    return JSONResponse(result)


@app.post("/cancel", dependencies=[Depends(require_auth), Depends(check_trade_rate_limit)])
def cancel_all():
    bot.cancel_all_orders()
    return {"ok": True}


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, api_key: str = ""):
    """Authenticated WebSocket. Connect as: /ws?api_key=<BOT_API_KEY>"""
    if not BOT_API_KEY:
        await websocket.close(code=4003, reason="Server auth not configured")
        return
    if not api_key:
        await websocket.close(code=4001, reason="api_key query param required")
        return
    try:
        valid = secrets.compare_digest(api_key.encode("utf-8"), BOT_API_KEY.encode("utf-8"))
    except Exception:
        valid = False
    if not valid:
        await websocket.close(code=4001, reason="Invalid API key")
        return

    await websocket.accept()
    bot.ws_clients.append(websocket)
    try:
        await websocket.send_text(json.dumps(bot.get_state()))
        while True:
            msg = await websocket.receive_text()
            try:
                data = json.loads(msg)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"error": "Invalid JSON"}))
                continue

            cmd = data.get("cmd")
            if cmd == "start":
                global _bot_task
                if not bot.running:
                    _bot_task = asyncio.create_task(bot.run_loop())
            elif cmd == "stop":
                bot.stop()
            elif cmd == "trade":
                try:
                    token_id = str(data.get("token_id", ""))
                    side     = str(data.get("side", ""))
                    amount   = float(data.get("amount", 0))
                    if not _TOKEN_ID_RE.match(token_id):
                        raise ValueError("Invalid token_id")
                    if side not in ("BUY", "SELL"):
                        raise ValueError("side must be BUY or SELL")
                    if amount <= 0 or amount > 10_000:
                        raise ValueError("amount out of range (0–10,000)")
                    bot.place_market_order(token_id, side, amount)
                except (ValueError, KeyError, TypeError) as e:
                    await websocket.send_text(json.dumps({"error": str(e)}))
            else:
                await websocket.send_text(json.dumps({"error": f"Unknown command: {cmd}"}))
    except WebSocketDisconnect:
        if websocket in bot.ws_clients:
            bot.ws_clients.remove(websocket)


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")  # Default to localhost; use 0.0.0.0 behind a reverse proxy
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("bot:app", host=host, port=port, reload=False)
