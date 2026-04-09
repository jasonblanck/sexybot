# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Deployment

The bot runs on a DigitalOcean VPS at `159.65.201.165`. SSH access: `ssh root@159.65.201.165`.

**After every file edit, changes are automatically pushed to GitHub (`jasonblanck/sexybot`) via a PostToolUse hook.**

To deploy changes to the server after editing locally:
```bash
rsync -avz --exclude='.git' --exclude='.DS_Store' --exclude='.env' \
  -e "ssh -p 22" \
  "/Users/jasonblanck/Desktop/untitled folder/" \
  root@159.65.201.165:/var/www/html/
```

For `.env` changes (never rsync'd automatically — contains secrets):
```bash
scp -P 22 "/Users/jasonblanck/Desktop/untitled folder/.env" root@159.65.201.165:/root/polybot/.env
```

## Bot Management (on server)

The bot runs as a systemd service called `sexybot`:
```bash
systemctl restart sexybot    # restart
systemctl status sexybot     # check status
journalctl -u sexybot -f     # live logs
tail -f /root/polybot/bot.log  # bot's own log file
```

The bot's working directory on the server is `/root/polybot/` (not `/var/www/html/`). The frontend (index.html) is served from `/var/www/html/` via nginx.

To start the trading loop after a restart (the bot connects on startup but doesn't trade until started):
```bash
curl -X POST http://localhost:8000/start
```

## Architecture

Two components:
1. **`bot.py`** — FastAPI app + trading logic. Runs on port 8000 via uvicorn. Exposes REST API and WebSocket at `/ws`.
2. **`index.html`** — Single-file dashboard. Connects to the bot via WebSocket (`ws://<host>/ws`) for real-time updates, with HTTP polling as fallback.

Nginx (port 80) proxies:
- `/api/` → `localhost:8000/`
- `/ws` → `localhost:8000/ws` (WebSocket upgrade)

### Bot internals

- `PolymarketBot` class owns all trading state: `trades`, `signals`, `log_lines`, `ws_clients`
- `connect()` authenticates with Polymarket CLOB using `PRIVATE_KEY`. If `POLYMARKET_API_KEY/SECRET/PASSPHRASE` are set, uses them directly; otherwise derives L2 credentials from the wallet key.
- `run_loop()` is the async trading loop — must be started via `POST /start` or the `/ws` `{"cmd":"start"}` message
- Trades and positions are persisted to SQLite at `/root/polybot/trades.db`
- `DRY_RUN=false` means real orders are placed. `MAX_ORDER_SIZE` caps each order in USDC.

### Strategies (set via `STRATEGY` in `.env`)
- `momentum` — buys direction when price deviates >5% from 0.5
- `meanReversion` — fades extremes when deviation >35%
- `arbitrage` — exploits YES+NO mispricing >3%
- `marketMaking` — posts bid/ask quotes around midpoint on illiquid markets

### API endpoints
- `GET /status` — full bot state (trades, signals, log)
- `POST /start?interval=30` — start trading loop
- `POST /stop` — stop and cancel all orders
- `GET /markets` — top 30 markets by 24h volume
- `POST /trade` — manual trade
- `WS /ws` — real-time state pushes + commands (`start`, `stop`, `trade`)

## Environment variables

See `.env.example`. Key vars:
- `PRIVATE_KEY` — 32-byte hex Polygon wallet key (with `0x` prefix)
- `POLYMARKET_FUNDER` — proxy wallet address (if using a proxy wallet setup)
- `DRY_RUN` — set to `false` for live trading
- `MAX_ORDER_SIZE` — per-trade cap in USDC
- `DAILY_LOSS_LIMIT` — bot stops trading for the day if total spend exceeds this
