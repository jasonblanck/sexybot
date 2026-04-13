# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Infrastructure

- **VPS**: DigitalOcean at `159.65.201.165` — always-on, runs the bot 24/7
- **Service**: `sexybot` (systemd) — auto-starts on boot
- **Code root**: `/root/polybot/` on the VPS
- **Dashboard**: nginx serves `index.html` from `/var/www/html/`, proxies API to port 8000
- **GitHub**: `github.com/jasonblanck/sexybot` — source of truth for all code

## Common Commands

```bash
# VPS management
ssh root@159.65.201.165
systemctl restart sexybot
systemctl status sexybot
journalctl -u sexybot -f          # live logs

# Deploy local changes to VPS
scp <file> root@159.65.201.165:/root/polybot/<file>
# Or: git push origin main → ssh into VPS → git pull origin main

# Sync dashboard to nginx web root (after editing index.html)
ssh root@159.65.201.165 "cp /root/polybot/index.html /var/www/html/index.html"

# Reload nginx config
ssh root@159.65.201.165 "nginx -t && systemctl reload nginx"
```

## Architecture

The bot is split into focused modules — `bot.py` is the FastAPI dashboard/API layer; `main_v2.py` is the standalone trading engine. They are independent processes but share the same codebase.

| File | Role |
|---|---|
| `main_v2.py` | Async trading engine: discovery → WebSocket → signal → execute |
| `bot.py` | FastAPI app (port 8000): dashboard API, WebSocket feed, manual controls |
| `executor.py` | CLOB order placement, balance fetch, cancel; wraps `py_clob_client` |
| `market_maker.py` | Two-sided MM: quote both legs, inventory skew, circuit breaker |
| `risk.py` | `BalanceInfo`, `ExecutionGate` (EV + spread check), `DrawdownGuard`, `kelly_size` |
| `orderbook_ws.py` | WebSocket book manager; `BookSnapshot` with `mid`, `vamp`, `obi`, `spread_cents` |
| `discovery.py` | Fetches markets from Gamma API; `MarketFilter` chain |
| `redeemer.py` | Claims resolved positions on-chain via Gnosis Safe |
| `signing.py` | EIP-712 order signing (rarely called directly — `executor.py` uses `py_clob_client`) |
| `index.html` | Single-file dashboard (Chart.js, vanilla JS, mobile/desktop toggle) |
| `nginx.conf` | Production nginx config (documented here, deployed to `/etc/nginx/sites-enabled/default`) |

### Signal pipeline (momentum strategy)

```
fetch_markets() → BookManager (WebSocket) → estimate_true_probability()
    ↳ VAMP fair value  ↳ volume spike detection  ↳ OBI confirmation
→ kelly_size() → ExecutionGate → place_limit_order()
→ open_positions → profit-target / stop-loss exits
```

### Key design decisions

- **VAMP over simple mid**: `BookSnapshot.vamp` depth-weights top-5 bid/ask levels — more accurate fair value on imbalanced books
- **Quarter-Kelly sizing**: `kelly_size(prob, price, balance, kelly_fraction=0.25)` scales bets with edge × balance; returns 0 on no-edge
- **DrawdownGuard**: halts strategy (without restarting) if balance drops `MAX_DRAWDOWN_USD` within `DRAWDOWN_WINDOW` seconds
- **mutable box pattern**: `markets_box = [markets]` persists list across `strategy_loop` crash-restarts without rebinding the outer variable
- **bypass_gate**: MM orders skip the EV gate (they earn spread, not edge) but the MM's own reserve guard still enforces the $10 floor
- **MM inventory skewing**: `record_fill()` is defined but not yet wired to fill events — inventory stays 0, skewing inactive (known gap)

## Environment Variables

See `.env.example` for all variables with descriptions. Critical ones:

| Variable | Notes |
|---|---|
| `PRIVATE_KEY` | 32-byte hex Polygon wallet key (with `0x` prefix) |
| `POLYMARKET_FUNDER` | Gnosis Safe proxy address; omit for EOA mode |
| `DRY_RUN` | `true` by default — **must set `false` for live trading** |
| `STRATEGY` | `momentum` or `market_making` |
| `ANTHROPIC_API_KEY` | Required for AI market analysis in momentum signal |
| `MAX_DRAWDOWN_USD` | Kill-switch threshold (default $50 in 10 min) |

The `.env` file lives on the VPS at `/root/polybot/.env` and is **never committed to git**.

## New Machine Setup

All code is in GitHub — setup takes ~10 minutes:

```bash
# 1. Clone
git clone https://github.com/jasonblanck/sexybot.git
cd sexybot

# 2. Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3. Pinned packages (must override after pip install)
pip install 'websockets<16.0.0' 'eth-keyfile<0.9.0'

# 4. Secrets — copy .env from old machine or recreate from .env.example
cp .env.example .env
# Fill in real values from 1Password / old machine

# 5. SSH access to VPS
# Option A: copy id_ed25519 from old Mac (~/.ssh/id_ed25519)
# Option B: generate new key and add to VPS:
ssh-keygen -t ed25519 -C "new-macbook"
ssh-copy-id -i ~/.ssh/id_ed25519.pub root@159.65.201.165
```

### What to transfer from old Mac

| Item | Location | How |
|---|---|---|
| SSH private key | `~/.ssh/id_ed25519` | AirDrop / encrypted USB / 1Password |
| Local `.env` | `Desktop/JB/untitled folder/.env` | AirDrop / 1Password |
| GitHub token | embedded in git remote URL | Generate new token on new Mac |

The **bot itself runs on the VPS** — it keeps trading during the Mac transition. Nothing on the Mac is required for the bot to operate.

### GitHub auth on new Mac

The current remote uses an HTTPS token embedded in the URL. On the new Mac, use SSH-based auth instead (more secure, no token rotation needed):

```bash
# 1. Generate GitHub SSH key (or reuse the VPS key)
ssh-keygen -t ed25519 -C "github-newmac"
cat ~/.ssh/id_ed25519.pub   # add this to github.com → Settings → SSH Keys

# 2. Switch remote to SSH
git remote set-url origin git@github.com:jasonblanck/sexybot.git
```

## Nginx Proxy Routes

The nginx config (`nginx.conf`) proxies these paths to FastAPI on port 8000:

```
/status /portfolio /markets /orderbook /weather /fmp
/start  /stop      /trade    /paper/*   /settings /cancel
/api/*  → http://127.0.0.1:8000/  (strips /api/ prefix)
/ws     → WebSocket upgrade
```

Static files (`index.html`) are served from `/var/www/html/`. After editing `index.html`, sync it manually:
```bash
ssh root@159.65.201.165 "cp /root/polybot/index.html /var/www/html/index.html"
```
