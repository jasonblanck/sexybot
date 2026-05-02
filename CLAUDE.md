# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Infrastructure

- **VPS**: DigitalOcean at `159.65.201.165` — always-on, runs the bot 24/7
- **Service**: `sexybot` (systemd) — auto-starts on boot. Unit file lives at `/etc/systemd/system/sexybot.service` on the VPS; canonical copy in repo at [scripts/sexybot.service](scripts/sexybot.service).
- **Code root**: `/root/polybot/` on the VPS
- **Dashboard**: nginx serves `index.html` from `/var/www/html/`, proxies API to port 8000
- **GitHub**: `github.com/jasonblanck/sexybot` — source of truth for all code

## When the bot stops trading

See [docs/disaster_recovery.md](docs/disaster_recovery.md) — runbook for `order_version_mismatch`, 401 on `/order`, `Could not derive api key`, and other failure modes that have actually bitten us. Includes the curl_cffi-based V2 API key bootstrap ([scripts/bootstrap_v2_api_keys.py](scripts/bootstrap_v2_api_keys.py)) needed when Polymarket rotates their auth schema.

## Common Commands

```bash
# VPS management
ssh root@159.65.201.165
systemctl restart sexybot
systemctl status sexybot
journalctl -u sexybot -f          # live logs
tail -f /var/log/sexybot-deploy.log  # auto-deploy log (cron writes here on every deploy)

# Deploy local changes to VPS — just push to main. The VPS cron pulls + restarts within 60s.
git push origin main
# Confirm deploy landed:
tail -1 /var/log/sexybot-deploy.log   # on VPS

# Manual override if you need to bypass the cron (e.g. hotfix a syntax error locally)
ssh root@159.65.201.165 "cd /root/polybot && /root/polybot/deploy-pull.sh"

# Reload nginx config (rare — only if you edited nginx.conf)
ssh root@159.65.201.165 "nginx -t && systemctl reload nginx"

# Emergency kill switch — drops the bot to rule-based strategies, no Claude calls
ssh root@159.65.201.165 "echo 'CLAUDE_MAX_DISABLE=true' >> /root/polybot/.env && systemctl restart sexybot"
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
| `STRATEGY` | `momentum`, `market_making`, `econFlow`, etc. |
| `ANTHROPIC_API_KEY` | Required for every Claude Max feature (regime, nightly, deep, etc.) |
| `API_SECRET_KEY` | Protects dashboard write endpoints — the one the frontend prompts for |
| `MAX_DRAWDOWN_USD` | Kill-switch threshold (default $50 in 10 min) |
| `CLAUDE_MODEL` / `CLAUDE_FAST_MODEL` | Sonnet 4.6 / Haiku 4.5 |
| `CLAUDE_MAX_DISABLE` | Emergency off switch for all AI features — set `true` and restart to drop to rule-based only |
| `REGIME_DETECTOR` / `NIGHTLY_REVIEW` / `PRETRADE_CHECK` / `DEEP_ANALYZE` / `BACKTEST_WEEKLY` | Per-feature toggles |
| `DEEP_ANALYZE_MIN_USD` | Trade size threshold for Opus + web_search verification |
| `TELEGRAM_TOKEN` / `TELEGRAM_CHAT_ID` | Required for operator alerts (hostile regime, deep abort, nightly review) |

The `.env` file lives on the VPS at `/root/polybot/.env` and is **never committed to git**. It also typically lives on the operator's dev Mac for local testing — same contents.

## Auto-Deploy (VPS cron pull)

The VPS pulls `main` from GitHub every minute via cron and restarts sexybot + nginx on any change.

```bash
# Installed script
/root/polybot/deploy-pull.sh

# Cron entry (run `crontab -l` to verify)
* * * * * /root/polybot/deploy-pull.sh

# Deploy log (one line per deploy, nothing when there's no change)
/var/log/sexybot-deploy.log
```

Flow: merge PR → `main` updates → within ≤60s the cron tick pulls, resets, copies `index.html` + `login.html` to nginx root, syncs `nginx.conf` to `/etc/nginx/sites-enabled/default` (auto-reverts if `nginx -t` rejects the new config), reloads nginx, restarts sexybot, logs the deploy. Failures are captured in `/var/log/sexybot-deploy.log`. To reinstall if it ever gets removed, see `scripts/deploy-pull.sh` header for the install commands.

## New Mac / New Machine Setup

**The bot runs on the VPS, not your Mac.** Migrating Macs does not require any downtime; the bot keeps trading the entire time.

### What to transfer from the old Mac

| Item | Path on old Mac | Why |
|---|---|---|
| SSH private key | `~/.ssh/id_ed25519` (+ `.pub`) | Needed for `ssh root@159.65.201.165` and GitHub SSH auth |
| `known_hosts` (optional) | `~/.ssh/known_hosts` | Skips the "do you trust this host" prompt on first connect |
| Local `.env` (optional) | `<wherever your local sexybot dir is>/.env` | Only needed if you want to run the bot locally for dev; contains the same contents as the VPS .env |

Transfer via: AirDrop / encrypted USB / 1Password / iCloud Drive. Do NOT email the `.env` or private key in plaintext.

### Setup on the new Mac

```bash
# 1. Xcode command line tools (git + ssh + compilers)
xcode-select --install

# 2. Restore SSH keys
mkdir -p ~/.ssh && chmod 700 ~/.ssh
# Move the transferred key files into ~/.ssh/
mv ~/Desktop/id_ed25519* ~/.ssh/  # wherever you parked them
chmod 600 ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub

# 3. Test VPS + GitHub access
ssh root@159.65.201.165 "echo ok"
ssh -T git@github.com   # should say "Hi jasonblanck!"

# 4. Clone the repo
cd ~/Desktop   # or wherever you want the local dev copy
git clone git@github.com:jasonblanck/sexybot.git
cd sexybot

# 5. Drop the .env back in (if you want to run locally)
mv ~/Desktop/.env .env

# 6. OPTIONAL — local Python env for dev/testing. NOT required for the
#    bot to run (bot runs on VPS). Only needed if you want to `python3
#    main_v2.py` or `python3 bot.py` on the Mac.
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install 'websockets<16.0.0' 'eth-keyfile<0.9.0'   # pinned per requirements comments

# 7. Bookmark the dashboard
open https://159.65.201.165/
```

After this, your dev loop is: edit locally → commit + push to main → cron auto-deploys within 60s. No SSH needed for normal development.

### If the SSH key from the old Mac isn't available (lost, not transferred)

Generate new keys on the new Mac and enroll them:

```bash
# New SSH key
ssh-keygen -t ed25519 -C "new-macbook"

# Enroll with GitHub — paste the .pub content at:
# https://github.com/settings/keys
cat ~/.ssh/id_ed25519.pub

# Enroll with the VPS — use the DigitalOcean web console (Cloud → Droplet → Access → Launch Console)
# to paste the .pub content into /root/.ssh/authorized_keys:
#   echo "<paste pub key here>" >> /root/.ssh/authorized_keys
```

### What's on the VPS that is NOT in git (and worth backing up)

These live on the VPS filesystem and would be lost if the droplet died. Periodically back up to a safe location:

| File | What | Why it matters |
|---|---|---|
| `/root/polybot/.env` | All API keys + wallet private key | **Losing `PRIVATE_KEY` loses access to the wallet funds** |
| `/root/polybot/trades.db` | SQLite DB: trades, brier_scores, strategy_reviews, regime_log, pretrade_log, deep_analyses, backtests, positions | Losing this loses all attribution / calibration history |
| `/var/log/sexybot-deploy.log` | Auto-deploy audit trail | Nice to have, not critical |

Quick backup command (run on VPS, outputs a single tarball):
```bash
tar -czf /tmp/sexybot-backup-$(date +%Y%m%d).tgz \
    /root/polybot/.env /root/polybot/trades.db 2>/dev/null
ls -lh /tmp/sexybot-backup-*.tgz
```
Then `scp` it to your Mac or to another safe location.

## Nginx Proxy Routes

The nginx config (`nginx.conf`) proxies these paths to FastAPI on port 8000:

```
/status /portfolio /markets /orderbook /weather /fmp
/start  /stop      /trade    /paper/*   /settings /cancel
/regime /reviews   /pretrade /pnl       /brier    /deep
/backtest /health
/api/*  → http://127.0.0.1:8000/  (strips /api/ prefix — used for /macro, /market-data, /research)
/ws     → WebSocket upgrade
```

Static files (`index.html`) are served from `/var/www/html/`. The auto-deploy cron syncs it on every commit; no manual sync needed.
