# SexyBot disaster recovery runbook

When the bot is "running but not trading," walk this list top to bottom.
Every step is short. The 2026-04-28 CLOB V2 incident was 5 days of
downtime because we didn't have one of these documented; don't repeat.

## 0. Confirm the dashboard isn't lying

The dashboard reads from the bot's in-memory state. The bot can show
`Cash: $X` while the wallet on Polymarket says something completely
different. **Source of truth is the proxy wallet on Polygon**, not the
dashboard, not `BOT_CAPITAL_USD`, and not the polymarket.com web UI
(which can be logged into a different account).

```sh
ssh root@159.65.201.165 \
  "/root/polybot/venv/bin/python3 -c '
import os, json, urllib.request as u
from dotenv import load_dotenv; load_dotenv(\"/root/polybot/.env\")
addr = os.getenv(\"POLYMARKET_FUNDER\")
pad = addr.lower().removeprefix(\"0x\").rjust(64, \"0\")
for name, tok in [(\"USDC.e\",\"0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174\"),
                  (\"USDC\",  \"0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359\"),
                  (\"pUSD\",  \"0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB\")]:
    p = {\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"id\":1,
         \"params\":[{\"to\":tok,\"data\":\"0x70a08231\"+pad},\"latest\"]}
    r = json.loads(u.urlopen(u.Request(\"https://polygon-bor-rpc.publicnode.com\",
        data=json.dumps(p).encode(), headers={\"Content-Type\":\"application/json\"})).read())
    bal = int(r.get(\"result\",\"0x0\"),16)/1_000_000
    print(f\"{name}: \${bal:.2f}\")
'"
```

Also check positions via Polymarket's data-api (no auth, no CF block):

```sh
curl "https://data-api.polymarket.com/positions?user=$(ssh root@159.65.201.165 'grep POLYMARKET_FUNDER /root/polybot/.env | cut -d= -f2')"
```

## 1. Is the service running?

```sh
ssh root@159.65.201.165 "systemctl is-active sexybot && journalctl -u sexybot -n 20 --no-pager"
```

If it's `failed` or `inactive`, look at the journal tail for the
exception. Most common causes: missing env var after a `.env` edit,
stale Python process holding the port, syntax error in latest deploy.

## 2. Did the deploy actually land?

```sh
ssh root@159.65.201.165 "tail -3 /var/log/sexybot-deploy.log; cd /root/polybot && git log -1 --oneline"
```

Compare to `git log -1 --oneline` on this Mac. If they don't match,
either the cron pull is broken (check `crontab -l`) or the GitHub fetch
is failing — the script logs `GIT FETCH FAILED` to the same log when
that happens.

## 3. Is it placing orders but they all fail the same way?

Bucket the error first — that tells you which fix:

```sh
ssh root@159.65.201.165 "journalctl -u sexybot --since '4 hours ago' --no-pager \
  | grep -oE 'status_code=[0-9]+|order_version_mismatch|allowance|Could not derive' \
  | sort | uniq -c | sort -rn"
```

| Symptom | Diagnosis | Fix |
|---|---|---|
| `order_version_mismatch` on every order | Polymarket bumped the EIP-712 schema or contract version (V2 cutover, future V3, etc.) | Upgrade `py_clob_client_v2` (or successor SDK) and redeploy |
| `Unauthorized/Invalid api key` on `/order` and `/cancel-all` | API creds invalid — server rotated or schema changed | Run `scripts/bootstrap_v2_api_keys.py` from a machine with `curl_cffi` installed; paste the three values into `.env`; restart |
| `Could not derive api key!` | No keys exist for this wallet under the current schema | Same as above — bootstrap mints a fresh one |
| `not enough allowance` on every order | New exchange contract isn't approved for the proxy's collateral | Log into polymarket.com with the bot's EOA (currently MetaMask "Account 7", address `0xBb1639…2909d9`) and let the UI prompt the approval |
| Many `OBI SKIP` / `pretrade abort`, no executions | Not a bug — books are thin or moving fast. Cooldowns will clear in 30 min | Watch and wait; only investigate if it persists past a normal trading window |

## 4. Can the bot read its balance?

```sh
ssh root@159.65.201.165 "journalctl -u sexybot --since '5 minutes ago' --no-pager \
  | grep -E 'Cash:|on-chain|get_balance' | tail -10"
```

If `Cash: $0.00` and you know the wallet has funds, the V2 collateral
moved to a token the RPC fallback doesn't query. The current list is
USDC.e / USDC / pUSD (`bot.py` → `get_balance`). Add the new contract
address to that list and redeploy.

If you ever see `BOT_CAPITAL_USD` being used (look for an absence of the
`on-chain (…)` log line), it's a temporary override — clear it from
`.env` once the on-chain read works again.

## 5. API keys: the V2-style bootstrap

Whenever the bot's L2-authed calls (e.g. `cancel-all`, `post_order`)
return 401 with otherwise-correct HMAC headers, the keys need rotating.
The SDK can't do it because Cloudflare blocks `/auth/api-key`. Use:

```sh
# from any machine with python ≥ 3.10 — does NOT need to be the VPS
pip install curl_cffi py_clob_client_v2 python-dotenv
scp root@159.65.201.165:/root/polybot/.env /tmp/polybot.env
python scripts/bootstrap_v2_api_keys.py /tmp/polybot.env
# paste the three POLYMARKET_API_* lines into /root/polybot/.env
ssh root@159.65.201.165 systemctl restart sexybot
shred -u /tmp/polybot.env
```

The script is deterministic per (wallet, current Polymarket schema), so
running it twice in a row gives the same key the second time — it's
safe to rerun.

## 6. Backups

`/root/polybot/.env` contains secrets and **is not in git** (see
`.gitignore`). It's the only thing on the VPS that can't be rebuilt
from this repo. Periodically refresh the iCloud copy:

```sh
scp root@159.65.201.165:/root/polybot/.env \
    "$HOME/Library/Mobile Documents/com~apple~CloudDocs/sexybot.env.$(date +%Y%m%d)"
```

The trade DB at `/root/polybot/sexybot.db` (SQLite) holds the realized
P&L history. If you care about preserving it through a VPS migration:

```sh
ssh root@159.65.201.165 "tar czf /tmp/sexybot-state.tgz -C /root/polybot .env sexybot.db"
scp root@159.65.201.165:/tmp/sexybot-state.tgz \
    "$HOME/Library/Mobile Documents/com~apple~CloudDocs/sexybot-vps-backup-$(date +%Y%m%d).tgz"
```
