# Troubleshooting: bot is up but not trading

The 2026-04-30 recovery turned up several independent failure modes that all
present the same way ("dashboard shows balance + service is `active` + journal
is rolling, but no trades fire"). This is the diagnostic order — run each
step until something is non-green, then jump to the matching fix.

> **First rule:** if `cycle_wallet_too_low` is anywhere in `skip_counts.cumulative`,
> nothing else matters until you confirm Polymarket UI shows the expected cash
> balance for the connected account. The bot reports `cash: 0.0` for both
> "wallet empty" and "wrong wallet configured."

## Quick diagnostic flow

```
                    ┌─ cumulative all 0 ──────► bot never completed a cycle → "stuck startup"
/status skip_counts │
                    └─ shows skip reasons ────► identify dominant gate ─┐
                                                                        ▼
                          ┌─ cycle_wallet_too_low ──► funds question
                          ├─ pretrade_abort  ────────► orderbook question
                          ├─ exec_cooldown   ────────► error-cooldown question
                          ├─ exec_price_ceiling ─────► MAX_ENTRY_PRICE / market choice
                          ├─ obi_oppose      ────────► signal direction vs book
                          ├─ edge_too_low    ────────► regime / volatility scaling
                          ├─ low_confidence  ────────► AI_MIN_CONFIDENCE
                          ├─ ai_skip / pretrade_abort ─► Claude sanity rejects
                          └─ exec_bad_*      ────────► Polymarket order failures
```

All checks below assume `APIKEY=$(grep '^API_SECRET_KEY=' /root/polybot/.env | cut -d= -f2-)`
already in your shell on the VPS.

## 1. Service alive + auth working?

```bash
systemctl is-active sexybot
journalctl -u sexybot --since '2 min ago' --no-pager | grep -E 'Connection failed|Could not derive|Unauthorized|Connected'
```

- `Connected to Polymarket CLOB — OK` → auth fine, jump to #2.
- `Could not derive api key` → L2 creds missing or stale. Polymarket's CLOB
  has API keys per-wallet. If `.env` lost the `POLYMARKET_API_*` lines, the bot
  retries derivation on every startup. From a residential IP (not the VPS's
  Cloudflare-blocked datacenter IP), run on Mac:
  ```bash
  /tmp/pmenv/bin/python3 - <<'PY'
  from py_clob_client.client import ClobClient
  c = ClobClient(host='https://clob.polymarket.com', chain_id=137,
                 key='<PRIVATE_KEY>', funder='<POLYMARKET_FUNDER>',
                 signature_type=2)
  creds = c.create_or_derive_api_creds()
  print('POLYMARKET_API_KEY=' + creds.api_key)
  print('POLYMARKET_API_SECRET=' + creds.api_secret)
  print('POLYMARKET_API_PASSPHRASE=' + creds.api_passphrase)
  PY
  ```
  Paste the three lines into VPS `.env`, restart.
- `Unauthorized/Invalid api key` → creds are present but stale, OR creds were
  derived against a different wallet than `PRIVATE_KEY` signs as. Both wallets
  must agree (signer can derive creds for the funder Safe only if it's a Safe
  owner). Check Safe ownership and re-derive with the correct EOA.

## 2. Polymarket UI vs bot's view of cash

```bash
curl -sk -H "x-api-key: $APIKEY" https://159.65.201.165/portfolio
```

vs Polymarket UI logged in as the same wallet. Three failure modes:

| Bot cash | UI cash (same wallet) | Diagnosis |
|---|---|---|
| 0.0 | 0.0 | Wallet really is empty. Deposit. |
| 0.0 | $X | `POLYMARKET_FUNDER` in `.env` doesn't match the Safe Polymarket created. Check `bot._has_real_trade_within` debug. |
| 0.0 | $X (different account) | Funds went to a different Polymarket account. Check the deposit transaction destination. |
| $X | $X | Bot can see funds, problem is downstream. Continue. |

## 3. Orderbook flowing?

If `pretrade_abort` is the dominant gate AND every abort cites
`bid=None, ask=None`, the SDK's orderbook return shape isn't being parsed
correctly. Should be fixed in the post-2026-04-30 codebase. To verify:

```bash
/root/polybot/venv/bin/python3 -c "
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
import re; env={}
for l in open('/root/polybot/.env'):
    m=re.match(r'\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)\s*\$', l)
    if m: env[m.group(1)] = m.group(2).strip().strip(chr(34)).strip(chr(39))
creds = ApiCreds(api_key=env['POLYMARKET_API_KEY'], api_secret=env['POLYMARKET_API_SECRET'], api_passphrase=env['POLYMARKET_API_PASSPHRASE'])
c = ClobClient(host='https://clob.polymarket.com', chain_id=137, key=env['PRIVATE_KEY'], creds=creds, funder=env['POLYMARKET_FUNDER'], signature_type=2)
import json, urllib.request
url = 'https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=1&order=volume24hr'
req = urllib.request.Request(url, headers={'User-Agent': 'polybot/1.0'})
m = json.loads(urllib.request.urlopen(req, timeout=8).read())[0]
ids = json.loads(m['clobTokenIds'])
book = c.get_order_book(ids[0])
print(type(book).__name__, 'bids:', len(getattr(book,'bids',[]) or []), 'asks:', len(getattr(book,'asks',[]) or []))
"
```

Should print `OrderBookSummary bids: N asks: M` with both N and M > 0 on a
top-volume market. If it returns 0/0 or errors, something changed in the
SDK's return shape — see `get_orderbook_depth` in `bot.py`.

## 4. Signals reaching the wire but rejected?

If `pretrade_abort` is dominant *with real reasons* (mentions actual prices,
not None), the safety harness is doing its job. The signaled markets are
likely:

- Near-decided (yes_p in 0.92-0.97 or 0.03-0.08 range) — tighten
  `NEAR_DECIDED_BAND`.
- Wide-spread thin books (book empty in mid range) — tighten `MAX_ENTRY_PRICE`
  or accept that some markets are intrinsically untradeable.

Look at recent SIGNAL/ABORT messages:
```bash
journalctl -u sexybot --since '10 min ago' --no-pager | grep -E 'SIGNAL|ABORT' | tail
```

If the same market repeats every cycle, the `pretrade_abort_cooldown` should
catch it after `PRETRADE_ABORT_THRESHOLD` aborts. Check the cooldown counter
in `skip_counts.cumulative`.

## 5. Anthropic dead?

```bash
journalctl -u sexybot --since '5 min ago' --no-pager | grep -E 'api.anthropic.com.*HTTP/1.1 [45]' | tail -3
```

If you see `400 ... credit balance is too low`, top up at
https://console.anthropic.com/settings/billing.

If you see `429`, the bot is rate-limited — typically self-resolves but you
can lower `SCAN_INTERVAL_S` × `AI_MIN_CONFIDENCE` to reduce burst load.

If `Claude response truncated (max_tokens)` warnings flood: bump
`CLAUDE_ANALYZE_MAX_TOKENS` (default 1200, 2500 is safe).

## 6. The "everything looks fine but no trades for hours" mode

The no-trade watchdog (`NO_TRADE_ALERT_HOURS`, default 6) sends a Telegram ping
in this case. If you're getting that ping but you've already verified #1-#5,
run the comprehensive bug check (`scripts/bugcheck.sh` if added, or this
inline block):

```bash
ssh root@159.65.201.165 bash <<'BC'
APIKEY=$(grep '^API_SECRET_KEY=' /root/polybot/.env | cut -d= -f2-)
echo "=== /status ==="
curl -sk -H "x-api-key: $APIKEY" https://159.65.201.165/status \
  | python3 -m json.tool | head -50
echo
echo "=== last 30m errors ==="
journalctl -u sexybot --since '30 min ago' --no-pager | grep -cE 'ERROR|WARNING'
echo
echo "=== last 30m signals ==="
journalctl -u sexybot --since '30 min ago' --no-pager | grep SIGNAL | wc -l
BC
```

## Recovery order summary

1. **Check `skip_counts.cumulative` first.** It tells you which gate is dominant
   without grepping logs.
2. If `cycle_wallet_too_low` → fund issue. Check Polymarket UI vs `.env` funder.
3. If `pretrade_abort` with `bid=None` → orderbook parser broken.
4. If `pretrade_abort` with real prices → market selection is poor; tighten
   `NEAR_DECIDED_BAND`.
5. If auth errors → re-derive L2 keys from a residential IP.
6. If Anthropic 4xx → check credit balance.
7. If everything looks fine + no trades → wait. The bot is conservative by
   design and may genuinely have no trade-worthy signals in the current
   regime.

## Useful one-liners

```bash
# Live config snapshot
curl -sk -H "x-api-key: $APIKEY" https://159.65.201.165/status \
  | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['skip_counts']['config'], indent=2))"

# Per-cycle skip reasons (most recent)
curl -sk -H "x-api-key: $APIKEY" https://159.65.201.165/status \
  | python3 -c "import sys,json; print(json.dumps(json.load(sys.stdin)['skip_counts']['last_cycle'], indent=2))"

# Cumulative since restart
curl -sk -H "x-api-key: $APIKEY" https://159.65.201.165/status \
  | python3 -c "import sys,json; sc=json.load(sys.stdin)['skip_counts']['cumulative']; [print(f'{v:>5} {k}') for k,v in sorted(sc.items(), key=lambda x: -x[1])]"

# Recent activity
journalctl -u sexybot --since '5 min ago' --no-pager | grep -E 'SIGNAL|EXECUTED|ABORT|EDGE SKIP|KELLY SKIP' | tail -10

# Force a deploy (bypassing the 1-min cron)
bash /root/polybot/deploy-pull.sh && tail -2 /var/log/sexybot-deploy.log
```
