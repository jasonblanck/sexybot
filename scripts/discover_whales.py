#!/usr/bin/env python3
"""discover_whales.py — Polymarket whale-wallet discovery.

Pulls the most recent N trades from Polymarket's public data API,
aggregates by `proxyWallet`, and prints the top wallets ranked by
total notional volume (size × price). Operator review the output and
manually copy the addresses they want to track into:

    POLYMARKET_WHALE_WALLETS=0xaaa...,0xbbb...,...

in /root/polybot/.env, then `systemctl restart sexybot`.

Notes:
- "Top by volume" ≠ "consistently profitable". Volume is a first-pass
  filter — it surfaces ACTIVE big traders, but you still want to verify
  on Polymarket's leaderboard / their public profile that they have
  positive realized PnL. The bot's whale tracker only learns from their
  CURRENT positions, so a high-volume losing trader is anti-signal.
- Uses Polymarket's public /trades endpoint. No auth required; rate-
  limit is generous for read-only pagination.

Usage:
    /root/polybot/venv/bin/python3 /root/polybot/scripts/discover_whales.py
    DISCOVER_PAGES=10 python3 discover_whales.py   # bigger sample
"""

import json
import os
import sys
import urllib.request
from collections import defaultdict
from typing import Optional

PAGES         = int(os.getenv("DISCOVER_PAGES", "5"))
PER_PAGE      = int(os.getenv("DISCOVER_LIMIT", "500"))
TOP_N         = int(os.getenv("DISCOVER_TOP", "20"))
MIN_NOTIONAL  = float(os.getenv("DISCOVER_MIN_NOTIONAL", "1000"))
MIN_DISTINCT  = int(os.getenv("DISCOVER_MIN_DISTINCT_TOKENS", "3"))
MIN_TRADES    = int(os.getenv("DISCOVER_MIN_TRADES", "3"))
STAGING_FILE  = os.getenv("DISCOVER_STAGING_FILE", "/root/polybot/.env.whale-suggestions")
WRITE_STAGING = os.getenv("DISCOVER_WRITE_STAGING", "true").lower() == "true"


def fetch_trades(offset: int) -> list:
    url = f"https://data-api.polymarket.com/trades?limit={PER_PAGE}&offset={offset}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 sexybot whale-discovery",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def main() -> None:
    print(f"Pulling {PAGES} pages × {PER_PAGE} trades = up to {PAGES * PER_PAGE} recent Polymarket trades…")
    by_wallet: dict[str, dict] = defaultdict(lambda: {
        "notional": 0.0, "trades": 0, "buys": 0, "sells": 0, "tokens": set(),
    })
    total_seen = 0
    for page in range(PAGES):
        try:
            trades = fetch_trades(page * PER_PAGE)
        except Exception as e:
            print(f"  page {page}: ERR {e}", file=sys.stderr)
            break
        if not trades:
            break
        for t in trades:
            wallet = (t.get("proxyWallet") or "").lower()
            if not wallet:
                continue
            try:
                size  = float(t.get("size") or 0)
                price = float(t.get("price") or 0)
            except (TypeError, ValueError):
                continue
            notional = size * price
            entry = by_wallet[wallet]
            entry["notional"] += notional
            entry["trades"]   += 1
            if (t.get("side") or "").upper() == "BUY":
                entry["buys"] += 1
            else:
                entry["sells"] += 1
            asset = t.get("asset") or t.get("conditionId") or ""
            if asset:
                entry["tokens"].add(asset)
            total_seen += 1
        print(f"  page {page+1}: +{len(trades)} trades, distinct wallets so far {len(by_wallet)}")

    if total_seen == 0:
        sys.exit("No trades fetched — endpoint may have changed or rate-limited.")
    print(f"\nProcessed {total_seen} trades from {len(by_wallet)} wallets.\n")

    ranked = [
        (w, d) for w, d in by_wallet.items()
        if d["notional"] >= MIN_NOTIONAL
    ]
    ranked.sort(key=lambda kv: kv[1]["notional"], reverse=True)

    if not ranked:
        print(f"No wallets above ${MIN_NOTIONAL:.0f} notional in this sample.")
        return

    print(f"=== TOP {min(TOP_N, len(ranked))} WALLETS BY NOTIONAL VOLUME ===\n")
    print(f"{'#':>3}  {'wallet':42s}  {'notional':>10s}  {'trades':>7s}  {'buy/sell':>9s}  {'distinct':>9s}")
    for i, (wallet, d) in enumerate(ranked[:TOP_N], 1):
        bs = f"{d['buys']}/{d['sells']}"
        print(f"{i:>3}  {wallet:42s}  ${d['notional']:>9,.0f}  {d['trades']:>7d}  {bs:>9s}  {len(d['tokens']):>9d}")

    # Filter to "diversified active" wallets — multi-token, multi-trade.
    # A single big bet doesn't tell us much; we want wallets that bet
    # across many markets with size, suggesting they're traders not
    # one-shot lottery players.
    diversified = [
        (w, d) for w, d in ranked
        if d["trades"] >= MIN_TRADES and len(d["tokens"]) >= MIN_DISTINCT
    ]

    print(f"\n=== DIVERSIFIED CANDIDATES (≥{MIN_TRADES} trades, ≥{MIN_DISTINCT} distinct tokens) ===\n")
    if diversified:
        for i, (wallet, d) in enumerate(diversified[:TOP_N], 1):
            print(f"  {i}. {wallet}  ${d['notional']:>9,.0f}  {d['trades']} trades, {len(d['tokens'])} markets")
    else:
        print("  (none in this sample — try DISCOVER_PAGES=10 for a bigger sample)")

    # Write staging file for operator review
    if WRITE_STAGING and diversified:
        try:
            wallets_csv = ",".join(w for w, _ in diversified[:TOP_N])
            with open(STAGING_FILE, "w") as f:
                f.write(f"# Suggested whale wallets — generated by discover_whales.py at {os.popen('date -u').read().strip()}\n")
                f.write(f"# Top {len(diversified[:TOP_N])} active diversified traders by recent volume.\n")
                f.write(f"# Verify each wallet's public profile on polymarket.com BEFORE copying into .env.\n")
                f.write(f"# Volume alone ≠ profitable — check realized PnL on their leaderboard page.\n#\n")
                f.write(f"# To apply: review the list, then in /root/polybot/.env replace the existing\n")
                f.write(f"# POLYMARKET_WHALE_WALLETS line with this one (or a vetted subset).\n#\n")
                f.write(f"POLYMARKET_WHALE_WALLETS={wallets_csv}\n")
            os.chmod(STAGING_FILE, 0o600)
            print(f"\n→ Staging file written: {STAGING_FILE}")
            print(f"  Review it, then `cat {STAGING_FILE}` and copy into /root/polybot/.env")
        except Exception as e:
            print(f"\n⚠️  Failed to write staging file {STAGING_FILE}: {e}", file=sys.stderr)

    print()
    print("Workflow:")
    print(f"  1. Review {STAGING_FILE} — verify each wallet's public Polymarket profile.")
    print("  2. Volume ≠ profitable. Filter to ones with positive realized PnL on the leaderboard.")
    print("  3. Replace the POLYMARKET_WHALE_WALLETS line in /root/polybot/.env.")
    print("  4. systemctl restart sexybot")
    print("  5. Their on-chain positions in candidate markets become AI signal.")


if __name__ == "__main__":
    main()
