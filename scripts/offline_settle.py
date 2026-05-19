#!/usr/bin/env python3
import json
import sqlite3
import time
import urllib.request as urllib_req
import urllib.error
import sys
from datetime import datetime

DB_PATH = 'trades.db'

def query_gamma_api(token_id: str):
    """Query the Polymarket Gamma API for definitive market resolution of a clob_token_id."""
    url = f"https://gamma-api.polymarket.com/markets?clob_token_ids={token_id}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    req = urllib_req.Request(url, headers=headers)
    data = None
    
    try:
        with urllib_req.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 403:
            print(f"  [Warning] urllib got 403 for token {token_id[:10]}... trying requests...")
            try:
                import requests
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code == 200:
                    data = r.json()
            except Exception as e2:
                print(f"  [Error] requests fallback failed: {e2}")
        else:
            print(f"  [Error] urllib query failed with code {e.code}: {e}")
    except Exception as e:
        print(f"  [Error] urllib query failed: {e}")
        
    if not data or not isinstance(data, list):
        return None
        
    m = data[0]
    
    # Check if market is closed or resolved
    if not (m.get("closed") or m.get("resolved")):
        return None
        
    prices = m.get("outcomePrices") or m.get("outcome_prices")
    if isinstance(prices, str):
        prices = json.loads(prices)
        
    ids = m.get("clobTokenIds") or m.get("clob_token_ids")
    if isinstance(ids, str):
        ids = json.loads(ids)
        
    if not prices or not ids or len(prices) < 2 or len(ids) < 2:
        return None
        
    # Identify which slot token is in
    try:
        slot = 0 if str(ids[0]) == str(token_id) else (1 if str(ids[1]) == str(token_id) else None)
        if slot is None:
            return None
        our_p = float(prices[slot])
    except (TypeError, ValueError, IndexError):
        return None
        
    if our_p >= 0.95:
        return 1
    if our_p <= 0.05:
        return 0
        
    return None

def main():
    print("Connecting to SQLite database...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 1. Fetch all unresolved, dry_run=0 BUY/YES/NO trades
    print("Querying unresolved trades...")
    trades = cursor.execute("""
        SELECT id, token_id, price, shares, side, market, time FROM trades
        WHERE resolved=0 AND dry_run=0 AND token_id IS NOT NULL AND token_id != '' AND shares > 0
          AND status IN ('matched','filled','simulated','delayed','live')
          AND UPPER(side) IN ('BUY', 'YES', 'NO')
        ORDER BY id
    """).fetchall()
    
    total_trades = len(trades)
    print(f"Found {total_trades} unresolved, non-dry-run trades.")
    if total_trades == 0:
        print("No unresolved trades found. Exiting.")
        conn.close()
        return
        
    # Group trades by token_id to reduce API queries
    token_map = {}
    for t in trades:
        tok = t['token_id']
        if tok not in token_map:
            token_map[tok] = []
        token_map[tok].append(t)
        
    print(f"Resolving outcomes for {len(token_map)} unique token IDs...")
    
    settled_count = 0
    resolved_cache = {}
    
    for idx, (token_id, matched_trades) in enumerate(token_map.items(), 1):
        market_name = matched_trades[0]['market']
        print(f"[{idx}/{len(token_map)}] Checking token {token_id[:12]}... on '{market_name[:40]}'")
        
        won = query_gamma_api(token_id)
        if won is None:
            print(f"  -> Market is still active or outcome is not clear yet.")
            time.sleep(0.5)  # Pace queries
            continue
            
        print(f"  -> Outcome: {'WON (1)' if won else 'LOST (0)'}")
        
        # Settle all trades matching this token_id
        for t in matched_trades:
            trade_id = t['id']
            entry_price = float(t['price'] or 0.0)
            shares = float(t['shares'] or 0.0)
            
            cost = entry_price * shares
            payout = shares if won else 0.0
            pnl = round(payout - cost, 4)
            
            resolved_at = datetime.utcnow().isoformat()
            
            # Apply db update
            cursor.execute("""
                UPDATE trades 
                SET resolved=1, won=?, realized_pnl=?, resolved_at=? 
                WHERE id=?
            """, (won, pnl, resolved_at, trade_id))
            
            # Delete corresponding positions row
            cursor.execute("DELETE FROM positions WHERE token_id=?", (token_id,))
            
            print(f"    Settle Trade ID {trade_id}: Side={t['side']}, Qty={shares:.2f}, Cost=${cost:.2f}, PnL=${pnl:+.2f}")
            settled_count += 1
            
        conn.commit()
        time.sleep(0.5)  # Pace queries
        
    print("\n" + "="*50)
    print(f"Done! Retroactively settled {settled_count} trades across the database.")
    
    # Print current total P&L in DB
    pnl_summary = cursor.execute("""
        SELECT COUNT(*), SUM(realized_pnl), 
               SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END)
        FROM trades
        WHERE resolved=1 AND dry_run=0
    """).fetchone()
    
    if pnl_summary and pnl_summary[0] > 0:
        cnt, total_pnl, wins = pnl_summary
        total_pnl = total_pnl or 0.0
        win_rate = (wins / cnt * 100) if cnt > 0 else 0.0
        print(f"Updated database summary (resolved=1, dry_run=0):")
        print(f"  Total Trades: {cnt}")
        print(f"  Win Rate:     {win_rate:.1f}% ({wins}/{cnt})")
        print(f"  Realized PnL: ${total_pnl:+.2f}")
    else:
        print("No resolved trades found in database summary.")
        
    conn.close()

if __name__ == '__main__':
    main()
