#!/usr/bin/env python3
import csv
import datetime
import os
import sqlite3
from collections import defaultdict

DB_PATH = 'trades.db'
CSV_PATH = 'Polymarket-History-2026-05-19.csv'

def clean_name(name):
    cleaned = ''.join(c for c in name.lower() if c.isalnum())
    return cleaned[:30]

def main():
    if not os.path.exists(DB_PATH):
        print(f"DB not found at {DB_PATH}")
        return
    if not os.path.exists(CSV_PATH):
        print(f"CSV not found at {CSV_PATH}")
        return

    # Load CSV transactions
    f = open(CSV_PATH, 'r', encoding='utf-8-sig')
    r = csv.reader(f)
    headers = next(r)
    csv_rows = []
    for row in r:
        market, action, usdc, tokens, token_name, timestamp, tx_hash = row
        csv_rows.append({
            'market': market,
            'action': action,
            'usdc': float(usdc),
            'shares': float(tokens or 0.0),
            'token_name': token_name,
            'time': datetime.datetime.fromtimestamp(int(timestamp), datetime.timezone.utc),
            'hash': tx_hash
        })
    f.close()

    # Group CSV transactions by market
    csv_by_market = defaultdict(list)
    for row in csv_rows:
        csv_by_market[clean_name(row['market'])].append(row)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Clean up legacy dummy test rows with empty token_id and empty market name
    print("Cleaning up empty legacy dummy trades in DB...")
    cursor.execute("""
        UPDATE trades
        SET resolved = 1, realized_pnl = 0.0, resolved_at = ?
        WHERE dry_run = 0 AND resolved = 0
          AND (token_id IS NULL OR token_id = '')
          AND (market IS NULL OR market = '')
    """, (datetime.datetime.utcnow().isoformat(),))
    dummy_cleaned = cursor.rowcount
    print(f"  Cleaned {dummy_cleaned} dummy trades.")

    # 2. Fetch all remaining unresolved BUY trades
    db_trades = cursor.execute("""
        SELECT id, market, side, amount, price, shares, status, dry_run, time, token_id
        FROM trades
        WHERE dry_run = 0 AND resolved = 0
          AND status IN ('matched', 'filled', 'delayed', 'live')
          AND side = 'BUY'
        ORDER BY time
    """).fetchall()

    print(f"Loaded {len(db_trades)} unresolved BUY trades from DB.")

    resolved_count = 0
    ignored_count = 0

    for idx, t in enumerate(db_trades, 1):
        market_name = t['market']
        clean_m = clean_name(market_name)
        trade_id = t['id']
        shares = float(t['shares'] or 0.0)
        amount = float(t['amount'] or 0.0)
        price = float(t['price'] or 0.0)
        token_id = t['token_id']
        
        # Match with CSV
        csv_matches = csv_by_market.get(clean_m, [])
        if not csv_matches:
            for csv_m_clean, rows in csv_by_market.items():
                if clean_m in csv_m_clean or csv_m_clean in clean_m:
                    csv_matches = rows
                    break
                    
        if csv_matches:
            # Look for Sell actions to resolve
            sells = [r for r in csv_matches if r['action'] == 'Sell']
            buys = [r for r in csv_matches if r['action'] == 'Buy']
            
            if sells:
                # If there are sells, we can compute the on-chain realized PnL
                total_sell_usdc = sum(r['usdc'] for r in sells)
                total_sell_shares = sum(r['shares'] for r in sells)
                total_buy_usdc = sum(r['usdc'] for r in buys)
                total_buy_shares = sum(r['shares'] for r in buys)
                
                # Check if this position is closed or if we can settle it.
                # If total_sell_shares is close to total_buy_shares, or if we have sells,
                # let's compute realized PnL as:
                # realized_pnl = total_sell_usdc - amount (if it is the only buy)
                # or proportional:
                if total_buy_shares > 0:
                    proportional_sell_usdc = (shares / total_buy_shares) * total_sell_usdc
                    realized_pnl = proportional_sell_usdc - amount
                else:
                    realized_pnl = total_sell_usdc - amount
                    
                resolved_at = sells[0]['time'].isoformat()
                
                # Update DB
                cursor.execute("""
                    UPDATE trades
                    SET resolved = 1, realized_pnl = ?, resolved_at = ?
                    WHERE id = ?
                """, (round(realized_pnl, 4), resolved_at, trade_id))
                resolved_count += 1
                print(f"Resolved Trade ID {trade_id} ('{market_name[:40]}'): PnL = ${realized_pnl:+.2f} via on-chain Sells")
            else:
                ignored_count += 1
        else:
            ignored_count += 1

    conn.commit()
    conn.close()
    print("\n" + "="*50)
    print(f"Reconciliation completed: resolved {resolved_count} trades, ignored {ignored_count} active/live trades.")

if __name__ == '__main__':
    main()
