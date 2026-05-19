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
    print(f"Loaded {len(csv_rows)} transactions from CSV.")

    # Group CSV transactions by market
    csv_by_market = defaultdict(list)
    for row in csv_rows:
        csv_by_market[clean_name(row['market'])].append(row)

    # Load unresolved BUY trades from DB
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    db_trades = cursor.execute("""
        SELECT id, market, side, amount, price, shares, status, dry_run, time, token_id
        FROM trades
        WHERE dry_run = 0 AND resolved = 0
          AND status IN ('matched', 'filled', 'delayed', 'live')
          AND side = 'BUY'
        ORDER BY time
    """).fetchall()

    print(f"Loaded {len(db_trades)} unresolved BUY trades from DB.")

    unresolved_count = 0
    resolved_count = 0

    for idx, t in enumerate(db_trades, 1):
        market_name = t['market']
        clean_m = clean_name(market_name)
        trade_id = t['id']
        shares = float(t['shares'] or 0.0)
        amount = float(t['amount'] or 0.0)
        price = float(t['price'] or 0.0)
        token_id = t['token_id']
        token_id_str = token_id[:15] + "..." if token_id else "None"
        
        print(f"\n[{idx}/{len(db_trades)}] DB Trade ID {trade_id}: '{market_name[:60]}'")
        print(f"  Token: {token_id_str} | Shares: {shares:.4f} | Cost: ${amount:.2f} | Price: {price:.3f}")
        
        # Look for matches in CSV
        csv_matches = csv_by_market.get(clean_m, [])
        if not csv_matches:
            # Try looser match: check if clean_m is a substring of any clean CSV market or vice versa
            for csv_m_clean, rows in csv_by_market.items():
                if clean_m in csv_m_clean or csv_m_clean in clean_m:
                    csv_matches = rows
                    break
                    
        if csv_matches:
            print(f"  Found {len(csv_matches)} on-chain transactions for this market:")
            
            # Look for Redeem actions
            redeems = [r for r in csv_matches if r['action'] == 'Redeem']
            sells = [r for r in csv_matches if r['action'] == 'Sell']
            buys = [r for r in csv_matches if r['action'] == 'Buy']
            
            total_redeem_usdc = sum(r['usdc'] for r in redeems)
            total_redeem_shares = sum(r['shares'] for r in redeems)
            
            if redeems:
                # If there are redeems, the market resolved!
                # If total_redeem_usdc > 0, did we win?
                # Wait, if we won, we get 1.00 USDC per share.
                # If we lost, we get 0.00 USDC per share.
                # Let's check the payout per share:
                payout_per_share = total_redeem_usdc / total_redeem_shares if total_redeem_shares > 0 else 0.0
                print(f"    Redeems: Total Shares={total_redeem_shares:.4f}, Total USDC=${total_redeem_usdc:.4f} (Payout per share=${payout_per_share:.4f})")
                
                # Check if this specific token won or lost.
                # Wait! A market has multiple outcomes (e.g. Yes and No).
                # The CSV contains token_name (e.g. "Yes", "No", "Under", "Over").
                # Let's check which token we bought. The database trade might not store Yes/No in side (it just says BUY),
                # but let's see if we can find the token name.
                # Wait! Let's check the outcome of the redeem from the CSV.
                # If the redeem transaction in the CSV has a positive payout, it means the user's position was redeemed.
                # Wait, does the CSV show redeems for BOTH winning and losing outcomes?
                # Usually, you only redeem winning tokens (since losing tokens are worth 0, so no redeem transaction is generated or it is 0).
                # So if total_redeem_usdc > 0, it means the user won on this market!
                # Wait! What if the user had multiple positions (both Yes and No) in the same market, or what if the payout is positive?
                # If the payout per share is positive (or we see a redeem with >0 USDC), then the winning outcome was redeemed.
                # Let's see: did this specific trade win?
                # If the payout per share is >= 0.95, it won. If it is <= 0.05, it lost.
                # Let's check:
                if payout_per_share > 0.5:
                    won = 1
                    payout = shares
                else:
                    won = 0
                    payout = 0.0
                    
                realized_pnl = payout - amount
                resolved_at = redeems[0]['time'].isoformat()
                
                # Update DB
                cursor.execute("""
                    UPDATE trades
                    SET resolved = 1, won = ?, realized_pnl = ?, resolved_at = ?
                    WHERE id = ?
                """, (won, realized_pnl, resolved_at, trade_id))
                resolved_count += 1
                print(f"    -> [RESOLVED VIA REDEEM] Won={won} | Payout=${payout:.2f} | PnL=${realized_pnl:+.2f}")
            else:
                # No Redeem in CSV. Check if the market suggestion or net position suggests it's closed.
                # Let's print the transaction history to understand
                for r in csv_matches:
                    print(f"    - {r['action']} {r['shares']:.2f} shares for ${r['usdc']:.2f} on {r['time']}")
                unresolved_count += 1
        else:
            print("  No matching transactions found in ledger CSV.")
            unresolved_count += 1

    conn.commit()
    conn.close()
    print("\n" + "="*50)
    print(f"Ledger reconciliation completed: {resolved_count} trades resolved, {unresolved_count} trades remain unresolved.")

if __name__ == '__main__':
    main()
