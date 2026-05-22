#!/usr/bin/env python3
import sqlite3
import datetime
from collections import defaultdict

DB_PATH = 'trades.db'

def main():
    print("Connecting to SQLite database...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Query all successful dry_run = 0 trades
    print("Querying all valid trades...")
    trades = cursor.execute("""
        SELECT id, token_id, side, price, shares, amount, resolved, realized_pnl, time, market
        FROM trades
        WHERE dry_run = 0 AND token_id IS NOT NULL AND token_id != ''
          AND status IN ('matched', 'filled')
        ORDER BY time, id
    """).fetchall()

    print(f"Loaded {len(trades)} trades from DB.")

    # Group trades by token_id
    token_trades = defaultdict(list)
    for t in trades:
        token_trades[t['token_id']].append(t)

    # Filter for tokens that have both BUY and SELL trades
    tokens_to_reconcile = {}
    for token_id, t_list in token_trades.items():
        has_buy = any(t['side'].upper() == 'BUY' for t in t_list)
        has_sell = any(t['side'].upper() == 'SELL' for t in t_list)
        if has_buy and has_sell:
            tokens_to_reconcile[token_id] = t_list

    print(f"Found {len(tokens_to_reconcile)} unique tokens with round-trip trades.")

    total_buys_updated = 0
    total_sells_updated = 0
    total_realized_pnl_change = 0.0

    # Process each token
    for idx, (token_id, t_list) in enumerate(tokens_to_reconcile.items(), 1):
        market_name = t_list[0]['market']
        # Sort chronologically by time, then ID
        sorted_trades = sorted(t_list, key=lambda x: (x['time'], x['id']))
        
        # FIFO queues
        buy_queue = []  # list of dicts for open buy trades
        sell_trades_to_update = []
        buy_trades_to_update = defaultdict(float) # trade_id -> matched_pnl
        buy_trades_latest_sell_time = {} # trade_id -> latest_sell_time
        
        for t in sorted_trades:
            side = t['side'].upper()
            trade_id = t['id']
            shares = float(t['shares'] or 0.0)
            price = float(t['price'] or 0.0)
            amount = float(t['amount'] or 0.0)
            trade_time = t['time']
            
            if side == 'BUY':
                buy_queue.append({
                    'id': trade_id,
                    'shares': shares,
                    'original_shares': shares,
                    'amount': amount,
                    'price': price,
                    'matched_pnl': 0.0,
                    'time': trade_time
                })
            elif side == 'SELL':
                # Match against buy queue
                remaining_sell_shares = shares
                sell_pnl = 0.0
                
                while remaining_sell_shares > 0.0001 and buy_queue:
                    buy_slot = buy_queue[0]
                    available_buy_shares = buy_slot['shares']
                    matched_shares = min(remaining_sell_shares, available_buy_shares)
                    
                    # Proportional cost of matched buy shares
                    cost_portion = (matched_shares / buy_slot['original_shares']) * buy_slot['amount']
                    # Proportional proceeds of matched sell shares
                    proceeds_portion = (matched_shares / shares) * amount
                    
                    pnl_portion = proceeds_portion - cost_portion
                    buy_slot['matched_pnl'] += pnl_portion
                    buy_slot['shares'] -= matched_shares
                    
                    buy_trades_to_update[buy_slot['id']] = buy_slot['matched_pnl']
                    buy_trades_latest_sell_time[buy_slot['id']] = trade_time
                    
                    remaining_sell_shares -= matched_shares
                    
                    if buy_slot['shares'] < 0.0001:
                        # Buy trade fully closed
                        buy_queue.pop(0)
                
                # Mark sell trade as resolved with realized_pnl = 0.0
                sell_trades_to_update.append((trade_id, trade_time))
        
        # Apply updates for this token ID
        for buy_id, matched_pnl in buy_trades_to_update.items():
            # Get old P&L to show diff
            old_pnl_row = cursor.execute("SELECT realized_pnl, resolved FROM trades WHERE id=?", (buy_id,)).fetchone()
            old_pnl = old_pnl_row['realized_pnl'] if old_pnl_row else None
            old_resolved = old_pnl_row['resolved'] if old_pnl_row else 0
            
            latest_time = buy_trades_latest_sell_time[buy_id]
            
            # Update BUY trade
            cursor.execute("""
                UPDATE trades
                SET resolved = 1, realized_pnl = ?, resolved_at = ?
                WHERE id = ?
            """, (round(matched_pnl, 4), latest_time, buy_id))
            total_buys_updated += 1
            
            # Print significant differences
            if old_pnl is not None:
                pnl_diff = matched_pnl - old_pnl
                if abs(pnl_diff) > 0.01:
                    print(f"  [Correct PnL] Buy ID {buy_id} on '{market_name[:40]}': ${old_pnl:+.2f} -> ${matched_pnl:+.2f} (diff ${pnl_diff:+.2f})")
                    total_realized_pnl_change += pnl_diff
            else:
                print(f"  [Settle New] Buy ID {buy_id} on '{market_name[:40]}': realized PnL = ${matched_pnl:+.2f}")
                total_realized_pnl_change += matched_pnl
                
        for sell_id, sell_time in sell_trades_to_update:
            cursor.execute("""
                UPDATE trades
                SET resolved = 1, realized_pnl = 0.0, resolved_at = ?
                WHERE id = ?
            """, (sell_time, sell_id))
            total_sells_updated += 1
            
        # Delete corresponding positions row since position is closed or updated
        cursor.execute("DELETE FROM positions WHERE token_id=?", (token_id,))

    conn.commit()
    print("\n" + "="*50)
    print("Reconcile Closed-Out Round-Trips Completed!")
    print(f"  BUY Trades Resolved/Updated:  {total_buys_updated}")
    print(f"  SELL Trades Resolved/Updated: {total_sells_updated}")
    print(f"  Net realized P&L change in DB: ${total_realized_pnl_change:+.2f}")

    # Print total resolved trades summary
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
        print(f"Current database summary (resolved=1, dry_run=0):")
        print(f"  Total Trades: {cnt}")
        print(f"  Win Rate:     {win_rate:.1f}% ({wins}/{cnt})")
        print(f"  Realized PnL: ${total_pnl:+.2f}")

    conn.close()

if __name__ == '__main__':
    main()
