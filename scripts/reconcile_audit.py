#!/usr/bin/env python3
import csv
import datetime
import os
import sqlite3
import itertools
import sys
from collections import defaultdict

# Add parent directory to path for discovery import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

DB_PATH = 'trades.db'
CSV_PATH = 'Polymarket-History-2026-05-19.csv'

def clean_name(name):
    # Take first 25 characters, lowercase, keep only alphanumeric
    cleaned = ''.join(c for c in name.lower() if c.isalnum())
    return cleaned[:25]

def parse_db_time(time_str):
    # Naive ISO format e.g. 2026-05-16T02:41:52.095105
    if '+' in time_str:
        time_str = time_str.split('+')[0]
    try:
        dt = datetime.datetime.fromisoformat(time_str)
        return dt.replace(tzinfo=datetime.timezone.utc)
    except ValueError:
        if '.' in time_str:
            time_str = time_str.split('.')[0]
        dt = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=datetime.timezone.utc)

def main():
    if not os.path.exists(DB_PATH):
        print(f"DB not found at {DB_PATH}")
        return
    if not os.path.exists(CSV_PATH):
        print(f"CSV not found at {CSV_PATH}")
        return

    # 1. Load CSV transactions
    f = open(CSV_PATH, 'r', encoding='utf-8-sig')
    r = csv.reader(f)
    headers = next(r)
    csv_rows = []
    for row in r:
        market, action, usdc, tokens, token_name, timestamp, tx_hash = row
        if action not in ('Buy', 'Sell', 'Redeem'):
            continue
        csv_rows.append({
            'market': market,
            'action': action,
            'usdc': float(usdc),
            'shares': float(tokens),
            'token_name': token_name,
            'time': datetime.datetime.fromtimestamp(int(timestamp), datetime.timezone.utc),
            'hash': tx_hash,
            'matched': False
        })
    f.close()
    print(f"Loaded {len(csv_rows)} Buy/Sell/Redeem transactions from CSV.")

    # 2. Load DB trades
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    db_rows = conn.execute("""
        SELECT id, market, side, amount, price, shares, status, dry_run, time, 
               ai_confidence, ai_probability, category, realized_pnl, resolved, won
        FROM trades
        WHERE dry_run = 0
          AND status IN ('matched', 'filled', 'delayed', 'live')
        ORDER BY time
    """).fetchall()
    conn.close()
    print(f"Loaded {len(db_rows)} trades from DB (dry_run=0, successful status).")

    # 3. Sort DB trades and match
    db_trades = []
    for row in db_rows:
        db_trades.append({
            'id': row['id'],
            'market': row['market'],
            'side': row['side'].upper(),
            'amount': float(row['amount'] or 0),
            'price': float(row['price'] or 0),
            'shares': float(row['shares'] or 0),
            'status': row['status'],
            'time': parse_db_time(row['time']),
            'ai_confidence': row['ai_confidence'],
            'category': row['category'],
            'realized_pnl': row['realized_pnl'],
            'resolved': row['resolved'],
            'won': row['won'],
        })

    # Perform matching to link DB trades (confidence, category) to CSV markets
    matches = []
    for db_t in db_trades:
        db_clean = clean_name(db_t['market'])
        db_time = db_t['time']
        db_shares = db_t['shares']
        db_action = 'Buy' if db_t['side'] in ('BUY', 'YES', 'NO') else 'Sell'

        best_match = None
        min_time_diff = datetime.timedelta(minutes=20)

        for csv_t in csv_rows:
            if csv_t['matched']:
                continue
            if csv_t['action'] == 'Redeem':
                continue
            if clean_name(csv_t['market']) != db_clean:
                continue
            if csv_t['action'] != db_action:
                continue

            time_diff = abs(csv_t['time'] - db_time)
            if time_diff > min_time_diff:
                continue

            share_diff = abs(csv_t['shares'] - db_shares)
            if db_shares > 0 and (share_diff / db_shares) > 0.10:
                continue

            if time_diff < min_time_diff:
                best_match = csv_t
                min_time_diff = time_diff

        if best_match:
            best_match['matched'] = True
            matches.append({
                'db': db_t,
                'csv': best_match
            })

    print(f"Successfully matched {len(matches)} / {len(db_trades)} DB trades.")

    # 4. Group actual cash flows by CSV market
    csv_markets = defaultdict(lambda: {
        'buys': 0.0, 'sells': 0.0, 'redeems': 0.0, 
        'shares_bought': 0.0, 'shares_sold': 0.0, 'shares_redeemed': 0.0,
        'first_buy_time': None, 'first_buy_price': None,
        'has_redeem': False,
        'tx_count': 0
    })

    # Sort CSV rows by time to find the first buy
    csv_rows_sorted = sorted(csv_rows, key=lambda x: x['time'])
    for csv_t in csv_rows_sorted:
        m = csv_markets[csv_t['market']]
        val = csv_t['usdc']
        sh = csv_t['shares']
        m['tx_count'] += 1
        if csv_t['action'] == 'Buy':
            m['buys'] += val
            m['shares_bought'] += sh
            if m['first_buy_time'] is None:
                m['first_buy_time'] = csv_t['time']
                m['first_buy_price'] = val / sh if sh > 0 else 0.5
        elif csv_t['action'] == 'Sell':
            m['sells'] += val
            m['shares_sold'] += sh
        elif csv_t['action'] == 'Redeem':
            m['redeems'] += val
            m['shares_redeemed'] += sh
            m['has_redeem'] = True

    # Reconcile markets and find their first matched DB properties (confidence)
    market_signals = {}
    for name, m_data in csv_markets.items():
        m_clean = clean_name(name)
        # Find the earliest matched trade in this market to attribute signals
        m_matches = [match for match in matches if clean_name(match['csv']['market']) == m_clean]
        if m_matches:
            # Sort by time and get the first one
            first_match = min(m_matches, key=lambda x: x['db']['time'])
            market_signals[name] = {
                'ai_confidence': first_match['db']['ai_confidence'],
                'category': first_match['db']['category'],
            }
        else:
            market_signals[name] = {
                'ai_confidence': None,
                'category': None,
            }

    # Identify resolved markets and their net on-chain P&L
    resolved_markets = []
    total_onchain_pnl = 0.0
    for name, m in csv_markets.items():
        net_shares = m['shares_bought'] - m['shares_sold'] - m['shares_redeemed']
        is_resolved = m['has_redeem'] or abs(net_shares) < 0.1 or m['shares_redeemed'] > 0
        pnl = (m['sells'] + m['redeems']) - m['buys']
        if is_resolved:
            resolved_markets.append({
                'name': name,
                'pnl': pnl,
                'confidence': market_signals[name]['ai_confidence'],
                'category': market_signals[name]['category'],
                'first_buy_price': m['first_buy_price']
            })
            total_onchain_pnl += pnl

    print(f"Total resolved markets on-chain in CSV: {len(resolved_markets)}")
    print(f"Total realized on-chain P&L: ${total_onchain_pnl:+.2f}")

    # 5. Grid Search
    CONFIDENCES = [20, 30, 40, 50, 60, 70, 80]
    MAX_PRICES = [0.75, 0.80, 0.85, 0.90, 0.95, 1.00]
    
    print("\n--- OPTIMIZATION GRID SEARCH ON RESOLVED MARKETS ---")
    print(f"  {'Min Conf':<10} | {'Max Price':<10} | {'Markets':<8} | {'PnL ($)':<10} | {'Win Rate':<8}")
    print(f"  {'-'*55}")

    grid_results = []
    for conf, max_price in itertools.product(CONFIDENCES, MAX_PRICES):
        markets_traded = 0
        wins = 0
        pnl_total = 0.0
        
        for rm in resolved_markets:
            # Check if this market had valid DB signals
            m_conf = rm['confidence']
            m_price = rm['first_buy_price']
            
            # If we don't have DB matching data, assume we skip or include? 
            # For backtesting, we can only evaluate markets that had matching signals.
            if m_conf is None or m_price is None:
                continue
                
            if m_conf < conf:
                continue
            if m_price > max_price:
                continue
                
            markets_traded += 1
            pnl_total += rm['pnl']
            if rm['pnl'] > 0:
                wins += 1
                
        win_rate = (wins / markets_traded * 100) if markets_traded > 0 else 0.0
        grid_results.append({
            'conf': conf,
            'max_price': max_price,
            'markets': markets_traded,
            'pnl': pnl_total,
            'win_rate': win_rate
        })

    grid_results.sort(key=lambda x: x['pnl'], reverse=True)
    for res in grid_results[:12]:
        print(f"  {res['conf']:8d}% | {res['max_price']:9.2f} | {res['markets']:8d} | ${res['pnl']:+9.2f} | {res['win_rate']:7.1f}%")

    print("\n=== RECOMMENDED PARAMETERS ===")
    if grid_results:
        best = grid_results[0]
        print(f"MIN_CONFIDENCE={best['conf']}")
        print(f"MAX_ENTRY_PRICE={best['max_price']}")
        print(f"Under these parameters, actual on-chain loss of ${abs(total_onchain_pnl):.2f} would be corrected to {best['pnl']:+.2f}!")

if __name__ == '__main__':
    main()
