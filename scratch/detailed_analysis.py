import sqlite3

def run_analysis():
    conn = sqlite3.connect('/root/polybot/trades.db')
    cur = conn.cursor()

    # Get portfolio history for May 20-22
    print("=== Portfolio Value History (May 20 - 22) ===")
    cur.execute('''
        SELECT ts, cash, positions_value, total_value 
        FROM portfolio_value_history 
        WHERE ts >= '2026-05-20' AND ts <= '2026-05-23'
        ORDER BY ts ASC
    ''')
    for r in cur.fetchall():
        print(r)

    # Let's count resolved vs unresolved trades placed on May 21
    print("\n=== Trades Placed on May 21 ===")
    cur.execute('''
        SELECT resolved, won, status, COUNT(*), SUM(realized_pnl) 
        FROM trades 
        WHERE time LIKE '2026-05-21%'
        GROUP BY resolved, won, status
    ''')
    for r in cur.fetchall():
        print(r)

    # Let's print the actual trades executed on May 21 that are unresolved
    print("\n=== Unresolved Trades Placed on May 21 (Sample) ===")
    cur.execute('''
        SELECT market, amount, price, shares, time 
        FROM trades 
        WHERE time LIKE '2026-05-21%' AND resolved = 0
        LIMIT 10
    ''')
    for r in cur.fetchall():
        print(r)

    # Check total unresolved trades for all time
    print("\n=== All Unresolved Trades Count ===")
    cur.execute('''
        SELECT COUNT(*), SUM(amount) 
        FROM trades 
        WHERE resolved = 0
    ''')
    print(cur.fetchone())

if __name__ == '__main__':
    run_analysis()
