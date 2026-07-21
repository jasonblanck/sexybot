import sqlite3

def check_btc():
    conn = sqlite3.connect('/root/polybot/trades.db')
    cur = conn.cursor()
    cur.execute('''
        SELECT side, price, amount, won, realized_pnl, time, market 
        FROM trades 
        WHERE market LIKE "%Bitcoin dip%"
        ORDER BY time DESC
    ''')
    for r in cur.fetchall():
        print(r)

if __name__ == '__main__':
    check_btc()
