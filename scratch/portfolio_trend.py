import sqlite3

def run_trend():
    conn = sqlite3.connect('/root/polybot/trades.db')
    cur = conn.cursor()

    # Get portfolio value over time (group by day)
    print("=== Portfolio Value Trend ===")
    cur.execute('''
        SELECT 
            substr(ts, 1, 10) as day,
            AVG(total_value) as avg_val,
            MIN(total_value) as min_val,
            MAX(total_value) as max_val
        FROM portfolio_value_history
        GROUP BY day
        ORDER BY day ASC
    ''')
    for r in cur.fetchall():
        day, avg_val, min_val, max_val = r
        print(f"{day}: Avg = ${avg_val:.2f}, Min = ${min_val:.2f}, Max = ${max_val:.2f}")

if __name__ == '__main__':
    run_trend()
