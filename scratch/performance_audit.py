import sqlite3

def run_audit():
    conn = sqlite3.connect('/root/polybot/trades.db')
    cur = conn.cursor()

    # Monthly stats YTD
    print('=== Monthly Stats (YTD) ===')
    cur.execute('''
        SELECT 
            substr(time, 1, 7) as month,
            SUM(realized_pnl) as pnl,
            COUNT(*) as count,
            SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END) as losses
        FROM trades 
        WHERE resolved = 1 AND time >= '2026-01-01'
        GROUP BY month 
        ORDER BY month
    ''')
    for r in cur.fetchall():
        month, pnl, count, wins, losses = r
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0
        pnl_val = pnl if pnl is not None else 0.0
        print(f'{month}: PnL = ${pnl_val:.2f}, Count = {count}, Wins = {wins}, Losses = {losses}, WinRate = {win_rate:.1f}%')

    # Strategy breakdown since April 2026
    print('\n=== Strategy Breakdown (Since April 2026) ===')
    cur.execute('''
        SELECT 
            strategy,
            SUM(realized_pnl) as pnl,
            COUNT(*) as count,
            SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END) as losses
        FROM trades 
        WHERE resolved = 1 AND time >= '2026-04-01'
        GROUP BY strategy
    ''')
    for r in cur.fetchall():
        strategy, pnl, count, wins, losses = r
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0
        pnl_val = pnl if pnl is not None else 0.0
        print(f'{strategy}: PnL = ${pnl_val:.2f}, Count = {count}, Wins = {wins}, Losses = {losses}, WinRate = {win_rate:.1f}%')

    # Category breakdown since April 2026
    print('\n=== Category Breakdown (Since April 2026) ===')
    cur.execute('''
        SELECT 
            category,
            SUM(realized_pnl) as pnl,
            COUNT(*) as count,
            SUM(CASE WHEN won = 1 THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN won = 0 THEN 1 ELSE 0 END) as losses
        FROM trades 
        WHERE resolved = 1 AND time >= '2026-04-01'
        GROUP BY category
    ''')
    for r in cur.fetchall():
        category, pnl, count, wins, losses = r
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0
        pnl_val = pnl if pnl is not None else 0.0
        print(f'{category}: PnL = ${pnl_val:.2f}, Count = {count}, Wins = {wins}, Losses = {losses}, WinRate = {win_rate:.1f}%')

    # Top 15 largest losing trades since April 2026
    print('\n=== Top 15 Largest Losing Trades ===')
    cur.execute('''
        SELECT 
            market, strategy, category, realized_pnl, time
        FROM trades 
        WHERE resolved = 1 AND time >= '2026-04-01' AND realized_pnl < 0
        ORDER BY realized_pnl ASC
        LIMIT 15
    ''')
    for r in cur.fetchall():
        market, strategy, category, pnl, time = r
        print(f'${pnl:.2f} | {strategy} | {category} | {time} | {market[:70]}')

if __name__ == '__main__':
    run_audit()
