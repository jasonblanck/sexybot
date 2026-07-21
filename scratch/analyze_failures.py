import sqlite3
from collections import Counter

def analyze():
    conn = sqlite3.connect('/root/polybot/trades.db')
    cur = conn.cursor()

    # Let's check the count of trades by status
    print("=== Trade Count by Status ===")
    cur.execute("SELECT status, COUNT(*) FROM trades GROUP BY status")
    for r in cur.fetchall():
        print(r)

    # Let's pull the last 100 trades that were not successful (status = 'error' or won = 0)
    print("\n=== Analyzing Last 100 Errors (status='error') ===")
    cur.execute('''
        SELECT market, category, side, price, status_detail, time 
        FROM trades 
        WHERE status = 'error'
        ORDER BY time DESC 
        LIMIT 100
    ''')
    errors = cur.fetchall()
    print(f"Found {len(errors)} error trades in total history.")
    
    if errors:
        categories = Counter([e[1] for e in errors])
        sides = Counter([e[2] for e in errors])
        details = Counter([e[4] for e in errors])
        
        print("\nCommon Categories:")
        for cat, cnt in categories.most_common(5):
            print(f"  {cat}: {cnt}")
            
        print("\nCommon Sides:")
        for side, cnt in sides.most_common(5):
            print(f"  {side}: {cnt}")
            
        print("\nCommon Error Details:")
        for det, cnt in details.most_common(10):
            print(f"  {det}: {cnt}")
            
        print("\nSample Errors:")
        for e in errors[:10]:
            print(f"  Time: {e[5]} | Cat: {e[1]} | Error: {e[4]} | Market: {e[0][:60]}")

    # Let's also check the last 100 losing trades (won = 0)
    print("\n=== Analyzing Last 100 Losses (won=0) ===")
    cur.execute('''
        SELECT market, category, side, price, realized_pnl, time 
        FROM trades 
        WHERE won = 0 AND realized_pnl < 0
        ORDER BY time DESC 
        LIMIT 100
    ''')
    losses = cur.fetchall()
    if losses:
        categories_l = Counter([e[1] for e in losses])
        sides_l = Counter([e[2] for e in losses])
        avg_price = sum([e[3] for e in losses]) / len(losses)
        avg_loss = sum([e[4] for e in losses]) / len(losses)
        
        print(f"Average Entry Price of Losses: {avg_price:.4f}")
        print(f"Average Loss PnL: ${avg_loss:.4f}")
        
        print("\nCommon Categories of Losses:")
        for cat, cnt in categories_l.most_common(5):
            print(f"  {cat}: {cnt}")
            
        print("\nCommon Sides of Losses:")
        for side, cnt in sides_l.most_common(5):
            print(f"  {side}: {cnt}")

        print("\nSample Losses:")
        for e in losses[:10]:
            print(f"  Time: {e[5]} | Price: {e[3]} | PnL: ${e[4]:.2f} | Market: {e[0][:60]}")

if __name__ == '__main__':
    analyze()
