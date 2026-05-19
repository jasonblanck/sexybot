import csv
from collections import defaultdict

def main():
    f = open('Polymarket-History-2026-05-19.csv', 'r', encoding='utf-8-sig')
    r = csv.reader(f)
    headers = next(r)
    rows = list(r)
    f.close()

    markets = defaultdict(lambda: {
        'buys_usdc': 0.0, 'buys_shares': 0.0,
        'sells_usdc': 0.0, 'sells_shares': 0.0,
        'redeems_usdc': 0.0, 'redeems_shares': 0.0,
        'has_redeem': False,
        'tx_count': 0
    })

    for row in rows:
        market, action, usdc, tokens, _, timestamp, _ = row
        if action not in ('Buy', 'Sell', 'Redeem'):
            continue
        m = markets[market]
        usdc_val = float(usdc)
        tokens_val = float(tokens)
        m['tx_count'] += 1
        if action == 'Buy':
            m['buys_usdc'] += usdc_val
            m['buys_shares'] += tokens_val
        elif action == 'Sell':
            m['sells_usdc'] += usdc_val
            m['sells_shares'] += tokens_val
        elif action == 'Redeem':
            m['redeems_usdc'] += usdc_val
            m['redeems_shares'] += tokens_val
            m['has_redeem'] = True

    print(f"Total unique markets in CSV: {len(markets)}")
    
    # Sort markets by cash flow
    resolved_count = 0
    open_count = 0
    for name, m in list(markets.items())[:15]:
        net_shares = m['buys_shares'] - m['sells_shares'] - m['redeems_shares']
        pnl = (m['sells_usdc'] + m['redeems_usdc']) - m['buys_usdc']
        is_resolved = m['has_redeem'] or abs(net_shares) < 0.1
        status = "RESOLVED" if is_resolved else "OPEN"
        print(f"Market: {name[:50]}")
        print(f"  Actions: {m['tx_count']} | Net Shares: {net_shares:.4f} | PnL: ${pnl:+.2f} | Status: {status}")

if __name__ == '__main__':
    main()
