#!/usr/bin/env python3
import os
import re
import sys
import sqlite3
import urllib.request as urllib_req
import json

def load_env(env_path):
    env = {}
    if not os.path.exists(env_path):
        return env
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)\s*", line)
            if m:
                key = m.group(1)
                val = m.group(2).strip().strip(chr(34)).strip(chr(39))
                env[key] = val
    return env

def main():
    env = load_env("/root/polybot/.env")
    if not env:
        print("Error: Could not load environment from /root/polybot/.env")
        sys.exit(1)

    print("==================================================")
    print("SEXYBOT VPS WALLET DIAGNOSTIC")
    print("==================================================")

    # 1. On-Chain Token Balances via Alchemy RPC
    proxy = env.get("POLYMARKET_FUNDER", "")
    alchemy_key = env.get("ALCHEMY_API_KEY", "")
    print(f"Proxy Wallet: {proxy}")
    print(f"Signer Wallet: 0xBb1639A73ae78aF9850D492D1D6Aa4D71d2909d9")
    
    tokens = [
        ("USDC.e", "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"),
        ("USDC",   "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"),
        ("pUSD",   "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"),
    ]
    
    alchemy_url = f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}" if alchemy_key else ""
    rpc_endpoints = []
    if alchemy_url:
        rpc_endpoints.append(alchemy_url)
    rpc_endpoints.extend([
        "https://polygon-rpc.com",
        "https://polygon.llamarpc.com",
        "https://polygon-bor-rpc.publicnode.com"
    ])
    
    addr_padded = proxy.lower().removeprefix("0x").rjust(64, "0")
    print("\n[On-Chain Balances]")
    balances = {}
    for tok_name, tok_addr in tokens:
        call_data = "0x70a08231" + addr_padded
        payload = {
            "jsonrpc": "2.0", "method": "eth_call", "id": 1,
            "params": [{"to": tok_addr, "data": call_data}, "latest"],
        }
        success = False
        for url in rpc_endpoints:
            try:
                # Mask API key in print
                print_url = url
                if "alchemy" in url:
                    print_url = url[:35] + "..." + url[-5:]
                
                req = urllib_req.Request(
                    url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json", "User-Agent": "sexybot/1.0"}
                )
                with urllib_req.urlopen(req, timeout=5) as r:
                    resp = json.loads(r.read().decode("utf-8"))
                if "result" in resp and resp["result"]:
                    raw = int(resp["result"], 16)
                    bal = round(raw / 1_000_000, 4)
                    balances[tok_name] = bal
                    print(f"  {tok_name}: ${bal:.4f} (via {print_url})")
                    success = True
                    break
            except Exception as e:
                # Try next endpoint silently
                continue
        if not success:
            print(f"  {tok_name}: FAILED to query all RPC endpoints")

    # 2. Polymarket CLOB Client Check
    print("\n[Polymarket CLOB Balance Allowance]")
    try:
        from py_clob_client_v2 import ClobClient
        from py_clob_client_v2.clob_types import ApiCreds, BalanceAllowanceParams, AssetType
        
        api_key = env.get("POLYMARKET_API_KEY", "")
        api_secret = env.get("POLYMARKET_API_SECRET", "")
        api_passphrase = env.get("POLYMARKET_API_PASSPHRASE", "")
        private_key = env.get("PRIVATE_KEY", "")
        clob_host = env.get("CLOB_HOST", "https://clob.polymarket.com")
        chain_id = int(env.get("CHAIN_ID", "137"))
        
        creds = None
        if api_key and api_secret and api_passphrase:
            creds = ApiCreds(
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase
            )
        
        client = ClobClient(
            host=clob_host,
            chain_id=chain_id,
            key=private_key,
            creds=creds,
            funder=proxy if proxy else None,
            signature_type=2 if creds else 0
        )
        
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=2)
        res = client.get_balance_allowance(params)
        clob_bal = float(res.get("balance", 0)) / 1_000_000
        clob_allow = float(res.get("allowance", 0)) / 1_000_000
        print(f"  CLOB Cash Balance:  ${clob_bal:.4f}")
        print(f"  CLOB Cash Allowance: ${clob_allow:.4f}")
    except Exception as e:
        print(f"  CLOB Client Query Failed: {e}")
        clob_bal = 0.0

    # 3. Check for Open CLOB Orders (Locking Collateral)
    print("\n[Open Orders on CLOB]")
    open_orders_total = 0.0
    try:
        # Check open orders
        orders = client.get_open_orders()
        print(f"  Total open orders returned: {len(orders)}")
        for o in orders:
            price = float(o.get("price", 0))
            size = float(o.get("size", 0))
            side = o.get("side", "")
            market = o.get("market", "")
            val = price * size
            open_orders_total += val
            print(f"  - {side} {size:.2f} shares @ {price:.3f} (${val:.2f}) on token {o.get('token_id', '')[:16]}...")
        print(f"  Total Cash Locked in Open Orders: ${open_orders_total:.4f}")
    except Exception as e:
        print(f"  Failed to query open orders: {e}")

    # 4. Check Database Positions and Current Valuation
    print("\n[Database Positions Valuation]")
    db_path = "/root/polybot/trades.db"
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT token_id, market, side, shares, cost, time FROM positions")
        positions_rows = cursor.fetchall()
        print(f"  Active positions in trades.db: {len(positions_rows)}")
        
        total_positions_val = 0.0
        total_positions_cost = 0.0
        
        for p in positions_rows:
            token_id, market_name, side, shares, cost, t_str = p
            # Fetch midpoint price from CLOB
            px = 0.0
            try:
                res_mid = client.get_midpoint(token_id)
                px = float(res_mid.get("mid", 0))
            except Exception:
                # Try getting last trade price or fallback
                px = cost / shares if shares > 0 else 0.0
            
            cur_val = shares * px
            total_positions_val += cur_val
            total_positions_cost += cost
            print(f"  - '{market_name[:45]}': {shares:.2f} shares @ current px {px:.3f} | Value: ${cur_val:.2f} (Cost: ${cost:.2f})")
            
        print(f"  Total Positions Cost:  ${total_positions_cost:.4f}")
        print(f"  Total Positions Value: ${total_positions_val:.4f}")
        conn.close()
    else:
        print("  trades.db not found!")
        total_positions_val = 0.0

    print("\n==================================================")
    print("SUMMARY OF VALUATION")
    on_chain_liquid = balances.get("pUSD", 0.0) + balances.get("USDC.e", 0.0) + balances.get("USDC", 0.0)
    print(f"  On-chain Liquid Cash (USDC.e + USDC + pUSD): ${on_chain_liquid:.4f}")
    print(f"  Cash Locked in Open Orders:                 ${open_orders_total:.4f}")
    print(f"  Active Positions Value:                     ${total_positions_val:.4f}")
    
    # Calculate Grand Total
    # Note: Polymarket balance_allowance usually returns the sum of liquid collateral.
    # On-chain liquid cash represents the liquid assets.
    grand_total = on_chain_liquid + open_orders_total + total_positions_val
    print(f"  GRAND TOTAL BOOK VALUE:                     ${grand_total:.4f}")
    print("==================================================")

if __name__ == "__main__":
    main()
