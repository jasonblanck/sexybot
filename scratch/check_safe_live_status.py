import os
import re
from web3 import Web3
from py_clob_client_v2 import ClobClient
from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType

# Constants
SAFE_ADDR = "0xD31801d84Dbc2D4D044fd080100b28a558886F23"
SIGNER_ADDR = "0xBb1639A73ae78aF9850D492D1D6Aa4D71d2909d9"
RPC = "https://polygon.drpc.org"

USDC_E_ADDRESS      = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_NATIVE_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
PUSD_ADDRESS        = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

ERC20_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    }
]

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
    env = load_env(".env")
    pk = env.get("PRIVATE_KEY", "")
    print(f"Signer Private Key Loaded: {pk[:6]}...")
    
    # 1. Check On-Chain Balances
    print("\n--- ON-CHAIN WALLET BALANCES (GNOSIS SAFE) ---")
    w3 = Web3(Web3.HTTPProvider(RPC))
    for name, addr in [("USDC.e", USDC_E_ADDRESS), ("USDC", USDC_NATIVE_ADDRESS), ("pUSD", PUSD_ADDRESS)]:
        try:
            contract = w3.eth.contract(address=w3.to_checksum_address(addr), abi=ERC20_ABI)
            bal_raw = contract.functions.balanceOf(SAFE_ADDR).call()
            print(f"  {name}: ${bal_raw / 1_000_000:.4f}")
        except Exception as e:
            print(f"  Failed to query {name}: {e}")
            
    # 2. Check Polymarket CLOB Balance
    print("\n--- POLYMARKET CLOB BALANCE ---")
    try:
        client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137,
            key=pk,
            funder=SAFE_ADDR,
            signature_type=2
        )
        res = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=2))
        clob_bal = float(res.get("balance", 0)) / 1_000_000
        print(f"  CLOB Cash Balance (pUSD): ${clob_bal:.4f}")
    except Exception as e:
        print(f"  Failed to query CLOB: {e}")

    # 3. Check Open Positions on Polymarket
    print("\n--- OPEN POSITIONS (FROM POLYMARKET DATA-API) ---")
    import requests
    try:
        resp = requests.get(
            "https://data-api.polymarket.com/positions",
            params={"user": SAFE_ADDR, "limit": 100},
            timeout=15,
        )
        resp.raise_for_status()
        positions = resp.json()
        print(f"  Total Positions Returned: {len(positions)}")
        for p in positions:
            title = p.get("title", "Unknown")
            side = p.get("outcome", "Unknown")
            size = float(p.get("size", 0))
            asset = p.get("asset", "")
            cur_price = float(p.get("curPrice", 0) or 0)
            val = size * cur_price
            redeemable = p.get("redeemable", False)
            print(f"  - '{title[:45]}': {size:.2f} shares of {side} | Price: {cur_price:.3f} | Value: ${val:.2f} (Redeemable: {redeemable})")
    except Exception as e:
        print(f"  Failed to query positions: {e}")

if __name__ == "__main__":
    main()
