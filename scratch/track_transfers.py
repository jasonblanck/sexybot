import os
import sqlite3
from dotenv import load_dotenv
from web3 import Web3

load_dotenv("/root/polybot/.env")
alchemy_key = os.getenv("ALCHEMY_API_KEY")
safe_addr = os.getenv("POLYMARKET_FUNDER")

rpc = f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}"
w3 = Web3(Web3.HTTPProvider(rpc))

safe_addr = Web3.to_checksum_address(safe_addr)
USDC_NATIVE = Web3.to_checksum_address("0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359")
USDC_E = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")

# Transfer event signature: Transfer(address,address,uint256)
TRANSFER_EVENT_SIGNATURE = w3.keccak(text="Transfer(address,address,uint256)").hex()

def get_transfers(token_address, token_name):
    print(f"\n=== {token_name} Transfers ===")
    
    # We want events where topic1 (from) is safe_addr or topic2 (to) is safe_addr
    safe_topic = "0x" + safe_addr.lower().removeprefix("0x").rjust(64, "0")
    
    # Check outgoing transfers (from safe)
    try:
        out_logs = w3.eth.get_logs({
            "fromBlock": 85000000, # check from mid-2026 or so
            "toBlock": "latest",
            "address": token_address,
            "topics": [TRANSFER_EVENT_SIGNATURE, safe_topic]
        })
    except Exception as e:
        print(f"Error fetching outgoing: {e}")
        out_logs = []
        
    # Check incoming transfers (to safe)
    try:
        in_logs = w3.eth.get_logs({
            "fromBlock": 85000000,
            "toBlock": "latest",
            "address": token_address,
            "topics": [TRANSFER_EVENT_SIGNATURE, None, safe_topic]
        })
    except Exception as e:
        print(f"Error fetching incoming: {e}")
        in_logs = []

    all_events = []
    for log in out_logs:
        tx_hash = log['transactionHash'].hex()
        to_addr = "0x" + log['topics'][2].hex()[-40:]
        val = int(log['data'].hex(), 16) / 1_000_000
        block = log['blockNumber']
        all_events.append((block, "OUT", to_addr, val, tx_hash))
        
    for log in in_logs:
        tx_hash = log['transactionHash'].hex()
        from_addr = "0x" + log['topics'][1].hex()[-40:]
        val = int(log['data'].hex(), 16) / 1_000_000
        block = log['blockNumber']
        all_events.append((block, "IN", from_addr, val, tx_hash))

    all_events.sort(key=lambda x: x[0], reverse=True)
    for block, direction, addr, val, tx_hash in all_events[:50]:
        print(f"Block {block} | {direction} | ${val:.2f} | Address: {addr} | Tx: {tx_hash}")

if __name__ == '__main__':
    get_transfers(USDC_NATIVE, "USDC (Native)")
    get_transfers(USDC_E, "USDC.e (Bridged)")
