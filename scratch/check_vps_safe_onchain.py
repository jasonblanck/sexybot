import sys
import os
from web3 import Web3

def main():
    safe_address = "0xD31801d84Dbc2D4D044fd080100b28a558886F23"
    signer_address = "0xBb1639A73ae78aF9850D492D1D6Aa4D71d2909d9"
    
    rpc = "https://polygon-bor-rpc.publicnode.com"
    w3 = Web3(Web3.HTTPProvider(rpc))
    
    if not w3.is_connected():
        print("Failed to connect to RPC")
        return
        
    print(f"Connected to RPC. Checking Safe at {safe_address}...")
    
    # 1. Nonce
    safe_abi = [
        {"name": "nonce", "type": "function", "inputs": [], "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view"},
        {"name": "VERSION", "type": "function", "inputs": [], "outputs": [{"name": "", "type": "string"}], "stateMutability": "view"},
        {"name": "getOwners", "type": "function", "inputs": [], "outputs": [{"name": "", "type": "address[]"}], "stateMutability": "view"}
    ]
    
    safe = w3.eth.contract(address=safe_address, abi=safe_abi)
    try:
        ver = safe.functions.VERSION().call()
        print(f"Safe Version: {ver}")
    except Exception as e:
        print(f"Error checking version: {e}")
        
    try:
        nonce = safe.functions.nonce().call()
        print(f"Safe Nonce: {nonce}")
    except Exception as e:
        print(f"Error checking nonce: {e}")
        
    try:
        owners = safe.functions.getOwners().call()
        print(f"Safe Owners: {owners}")
    except Exception as e:
        print(f"Error checking owners: {e}")
        
    signer_nonce = w3.eth.get_transaction_count(signer_address)
    print(f"Signer Nonce (Transaction Count): {signer_nonce}")
    
    signer_balance = w3.eth.get_balance(signer_address)
    print(f"Signer Balance: {signer_balance / 1e18:.6f} POL")

if __name__ == '__main__':
    main()
