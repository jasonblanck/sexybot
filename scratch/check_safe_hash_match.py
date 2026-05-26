import sys
import os
from web3 import Web3
import eth_abi

sys.path.append("/Users/jasonblanck/Documents/sexybot")
from redeemer import _safe_tx_hash

def main():
    safe_address = "0xD31801d84Dbc2D4D044fd080100b28a558886F23"
    rpc = "https://polygon-bor-rpc.publicnode.com"
    w3 = Web3(Web3.HTTPProvider(rpc))
    
    # 1. Inputs for transaction
    to = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
    # Some dummy data
    data = b"\x00" * 20
    nonce = 59
    
    # Python-computed hash
    py_hash = _safe_tx_hash(safe_address, to, data, nonce)
    print(f"Python Hash:  {py_hash.hex()}")
    
    # On-Chain Hash
    safe_abi = [
        {
            "name": "getTransactionHash",
            "type": "function",
            "inputs": [
                {"name": "to",             "type": "address"},
                {"name": "value",          "type": "uint256"},
                {"name": "data",           "type": "bytes"},
                {"name": "operation",      "type": "uint8"},
                {"name": "safeTxGas",      "type": "uint256"},
                {"name": "baseGas",        "type": "uint256"},
                {"name": "gasPrice",       "type": "uint256"},
                {"name": "gasToken",       "type": "address"},
                {"name": "refundReceiver", "type": "address"},
                {"name": "nonce",          "type": "uint256"}
            ],
            "outputs": [{"name": "", "type": "bytes32"}],
            "stateMutability": "view"
        }
    ]
    safe = w3.eth.contract(address=safe_address, abi=safe_abi)
    onchain_hash = safe.functions.getTransactionHash(
        to,
        0,
        data,
        0, # operation
        0, # safeTxGas
        0, # baseGas
        0, # gasPrice
        "0x0000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000",
        nonce
    ).call()
    
    print(f"Onchain Hash: {onchain_hash.hex()}")
    if py_hash.hex() == onchain_hash.hex():
        print("MATCH! The Python hash matches the on-chain hash.")
    else:
        print("MISMATCH! There is a bug in the Python hashing logic.")

if __name__ == '__main__':
    main()
