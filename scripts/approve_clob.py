#!/usr/bin/env python3
import os
import re
import sys
from web3 import Web3

# ERC20 ABI for approve and allowance
ERC20_ABI = [
    {
        "name": "approve",
        "type": "function",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "value", "type": "uint256"}
        ],
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable"
    },
    {
        "name": "allowance",
        "type": "function",
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"}
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view"
    }
]

SAFE_ABI = [
    {
        "name": "execTransaction",
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
            {"name": "signatures",     "type": "bytes"},
        ],
        "outputs": [{"name": "success", "type": "bool"}],
        "stateMutability": "payable",
    },
    {
        "name": "nonce",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]

# EIP-712 type hashes for Gnosis Safe
POLYGON_CHAIN = 137
_SAFE_TX_TYPEHASH = Web3.keccak(
    text="SafeTx(address to,uint256 value,bytes data,uint8 operation,"
         "uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,"
         "address refundReceiver,uint256 nonce)"
)
_DOMAIN_TYPEHASH = Web3.keccak(
    text="EIP712Domain(uint256 chainId,address verifyingContract)"
)

def _safe_tx_hash(safe_address: str, to: str, data: bytes, nonce: int) -> bytes:
    import eth_abi
    domain_sep = Web3.keccak(eth_abi.encode(
        ["bytes32", "uint256", "address"],
        [_DOMAIN_TYPEHASH, POLYGON_CHAIN, safe_address],
    ))
    safe_tx_hash = Web3.keccak(eth_abi.encode(
        ["bytes32", "address", "uint256", "bytes32",
         "uint8",   "uint256", "uint256", "uint256",
         "address", "address", "uint256"],
        [
            _SAFE_TX_TYPEHASH,
            to,
            0,
            Web3.keccak(data),
            0, # CALL
            0,
            0,
            0,
            "0x0000000000000000000000000000000000000000",
            "0x0000000000000000000000000000000000000000",
            nonce,
        ],
    ))
    return Web3.keccak(b"\x19\x01" + domain_sep + safe_tx_hash)

def _sign_safe_tx(private_key: str, tx_hash: bytes) -> bytes:
    from eth_keys import keys as eth_keys
    pk_bytes = bytes.fromhex(private_key.removeprefix("0x"))
    pk_obj   = eth_keys.PrivateKey(pk_bytes)
    sig      = pk_obj.sign_msg_hash(tx_hash)
    r = int.from_bytes(sig.to_bytes()[:32], "big")
    s = int.from_bytes(sig.to_bytes()[32:64], "big")
    v = sig.v + 27
    return r.to_bytes(32, "big") + s.to_bytes(32, "big") + bytes([v])

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
    env_path = "/root/polybot/.env" if os.path.exists("/root/polybot/.env") else ".env"
    env = load_env(env_path)
    if not env:
        print("Error: Could not load environment variables")
        sys.exit(1)

    private_key = env.get("PRIVATE_KEY")
    safe_address = env.get("POLYMARKET_FUNDER")
    alchemy_key = env.get("ALCHEMY_API_KEY")

    if not private_key or not safe_address:
        print("Error: PRIVATE_KEY or POLYMARKET_FUNDER missing from environment")
        sys.exit(1)

    rpc_url = f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}" if alchemy_key else "https://polygon-rpc.com"
    print(f"Connecting to RPC: {rpc_url[:35]}...")
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        print("Error: Failed to connect to Polygon RPC")
        sys.exit(1)

    signer_addr = w3.eth.account.from_key(private_key).address
    print(f"Signer wallet: {signer_addr}")
    print(f"Proxy Gnosis Safe: {safe_address}")

    # PMUSD Collateral Token on Polygon
    pmusd_address = Web3.to_checksum_address("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")
    
    # Spenders to approve
    spenders = {
        "CTF Exchange V2": Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"),
        "NegRisk CTF": Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a")
    }

    pmusd = w3.eth.contract(address=pmusd_address, abi=ERC20_ABI)
    safe = w3.eth.contract(address=Web3.to_checksum_address(safe_address), abi=SAFE_ABI)

    max_uint256 = 115792089237316195423570985008687907853269984665640564039457584007913129639935

    for name, spender in spenders.items():
        print(f"\nChecking allowance for {name} ({spender})...")
        current_allowance = pmusd.functions.allowance(safe_address, spender).call()
        print(f"Current allowance: {current_allowance / 1_000_000:.2f} pUSD")

        if current_allowance > 1_000_000 * 1_000: # Over $1,000 allowance is plenty
            print(f"Allowance is sufficient, skipping approval.")
            continue

        print(f"Approving {name} on-chain...")
        
        # 1. Encode approve calldata
        approve_data_hex = pmusd.encode_abi("approve", args=[spender, max_uint256])
        approve_data = bytes.fromhex(approve_data_hex[2:])
        
        # 2. Get safe nonce
        nonce = safe.functions.nonce().call()
        print(f"Safe Nonce: {nonce}")
        
        # 3. Hash and sign Safe tx
        tx_hash = _safe_tx_hash(safe.address, pmusd.address, approve_data, nonce)
        sig = _sign_safe_tx(private_key, tx_hash)
        
        # 4. Build, estimate and broadcast
        gas_price = w3.eth.gas_price
        tx = safe.functions.execTransaction(
            pmusd.address,  # to
            0,              # value
            approve_data,   # data
            0,              # operation CALL
            0,              # safeTxGas
            0,              # baseGas
            0,              # gasPrice
            "0x0000000000000000000000000000000000000000",
            "0x0000000000000000000000000000000000000000",
            sig,
        ).build_transaction({
            "from":     signer_addr,
            "nonce":    w3.eth.get_transaction_count(signer_addr),
            "gasPrice": int(gas_price * 1.25),
            "chainId":  POLYGON_CHAIN,
        })
        
        tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.3)
        
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash_sent = w3.eth.send_raw_transaction(signed.raw_transaction)
        print(f"Broadcasted approval transaction: {tx_hash_sent.hex()}")
        
        # Wait for transaction receipt
        print("Waiting for transaction confirmation...")
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash_sent, timeout=120)
        if receipt.status == 1:
            print(f"Successfully approved {name}!")
        else:
            print(f"Transaction failed for {name}!")

if __name__ == "__main__":
    main()
