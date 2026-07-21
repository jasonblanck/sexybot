import os
import sys
from dotenv import load_dotenv
from web3 import Web3
from eth_account import Account

# Load environment
load_dotenv("/root/polybot/.env" if os.path.exists("/root/polybot/.env") else ".env")

alchemy_key = os.getenv("ALCHEMY_API_KEY")
pk = os.getenv("PRIVATE_KEY")
safe_addr = os.getenv("POLYMARKET_FUNDER")

if not alchemy_key or not pk or not safe_addr:
    print("Error: Missing ALCHEMY_API_KEY, PRIVATE_KEY, or POLYMARKET_FUNDER in .env")
    sys.exit(1)

# Initialize Web3
rpc = f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}"
w3 = Web3(Web3.HTTPProvider(rpc))

# Addresses
USDC_NATIVE_ADDRESS = Web3.to_checksum_address("0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359")
safe_checksum = Web3.to_checksum_address(safe_addr)
signer_acc = Account.from_key(pk)
signer_checksum = Web3.to_checksum_address(signer_acc.address)

print(f"Safe Address: {safe_checksum}")
print(f"Signer Address: {signer_checksum}")

# ERC20 ABI & Gnosis Safe ABI
ERC20_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "balance", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "transfer",
        "type": "function",
        "inputs": [{"name": "recipient", "type": "address"}, {"name": "amount", "type": "uint256"}],
        "outputs": [{"name": "success", "type": "bool"}],
        "stateMutability": "nonpayable",
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

# Fetch USDC balance of Safe
usdc_contract = w3.eth.contract(address=USDC_NATIVE_ADDRESS, abi=ERC20_ABI)
balance = usdc_contract.functions.balanceOf(safe_checksum).call()
print(f"USDC Balance in Safe: {balance / 1_000_000} USDC")

if balance == 0:
    print("No native USDC found in Safe.")
    sys.exit(0)

# Sign Safe transaction helper
def _sign_safe_tx(private_key: str, tx_hash: bytes) -> bytes:
    from eth_keys import keys as eth_keys
    pk_bytes = bytes.fromhex(private_key.removeprefix("0x"))
    pk_obj   = eth_keys.PrivateKey(pk_bytes)
    sig      = pk_obj.sign_msg_hash(tx_hash)
    r = int.from_bytes(sig.to_bytes()[:32], "big")
    s = int.from_bytes(sig.to_bytes()[32:64], "big")
    v = sig.v + 27
    return r.to_bytes(32, "big") + s.to_bytes(32, "big") + bytes([v])

# Build the transfer data: USDC.transfer(recipient, amount)
transfer_data = usdc_contract.encode_abi(
    abi_element_identifier="transfer",
    args=[signer_checksum, balance]
)

# Instantiate Safe contract
safe_contract = w3.eth.contract(address=safe_checksum, abi=SAFE_ABI)
nonce = safe_contract.functions.nonce().call()

# Get transaction hash
tx_hash = safe_contract.functions.getTransactionHash(
    USDC_NATIVE_ADDRESS,
    0,
    bytes.fromhex(transfer_data[2:]),
    0,
    0,
    0,
    0,
    "0x0000000000000000000000000000000000000000",
    "0x0000000000000000000000000000000000000000",
    nonce
).call()

# Sign the transaction
sig = _sign_safe_tx(pk, tx_hash)

# Send executive transaction
gas_price = w3.eth.gas_price
tx = safe_contract.functions.execTransaction(
    USDC_NATIVE_ADDRESS,
    0,
    bytes.fromhex(transfer_data[2:]),
    0,
    0,
    0,
    0,
    "0x0000000000000000000000000000000000000000",
    "0x0000000000000000000000000000000000000000",
    sig
).build_transaction({
    "from":     signer_checksum,
    "nonce":    w3.eth.get_transaction_count(signer_checksum),
    "gasPrice": int(gas_price * 1.5),
    "chainId":  137
})

signed_tx = w3.eth.account.sign_transaction(tx, private_key=pk)
tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
print(f"Transaction sent! Hash: {tx_hash.hex()}")
receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
print(f"Transaction confirmed! Status: {receipt['status']}")
