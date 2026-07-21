from web3 import Web3

RPC = "https://polygon.drpc.org"

# ABI for Gnosis Safe execTransaction to decode what was called
EXEC_TX_ABI = [
    {
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures", "type": "bytes"}
        ],
        "name": "execTransaction",
        "type": "function"
    }
]

# CTF redeemPositions ABI to decode the inner call
CTF_REDEEM_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ]
    }
]

def decode_safe_tx(tx_hash):
    w3 = Web3(Web3.HTTPProvider(RPC))
    print(f"\n--- Decoding Safe Tx: {tx_hash} ---")
    try:
        tx = w3.eth.get_transaction(tx_hash)
        # Decode Gnosis Safe execTransaction
        safe_contract = w3.eth.contract(address=tx["to"], abi=EXEC_TX_ABI)
        func_obj, func_params = safe_contract.decode_function_input(tx["input"])
        
        inner_to = func_params["to"]
        inner_data = func_params["data"]
        inner_value = func_params["value"]
        
        print(f"Safe executed call to: {inner_to}")
        print(f"Value sent: {inner_value / 10**18} POL")
        
        # Try to decode inner call if it's to CTF
        if inner_to.lower() == "0x4d97dcd97ec945f40cf65f87097ace5ea0476045":
            ctf_contract = w3.eth.contract(address=inner_to, abi=CTF_REDEEM_ABI)
            inner_func, inner_params = ctf_contract.decode_function_input(inner_data)
            print(f"Inner Method: {inner_func.fn_name}")
            print(f"Collateral Token: {inner_params['collateralToken']}")
            print(f"Condition ID: {inner_params['conditionId'].hex()}")
            print(f"Index Sets: {inner_params['indexSets']}")
        else:
            print(f"Inner Data hex (first 100 bytes): {inner_data.hex()[:100]}")
            
    except Exception as e:
        print(f"Error decoding: {e}")

def main():
    decode_safe_tx("0x1dacf695f1e193db966b7799eb08456875d6222a52657496cbc565fb03922513")
    decode_safe_tx("0x6beadcd1aa429ca11a8461837fa5e042a4878f3f36d3439c65fabfc76b83942f")

if __name__ == "__main__":
    main()
