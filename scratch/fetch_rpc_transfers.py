from web3 import Web3

SAFE_ADDR = "0xD31801d84Dbc2D4D044fd080100b28a558886F23"
RPC = "https://polygon.drpc.org"

USDC_E_ADDRESS      = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_NATIVE_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
PUSD_ADDRESS        = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

# Transfer event signature hash
TRANSFER_EVENT_SIGNATURE = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

def address_to_topic(addr):
    return "0x" + addr[2:].lower().zfill(64)

def main():
    w3 = Web3(Web3.HTTPProvider(RPC))
    print(f"Connected to Web3: {w3.is_connected()}")
    
    current_block = w3.eth.block_number
    # Query past 200,000 blocks (~5 days of Polygon history)
    from_block = current_block - 200000
    print(f"Scanning from block {from_block} to {current_block}...")
    
    topic_safe = address_to_topic(SAFE_ADDR)
    
    # 1. Filter transfers to the Safe
    print("\n--- TO THE SAFE (INCOMING) ---")
    try:
        logs_in = w3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": current_block,
            "topics": [TRANSFER_EVENT_SIGNATURE, None, topic_safe]
        })
        print(f"Found {len(logs_in)} incoming transfer logs.")
        for log in logs_in:
            token = log["address"].lower()
            token_name = "Unknown"
            if token == USDC_E_ADDRESS.lower():
                token_name = "USDC.e"
            elif token == USDC_NATIVE_ADDRESS.lower():
                token_name = "USDC"
            elif token == PUSD_ADDRESS.lower():
                token_name = "pUSD"
            
            value_raw = int(log["data"].hex(), 16)
            value = value_raw / 1_000_000
            
            from_addr = "0x" + log["topics"][1].hex()[-40:]
            tx_hash = log["transactionHash"].hex()
            block = log["blockNumber"]
            print(f"Block {block} | {token_name} | +${value:.4f} | From: {from_addr} | Hash: {tx_hash}")
    except Exception as e:
        print(f"Failed to query incoming transfers: {e}")
        
    # 2. Filter transfers from the Safe
    print("\n--- FROM THE SAFE (OUTGOING) ---")
    try:
        logs_out = w3.eth.get_logs({
            "fromBlock": from_block,
            "toBlock": current_block,
            "topics": [TRANSFER_EVENT_SIGNATURE, topic_safe, None]
        })
        print(f"Found {len(logs_out)} outgoing transfer logs.")
        for log in logs_out:
            token = log["address"].lower()
            token_name = "Unknown"
            if token == USDC_E_ADDRESS.lower():
                token_name = "USDC.e"
            elif token == USDC_NATIVE_ADDRESS.lower():
                token_name = "USDC"
            elif token == PUSD_ADDRESS.lower():
                token_name = "pUSD"
            
            value_raw = int(log["data"].hex(), 16)
            value = value_raw / 1_000_000
            
            to_addr = "0x" + log["topics"][2].hex()[-40:]
            tx_hash = log["transactionHash"].hex()
            block = log["blockNumber"]
            print(f"Block {block} | {token_name} | -${value:.4f} | To: {to_addr} | Hash: {tx_hash}")
    except Exception as e:
        print(f"Failed to query outgoing transfers: {e}")

if __name__ == "__main__":
    main()
