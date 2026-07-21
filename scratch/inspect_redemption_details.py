from web3 import Web3

RPC = "https://polygon.drpc.org"

def main():
    w3 = Web3(Web3.HTTPProvider(RPC))
    tx_hash = "0xb2ddf00d41e009fded06677e03a508b9c7b8a662e857ad91090b790167aeae24"
    print(f"--- Inspecting ALL Transfers for Tx: {tx_hash} ---")
    
    try:
        tx = w3.eth.get_transaction(tx_hash)
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        
        USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174".lower()
        USDC_N = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359".lower()
        PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB".lower()
        
        for i, log in enumerate(receipt['logs']):
            topics = log['topics']
            addr = log['address'].lower()
            if not topics:
                continue
                
            if topics[0].hex() == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and len(topics) >= 3:
                from_addr = "0x" + topics[1].hex()[-40:]
                to_addr = "0x" + topics[2].hex()[-40:]
                val_raw = int(log['data'].hex(), 16) if log['data'] else 0
                val = val_raw / 1_000_000
                
                token_symbol = "Unknown"
                if addr == USDC_E:
                    token_symbol = "USDC.e"
                elif addr == USDC_N:
                    token_symbol = "USDC"
                elif addr == PUSD:
                    token_symbol = "pUSD"
                
                print(f"  Log {i}: Transfer of {val:.4f} {token_symbol} | From: {from_addr} | To: {to_addr} | Contract: {addr}")
                    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
