from web3 import Web3

RPC = "https://polygon.drpc.org"

def inspect_tx(tx_hash):
    w3 = Web3(Web3.HTTPProvider(RPC))
    print(f"\n--- Inspecting Transaction: {tx_hash} ---")
    try:
        tx = w3.eth.get_transaction(tx_hash)
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        
        print(f"Block: {tx['blockNumber']}")
        print(f"From: {tx['from']}")
        print(f"To: {tx['to']}")
        print(f"Value: {tx['value'] / 10**18} POL")
        print(f"Gas Used: {receipt['gasUsed']}")
        print(f"Status: {receipt['status']}") # 1 = success, 0 = revert
        
        print("Logs/Events count:", len(receipt['logs']))
        for i, log in enumerate(receipt['logs']):
            addr = log['address']
            topics = [t.hex() for t in log['topics']]
            data = log['data'].hex()
            # If it's a Transfer event
            if topics[0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
                from_addr = "0x" + topics[1][-40:]
                to_addr = "0x" + topics[2][-40:]
                val = int(data, 16) if data else 0
                val_formatted = val / 1_000_000 # assume 6 decimals for USDC/pUSD
                print(f"  Log {i}: Transfer of {val_formatted:.4f} token from {from_addr} to {to_addr} (Contract: {addr})")
            else:
                print(f"  Log {i}: Address={addr} | Topics={topics} | Data={data[:64]}...")
    except Exception as e:
        print(f"Error inspecting tx: {e}")

def main():
    inspect_tx("0xb2ddf00d41e009fded06677e03a508b9c7b8a662e857ad91090b790167aeae24")

if __name__ == "__main__":
    main()
