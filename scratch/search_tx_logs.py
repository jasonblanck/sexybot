from web3 import Web3

RPC = "https://polygon.drpc.org"

def main():
    w3 = Web3(Web3.HTTPProvider(RPC))
    tx_hash = "0xb2ddf00d41e009fded06677e03a508b9c7b8a662e857ad91090b790167aeae24"
    safe = "0xD31801d84Dbc2D4D044fd080100b28a558886F23".lower()
    signer = "0xBb1639A73ae78aF9850D492D1D6Aa4D71d2909d9".lower()
    
    print(f"--- Searching logs for Safe: {safe} and Signer: {signer} ---")
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        found = False
        for i, log in enumerate(receipt['logs']):
            log_str = log['address'].lower() + " " + " ".join([t.hex() for t in log['topics']]) + " " + log['data'].hex()
            log_str = log_str.lower()
            if safe in log_str or signer in log_str:
                found = True
                print(f"Match in Log {i}:")
                print(f"  Address: {log['address']}")
                print(f"  Topics: {[t.hex() for t in log['topics']]}")
                print(f"  Data: {log['data'].hex()[:100]}...")
        if not found:
            print("No matching logs found in this transaction.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
