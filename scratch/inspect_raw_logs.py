from web3 import Web3

RPC = "https://polygon.drpc.org"

def main():
    w3 = Web3(Web3.HTTPProvider(RPC))
    tx_hash = "0xb2ddf00d41e009fded06677e03a508b9c7b8a662e857ad91090b790167aeae24"
    print(f"--- Printing ALL raw logs for Tx: {tx_hash} ---")
    try:
        receipt = w3.eth.get_transaction_receipt(tx_hash)
        print("Logs count:", len(receipt['logs']))
        for i, log in enumerate(receipt['logs']):
            print(f"Log {i}: Address={log['address']} | Topics={[t.hex() for t in log['topics']]} | Data={log['data'].hex()[:64]}...")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
