import requests

SAFE_ADDR = "0xD31801d84Dbc2D4D044fd080100b28a558886F23"

def main():
    print(f"--- Fetching Safe Transactions for: {SAFE_ADDR} ---")
    url = f"https://safe-transaction-polygon.safe.global/api/v1/safes/{SAFE_ADDR}/all-transactions/"
    try:
        resp = requests.get(url, params={"limit": 20}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        print(f"Total transactions returned: {len(results)}")
        for tx in results:
            tx_type = tx.get("type", "UNKNOWN")
            execution_date = tx.get("executionDate", "")
            tx_hash = tx.get("transactionHash", "")
            
            if tx_type == "MULTISIG_TRANSACTION":
                to_addr = tx.get("to", "")
                value = int(tx.get("value", 0)) / 10**18
                data_decoded = tx.get("dataDecoded", {})
                method = data_decoded.get("method", "UnknownMethod") if data_decoded else "Unknown"
                print(f"[{execution_date}] MULTISIG | To: {to_addr[:10]}... | Value: {value:.4f} | Method: {method} | Hash: {tx_hash}")
            elif tx_type == "ETHER_TRANSFER":
                from_addr = tx.get("from", "")
                to_addr = tx.get("to", "")
                value = int(tx.get("value", 0)) / 10**18
                print(f"[{execution_date}] ETH_TRANSFER | From: {from_addr[:10]}... | To: {to_addr[:10]}... | Value: {value:.4f} | Hash: {tx_hash}")
            elif tx_type == "MODULE_TRANSACTION":
                to_addr = tx.get("to", "")
                value = int(tx.get("value", 0)) / 10**18
                print(f"[{execution_date}] MODULE_TX | To: {to_addr[:10]}... | Value: {value:.4f} | Hash: {tx_hash}")
            else:
                print(f"[{execution_date}] {tx_type} | Hash: {tx_hash}")
    except Exception as e:
        print(f"Error fetching safe transactions: {e}")

if __name__ == "__main__":
    main()
