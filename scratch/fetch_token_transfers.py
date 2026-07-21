import requests

SAFE_ADDR = "0xD31801d84Dbc2D4D044fd080100b28a558886F23"

def main():
    print(f"--- Fetching Token Transfers for Safe: {SAFE_ADDR} ---")
    # We will use the free Polygonscan API to get token transfers
    url = "https://api.polygonscan.com/api"
    params = {
        "module": "account",
        "action": "tokentx",
        "address": SAFE_ADDR,
        "startblock": 0,
        "endblock": 99999999,
        "sort": "desc",
        "apikey": "FRWJB6DEAZSBS566VU3ZNTSRNM249I39EJ" # Using the key in env if it works
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "0":
            print(f"Polygonscan API message: {data.get('message')}")
            # Try without API key
            params["apikey"] = ""
            resp = requests.get(url, params=params, timeout=15)
            data = resp.json()
        
        transfers = data.get("result", [])
        print(f"Total transfers found: {len(transfers)}")
        for tx in transfers[:30]:
            token_symbol = tx.get("tokenSymbol", "")
            token_name = tx.get("tokenName", "")
            from_addr = tx.get("from", "")
            to_addr = tx.get("to", "")
            value = float(tx.get("value", 0)) / (10**int(tx.get("tokenDecimal", 18)))
            tx_hash = tx.get("hash", "")
            block = tx.get("blockNumber", "")
            print(f"Block {block} | {token_symbol} | {value:.4f} | From: {from_addr[:10]}... | To: {to_addr[:10]}... | Hash: {tx_hash}")
    except Exception as e:
        print(f"Error fetching transfers: {e}")

if __name__ == "__main__":
    main()
