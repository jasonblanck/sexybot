import requests

SAFE_ADDR = "0xD31801d84Dbc2D4D044fd080100b28a558886F23"

def main():
    print("--- FETCHING TRADES FROM DATA API ---")
    url = f"https://data-api.polymarket.com/activity"
    # Polymarket data API activity endpoint
    try:
        resp = requests.get(
            url,
            params={"user": SAFE_ADDR, "limit": 50},
            timeout=15,
        )
        resp.raise_for_status()
        activity = resp.json()
        print(f"Total activity events: {len(activity)}")
        for act in activity:
            title = act.get("title", "")
            side = act.get("side", "")
            size = act.get("size", "")
            price = act.get("price", "")
            tx_hash = act.get("transactionHash", "")
            mkt_type = act.get("marketType", "")
            ts = act.get("timestamp", "")
            print(f"[{ts}] {side.upper()} {size} shares @ {price} | Market: {title[:50]} | Hash: {tx_hash}")
    except Exception as e:
        print(f"Failed to query activity: {e}")

if __name__ == "__main__":
    main()
