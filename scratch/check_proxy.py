import requests

SIGNER = "0xBb1639A73ae78aF9850D492D1D6Aa4D71d2909d9"

def main():
    print(f"--- Fetching Polymarket Proxy for Signer: {SIGNER} ---")
    url = f"https://data-api.polymarket.com/users/{SIGNER}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        print("User Profile Data:")
        print(f"  Proxy Wallet: {data.get('proxyAddress')}")
        print(f"  Gnosis Safe:  {data.get('gnosisSafeAddress')}")
        print(f"  Display Name: {data.get('displayName')}")
        print(f"  Balances:     {data.get('balances')}")
    except Exception as e:
        print(f"Error fetching profile: {e}")

if __name__ == "__main__":
    main()
