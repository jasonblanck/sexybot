#!/usr/bin/env python3
import os
import sys
import re
from web3 import Web3

# Add parent directory to sys.path to import redeemer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from redeemer import PositionRedeemer

def load_env(env_path):
    env = {}
    if not os.path.exists(env_path):
        return env
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r"\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)\s*", line)
            if m:
                key = m.group(1)
                val = m.group(2).strip().strip(chr(34)).strip(chr(39))
                env[key] = val
    return env

def main():
    env_path = "/root/polybot/.env" if os.path.exists("/root/polybot/.env") else ".env"
    env = load_env(env_path)
    if not env:
        print("Error: Could not load environment variables")
        sys.exit(1)

    private_key = env.get("PRIVATE_KEY")
    safe_address = env.get("POLYMARKET_FUNDER")
    dry_run = env.get("DRY_RUN", "true").lower() == "true"

    if not private_key or not safe_address:
        print("Error: PRIVATE_KEY or POLYMARKET_FUNDER missing from environment")
        sys.exit(1)

    w3 = Web3()
    signer_address = w3.eth.account.from_key(private_key).address

    print("==================================================")
    print("SEXYBOT MANUALLY CLAIMING ON-CHAIN WINS")
    print("==================================================")
    print(f"Signer EOA:     {signer_address}")
    print(f"Proxy Safe:     {safe_address}")
    print(f"Dry Run Mode:   {dry_run}")
    print("==================================================")

    # Initialize redeemer (forced CHECK_INTERVAL=0 to run immediately)
    redeemer = PositionRedeemer(
        private_key=private_key,
        safe_address=safe_address,
        signer_address=signer_address,
        dry_run=dry_run
    )
    redeemer.CHECK_INTERVAL = 0

    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    print("\nScanning for redeemable or mergeable positions...")
    tx_count = redeemer.run_once()
    print(f"\nOn-chain execution complete. Total transactions sent: {tx_count}")

if __name__ == "__main__":
    main()
