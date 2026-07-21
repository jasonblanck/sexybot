import os
from dotenv import load_dotenv
from redeemer import PositionRedeemer

# Load environment variables
load_dotenv()

pk = os.getenv("PRIVATE_KEY")
safe_addr = os.getenv("POLYMARKET_FUNDER", "0xD31801d84Dbc2D4D044fd080100b28a558886F23")
signer_addr = "0xBb1639A73ae78aF9850D492D1D6Aa4D71d2909d9"  # from summary

if not pk:
    print("PRIVATE_KEY not found in .env, using dummy key")
    pk = "0000000000000000000000000000000000000000000000000000000000000001"

print(f"Initializing PositionRedeemer with safe={safe_addr}, signer={signer_addr}, dry_run=True...")
r = PositionRedeemer(
    private_key=pk,
    safe_address=safe_addr,
    signer_address=signer_addr,
    dry_run=True
)

# Reset last check so it runs immediately
r._last_check = 0.0

print("Running redeemer.run_once()...")
txs = r.run_once()
print(f"Success! run_once completed, returned txs={txs}")
