#!/usr/bin/env python3
"""
bootstrap_v2_api_keys.py — mint fresh CLOB V2 API creds for the bot wallet.

Why this script exists:
  Polymarket's `POST /auth/api-key` is fronted by Cloudflare with TLS
  fingerprint blocking. The official py_clob_client_v2 SDK uses a plain
  httpx client whose TLS handshake gets 403'd from any IP. The matching
  GET /auth/derive-api-key only returns existing keys, so a wallet with
  no V2 key on record can't bootstrap via the SDK alone.

  This happened during the 2026-04-28 V2 cutover: every V1 key 401'd,
  the SDK couldn't mint a V2 key, and the bot was stuck for ~5 days
  before this workaround was found.

What it does:
  1. Reads PRIVATE_KEY (+ optionally POLYMARKET_FUNDER) from .env.
  2. Builds the L1 ClobAuth EIP-712 signature using the SDK's own helper
     (so this stays in lockstep with however Polymarket's signing evolves).
  3. POSTs /auth/api-key via curl_cffi impersonating Chrome — its TLS
     handshake matches a real browser and Cloudflare lets it through.
  4. Prints the new apiKey / secret / passphrase to stdout.

Usage:
  pip install curl_cffi py_clob_client_v2 python-dotenv
  python scripts/bootstrap_v2_api_keys.py            # read .env in cwd
  python scripts/bootstrap_v2_api_keys.py /path/.env # explicit path

Then paste the three values into POLYMARKET_API_KEY / POLYMARKET_API_SECRET /
POLYMARKET_API_PASSPHRASE in /root/polybot/.env on the VPS, and:

  systemctl restart sexybot

Refs: github.com/Polymarket/py-clob-client issues #335 #337
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path


def main() -> int:
    env_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd() / ".env"
    if not env_path.exists():
        print(f"ERROR: {env_path} not found", file=sys.stderr)
        return 2

    try:
        from dotenv import load_dotenv
        from curl_cffi import requests as cffi
        from py_clob_client_v2.signing.eip712 import sign_clob_auth_message
        from py_clob_client_v2.signer import Signer
    except ImportError as e:
        print(f"ERROR: missing dependency ({e}). Install with:\n"
              "  pip install curl_cffi py_clob_client_v2 python-dotenv",
              file=sys.stderr)
        return 2

    load_dotenv(env_path)
    pk = os.getenv("PRIVATE_KEY")
    if not pk:
        print("ERROR: PRIVATE_KEY not set in .env", file=sys.stderr)
        return 2

    signer = Signer(pk, chain_id=int(os.getenv("CHAIN_ID", "137")))
    ts = int(time.time())
    nonce = 0
    sig = sign_clob_auth_message(signer, ts, nonce)

    print(f"Signer EOA: {signer.address()}", file=sys.stderr)
    print(f"Polymarket funder/proxy (informational): "
          f"{os.getenv('POLYMARKET_FUNDER', '(none — using EOA)')}", file=sys.stderr)
    print("Calling POST /auth/api-key (via curl_cffi impersonate=chrome)…",
          file=sys.stderr)

    headers = {
        "POLY_ADDRESS":   signer.address(),
        "POLY_SIGNATURE": sig,
        "POLY_TIMESTAMP": str(ts),
        "POLY_NONCE":     str(nonce),
        "Origin":         "https://polymarket.com",
        "Referer":        "https://polymarket.com/",
    }
    r = cffi.post(
        "https://clob.polymarket.com/auth/api-key",
        headers=headers, impersonate="chrome", timeout=30,
    )
    if r.status_code != 200:
        print(f"ERROR: status={r.status_code}\n{r.text[:500]}", file=sys.stderr)
        return 1

    creds = r.json()
    print(f"\nPOLYMARKET_API_KEY={creds['apiKey']}")
    print(f"POLYMARKET_API_SECRET={creds['secret']}")
    print(f"POLYMARKET_API_PASSPHRASE={creds['passphrase']}")
    print("\nPaste those three lines into /root/polybot/.env (replace the "
          "existing POLYMARKET_API_* lines), then:", file=sys.stderr)
    print("  systemctl restart sexybot", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
