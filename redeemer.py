"""
redeemer.py
Auto-claim resolved positions (redeemable) and merge back-to-back positions
(mergeable) back to USDC through the Gnosis Safe proxy wallet.

Contracts (Polygon):
  ConditionalTokens : 0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
  NegRisk Adapter   : 0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296
  USDC.e            : 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
"""

from __future__ import annotations

import logging
import time

import requests
from web3 import Web3

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

POLYGON_RPC    = "https://polygon.drpc.org"
POLYGON_CHAIN  = 137
DATA_API       = "https://data-api.polymarket.com"

CTF_ADDRESS       = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
NEG_RISK_ADAPTER  = Web3.to_checksum_address("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296")
USDC_ADDRESS      = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
NULL_BYTES32      = b"\x00" * 32

CTF_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken",    "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId",        "type": "bytes32"},
            {"name": "indexSets",          "type": "uint256[]"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "name": "mergePositions",
        "type": "function",
        "inputs": [
            {"name": "collateralToken",    "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId",        "type": "bytes32"},
            {"name": "indexSets",          "type": "uint256[]"},
            {"name": "amount",             "type": "uint256"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "name": "getCollectionId",
        "type": "function",
        "inputs": [
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId",        "type": "bytes32"},
            {"name": "indexSet",           "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "view",
    },
    {
        "name": "getPositionId",
        "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "collectionId",    "type": "bytes32"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
]

NEG_RISK_ABI = [
    {
        "name": "redeemPositions",
        "type": "function",
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amounts",     "type": "uint256[]"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
    {
        "name": "mergePositions",
        "type": "function",
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amounts",     "type": "uint256[]"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
]

SAFE_ABI = [
    {
        "name": "execTransaction",
        "type": "function",
        "inputs": [
            {"name": "to",             "type": "address"},
            {"name": "value",          "type": "uint256"},
            {"name": "data",           "type": "bytes"},
            {"name": "operation",      "type": "uint8"},
            {"name": "safeTxGas",      "type": "uint256"},
            {"name": "baseGas",        "type": "uint256"},
            {"name": "gasPrice",       "type": "uint256"},
            {"name": "gasToken",       "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures",     "type": "bytes"},
        ],
        "outputs": [{"name": "success", "type": "bool"}],
        "stateMutability": "payable",
    },
    {
        "name": "nonce",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
    },
    {
        "name": "getTransactionHash",
        "type": "function",
        "inputs": [
            {"name": "to",             "type": "address"},
            {"name": "value",          "type": "uint256"},
            {"name": "data",           "type": "bytes"},
            {"name": "operation",      "type": "uint8"},
            {"name": "safeTxGas",      "type": "uint256"},
            {"name": "baseGas",        "type": "uint256"},
            {"name": "gasPrice",       "type": "uint256"},
            {"name": "gasToken",       "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "nonce",          "type": "uint256"}
        ],
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "view"
    }
]

# EIP-712 type hashes for Gnosis Safe
_SAFE_TX_TYPEHASH = Web3.keccak(
    text="SafeTx(address to,uint256 value,bytes data,uint8 operation,"
         "uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,address gasToken,"
         "address refundReceiver,uint256 nonce)"
)
_DOMAIN_TYPEHASH = Web3.keccak(
    text="EIP712Domain(uint256 chainId,address verifyingContract)"
)


# ── Safe transaction helpers ───────────────────────────────────────────────────

def _safe_tx_hash(
    safe_address: str,
    to:           str,
    data:         bytes,
    nonce:        int,
) -> bytes:
    """Compute the EIP-712 hash that the Safe owner must sign."""
    import eth_abi
    domain_sep = Web3.keccak(eth_abi.encode(
        ["bytes32", "uint256", "address"],
        [_DOMAIN_TYPEHASH, POLYGON_CHAIN, safe_address],
    ))
    safe_tx_hash = Web3.keccak(eth_abi.encode(
        ["bytes32", "address", "uint256", "bytes32",
         "uint8",   "uint256", "uint256", "uint256",
         "address", "address", "uint256"],
        [
            _SAFE_TX_TYPEHASH,
            to,               # to
            0,                # value
            Web3.keccak(data),# keccak of calldata
            0,                # operation = CALL
            0,                # safeTxGas
            0,                # baseGas
            0,                # gasPrice
            "0x0000000000000000000000000000000000000000",  # gasToken
            "0x0000000000000000000000000000000000000000",  # refundReceiver
            nonce,
        ],
    ))
    return Web3.keccak(b"\x19\x01" + domain_sep + safe_tx_hash)


def _sign_safe_tx(private_key: str, tx_hash: bytes) -> bytes:
    """Sign a Safe tx hash (raw 32-byte hash, no Ethereum message prefix)."""
    # eth_account 0.13+ removed signHash; use eth_keys directly.
    from eth_keys import keys as eth_keys
    pk_bytes = bytes.fromhex(private_key.removeprefix("0x"))
    pk_obj   = eth_keys.PrivateKey(pk_bytes)
    sig      = pk_obj.sign_msg_hash(tx_hash)
    r = int.from_bytes(sig.to_bytes()[:32], "big")
    s = int.from_bytes(sig.to_bytes()[32:64], "big")
    v = sig.v + 27   # eth_keys returns 0/1; Gnosis Safe expects 27/28
    return r.to_bytes(32, "big") + s.to_bytes(32, "big") + bytes([v])


def _exec_safe_tx(
    w3:          Web3,
    safe:        any,   # Contract
    private_key: str,
    signer_addr: str,
    to:          str,
    data:        bytes,
) -> str:
    """Build, sign, and broadcast a Safe transaction. Returns tx hash after confirmation."""
    nonce = safe.functions.nonce().call()
    
    # Query the on-chain Safe contract to get the exact expected transaction hash
    tx_hash = safe.functions.getTransactionHash(
        to,
        0,
        data,
        0, # operation = CALL
        0, # safeTxGas
        0, # baseGas
        0, # gasPrice
        "0x0000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000",
        nonce
    ).call()
    
    sig = _sign_safe_tx(private_key, tx_hash)

    gas_price = w3.eth.gas_price
    tx = safe.functions.execTransaction(
        to,       # to
        0,        # value
        data,     # data
        0,        # operation CALL
        0,        # safeTxGas
        0,        # baseGas
        0,        # gasPrice
        "0x0000000000000000000000000000000000000000",
        "0x0000000000000000000000000000000000000000",
        sig,
    ).build_transaction({
        "from":     signer_addr,
        "nonce":    w3.eth.get_transaction_count(signer_addr),
        "gasPrice": int(gas_price * 1.2),
        "chainId":  POLYGON_CHAIN,
    })
    tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.3)

    signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
    tx_hash_sent = w3.eth.send_raw_transaction(signed.raw_transaction)
    
    # Wait for the transaction to be mined to guarantee sequential nonce updates
    log.info("Safe Tx sent, hash: %s. Waiting for confirmation...", tx_hash_sent.hex())
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash_sent, timeout=120)
    if receipt.get("status") != 1:
        raise RuntimeError(f"Safe transaction failed: {tx_hash_sent.hex()}")
    log.info("Safe Tx confirmed in block %d", receipt.get("blockNumber"))
    return tx_hash_sent.hex()


# ── Main redeemer class ────────────────────────────────────────────────────────

class PositionRedeemer:
    """
    Checks for redeemable/mergeable positions every `check_interval` seconds
    and claims them on-chain through the Gnosis Safe proxy wallet.

    Usage
    -----
    redeemer = PositionRedeemer(private_key=..., safe_address=..., signer_address=...)
    # In scan loop:
    await asyncio.to_thread(redeemer.run_once)
    """

    CHECK_INTERVAL = 120   # check positions every 2 minutes

    def __init__(
        self,
        private_key:     str,
        safe_address:    str,   # funder / proxy wallet (Gnosis Safe)
        signer_address:  str,   # EOA that owns the safe
        dry_run:         bool = False,
    ):
        pk = private_key if private_key.startswith("0x") else f"0x{private_key}"
        self._pk          = pk
        self._safe_addr   = Web3.to_checksum_address(safe_address)
        self._signer_addr = Web3.to_checksum_address(signer_address)
        self._dry_run     = dry_run
        self._last_check  = 0.0
        
        # Build robust multi-RPC fallback pool
        import os
        alchemy_key = os.getenv("ALCHEMY_API_KEY", "")
        self._rpc_urls = []
        if alchemy_key:
            self._rpc_urls.append(f"https://polygon-mainnet.g.alchemy.com/v2/{alchemy_key}")
        
        # Add public endpoints as fallbacks
        self._rpc_urls.extend([
            "https://polygon-rpc.com",
            POLYGON_RPC,  # "https://polygon.drpc.org"
            "https://polygon.llamarpc.com",
            "https://polygon-bor-rpc.publicnode.com"
        ])
        
        self._current_rpc_index = 0
        self._active_w3 = None
        self._safe = None
        self._ctf = None
        self._neg_risk = None
        
        # Eagerly initialize Web3 with the first working endpoint
        self._get_w3()

    def _get_w3(self) -> Web3:
        """
        Returns a Web3 instance using the currently active RPC.
        Does not recreate the Web3 instance unless necessary.
        """
        if self._active_w3 is None:
            rpc_url = self._rpc_urls[self._current_rpc_index]
            # Mask API key in print/log
            print_url = rpc_url
            if "alchemy" in rpc_url:
                print_url = rpc_url[:35] + "..." + rpc_url[-5:]
            log.info("PositionRedeemer: initializing Web3 with RPC %s", print_url)
            
            self._active_w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 20}))
            self._safe      = self._active_w3.eth.contract(address=self._safe_addr, abi=SAFE_ABI)
            self._ctf       = self._active_w3.eth.contract(address=CTF_ADDRESS, abi=CTF_ABI)
            self._neg_risk  = self._active_w3.eth.contract(address=NEG_RISK_ADAPTER, abi=NEG_RISK_ABI)
        return self._active_w3

    def _execute_with_rpc_fallback(self, func, *args, **kwargs):
        """
        Executes a function that makes RPC calls.
        If it fails, rotates the RPC endpoint and retries.
        """
        max_attempts = len(self._rpc_urls)
        last_exc = None
        for attempt in range(max_attempts):
            try:
                w3 = self._get_w3()
                return func(w3, *args, **kwargs)
            except Exception as exc:
                last_exc = exc
                active_url = self._rpc_urls[self._current_rpc_index]
                if "alchemy" in active_url:
                    active_url = active_url[:35] + "..." + active_url[-5:]
                log.warning(
                    "PositionRedeemer: RPC error on attempt %d/%d (using %s): %s",
                    attempt + 1, max_attempts, active_url, exc
                )
                # Rotate to the next RPC
                self._current_rpc_index = (self._current_rpc_index + 1) % len(self._rpc_urls)
                self._active_w3 = None  # Force re-creation
        
        # If all attempts fail, raise the last exception
        if last_exc:
            raise last_exc

    def run_once(self) -> int:
        """
        Check for and claim all redeemable/mergeable positions.
        Returns number of on-chain transactions sent.
        Call via asyncio.to_thread() — blocks while doing RPC calls.
        Rate-limited to CHECK_INTERVAL seconds between runs.
        """
        now = time.time()
        if now - self._last_check < self.CHECK_INTERVAL:
            return 0
        self._last_check = now

        try:
            positions = self._fetch_positions()
        except Exception as exc:
            log.warning("PositionRedeemer: failed to fetch positions: %s", exc)
            return 0

        redeemable = [p for p in positions if p.get("redeemable") and float(p.get("size", 0)) > 0]
        mergeable_pairs = self._group_mergeable(positions)

        tx_count = 0

        for pos in redeemable:
            try:
                sent = self._redeem(pos)
                if sent:
                    tx_count += 1
            except Exception as exc:
                log.error("PositionRedeemer: redeem error for %s: %s", pos.get("title", "?")[:40], exc)

        for pair in mergeable_pairs:
            try:
                sent = self._merge(pair)
                if sent:
                    tx_count += 1
            except Exception as exc:
                log.error("PositionRedeemer: merge error: %s", exc)

        return tx_count

    # ── Internal ──────────────────────────────────────────────────────────────

    def _fetch_positions(self) -> list[dict]:
        resp = requests.get(
            f"{DATA_API}/positions",
            params={"user": self._safe_addr, "limit": 500},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    def _group_mergeable(self, positions: list[dict]) -> list[list[dict]]:
        """
        Pair up YES/NO positions for the same conditionId that are both mergeable.
        Returns list of [yes_pos, no_pos] pairs.
        Only merges standard binary CTF markets to avoid NegRisk multi-outcome reverts.
        """
        by_condition: dict[str, list[dict]] = {}
        for p in positions:
            if not p.get("mergeable"):
                continue
            if p.get("negativeRisk"):
                continue  # Skip NegRisk merges to avoid multi-outcome GS013 reverts
            cid = p["conditionId"]
            by_condition.setdefault(cid, []).append(p)

        pairs = []
        for cid, group in by_condition.items():
            if len(group) >= 2:
                pairs.append(group[:2])
        return pairs

    def _condition_id_bytes(self, condition_id: str) -> bytes:
        """Convert hex conditionId string to 32 bytes."""
        cid = condition_id.replace("0x", "")
        return bytes.fromhex(cid.zfill(64))

    def _resolve_collateral_address(self, pos: dict) -> str:
        """
        Dynamically determine the collateral token address for standard positions
        by comparing the fetched asset ID with the on-chain getPositionId for USDC.e and pUSD.
        """
        # Default to pUSD address because all new CLOB v2 markets use pUSD
        pUSD_addr = Web3.to_checksum_address("0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB")
        USDC_addr = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
        
        try:
            asset_id = int(pos.get("asset", "0"))
            if asset_id == 0:
                return pUSD_addr
                
            cid = self._condition_id_bytes(pos["conditionId"])
            outcome_index = pos.get("outcomeIndex", 0)
            index_set = 1 << outcome_index
            
            def _do_rpc(w3):
                # Query the on-chain CTF contract
                coll_id = self._ctf.functions.getCollectionId(NULL_BYTES32, cid, index_set).call()
                
                # Check USDC first
                usdc_id = self._ctf.functions.getPositionId(USDC_addr, coll_id).call()
                if usdc_id == asset_id:
                    return USDC_addr
                    
                # Check pUSD
                pusd_id = self._ctf.functions.getPositionId(pUSD_addr, coll_id).call()
                if pusd_id == asset_id:
                    return pUSD_addr
                    
                return pUSD_addr

            return self._execute_with_rpc_fallback(_do_rpc)
                
        except Exception as exc:
            log.warning("PositionRedeemer: failed to dynamically resolve collateral address: %s", exc)
            
        return pUSD_addr

    def _redeem(self, pos: dict) -> bool:
        """Redeem a resolved winning position."""
        title    = pos.get("title", "?")[:45]
        cid      = self._condition_id_bytes(pos["conditionId"])
        neg_risk = pos.get("negativeRisk", False)
        outcome_index = pos.get("outcomeIndex", 0)
        # indexSet: bit position for this outcome (outcome 0 = bit 0 = indexSet 1, outcome 1 = bit 1 = indexSet 2)
        index_set = 1 << outcome_index

        log.info("REDEEM | %s  outcome=%s  neg_risk=%s", title, pos.get("outcome"), neg_risk)

        if self._dry_run:
            log.info("DRY RUN — redeem skipped")
            return True

        if neg_risk:
            # NegRisk adapter: redeemPositions(bytes32 conditionId, uint256[] amounts)
            # amounts array has one entry per outcome; put size at outcome_index
            size_scaled = int(float(pos.get("size", 0)) * 1_000_000)
            # Build array large enough for this outcome index (NegRisk can have >2 outcomes)
            amounts = [0] * max(2, outcome_index + 1)
            amounts[outcome_index] = size_scaled
            data = self._neg_risk.encode_abi(abi_element_identifier="redeemPositions", args=[cid, amounts])
            target = NEG_RISK_ADAPTER
        else:
            # Resolve collateral token address dynamically (pUSD vs USDC.e)
            collateral_addr = self._resolve_collateral_address(pos)
            # Regular CTF: redeemPositions(collateral, parentCollectionId, conditionId, indexSets)
            data = self._ctf.encode_abi(
                abi_element_identifier="redeemPositions",
                args=[collateral_addr, NULL_BYTES32, cid, [index_set]],
            )
            target = CTF_ADDRESS

        def _do_redeem(w3):
            return _exec_safe_tx(w3, self._safe, self._pk, self._signer_addr, target, bytes.fromhex(data[2:]))

        tx = self._execute_with_rpc_fallback(_do_redeem)
        log.info("REDEEMED | %s  tx=%s", title, tx)
        return True

    def _merge(self, pair: list[dict]) -> bool:
        """Merge YES+NO token pair back to USDC/pUSD."""
        pos0, pos1 = pair
        title    = pos0.get("title", "?")[:45]
        cid      = self._condition_id_bytes(pos0["conditionId"])
        neg_risk = pos0.get("negativeRisk", False)

        # Merge amount = minimum of both sides (in tokens, scaled to 1e6)
        amount = int(min(pos0["size"], pos1["size"]) * 1_000_000)
        if amount <= 0:
            return False

        log.info(
            "MERGE | %s  sizes=[%.2f, %.2f]  merge_qty=%d  neg_risk=%s",
            title, pos0["size"], pos1["size"], amount, neg_risk,
        )

        if self._dry_run:
            log.info("DRY RUN — merge skipped")
            return True

        if neg_risk:
            amounts = [amount, amount]
            data = self._neg_risk.encode_abi(abi_element_identifier="mergePositions", args=[cid, amounts])
            target = NEG_RISK_ADAPTER
        else:
            # Resolve collateral token address dynamically (pUSD vs USDC.e)
            collateral_addr = self._resolve_collateral_address(pos0)
            # indexSets [1, 2] = YES (bit 0) and NO (bit 1) for a binary market
            data = self._ctf.encode_abi(
                abi_element_identifier="mergePositions",
                args=[collateral_addr, NULL_BYTES32, cid, [1, 2], amount],
            )
            target = CTF_ADDRESS

        def _do_merge(w3):
            return _exec_safe_tx(w3, self._safe, self._pk, self._signer_addr, target, bytes.fromhex(data[2:]))

        tx = self._execute_with_rpc_fallback(_do_merge)
        log.info("MERGED | %s  tx=%s", title, tx)
        return True
