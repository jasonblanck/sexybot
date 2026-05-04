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
    """Build, sign, and broadcast a Safe transaction. Returns tx hash."""
    nonce = safe.functions.nonce().call()
    tx_hash = _safe_tx_hash(safe.address, to, data, nonce)
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
        self._w3          = Web3(Web3.HTTPProvider(POLYGON_RPC, request_kwargs={"timeout": 20}))
        self._safe        = self._w3.eth.contract(address=self._safe_addr, abi=SAFE_ABI)
        self._ctf         = self._w3.eth.contract(address=CTF_ADDRESS, abi=CTF_ABI)
        self._neg_risk    = self._w3.eth.contract(address=NEG_RISK_ADAPTER, abi=NEG_RISK_ABI)

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

        redeemable = [p for p in positions if p.get("redeemable")]
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
        """
        by_condition: dict[str, list[dict]] = {}
        for p in positions:
            if not p.get("mergeable"):
                continue
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
            # web3.py 7.x renamed encodeABI(fn_name=…) to
            # encode_abi(abi_element_identifier=…) — the old camelCase
            # name silently raised AttributeError on every redeem,
            # leaving winning positions stuck on Polymarket as
            # unredeemed value (showed up as a cash mismatch between
            # the bot dashboard and polymarket.com).
            data = self._neg_risk.encode_abi(abi_element_identifier="redeemPositions", args=[cid, amounts])
            target = NEG_RISK_ADAPTER
        else:
            # Regular CTF: redeemPositions(collateral, parentCollectionId, conditionId, indexSets)
            data = self._ctf.encode_abi(
                abi_element_identifier="redeemPositions",
                args=[USDC_ADDRESS, NULL_BYTES32, cid, [index_set]],
            )
            target = CTF_ADDRESS

        tx = _exec_safe_tx(self._w3, self._safe, self._pk, self._signer_addr, target, bytes.fromhex(data[2:]))
        log.info("REDEEMED | %s  tx=%s", title, tx)
        return True

    def _merge(self, pair: list[dict]) -> bool:
        """Merge YES+NO token pair back to USDC."""
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
            # indexSets [1, 2] = YES (bit 0) and NO (bit 1) for a binary market
            data = self._ctf.encode_abi(
                abi_element_identifier="mergePositions",
                args=[USDC_ADDRESS, NULL_BYTES32, cid, [1, 2], amount],
            )
            target = CTF_ADDRESS

        tx = _exec_safe_tx(self._w3, self._safe, self._pk, self._signer_addr, target, bytes.fromhex(data[2:]))
        log.info("MERGED | %s  tx=%s", title, tx)
        return True
