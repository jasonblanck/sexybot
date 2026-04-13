"""
signing.py
Polymarket V2 — EIP-712 Order Signing (EOA, Signature Type 0)
Collateral : PMUSD (6 decimals)
Chain      : Polygon mainnet (137)

IMPORTANT: verify CTF_EXCHANGE_ADDRESS against
https://docs.polymarket.com/developers/contracts before mainnet deployment.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional

from eth_account import Account
from eth_account.messages import encode_typed_data
from web3 import Web3

POLYGON_CHAIN_ID      = 137
PMUSD_DECIMALS        = 6
PMUSD_SCALAR          = 10 ** PMUSD_DECIMALS

# Verify at https://docs.polymarket.com/developers/contracts
CTF_EXCHANGE_ADDRESS      = Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
NEG_RISK_EXCHANGE_ADDRESS = Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1a143C2adB3")
ZERO_ADDRESS              = "0x0000000000000000000000000000000000000000"


class OrderSide(IntEnum):
    BUY  = 0
    SELL = 1

class SignatureType(IntEnum):
    EOA              = 0
    POLY_PROXY       = 1
    POLY_GNOSIS_SAFE = 2


_EIP712_DOMAIN = {
    "name":              "Polymarket CTF Exchange",
    "version":           "1",
    "chainId":           POLYGON_CHAIN_ID,
    "verifyingContract": CTF_EXCHANGE_ADDRESS,
}

_ORDER_TYPES = {
    "EIP712Domain": [
        {"name": "name",              "type": "string"},
        {"name": "version",           "type": "string"},
        {"name": "chainId",           "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "Order": [
        {"name": "salt",          "type": "uint256"},
        {"name": "maker",         "type": "address"},
        {"name": "signer",        "type": "address"},
        {"name": "taker",         "type": "address"},
        {"name": "tokenId",       "type": "uint256"},
        {"name": "makerAmount",   "type": "uint256"},
        {"name": "takerAmount",   "type": "uint256"},
        {"name": "expiration",    "type": "uint256"},
        {"name": "nonce",         "type": "uint256"},
        {"name": "feeRateBps",    "type": "uint256"},
        {"name": "side",          "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
    ],
}


@dataclass
class LimitOrder:
    salt:           int
    maker:          str
    signer:         str
    taker:          str
    token_id:       int
    maker_amount:   int
    taker_amount:   int
    expiration:     int
    nonce:          int
    fee_rate_bps:   int
    side:           OrderSide
    signature_type: SignatureType = SignatureType.EOA
    signature:      str           = field(default="", repr=False)

    @property
    def price(self) -> float:
        if self.side == OrderSide.BUY:
            return self.maker_amount / self.taker_amount if self.taker_amount else 0.0
        return self.taker_amount / self.maker_amount if self.maker_amount else 0.0

    @property
    def size_pmusd(self) -> float:
        if self.side == OrderSide.BUY:
            return self.maker_amount / PMUSD_SCALAR
        return self.taker_amount / PMUSD_SCALAR


@dataclass
class SignedOrderPayload:
    order:      dict
    owner:      str
    order_type: str
    signature:  str


class OrderSigner:
    def __init__(self, private_key: str, *, neg_risk: bool = False):
        if not private_key:
            raise ValueError("private_key must not be empty")
        pk = private_key if private_key.startswith("0x") else f"0x{private_key}"
        self._account = Account.from_key(pk)
        self._address = Web3.to_checksum_address(self._account.address)
        self._domain  = dict(_EIP712_DOMAIN)
        if neg_risk:
            self._domain["verifyingContract"] = NEG_RISK_EXCHANGE_ADDRESS

    @property
    def address(self) -> str:
        return self._address

    def build_order(
        self,
        token_id:     str | int,
        side:         OrderSide,
        price:        float,
        size_pmusd:   float,
        *,
        expiration:   int = 0,
        nonce:        int = 0,
        fee_rate_bps: int = 0,
        taker:        str = ZERO_ADDRESS,
    ) -> LimitOrder:
        if not (0 < price < 1):
            raise ValueError(f"price must be in (0, 1), got {price}")
        if size_pmusd <= 0:
            raise ValueError(f"size_pmusd must be positive, got {size_pmusd}")

        token_id_int = int(token_id)

        if side == OrderSide.BUY:
            maker_amount = round(size_pmusd * PMUSD_SCALAR)
            taker_amount = round(size_pmusd / price * PMUSD_SCALAR)
        else:
            taker_amount = round(size_pmusd * PMUSD_SCALAR)
            maker_amount = round(size_pmusd / price * PMUSD_SCALAR)

        return LimitOrder(
            salt          = secrets.randbelow(2**128),
            maker         = self._address,
            signer        = self._address,
            taker         = Web3.to_checksum_address(taker),
            token_id      = token_id_int,
            maker_amount  = maker_amount,
            taker_amount  = taker_amount,
            expiration    = expiration,
            nonce         = nonce,
            fee_rate_bps  = fee_rate_bps,
            side          = side,
            signature_type= SignatureType.EOA,
        )

    def sign_order(self, order: LimitOrder) -> LimitOrder:
        structured_data = {
            "types":       _ORDER_TYPES,
            "domain":      self._domain,
            "primaryType": "Order",
            "message": {
                "salt":          order.salt,
                "maker":         order.maker,
                "signer":        order.signer,
                "taker":         order.taker,
                "tokenId":       order.token_id,
                "makerAmount":   order.maker_amount,
                "takerAmount":   order.taker_amount,
                "expiration":    order.expiration,
                "nonce":         order.nonce,
                "feeRateBps":    order.fee_rate_bps,
                "side":          int(order.side),
                "signatureType": int(order.signature_type),
            },
        }
        signable = encode_typed_data(full_message=structured_data)
        signed   = self._account.sign_message(signable)
        order.signature = signed.signature.hex()
        if not order.signature.startswith("0x"):
            order.signature = "0x" + order.signature
        return order

    def build_and_sign(
        self,
        token_id:   str | int,
        side:       OrderSide,
        price:      float,
        size_pmusd: float,
        **kwargs,
    ) -> LimitOrder:
        order = self.build_order(token_id, side, price, size_pmusd, **kwargs)
        return self.sign_order(order)

    def to_api_payload(self, order: LimitOrder, order_type: str = "GTC") -> SignedOrderPayload:
        if not order.signature:
            raise ValueError("Order must be signed before converting to payload")
        return SignedOrderPayload(
            order={
                "salt":          str(order.salt),
                "maker":         order.maker,
                "signer":        order.signer,
                "taker":         order.taker,
                "tokenId":       str(order.token_id),
                "makerAmount":   str(order.maker_amount),
                "takerAmount":   str(order.taker_amount),
                "expiration":    str(order.expiration),
                "nonce":         str(order.nonce),
                "feeRateBps":    str(order.fee_rate_bps),
                "side":          int(order.side),
                "signatureType": int(order.signature_type),
            },
            owner      = order.maker,
            order_type = order_type,
            signature  = order.signature,
        )
