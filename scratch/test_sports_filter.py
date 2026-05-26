import os
import sys
import asyncio
import logging

# Setup dummy PRIVATE_KEY before importing main_v2
os.environ["PRIVATE_KEY"] = "0x" + "1" * 64

# Add parent directory to sys.path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

from main_v2 import estimate_true_probability, Signal
from discovery import PolyMarket
from orderbook_ws import BookSnapshot, Level

# Mock _get_recent_trades to avoid real API requests.
# We will return dummy trades or none depending on the test.
async def mock_get_recent_trades(token_id: str):
    # Return empty trades to trigger OBI solo fallback
    return []

# Monkeypatch main_v2._get_recent_trades
import main_v2
main_v2._get_recent_trades = mock_get_recent_trades

async def run_tests():
    print("==================================================")
    print("RUNNING SPORTS CONFIDENCE BAND FILTER TESTS")
    print("==================================================")

    # 1. Test case: Sports market, OBI solo, confidence = 45.0
    # OBI = 0.45, so confidence = abs(obi) * 100 = 45.0. This is in the 40-59 band.
    # classify_internal_category for "Will Lakers win on 2026-05-25?" will return "sports".
    # Expected result: None (skipped by filter)
    sports_market_1 = PolyMarket(
        id="m1",
        question="Will Lakers win on 2026-05-25?",
        slug="lakers-win-2026-05-25",
        active=True,
        closed=False,
        liquidity=50000.0,
        volume_24h=10000.0,
        yes_token_id="t1_yes",
        no_token_id="t1_no",
        yes_price=0.55,
        no_price=0.45,
        end_date="2026-05-26T00:00:00Z"
    )

    # Book with vamp = 0.55, bids total/asks total >= MIN_VAMP_VOL, book depth >= MIN_BOOK_DEPTH_USDC
    # bids best bid = 0.54, asks best ask = 0.56
    # OBI solo: abs(obi) = 0.45, so bid_vol / total - ask_vol / total = 0.45
    # Let's say bid_vol = 72.5, ask_vol = 27.5, total = 100. obi = 45.0 / 100.0 = 0.45
    book_sports_1 = BookSnapshot(
        token_id="t1_yes",
        bids=[Level(price=0.54, size=72.5)],
        asks=[Level(price=0.56, size=27.5)]
    )

    # Set OBI_SOLO_MIN low to allow OBI solo fallback (default is 0.55, let's override main_v2.OBI_SOLO_MIN)
    main_v2.OBI_SOLO_MIN = 0.30
    main_v2.MIN_BOOK_DEPTH_USDC = 10.0
    main_v2.MIN_EDGE = 0.01
    main_v2.MIN_EDGE_YES = 0.01

    print("Test 1: Sports market, confidence = 45.0")
    sig = await estimate_true_probability(sports_market_1, book_sports_1)
    print(f"Result: {sig}")
    assert sig is None, "Failed: Sports market with confidence 45.0 was NOT filtered out!"
    print("Test 1 passed successfully.\n")

    # 2. Test case: Sports market, OBI solo, confidence = 70.0 (outside 40-59 band)
    # Expected result: Not None (not skipped by filter, assuming other checks pass)
    # Let's use OBI = 0.70 (bid_vol = 85.0, ask_vol = 15.0, total = 100. obi = 0.70)
    # We set OBI_CONFIRM_MIN to 0.10 to pass other checks if any, or OBI opposition check.
    main_v2.OBI_CONFIRM_MIN = 0.10
    book_sports_2 = BookSnapshot(
        token_id="t1_yes",
        bids=[Level(price=0.54, size=85.0)],
        asks=[Level(price=0.56, size=15.0)]
    )

    print("Test 2: Sports market, confidence = 70.0")
    sig = await estimate_true_probability(sports_market_1, book_sports_2)
    print(f"Result: {sig}")
    assert sig is not None, "Failed: Sports market with confidence 70.0 was filtered out!"
    print("Test 2 passed successfully.\n")

    # 3. Test case: Non-Sports market (e.g. politics), OBI solo, confidence = 45.0
    # Expected result: Not None (not skipped by filter)
    politics_market = PolyMarket(
        id="m2",
        question="Will Donald Trump win the presidential election?",
        slug="trump-win-election",
        active=True,
        closed=False,
        liquidity=50000.0,
        volume_24h=10000.0,
        yes_token_id="t2_yes",
        no_token_id="t2_no",
        yes_price=0.55,
        no_price=0.45,
        end_date="2026-05-26T00:00:00Z"
    )

    print("Test 3: Non-sports market, confidence = 45.0")
    sig = await estimate_true_probability(politics_market, book_sports_1)
    print(f"Result: {sig}")
    assert sig is not None, "Failed: Non-sports market with confidence 45.0 was filtered out!"
    print("Test 3 passed successfully.\n")

    print("==================================================")
    print("ALL TESTS PASSED SUCCESSFULLY!")
    print("==================================================")

if __name__ == "__main__":
    asyncio.run(run_tests())
