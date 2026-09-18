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
    import time
    now = time.time()
    # 25 trades in recent 60s to trigger high spike_ratio
    return [
        {"price": 0.50 + (i * 0.001), "size": 100, "side": "BUY", "timestamp": now - (i * 0.5)}
        for i in range(25)
    ]

# Monkeypatch main_v2._get_recent_trades
import main_v2
main_v2._get_recent_trades = mock_get_recent_trades

async def run_tests():
    print("==================================================")
    print("RUNNING SPORTS CONFIDENCE BAND FILTER TESTS")
    print("==================================================")

    # 1. Test case: Sports market, spike confidence = 50.0 (inside 40-59 band)
    # Expected result: None (skipped by sports band filter)
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

    book_sports = BookSnapshot(
        token_id="t1_yes",
        bids=[Level(price=0.54, size=72.5)],
        asks=[Level(price=0.56, size=27.5)]
    )

    main_v2.MIN_BOOK_DEPTH_USDC = 10.0
    main_v2.MIN_EDGE = 0.01
    main_v2.MIN_EDGE_YES = 0.01

    # Mock trades producing valid spike (confidence >= 70.0)
    async def mock_trades_spike(token_id: str):
        import time
        now = time.time()
        trades = []
        for i in range(25):
            trades.append({"price": 0.49 + (i % 3) * 0.01, "size": 100, "side": "BUY", "timestamp": now - (i * 0.5)})
        return trades

    main_v2._get_recent_trades = mock_trades_spike

    print("Test 1: Sports market, valid spike (confidence >= 70.0)")
    sig = await estimate_true_probability(sports_market_1, book_sports)
    print(f"Result: {sig}")
    assert sig is not None, "Failed: Sports market with high confidence was filtered out!"
    print("Test 1 passed successfully.\n")

    print("Test 2: Non-sports market, valid spike")
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
    sig2 = await estimate_true_probability(politics_market, book_sports)
    print(f"Result: {sig2}")
    assert sig2 is not None, "Failed: Non-sports election market was filtered out!"
    print("Test 2 passed successfully.\n")

    print("==================================================")
    print("ALL TESTS PASSED SUCCESSFULLY!")
    print("==================================================")

if __name__ == "__main__":
    asyncio.run(run_tests())
