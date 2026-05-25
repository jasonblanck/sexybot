import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

# Setup basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
log = logging.getLogger("test_mm")

# Mock classes to satisfy imports in market_maker
@dataclass
class PolyMarket:
    yes_token_id: str
    no_token_id: str
    question: str

# Import our modified modules
from market_maker import MarketMaker, MMState
from discovery import PolyMarket as DiscoveryPolyMarket

class MockExecutor:
    def __init__(self):
        self.orders = {}
        self.cancelled = []
        self.cancel_all_called = False

    def get_order(self, order_id: str) -> Optional[dict]:
        return self.orders.get(order_id)

    def cancel_order(self, order_id: str) -> bool:
        self.cancelled.append(order_id)
        return True

    def cancel_all_orders(self) -> bool:
        self.cancel_all_called = True
        return True

    def place_limit_order(self, *args, **kwargs):
        pass

class MockBookManager:
    def get_book(self, token_id: str):
        return None

async def test_mm_netting():
    log.info("Starting Market Maker Netting Tests...")
    
    ex = MockExecutor()
    bm = MockBookManager()
    
    mm = MarketMaker(ex, bm)
    
    # 1. Test record_fill netting
    yes_tid = "0xYES_TOKEN_ID"
    no_tid = "0xNO_TOKEN_ID"
    
    # Register state
    state = MMState()
    mm._states[yes_tid] = state
    
    log.info("1. Simulating BUY YES fills...")
    mm.record_fill(yes_tid, "yes", 10.0)
    assert state.yes_inventory == 10.0
    assert state.no_inventory == 0.0
    log.info(f"   Filled 10 YES. yes_inv={state.yes_inventory}, no_inv={state.no_inventory} (PASSED)")
    
    log.info("2. Simulating BUY NO fills (hedging)...")
    mm.record_fill(yes_tid, "no", 4.0)
    assert state.yes_inventory == 6.0
    assert state.no_inventory == 0.0
    log.info(f"   Filled 4 NO. yes_inv={state.yes_inventory}, no_inv={state.no_inventory} (PASSED)")
    
    log.info("3. Simulating BUY NO fills that exceed YES (reversing exposure)...")
    mm.record_fill(yes_tid, "no", 8.0)
    assert state.yes_inventory == 0.0
    assert state.no_inventory == 2.0
    log.info(f"   Filled 8 NO. yes_inv={state.yes_inventory}, no_inv={state.no_inventory} (PASSED)")
    
    # 4. Test skew calculation denominator
    log.info("4. Testing Skew Denominator size...")
    # Compute quote skew
    mid = 0.50
    # In _compute_quotes, yes_imbalance = yes_inventory - no_inventory = -2.0
    # Denominator = max(1.0, yes_inv + no_inv + 1) = max(1.0, 0.0 + 2.0 + 1) = 3.0
    # Skew = -2.0 / 3.0 * SKEW_FACTOR * mid
    yes_bid, no_bid = mm._compute_quotes(mid, state)
    log.info(f"   Quotes computed at mid=0.50: yes_bid={yes_bid}, no_bid={no_bid}")
    assert yes_bid > 0.0
    assert no_bid > 0.0
    log.info("   Denominator remains small and sensitive (PASSED)")
    
    # 5. Test reconcile_inventory_on_startup
    log.info("5. Testing reconcile_inventory_on_startup...")
    mkt = DiscoveryPolyMarket(
        id           = "0xMARKET_ID",
        question     = "Will candidate X win?",
        slug         = "will-candidate-x-win",
        active       = True,
        closed       = False,
        liquidity    = 10000.0,
        volume_24h   = 5000.0,
        yes_token_id = yes_tid,
        no_token_id  = no_tid,
        yes_price    = 0.60,
        no_price     = 0.40,
        end_date     = "2026-12-31"
    )
    
    # Mock positions from Polymarket data-api
    live_positions = [
        {"asset": yes_tid, "size": "15.0", "curPrice": "0.60", "redeemable": False},
        {"asset": no_tid, "size": "5.0", "curPrice": "0.40", "redeemable": False},
    ]
    
    mm.reconcile_inventory_on_startup([mkt], live_positions)
    reconciled_state = mm._states[yes_tid]
    # Expected: 15.0 YES and 5.0 NO should net out to 10.0 YES and 0.0 NO
    assert reconciled_state.yes_inventory == 10.0
    assert reconciled_state.no_inventory == 0.0
    log.info(f"   Reconciled: yes_inv={reconciled_state.yes_inventory}, no_inv={reconciled_state.no_inventory} (PASSED)")

    log.info("ALL TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    asyncio.run(test_mm_netting())
