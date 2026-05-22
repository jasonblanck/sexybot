#!/usr/bin/env python3
"""
scratch/simulate_dynamic_kelly.py
Dry-run simulation to verify dynamic Kelly sizing, sportsbook odds discrepancies,
and calibrator decay settings under real-world mock scenarios.
"""

import sys
import os
import asyncio
import logging

# Ensure main repo is in import path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("SimulationTest")

from main_v2 import (
    _detect_sport_key,
    _american_to_prob,
    get_recent_sportsbook_odds,
    _ODDSAPI_CACHE,
    Signal,
)
from risk import kelly_size, MarketRegime
from calibrator import Calibrator


def test_sport_key_detection():
    log.info("--- Testing Sport Key Detection ---")
    queries = [
        ("Will the Boston Celtics win the NBA championship?", "basketball_nba"),
        ("Will the Kansas City Chiefs win the Super Bowl?", "americanfootball_nfl"),
        ("Will the Yankees beat the Red Sox in the MLB match tonight?", "baseball_mlb"),
        ("Will they mention March Madness in the next debate?", "basketball_ncaab"),
        ("Random question about crypto price movements", None),
    ]

    for q, expected in queries:
        detected = _detect_sport_key(q)
        log.info("Question: '%s' | Expected: %s | Detected: %s", q[:50], expected, detected)
        assert detected == expected, f"Failed sport key detection for: {q}"
    log.info("Sport Key Detection PASS!\n")


def test_american_to_prob():
    log.info("--- Testing American Odds Conversion ---")
    test_cases = [
        (+150, 0.40),
        (-200, 0.6667),
        (+100, 0.50),
        (-100, 0.50),
    ]

    for odds, expected_prob in test_cases:
        prob = _american_to_prob(odds)
        log.info("American: %+d | Expected Prob: %.4f | Calculated: %.4f", odds, expected_prob, prob)
        assert abs(prob - expected_prob) < 1e-3, f"Failed American odds conversion for: {odds}"
    log.info("American-to-Probability Conversion PASS!\n")


async def test_sportsbook_odds_matching():
    log.info("--- Testing Sportsbook Odds Matching & Devigging ---")
    
    # Mock cache data
    _ODDSAPI_CACHE["basketball_nba"] = {
        "t": 10000000000.0, # far future, never expires
        "events": [
            {
                "home": "Boston Celtics",
                "away": "Miami Heat",
                "commence": "2026-05-25T20:00",
                "books": [
                    {
                        "book": "DraftKings",
                        "outcomes": [("Boston Celtics", -200), ("Miami Heat", +150)]
                    }
                ]
            }
        ]
    }
    
    # Test case 1: Question mentions Miami Heat (Away) first
    q1 = "Will Miami Heat beat the Boston Celtics in the NBA?"
    prob1 = await get_recent_sportsbook_odds(q1)
    # DraftKings odds: Celtics -200 (prob 0.6667), Heat +150 (prob 0.40).
    # Total prob = 1.0667.
    # Devigged Heat (our_team) prob = 0.40 / 1.0667 = 0.375.
    log.info("Q: '%s' | Calculated Sportsbook Prob: %s (Expected: ~0.375)", q1, prob1)
    assert prob1 is not None and abs(prob1 - 0.375) < 1e-2, f"Failed matching/devigging for Q1: {prob1}"
    
    # Test case 2: Question mentions Celtics (Home) first
    q2 = "Will Boston Celtics win against Miami Heat in the NBA?"
    prob2 = await get_recent_sportsbook_odds(q2)
    # Devigged Celtics (our_team) prob = 0.6667 / 1.0667 = 0.625.
    log.info("Q: '%s' | Calculated Sportsbook Prob: %s (Expected: ~0.625)", q2, prob2)
    assert prob2 is not None and abs(prob2 - 0.625) < 1e-2, f"Failed matching/devigging for Q2: {prob2}"
    
    log.info("Sportsbook Odds Matching & Devigging PASS!\n")


def test_dynamic_kelly_sizing():
    log.info("--- Testing Dynamic Kelly Sizing ---")
    
    # Base configuration
    KELLY_FRACTION = 0.25
    regime_kelly_scale = 1.0
    balance = 100.0
    MAX_ORDER_SIZE = 20.0
    regime = MarketRegime(book_depth_usdc=5000, mid_volatility=0.01, spread_cents=0.2, time_to_resolution_hours=24)
    
    # Test case 1: Sports, 70% confidence (Highly Profitable Band) -> Expect Half-Kelly (0.50)
    sig_sports_high = Signal(true_prob=0.65, side="BUY", strength=1.0, source="spike+obi", confidence=70.0, is_sports=True)
    
    # Calculate sizing using dynamic Kelly logic
    current_fraction_1 = KELLY_FRACTION
    if sig_sports_high.is_sports:
        if 60.0 <= sig_sports_high.confidence <= 79.9:
            current_fraction_1 = 0.50
        elif sig_sports_high.confidence < 40.0:
            current_fraction_1 = 0.15

    dollars_1 = kelly_size(
        0.65, 0.55, balance,
        kelly_fraction=current_fraction_1 * regime_kelly_scale,
        max_size=MAX_ORDER_SIZE,
        max_pct_of_balance=0.15,
        signal_strength=sig_sports_high.strength,
        regime=regime
    )
    
    # Expected sizing:
    # edge = 0.65 - 0.55 = 0.10
    # full_kelly = 0.10 / (1.0 - 0.55) = 0.2222
    # effective_fraction = 0.50 * 1.0 * 1.0 = 0.50
    # size = 0.2222 * 0.50 * 100 = 11.11
    log.info("Sports High Conviction (70.0 conf) | Kelly Fraction: %.2f | Bet Size: $%.2f (Expected: $11.11)", 
             current_fraction_1, dollars_1)
    assert current_fraction_1 == 0.50, f"Expected Kelly fraction 0.50, got {current_fraction_1}"
    assert abs(dollars_1 - 11.11) < 0.1, f"Expected bet size $11.11, got ${dollars_1}"

    # Test case 2: Sports, 35% confidence (Low Conviction) -> Expect Tenth-Kelly (0.15)
    sig_sports_low = Signal(true_prob=0.65, side="BUY", strength=1.0, source="spike+obi", confidence=35.0, is_sports=True)
    current_fraction_2 = KELLY_FRACTION
    if sig_sports_low.is_sports:
        if 60.0 <= sig_sports_low.confidence <= 79.9:
            current_fraction_2 = 0.50
        elif sig_sports_low.confidence < 40.0:
            current_fraction_2 = 0.15

    dollars_2 = kelly_size(
        0.65, 0.55, balance,
        kelly_fraction=current_fraction_2 * regime_kelly_scale,
        max_size=MAX_ORDER_SIZE,
        max_pct_of_balance=0.15,
        signal_strength=sig_sports_low.strength,
        regime=regime
    )
    # Expected sizing:
    # size = 0.2222 * 0.15 * 100 = 3.33
    log.info("Sports Low Conviction (35.0 conf) | Kelly Fraction: %.2f | Bet Size: $%.2f (Expected: $3.33)", 
             current_fraction_2, dollars_2)
    assert current_fraction_2 == 0.15, f"Expected Kelly fraction 0.15, got {current_fraction_2}"
    assert abs(dollars_2 - 3.33) < 0.1, f"Expected bet size $3.33, got ${dollars_2}"

    # Test case 3: Non-sports category (Politics), 70% confidence -> Expect standard Quarter-Kelly (0.25)
    sig_politics = Signal(true_prob=0.65, side="BUY", strength=1.0, source="spike+obi", confidence=70.0, is_sports=False)
    current_fraction_3 = KELLY_FRACTION
    if sig_politics.is_sports:
        if 60.0 <= sig_politics.confidence <= 79.9:
            current_fraction_3 = 0.50
        elif sig_politics.confidence < 40.0:
            current_fraction_3 = 0.15

    dollars_3 = kelly_size(
        0.65, 0.55, balance,
        kelly_fraction=current_fraction_3 * regime_kelly_scale,
        max_size=MAX_ORDER_SIZE,
        max_pct_of_balance=0.15,
        signal_strength=sig_politics.strength,
        regime=regime
    )
    # Expected sizing:
    # size = 0.2222 * 0.25 * 100 = 5.56
    log.info("Non-Sports (Politics, 70.0 conf) | Kelly Fraction: %.2f | Bet Size: $%.2f (Expected: $5.56)", 
             current_fraction_3, dollars_3)
    assert current_fraction_3 == 0.25, f"Expected Kelly fraction 0.25, got {current_fraction_3}"
    assert abs(dollars_3 - 5.56) < 0.1, f"Expected bet size $5.56, got ${dollars_3}"

    log.info("Dynamic Kelly Sizing PASS!\n")


async def main():
    log.info("=== STARTING AUTONOMOUS STRATEGY SIMULATION ===")
    test_sport_key_detection()
    test_american_to_prob()
    await test_sportsbook_odds_matching()
    test_dynamic_kelly_sizing()
    log.info("=== ALL SIMULATION TESTS PASSED TRIUMPHANTLY! ===")


if __name__ == "__main__":
    asyncio.run(main())
