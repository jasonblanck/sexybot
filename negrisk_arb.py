"""
negrisk_arb.py
Polymarket V2 — Neg-Risk Risk-Free Arbitrage Engine
Scans multi-outcome Neg-Risk market clusters for pricing misalignments where
sum of all outcome ask prices < 0.975 (guaranteeing 2.5%+ risk-free return at resolution).
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

MIN_NEGRISK_ARBITRAGE_MARGIN = 0.025  # 2.5% minimum risk-free profit margin


@dataclass
class NegRiskArbOpportunity:
    market_cluster_id: str
    question: str
    outcomes: List[Tuple[str, float]]  # (token_id, ask_price)
    total_cost: float
    guaranteed_profit_pct: float


class NegRiskArbitrageScanner:
    def __init__(self, min_margin: float = MIN_NEGRISK_ARBITRAGE_MARGIN):
        self.min_margin = min_margin

    def scan_cluster(
        self, question: str, outcome_books: Dict[str, float]
    ) -> Optional[NegRiskArbOpportunity]:
        """
        outcome_books: map of token_id -> best_ask_price for all outcomes in a multi-outcome market.
        Returns NegRiskArbOpportunity if total_cost < 1.0 - min_margin.
        """
        if not outcome_books or len(outcome_books) < 2:
            return None

        total_cost = sum(outcome_books.values())
        if total_cost <= 0:
            return None

        margin = 1.0 - total_cost
        if margin >= self.min_margin:
            outcomes = list(outcome_books.items())
            log.info(
                "NEG-RISK ARBITRAGE OPPORTUNITY 💰 | %s: sum of asks=%.4f"
                " (Guaranteed Profit: +%.2f%%)",
                question[:45],
                total_cost,
                margin * 100,
            )
            return NegRiskArbOpportunity(
                market_cluster_id=question,
                question=question,
                outcomes=outcomes,
                total_cost=total_cost,
                guaranteed_profit_pct=margin * 100,
            )

        return None
