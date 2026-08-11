"""
odds_arbitrage.py
Polymarket V2 — Sharp Sportsbook Arbitrage Engine
Integrates The Odds API to compare sharp sportsbook probabilities (Pinnacle, DraftKings, FanDuel)
against Polymarket prices, capturing cross-venue latency arbitrage.
"""

import time
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import requests

log = logging.getLogger(__name__)

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
DEFAULT_KEY = "5de376180beea7063706c8abd03097a5"

# Priority sharp bookmakers (Pinnacle is sharpest global benchmark, DraftKings/FanDuel for US sports)
SHARP_BOOKMAKERS = {"pinnacle", "draftkings", "fanduel", "betonlineag", "bovada"}


@dataclass
class SharpOddsSignal:
    event_name: str
    team_name: str
    sharp_prob: float
    sharp_book: str
    edge: float


class OddsArbitrageEngine:
    def __init__(self, api_key: str = DEFAULT_KEY, cache_ttl_sec: float = 300.0):
        self.api_key = api_key
        self.cache_ttl_sec = cache_ttl_sec
        self._cache: Dict[str, Tuple[float, Dict[str, float]]] = {}  # sport -> (ts, team_probs)
        self.last_fetch_ts = 0.0

    def _devig_probabilities(self, home_price: float, away_price: float) -> Tuple[float, float]:
        """Convert decimal odds to devigged (fair) implied probabilities."""
        if home_price <= 1.0 or away_price <= 1.0:
            return 0.5, 0.5
        raw_home = 1.0 / home_price
        raw_away = 1.0 / away_price
        total_overround = raw_home + raw_away
        if total_overround <= 0:
            return 0.5, 0.5
        return round(raw_home / total_overround, 4), round(raw_away / total_overround, 4)

    def fetch_sharp_probabilities(self, sport_key: str = "baseball_mlb") -> Dict[str, float]:
        """
        Fetch sharp sportsbook odds for a given sport.
        Returns map of normalized team/outcome name -> sharp implied probability.
        """
        now = time.time()
        if sport_key in self._cache:
            ts, cached_data = self._cache[sport_key]
            if now - ts < self.cache_ttl_sec:
                return cached_data

        url = f"{ODDS_API_BASE}/sports/{sport_key}/odds/"
        params = {
            "apiKey": self.api_key,
            "regions": "us,eu",
            "markets": "h2h",
            "oddsFormat": "decimal",
        }
        try:
            resp = requests.get(url, params=params, timeout=5)
            if resp.status_code != 200:
                log.warning("Odds API returned status %d for %s", resp.status_code, sport_key)
                return {}

            data = resp.json()
            results: Dict[str, float] = {}

            for event in data:
                home_team = event.get("home_team", "")
                away_team = event.get("away_team", "")
                bookmakers = event.get("bookmakers", [])

                # Find sharpest bookmaker available
                selected_book = None
                for b in bookmakers:
                    if b.get("key") in SHARP_BOOKMAKERS:
                        selected_book = b
                        break
                if not selected_book and bookmakers:
                    selected_book = bookmakers[0]

                if not selected_book:
                    continue

                markets = selected_book.get("markets", [])
                h2h = next((m for m in markets if m.get("key") == "h2h"), None)
                if not h2h:
                    continue

                outcomes = h2h.get("outcomes", [])
                if len(outcomes) >= 2:
                    o1, o2 = outcomes[0], outcomes[1]
                    p1_name, p1_price = o1.get("name"), o1.get("price", 0)
                    p2_name, p2_price = o2.get("name"), o2.get("price", 0)
                    if p1_price > 1.0 and p2_price > 1.0:
                        prob1, prob2 = self._devig_probabilities(p1_price, p2_price)
                        results[p1_name.lower()] = prob1
                        results[p2_name.lower()] = prob2

            self._cache[sport_key] = (now, results)
            return results

        except Exception as exc:
            log.error("Failed to fetch odds from Odds API: %s", exc)
            return {}

    def get_sharp_edge(self, question: str, polymarket_price: float) -> Optional[float]:
        """
        Match question against sharp sportsbook probabilities across MLB, NFL, NBA, Soccer, and Tennis.
        Returns positive edge if sharp prob > polymarket_price + min_threshold, else None.
        """
        q_lower = question.lower()

        # Determine relevant sport keys dynamically
        sport_keys = []
        if any(w in q_lower for w in ("mlb", "mets", "braves", "yankees", "reds", "dodgers", "cubs", "red sox", "white sox", "baseball", "athletics", "angels", "orioles", "phillies", "guardians", "astros", "mariners", "rangers", "padres", "giants", "diamondbacks", "brewers", "twins")):
            sport_keys.append("baseball_mlb")
        if any(w in q_lower for w in ("nfl", "super bowl", "touchdown", "quarterback", "patriots", "chiefs", "eagles", "cowboys", "49ers", "packers")):
            sport_keys.append("americanfootball_nfl")
        if any(w in q_lower for w in ("nba", "wnba", "basketball", "celtics", "lakers", "warriors", "bucks", "heat")):
            sport_keys.extend(["basketball_nba", "basketball_wnba"])
        if any(w in q_lower for w in ("soccer", "epl", "premier league", "champions league", "mls", "real madrid", "barcelona")):
            sport_keys.extend(["soccer_epl", "soccer_usa_mls"])
        if any(w in q_lower for w in ("tennis", "us open", "wimbledon", "atp", "wta", "djokovic", "alcaraz", "sinner", "swiatek", "gauff")):
            sport_keys.extend(["tennis_atp", "tennis_wta"])

        if not sport_keys and ("vs" in q_lower or "vs." in q_lower):
            sport_keys = ["baseball_mlb", "americanfootball_nfl", "soccer_epl"]

        if not sport_keys:
            return None

        for sk in sport_keys:
            sharp_probs = self.fetch_sharp_probabilities(sk)
            if not sharp_probs:
                continue

            for team, prob in sharp_probs.items():
                if len(team) > 3 and team in q_lower:
                    edge = prob - polymarket_price
                    if edge >= 0.04:  # At least 4% sharp edge
                        log.info("SHARP EDGE DETECTED ⚡ [%s] | %s: Sharp Prob=%.4f vs Poly Ask=%.4f (Edge=%+.2f%%)",
                                 sk, question[:40], prob, polymarket_price, edge * 100)
                        return edge

        return None
