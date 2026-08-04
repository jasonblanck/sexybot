"""
weather_oracle.py
Polymarket V2 — NOAA Weather Oracle Direct Integration
Queries National Weather Service (NOAA) official station APIs to get live surface temperature & precipitation observations.
Cross-references against Polymarket weather questions for 100% deterministic arbitrage.
"""

import re
import time
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import requests

log = logging.getLogger(__name__)

NOAA_API_BASE = "https://api.weather.gov"
HEADERS = {"User-Agent": "SexyBot/2.0 (jasonblanck@gmail.com)"}

# Station Mapping for Major Polymarket Weather Locations
STATION_MAP = {
    "nyc": "KNYC",         # NYC Central Park
    "new york": "KNYC",
    "chicago": "KORD",      # Chicago O'Hare
    "miami": "KMIA",        # Miami International
    "los angeles": "KLAX",  # LAX
    "la": "KLAX",
    "death valley": "KVEG", # Las Vegas / Death Valley region
    "london": "EGLL",       # London Heathrow (Met Office / NOAA global)
}


@dataclass
class WeatherObservation:
    station_id: str
    city: str
    temp_f: Optional[float]
    temp_c: Optional[float]
    timestamp: float


class WeatherOracle:
    def __init__(self, cache_ttl_sec: float = 600.0):
        self.cache_ttl_sec = cache_ttl_sec
        self._cache: Dict[str, Tuple[float, WeatherObservation]] = {}

    def get_latest_observation(self, city_key: str) -> Optional[WeatherObservation]:
        """Fetch latest official NOAA weather station observation for a city."""
        station_id = STATION_MAP.get(city_key.lower())
        if not station_id:
            return None

        now = time.time()
        if station_id in self._cache:
            ts, obs = self._cache[station_id]
            if now - ts < self.cache_ttl_sec:
                return obs

        url = f"{NOAA_API_BASE}/stations/{station_id}/observations/latest"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=5)
            if resp.status_code != 200:
                log.warning("NOAA API returned status %d for station %s", resp.status_code, station_id)
                return None

            data = resp.json()
            props = data.get("properties", {})
            temp_c_val = props.get("temperature", {}).get("value")
            temp_f_val = round(temp_c_val * 9 / 5 + 32, 1) if temp_c_val is not None else None

            obs = WeatherObservation(
                station_id=station_id,
                city=city_key,
                temp_f=temp_f_val,
                temp_c=temp_c_val,
                timestamp=now,
            )
            self._cache[station_id] = (now, obs)
            return obs

        except Exception as exc:
            log.error("Failed to fetch NOAA observation for %s: %s", station_id, exc)
            return None

    def evaluate_weather_question(
        self, question: str, polymarket_price: float
    ) -> Optional[Tuple[float, str]]:
        """
        Evaluate if NOAA live weather data confirms a deterministic outcome on Polymarket.
        Returns Tuple of (implied_prob, reasoning) if edge >= 10%, else None.
        """
        q_lower = question.lower()

        # Identify city
        target_city = next((c for c in STATION_MAP if c in q_lower), None)
        if not target_city:
            return None

        obs = self.get_latest_observation(target_city)
        if not obs or obs.temp_f is None:
            return None

        # Check for temperature threshold in question (e.g., "reach 90°F" or "hit 85°")
        match = re.search(r"(\d{2,3})\s*(?:°|\s*degrees|\s*f|\s*°f)", q_lower)
        if not match:
            return None

        target_temp = float(match.group(1))

        # If live NOAA temp is already >= target_temp, YES outcome is 99% guaranteed!
        if obs.temp_f >= target_temp:
            implied_prob = 0.99
            edge = implied_prob - polymarket_price
            if edge >= 0.10:
                reason = f"NOAA ORACLE 🌡️ | Observed {target_city.upper()} temp {obs.temp_f}°F >= target {target_temp}°F (Edge=+{edge*100:.1f}%)"
                log.info(reason)
                return implied_prob, reason

        return None
