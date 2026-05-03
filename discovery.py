"""
discovery.py
Polymarket V2 — Market Discovery & Filtering via Gamma API
Collateral: PMUSD only | Sig Type: EOA (0) | Chain: Polygon (137)
"""

from __future__ import annotations

import json
import re
import time
import logging
from dataclasses import dataclass, field
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

GAMMA_BASE      = "https://gamma-api.polymarket.com"
CLOB_BASE       = "https://clob.polymarket.com"
POLYGON_CHAIN   = 137
PAGE_LIMIT      = 100
REQUEST_TIMEOUT = 10
MAX_RETRIES     = 3


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({"Accept": "application/json"})
    return session

_SESSION = _build_session()


@dataclass
class PolyMarket:
    id:           str
    question:     str
    slug:         str
    active:       bool
    closed:       bool
    liquidity:    float
    volume_24h:   float
    yes_token_id: str
    no_token_id:  str
    yes_price:    float
    no_price:     float
    end_date:     str
    description:  str  = ""
    category:     str  = ""
    raw:          dict = field(default_factory=dict, repr=False)

    @property
    def spread(self) -> float:
        return round((1 - self.yes_price - self.no_price) * 100, 4)

    @property
    def is_tradeable(self) -> bool:
        return self.active and not self.closed

    @property
    def mid_price(self) -> float:
        return round(self.yes_price, 4)


def _parse_market(raw: dict) -> Optional[PolyMarket]:
    try:
        raw_ids = raw.get("clobTokenIds", "[]")
        token_ids: list[str] = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        if len(token_ids) < 2:
            return None

        raw_prices = raw.get("outcomePrices", "[0.5, 0.5]")
        prices: list[str] = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
        yes_price = float(prices[0]) if prices else 0.5
        no_price  = float(prices[1]) if len(prices) > 1 else round(1 - yes_price, 4)

        return PolyMarket(
            id           = str(raw.get("id", "")),
            question     = raw.get("question", ""),
            slug         = raw.get("slug", ""),
            active       = bool(raw.get("active", False)),
            closed       = bool(raw.get("closed", True)),
            liquidity    = float(raw.get("liquidity", raw.get("liquidityNum", 0)) or 0),
            volume_24h   = float(raw.get("volume24hr", 0) or 0),
            yes_token_id = token_ids[0],
            no_token_id  = token_ids[1],
            yes_price    = yes_price,
            no_price     = no_price,
            end_date     = raw.get("endDate", raw.get("end_date_iso", "")),
            description  = raw.get("description", ""),
            category     = raw.get("category", ""),
            raw          = raw,
        )
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        log.debug("_parse_market skip — %s", exc)
        return None


def fetch_markets(
    *,
    active:        bool         = True,
    closed:        bool         = False,
    order_by:      str          = "volume24hr",
    ascending:     bool         = False,
    min_liquidity: float        = 5_000.0,
    min_volume:    float        = 500.0,
    limit:         int          = PAGE_LIMIT,
    max_pages:     int          = 5,
    tag_slug:      Optional[str]= None,
) -> list[PolyMarket]:
    results: list[PolyMarket] = []
    offset = 0

    for page in range(max_pages):
        params: dict = {
            "active":    str(active).lower(),
            "closed":    str(closed).lower(),
            "limit":     limit,
            "offset":    offset,
            "order":     order_by,
            "ascending": str(ascending).lower(),
        }
        if tag_slug:
            params["tag_slug"] = tag_slug

        try:
            resp = _SESSION.get(f"{GAMMA_BASE}/markets", params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.error("Gamma /markets request failed (page %d): %s", page, exc)
            break

        batch: list[dict] = resp.json()
        if not batch:
            break

        for raw in batch:
            mkt = _parse_market(raw)
            if mkt is None:
                continue
            if mkt.liquidity < min_liquidity:
                continue
            if mkt.volume_24h < min_volume:
                continue
            if mkt.yes_price <= 0.02 or mkt.yes_price >= 0.98:
                continue
            results.append(mkt)

        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.1)

    log.info("fetch_markets → %d tradeable markets found", len(results))
    return results


def get_market_by_slug(slug: str) -> Optional[PolyMarket]:
    try:
        resp = _SESSION.get(
            f"{GAMMA_BASE}/markets",
            params={"slug": slug, "limit": 1},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        batch = resp.json()
    except requests.RequestException as exc:
        log.error("get_market_by_slug(%s) failed: %s", slug, exc)
        return None

    if not batch:
        return None
    return _parse_market(batch[0])


def get_token_ids_for_slug(slug: str) -> Optional[dict[str, str]]:
    mkt = get_market_by_slug(slug)
    if mkt is None:
        return None
    return {"yes": mkt.yes_token_id, "no": mkt.no_token_id}


class MarketFilter:
    def __init__(self, markets: list[PolyMarket]):
        self._markets = list(markets)

    def min_liquidity(self, usd: float) -> "MarketFilter":
        self._markets = [m for m in self._markets if m.liquidity >= usd]
        return self

    def min_volume(self, usd: float) -> "MarketFilter":
        self._markets = [m for m in self._markets if m.volume_24h >= usd]
        return self

    def max_spread_cents(self, cents: float) -> "MarketFilter":
        self._markets = [m for m in self._markets if m.spread <= cents]
        return self

    def price_range(self, lo: float = 0.05, hi: float = 0.95) -> "MarketFilter":
        self._markets = [m for m in self._markets if lo <= m.yes_price <= hi]
        return self

    def category(self, cat: str) -> "MarketFilter":
        cat_lower = cat.lower()
        self._markets = [m for m in self._markets if cat_lower in m.category.lower()]
        return self

    def exclude_categories(self, cats: list[str]) -> "MarketFilter":
        """Drop markets whose category contains any of the given substrings.
        Case-insensitive. Used to skip structurally unfavorable segments
        (e.g. short-horizon crypto price targets) identified by backtests."""
        lowered = [c.lower() for c in cats if c]
        if not lowered:
            return self
        self._markets = [
            m for m in self._markets
            if not any(c in (m.category or "").lower() for c in lowered)
        ]
        return self

    def exclude_keywords(self, keywords: list[str]) -> "MarketFilter":
        """Drop markets whose question/slug contains any of the given keywords
        as whole words (case-insensitive). Polymarket's `category` field is
        unreliable — many political markets come back as "other" — so a
        keyword filter on the question text is the only way to actually skip
        a topic. Word-boundary matching is critical: a plain substring match
        on "iran" would also catch "tyrannical", "Beirut", "Aquarian", etc.
        and silently filter legitimate markets."""
        valid = [k for k in keywords if k]
        if not valid:
            return self
        pattern = re.compile(
            r"\b(?:" + "|".join(re.escape(k) for k in valid) + r")\b",
            flags=re.IGNORECASE,
        )
        self._markets = [
            m for m in self._markets
            if not pattern.search(m.question or "")
            and not pattern.search(m.slug or "")
        ]
        return self

    def keyword(self, kw: str) -> "MarketFilter":
        kw_lower = kw.lower()
        self._markets = [
            m for m in self._markets
            if kw_lower in m.question.lower() or kw_lower in m.slug.lower()
        ]
        return self

    def top(self, n: int, key: str = "volume_24h") -> "MarketFilter":
        self._markets = sorted(
            self._markets, key=lambda m: getattr(m, key, 0), reverse=True
        )[:n]
        return self

    def results(self) -> list[PolyMarket]:
        return self._markets

    def __len__(self) -> int:
        return len(self._markets)


def print_market_summary(markets: list[PolyMarket], top_n: int = 10) -> None:
    header = f"{'Question':<55} {'YES':>5} {'Liq($k)':>8} {'Vol24h($k)':>10} {'Spread(c)':>9}"
    print(header)
    print("─" * len(header))
    for m in markets[:top_n]:
        print(
            f"{m.question[:54]:<55} "
            f"{m.yes_price:>5.3f} "
            f"{m.liquidity/1000:>8.1f} "
            f"{m.volume_24h/1000:>10.1f} "
            f"{m.spread:>9.2f}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    all_markets = fetch_markets(min_liquidity=10_000, min_volume=1_000, max_pages=2)
    print_market_summary(all_markets, top_n=10)
