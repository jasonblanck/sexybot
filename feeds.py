"""
feeds.py
Free / no-auth external data feeds — pure ingest sinks.

Every public function here is a pure fetcher: it makes one HTTP call, parses
the response, and returns a list[dict] ready for `observability.record_external_feeds`.
None of these touch the trading process, the wallet, or place orders.

Design rules:
- No API keys, no auth, no signups.
- One HTTP call per fetcher (timeouts at 10s) — if it 4xx/5xx/times-out we
  log a warning and return [] so a single source going down never kills
  the whole pass.
- Returns a list of dicts with keys: source, category, external_id, title,
  url, numeric_value, metadata, published_at. The schema is identical
  for all sources so the writer doesn't need source-specific code.
- Dedup is the caller's problem via the (source, external_id) UNIQUE index.

To add a new feed: add a function, append it to FEEDS at the bottom.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Callable
from xml.etree import ElementTree as ET

import requests

log = logging.getLogger(__name__)

HTTP_TIMEOUT = 10
DEFAULT_HEADERS = {
    # Some endpoints (Reddit, NWS, some news RSS) gate on User-Agent and
    # return 403/429 to default python-requests. A descriptive UA also
    # makes it easy for operators on the other side to ban us cleanly if
    # we ever misbehave.
    "User-Agent": "sexybot-feeds/1.0 (+research/observability; trades.db sink)",
    "Accept": "application/json,application/xml,text/xml,*/*;q=0.5",
}


def _get_json(url: str, *, params: dict | None = None,
              headers: dict | None = None, timeout: int = HTTP_TIMEOUT):
    h = {**DEFAULT_HEADERS, **(headers or {})}
    resp = requests.get(url, params=params, headers=h, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _get_text(url: str, *, params: dict | None = None,
              headers: dict | None = None, timeout: int = HTTP_TIMEOUT) -> str:
    h = {**DEFAULT_HEADERS, **(headers or {})}
    resp = requests.get(url, params=params, headers=h, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _safe(fn: Callable, label: str) -> list[dict]:
    """Run a fetcher, catch everything, return [] on failure. The poller
    runs every 10 min so a transient failure resolves itself by next pass;
    a chronic failure is visible in the log as a steady warning rate."""
    try:
        t0 = time.time()
        rows = fn() or []
        log.info("feeds[%s]: %d rows in %.1fs", label, len(rows), time.time() - t0)
        return rows
    except Exception as exc:
        log.warning("feeds[%s] failed: %s", label, exc)
        return []


# ── RSS helpers ────────────────────────────────────────────────────────────────

def _parse_rss(xml_text: str, source: str, category: str,
               limit: int = 50) -> list[dict]:
    """Minimal RSS 2.0 / Atom parser using stdlib. Robust against feeds
    that mix conventions because every namespace lookup is wrapped in a
    try/except — a single weird item never poisons the rest of the feed."""
    out: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        log.warning("RSS parse error for %s: %s", source, exc)
        return out

    items = root.findall(".//item")                # RSS 2.0
    if not items:
        items = root.findall("{http://www.w3.org/2005/Atom}entry")  # Atom

    for it in items[:limit]:
        def text_of(tag: str) -> str | None:
            # `Element or fallback` is unsafe in stdlib ElementTree — an
            # element with text but no children evaluates as False. Use
            # explicit `is None` checks instead.
            el = it.find(tag)
            if el is None:
                el = it.find(f"{{http://www.w3.org/2005/Atom}}{tag}")
            if el is None or el.text is None:
                return None
            return el.text.strip() or None

        link_text = text_of("link")
        if link_text is None:
            link_el = it.find("{http://www.w3.org/2005/Atom}link")
            if link_el is not None:
                link_text = link_el.get("href")

        guid = text_of("guid") or link_text or text_of("id")
        if not guid:
            continue   # no dedup key → skip

        out.append({
            "source":       source,
            "category":     category,
            "external_id":  guid,
            "title":        text_of("title"),
            "url":          link_text,
            "published_at": text_of("pubDate") or text_of("updated") or text_of("published"),
            "metadata":     None,
        })
    return out


# ── Prediction markets ─────────────────────────────────────────────────────────

def metaculus(limit: int = 100) -> list[dict]:
    """Metaculus open questions, ordered by most recent activity. The
    `community_prediction.full.q2` field is the median community forecast
    — useful for cross-market comparison with Polymarket pricing on the
    same topic. Their WAF now 403s on default python-requests, so we
    spoof a browser UA — purely for read-only public API access."""
    data = _get_json(
        "https://www.metaculus.com/api2/questions/",
        params={"status": "open", "order_by": "-activity", "limit": limit},
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
        },
    )
    rows = []
    for q in data.get("results", []):
        cp = (q.get("community_prediction") or {}).get("full", {})
        median = cp.get("q2")
        rows.append({
            "source":       "metaculus",
            "category":     "prediction_market",
            "external_id":  str(q.get("id")),
            "title":        q.get("title"),
            "url":          f"https://www.metaculus.com{q.get('page_url', '')}",
            "numeric_value": float(median) if median is not None else None,
            "metadata":     json.dumps({
                "categories":      q.get("categories", []),
                "close_time":      q.get("close_time"),
                "predictions":     q.get("number_of_predictions"),
                "publish_time":    q.get("publish_time"),
            }),
            "published_at": q.get("publish_time"),
        })
    return rows


# ── Social / news aggregators ──────────────────────────────────────────────────

def hacker_news_top(n: int = 30) -> list[dict]:
    """Top N HN stories. The Algolia search endpoint returns the full
    story payload in one call instead of the firebaseio (1 + N) pattern
    that took ~30s end-to-end. Same data, ~1s."""
    data = _get_json(
        "https://hn.algolia.com/api/v1/search",
        params={"tags": "front_page", "hitsPerPage": n},
    )
    rows = []
    for h in data.get("hits", []):
        sid = h.get("objectID")
        if not sid:
            continue
        rows.append({
            "source":       "hn_top",
            "category":     "social",
            "external_id":  str(sid),
            "title":        h.get("title"),
            "url":          h.get("url") or f"https://news.ycombinator.com/item?id={sid}",
            "numeric_value": float(h.get("points") or 0),
            "metadata":     json.dumps({
                "by":            h.get("author"),
                "num_comments":  h.get("num_comments"),
            }),
            "published_at": h.get("created_at"),
        })
    return rows


def reddit_hot(subreddit: str, limit: int = 25) -> list[dict]:
    """Reddit hot posts for one subreddit. Requires a non-default User-Agent
    or 429s. The free /hot.json endpoint is good for ~60 req/min per UA."""
    data = _get_json(
        f"https://www.reddit.com/r/{subreddit}/hot.json",
        params={"limit": limit},
    )
    rows = []
    for child in data.get("data", {}).get("children", []):
        p = child.get("data", {})
        rows.append({
            "source":       f"reddit_{subreddit.lower()}",
            "category":     "social",
            "external_id":  p.get("name") or p.get("id"),
            "title":        p.get("title"),
            "url":          f"https://www.reddit.com{p.get('permalink', '')}",
            "numeric_value": float(p.get("score") or 0),
            "metadata":     json.dumps({
                "subreddit":     p.get("subreddit"),
                "num_comments":  p.get("num_comments"),
                "author":        p.get("author"),
                "is_self":       p.get("is_self"),
            }),
            "published_at": None,
        })
    return rows


def gdelt(query: str = "Polymarket OR prediction market OR election OR Fed OR CPI",
          max_records: int = 75) -> list[dict]:
    """GDELT 2.0 article search. ArtList mode returns up to 250 articles.
    Free, no key, but the response format is undocumented enough that
    we parse defensively. Useful as a broad event detector for the
    bot's macro/political market exposure. The endpoint is slow under
    load — give it 30s instead of the default 10s."""
    data = _get_json(
        "https://api.gdeltproject.org/api/v2/doc/doc",
        params={
            "query":     query,
            "mode":      "ArtList",
            "maxrecords": max_records,
            "format":    "json",
            "sort":      "DateDesc",
        },
        timeout=30,
    )
    rows = []
    for art in data.get("articles", []):
        url = art.get("url")
        if not url:
            continue
        rows.append({
            "source":       "gdelt",
            "category":     "news",
            "external_id":  url,
            "title":        art.get("title"),
            "url":          url,
            "metadata":     json.dumps({
                "domain":      art.get("domain"),
                "language":    art.get("language"),
                "sourcecountry": art.get("sourcecountry"),
                "tone":        art.get("tone"),
            }),
            "published_at": art.get("seendate"),
        })
    return rows


# ── News RSS ───────────────────────────────────────────────────────────────────

def news_bbc() -> list[dict]:
    return _parse_rss(_get_text("https://feeds.bbci.co.uk/news/world/rss.xml"),
                      "news_bbc", "news")


def news_nyt() -> list[dict]:
    return _parse_rss(_get_text("https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml"),
                      "news_nyt", "news")


def news_reuters() -> list[dict]:
    # Reuters' own RSS endpoints have been flaky since 2024 — Google News
    # site-search is a more reliable mirror that requires no auth.
    return _parse_rss(
        _get_text("https://news.google.com/rss/search",
                  params={"q": "site:reuters.com when:1h"}),
        "news_reuters", "news",
    )


def news_politico() -> list[dict]:
    return _parse_rss(_get_text("https://rss.politico.com/politics-news.xml"),
                      "news_politico", "news")


def news_google_topstories() -> list[dict]:
    return _parse_rss(_get_text("https://news.google.com/rss"),
                      "news_google", "news")


# ── Weather / disaster ─────────────────────────────────────────────────────────

def nws_alerts() -> list[dict]:
    """Active NWS alerts (US). Filtered to severe/extreme so we don't
    drown in routine advisories. Hurricane / flood / heat-wave events
    are the ones that move Polymarket weather markets."""
    data = _get_json(
        "https://api.weather.gov/alerts/active",
        params={"severity": "Severe,Extreme"},
        headers={"User-Agent": DEFAULT_HEADERS["User-Agent"]},
    )
    rows = []
    for f in data.get("features", []):
        props = f.get("properties", {})
        rows.append({
            "source":       "nws_alerts",
            "category":     "weather",
            "external_id":  props.get("id") or f.get("id"),
            "title":        props.get("headline") or props.get("event"),
            "url":          props.get("@id"),
            "metadata":     json.dumps({
                "event":     props.get("event"),
                "severity":  props.get("severity"),
                "urgency":   props.get("urgency"),
                "areaDesc":  props.get("areaDesc"),
                "effective": props.get("effective"),
                "expires":   props.get("expires"),
            }),
            "published_at": props.get("sent"),
        })
    return rows


def usgs_quakes(period: str = "all_day", min_magnitude: float = 4.0) -> list[dict]:
    """Significant earthquakes in the period. `all_day` is a rolling 24h
    window. Magnitude filter trims the long tail of M2 events that don't
    matter for prediction markets."""
    data = _get_json(
        f"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{period}.geojson"
    )
    rows = []
    for f in data.get("features", []):
        props = f.get("properties", {})
        mag = props.get("mag")
        if mag is None or mag < min_magnitude:
            continue
        rows.append({
            "source":       "usgs_quakes",
            "category":     "disaster",
            "external_id":  f.get("id"),
            "title":        props.get("title") or props.get("place"),
            "url":          props.get("url"),
            "numeric_value": float(mag),
            "metadata":     json.dumps({
                "place":   props.get("place"),
                "tsunami": props.get("tsunami"),
                "alert":   props.get("alert"),
                "type":    props.get("type"),
                "felt":    props.get("felt"),
            }),
            "published_at": props.get("time"),
        })
    return rows


def nhc_storms() -> list[dict]:
    """National Hurricane Center current active storms (Atlantic + Pacific).
    Empty list outside hurricane season — that's the expected normal."""
    data = _get_json("https://www.nhc.noaa.gov/CurrentStorms.json")
    rows = []
    for storm in data.get("activeStorms", []):
        sid = storm.get("id")
        if not sid:
            continue
        rows.append({
            "source":       "nhc_storms",
            "category":     "weather",
            "external_id":  sid,
            "title":        f"{storm.get('classification')} {storm.get('name')}",
            "url":          (storm.get("publicAdvisory") or {}).get("url"),
            "numeric_value": float(storm.get("intensity") or 0),
            "metadata":     json.dumps({
                "binNumber":      storm.get("binNumber"),
                "classification": storm.get("classification"),
                "wallet":         storm.get("wallet"),
                "lastUpdate":     storm.get("lastUpdate"),
            }),
            "published_at": storm.get("lastUpdate"),
        })
    return rows


# ── Sports ─────────────────────────────────────────────────────────────────────

def _espn_scoreboard(sport: str, league: str, source_label: str) -> list[dict]:
    data = _get_json(
        f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard"
    )
    rows = []
    for ev in data.get("events", []):
        rows.append({
            "source":       source_label,
            "category":     "sports",
            "external_id":  str(ev.get("id")),
            "title":        ev.get("shortName") or ev.get("name"),
            "url":          ev.get("links", [{}])[0].get("href"),
            "metadata":     json.dumps({
                "status": (ev.get("status") or {}).get("type", {}).get("description"),
                "competitors": [
                    {"abbr": c.get("team", {}).get("abbreviation"),
                     "score": c.get("score")}
                    for c in (ev.get("competitions", [{}])[0].get("competitors", []))
                ],
                "date":   ev.get("date"),
            }),
            "published_at": ev.get("date"),
        })
    return rows


def espn_nfl() -> list[dict]:    return _espn_scoreboard("football", "nfl", "espn_nfl")
def espn_nba() -> list[dict]:    return _espn_scoreboard("basketball", "nba", "espn_nba")
def espn_mlb() -> list[dict]:    return _espn_scoreboard("baseball", "mlb", "espn_mlb")
def espn_nhl() -> list[dict]:    return _espn_scoreboard("hockey", "nhl", "espn_nhl")
def espn_soccer_eng() -> list[dict]: return _espn_scoreboard("soccer", "eng.1", "espn_epl")
def espn_soccer_uefa() -> list[dict]: return _espn_scoreboard("soccer", "uefa.champions", "espn_ucl")


def mlb_schedule() -> list[dict]:
    """MLB official stats API — has more depth than ESPN (probable pitchers,
    weather, full box once games end). Use as a richer secondary source."""
    data = _get_json("https://statsapi.mlb.com/api/v1/schedule",
                     params={"sportId": 1})
    rows = []
    for d in data.get("dates", []):
        for g in d.get("games", []):
            rows.append({
                "source":       "mlb_schedule",
                "category":     "sports",
                "external_id":  str(g.get("gamePk")),
                "title":        f"{(g.get('teams') or {}).get('away', {}).get('team', {}).get('name')} @ "
                                f"{(g.get('teams') or {}).get('home', {}).get('team', {}).get('name')}",
                "url":          f"https://www.mlb.com/gameday/{g.get('gamePk')}/",
                "metadata":     json.dumps({
                    "status": (g.get("status") or {}).get("detailedState"),
                    "venue":  (g.get("venue") or {}).get("name"),
                }),
                "published_at": g.get("gameDate"),
            })
    return rows


# ── Crypto ─────────────────────────────────────────────────────────────────────

def coingecko_top(per_page: int = 30) -> list[dict]:
    """Top N coins by market cap. CoinGecko free tier allows ~30 req/min,
    so a single 10-min cron pass is well under the limit. The free tier
    has occasionally added auth requirements — fall back gracefully on 401."""
    data = _get_json(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd",
            "order":       "market_cap_desc",
            "per_page":    per_page,
            "page":        1,
            "sparkline":   "false",
        },
    )
    rows = []
    for c in data:
        rows.append({
            "source":       "coingecko",
            "category":     "crypto",
            # Include the timestamp in the dedup key so a 10-min cron writes
            # one fresh row per coin per pass — otherwise INSERT OR IGNORE
            # would silently drop every snapshot after the first.
            "external_id":  f"{c.get('id')}@{int(time.time() // 600)}",
            "title":        f"{c.get('name')} ({c.get('symbol','').upper()})",
            "url":          f"https://www.coingecko.com/en/coins/{c.get('id')}",
            "numeric_value": float(c.get("current_price") or 0),
            "metadata":     json.dumps({
                "id":             c.get("id"),
                "symbol":         c.get("symbol"),
                "market_cap":     c.get("market_cap"),
                "volume_24h":     c.get("total_volume"),
                "change_24h_pct": c.get("price_change_percentage_24h"),
            }),
            "published_at": c.get("last_updated"),
        })
    return rows


def binance_ticker(symbols: tuple[str, ...] = ("BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT")) -> list[dict]:
    """24h ticker for selected pairs. Free, no auth. We restrict to a few
    headline pairs so the row count stays small and proportional to signal."""
    data = _get_json("https://api.binance.com/api/v3/ticker/24hr")
    wanted = set(symbols)
    rows = []
    for t in data:
        sym = t.get("symbol")
        if sym not in wanted:
            continue
        rows.append({
            "source":       "binance_ticker",
            "category":     "crypto",
            "external_id":  f"{sym}@{int(time.time() // 600)}",
            "title":        sym,
            "url":          f"https://www.binance.com/en/trade/{sym}",
            "numeric_value": float(t.get("lastPrice") or 0),
            "metadata":     json.dumps({
                "priceChangePercent": t.get("priceChangePercent"),
                "highPrice":          t.get("highPrice"),
                "lowPrice":           t.get("lowPrice"),
                "quoteVolume":        t.get("quoteVolume"),
            }),
            "published_at": None,
        })
    return rows


def defillama_tvl() -> list[dict]:
    """Top protocols by TVL. The /protocols endpoint returns ~3000 protocols
    which is too many — we cap to top 50 by current TVL."""
    data = _get_json("https://api.llama.fi/protocols")
    data.sort(key=lambda p: p.get("tvl") or 0, reverse=True)
    rows = []
    for p in data[:50]:
        rows.append({
            "source":       "defillama_tvl",
            "category":     "crypto",
            "external_id":  f"{p.get('slug')}@{int(time.time() // 600)}",
            "title":        p.get("name"),
            "url":          p.get("url"),
            "numeric_value": float(p.get("tvl") or 0),
            "metadata":     json.dumps({
                "slug":     p.get("slug"),
                "symbol":   p.get("symbol"),
                "category": p.get("category"),
                "chain":    p.get("chain"),
                "change_1d": p.get("change_1d"),
                "change_7d": p.get("change_7d"),
            }),
            "published_at": None,
        })
    return rows


def mempool_fees() -> list[dict]:
    """BTC mempool fee + price snapshot. One row per pass."""
    fees = _get_json("https://mempool.space/api/v1/fees/recommended")
    try:
        prices = _get_json("https://mempool.space/api/v1/prices")
    except Exception:
        prices = {}
    return [{
        "source":       "mempool_fees",
        "category":     "crypto",
        "external_id":  f"snapshot@{int(time.time() // 600)}",
        "title":        f"BTC ${prices.get('USD','?')} | fast {fees.get('fastestFee')} sat/vB",
        "url":          "https://mempool.space",
        "numeric_value": float(prices.get("USD") or 0),
        "metadata":     json.dumps({"fees": fees, "prices": prices}),
        "published_at": None,
    }]


# ── Bluesky public search ──────────────────────────────────────────────────────

def _bluesky_search(query: str, source_label: str, limit: int = 25) -> list[dict]:
    """Public Bluesky search — no auth, no rate-limit headers documented but
    light usage (a handful of queries every 10 min) has never tripped throttling.
    Useful as a Reddit/Twitter substitute now that Twitter's free API is gone
    and Reddit's JSON endpoint 403s from data-centre IPs."""
    data = _get_json(
        "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts",
        params={"q": query, "limit": limit, "sort": "latest"},
    )
    rows = []
    for p in data.get("posts", []):
        uri = p.get("uri")
        if not uri:
            continue
        record = p.get("record") or {}
        author = p.get("author") or {}
        rows.append({
            "source":       source_label,
            "category":     "social",
            "external_id":  uri,
            "title":        (record.get("text") or "")[:280],
            "url":          f"https://bsky.app/profile/{author.get('handle')}/post/{uri.split('/')[-1]}",
            "numeric_value": float(p.get("likeCount") or 0),
            "metadata":     json.dumps({
                "handle":       author.get("handle"),
                "displayName":  author.get("displayName"),
                "replyCount":   p.get("replyCount"),
                "repostCount":  p.get("repostCount"),
                "indexedAt":    p.get("indexedAt"),
            }),
            "published_at": record.get("createdAt"),
        })
    return rows


def bluesky_polymarket() -> list[dict]:
    return _bluesky_search("polymarket", "bluesky_polymarket")


def bluesky_prediction() -> list[dict]:
    return _bluesky_search("prediction market", "bluesky_prediction")


def bluesky_macro() -> list[dict]:
    return _bluesky_search("FOMC OR \"rate cut\" OR \"rate hike\" OR \"CPI\"", "bluesky_macro")


def bluesky_elections() -> list[dict]:
    return _bluesky_search("election OR primary OR poll", "bluesky_elections")


# ── Reddit RSS fallback (Cloudflare blocks the JSON endpoints from VPS) ────────

def reddit_rss(subreddit: str, limit: int = 25) -> list[dict]:
    """Try the .rss endpoint instead of /hot.json. Cloudflare's bot-management
    layer treats these differently — JSON is blocked from data-centre IPs,
    RSS sometimes isn't. Use as a fallback when reddit_hot() is 403'd."""
    xml = _get_text(f"https://www.reddit.com/r/{subreddit}/hot/.rss")
    rows = _parse_rss(xml, f"reddit_rss_{subreddit.lower()}", "social", limit=limit)
    return rows


# ── Macro / fixed income ───────────────────────────────────────────────────────

def treasury_avg_interest_rates() -> list[dict]:
    """Treasury fiscaldata — avg interest on outstanding marketable debt by
    security type. Slow-moving but a useful background macro signal."""
    data = _get_json(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
        "/v2/accounting/od/avg_interest_rates",
        params={"sort": "-record_date", "page[size]": 25},
    )
    rows = []
    for r in data.get("data", []):
        sec = r.get("security_desc") or "?"
        date = r.get("record_date") or ""
        rows.append({
            "source":       "treasury_rates",
            "category":     "macro",
            "external_id":  f"{date}|{sec}",
            "title":        f"{sec} avg rate {r.get('avg_interest_rate_amt')}% ({date})",
            "url":          "https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/",
            "numeric_value": float(r.get("avg_interest_rate_amt") or 0),
            "metadata":     json.dumps(r),
            "published_at": date,
        })
    return rows


def world_bank_indicator(country: str, indicator: str, label: str) -> list[dict]:
    """One indicator for one country from the World Bank. The API returns a
    paginated array; we only take the latest non-null observation."""
    data = _get_json(
        f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}",
        params={"format": "json", "per_page": 5},
    )
    if not isinstance(data, list) or len(data) < 2:
        return []
    rows = []
    for r in (data[1] or []):
        val = r.get("value")
        if val is None:
            continue
        date = r.get("date") or ""
        rows.append({
            "source":       f"worldbank_{label}",
            "category":     "macro",
            "external_id":  f"{country}|{indicator}|{date}",
            "title":        f"{(r.get('country') or {}).get('value')} {(r.get('indicator') or {}).get('value')} {date}",
            "url":          f"https://data.worldbank.org/indicator/{indicator}",
            "numeric_value": float(val),
            "metadata":     json.dumps({"country": country, "indicator": indicator}),
            "published_at": date,
        })
        break   # only the most recent observation
    return rows


def worldbank_us_cpi() -> list[dict]:
    return world_bank_indicator("USA", "FP.CPI.TOTL.ZG", "us_cpi")


def worldbank_us_gdp_growth() -> list[dict]:
    return world_bank_indicator("USA", "NY.GDP.MKTP.KD.ZG", "us_gdp_growth")


def worldbank_world_inflation() -> list[dict]:
    return world_bank_indicator("WLD", "FP.CPI.TOTL.ZG", "world_inflation")


_FRED_SERIES = [
    # (series_id, friendly_name, "monthly" | "daily" | "weekly")
    ("FEDFUNDS",  "Federal Funds Rate (effective, monthly)",      "monthly"),
    ("DFF",       "Federal Funds Rate (daily)",                   "daily"),
    ("CPIAUCSL",  "CPI All Urban Consumers (SA, monthly)",        "monthly"),
    ("CPILFESL",  "Core CPI (SA, monthly)",                       "monthly"),
    ("UNRATE",    "Unemployment Rate (monthly)",                  "monthly"),
    ("PAYEMS",    "Nonfarm Payrolls (thousands, monthly)",        "monthly"),
    ("ICSA",      "Initial Jobless Claims (weekly)",              "weekly"),
    ("VIXCLS",    "VIX (CBOE Volatility Index, daily)",           "daily"),
    ("DGS10",     "10-Year Treasury Yield (daily)",               "daily"),
    ("DGS2",      "2-Year Treasury Yield (daily)",                "daily"),
    ("T10Y2Y",    "10Y minus 2Y Treasury spread (daily)",         "daily"),
    ("T10Y3M",    "10Y minus 3M Treasury spread (daily)",         "daily"),
    ("M2SL",      "M2 Money Stock (SA, billions, monthly)",       "monthly"),
    ("DCOILWTICO", "WTI Crude Oil spot price (daily)",            "daily"),
    ("GOLDPMGBD228NLBM", "Gold price PM London fixing (daily)",   "daily"),
]


def _fred_one(series_id: str, label: str, api_key: str) -> list[dict]:
    """Fetch the latest non-null observation for one FRED series."""
    data = _get_json(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id":  series_id,
            "api_key":    api_key,
            "sort_order": "desc",
            "limit":      5,           # tolerate one or two NaN tails
            "file_type":  "json",
        },
        timeout=15,
    )
    for obs in data.get("observations", []):
        val = obs.get("value")
        date = obs.get("date")
        if val in (None, "", "."):
            continue
        try:
            num = float(val)
        except ValueError:
            continue
        return [{
            "source":       f"fred_{series_id.lower()}",
            "category":     "macro",
            # Dedupe key is (series, observation_date) — FRED revises the
            # latest value occasionally; INSERT OR IGNORE drops the dupe.
            "external_id":  f"FRED|{series_id}|{date}",
            "title":        f"{label}: {num} ({date})",
            "url":          f"https://fred.stlouisfed.org/series/{series_id}",
            "numeric_value": num,
            "metadata":     json.dumps({"series_id": series_id, "obs_date": date}),
            "published_at": date,
        }]
    return []


def fred_macro_series() -> list[dict]:
    """Fetch the latest observation for each entry in _FRED_SERIES.

    FRED's free API allows 120 req/min with no daily cap. 15 series at
    10-min cron interval is ~90 req/hour — well under any limit. Returns
    [] silently if FRED_API_KEY isn't set (e.g. local dev without secret)."""
    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        log.debug("FRED_API_KEY not set; skipping FRED fetch")
        return []
    rows: list[dict] = []
    for series_id, label, _cadence in _FRED_SERIES:
        try:
            rows.extend(_fred_one(series_id, label, api_key))
        except Exception as exc:
            log.debug("FRED %s fetch failed: %s", series_id, exc)
    return rows


_BLS_SERIES = [
    # (series_id, friendly_name)
    # CPI / inflation
    ("CUUR0000SA0",     "CPI-U All Items (NSA, monthly)"),
    ("CUUR0000SA0L1E",  "Core CPI-U less food & energy (NSA, monthly)"),
    # Employment situation report
    ("LNS14000000",     "Unemployment Rate (SA, monthly)"),
    ("CES0000000001",   "Total Nonfarm Payrolls (SA, thousands, monthly)"),
    ("CES0500000003",   "Avg Hourly Earnings, total private (SA, monthly)"),
    # Producer prices
    ("WPSFD4",          "PPI Final Demand (SA, monthly)"),
]


def bls_releases() -> list[dict]:
    """Fetch the latest data point for each BLS series in _BLS_SERIES.

    BLS API v2 takes one POST with all series in the body — 1 HTTP call
    covers everything, well under the 500 req/day registered-key limit.
    Returns [] silently if BLS_API_KEY isn't set. The edge over FRED's
    mirrors of the same series is timing: BLS publishes at 8:30am ET on
    release day; FRED imports within minutes-to-hours."""
    api_key = os.getenv("BLS_API_KEY", "").strip()
    if not api_key:
        log.debug("BLS_API_KEY not set; skipping BLS fetch")
        return []
    from datetime import datetime
    year = datetime.utcnow().year
    body = {
        "seriesid":        [s for s, _ in _BLS_SERIES],
        "startyear":       str(year - 1),
        "endyear":         str(year),
        "registrationkey": api_key,
    }
    resp = requests.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        json=body,
        headers={**DEFAULT_HEADERS, "Content-Type": "application/json"},
        timeout=20,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("status") != "REQUEST_SUCCEEDED":
        log.warning("BLS request not OK: %s", payload.get("status"))
        return []
    label_by_id = dict(_BLS_SERIES)
    rows: list[dict] = []
    for ser in payload.get("Results", {}).get("series", []):
        sid = ser.get("seriesID", "")
        data = ser.get("data", [])
        if not data:
            continue
        latest = data[0]
        try:
            num = float(latest.get("value"))
        except (TypeError, ValueError):
            continue
        period_label = f"{latest.get('year','')}-{latest.get('period','')}"
        rows.append({
            "source":        f"bls_{sid.lower()}",
            "category":      "macro",
            "external_id":   f"BLS|{sid}|{period_label}",
            "title":         f"{label_by_id.get(sid, sid)}: {num} ({period_label})",
            "url":           f"https://data.bls.gov/timeseries/{sid}",
            "numeric_value": num,
            "metadata":      json.dumps({"series_id": sid, "period": period_label}),
            "published_at": f"{latest.get('year','')}-{(latest.get('period','M00')[1:] or '01')}-01",
        })
    return rows


def etherscan_chain() -> list[dict]:
    """Etherscan V2 multi-chain API — gas oracle + ETH price (chain id 1 = mainnet).

    Two calls per pass. Free tier is 5 calls/sec, 100k/day — we're well
    under. No-op if ETHERSCAN_API_KEY isn't set. Useful for prediction
    markets on ETH price thresholds and gas-fee regimes."""
    api_key = os.getenv("ETHERSCAN_API_KEY", "").strip()
    if not api_key:
        log.debug("ETHERSCAN_API_KEY not set; skipping Etherscan fetch")
        return []
    rows: list[dict] = []
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    # Gas oracle — safe/propose/fast gwei + suggested base fee.
    try:
        data = _get_json(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": 1,
                "module":  "gastracker",
                "action":  "gasoracle",
                "apikey":  api_key,
            },
        )
        res = data.get("result") or {}
        if isinstance(res, dict) and res.get("ProposeGasPrice"):
            try:
                propose = float(res.get("ProposeGasPrice"))
            except (TypeError, ValueError):
                propose = None
            if propose is not None:
                rows.append({
                    "source":        "etherscan_gas",
                    "category":      "crypto",
                    "external_id":   f"ETHERSCAN|gas|{now_iso[:16]}",
                    "title":         (
                        f"ETH gas (gwei): safe={res.get('SafeGasPrice')} "
                        f"propose={res.get('ProposeGasPrice')} fast={res.get('FastGasPrice')} "
                        f"baseFee={res.get('suggestBaseFee')}"
                    ),
                    "url":           "https://etherscan.io/gastracker",
                    "numeric_value": propose,
                    "metadata":      json.dumps(res),
                    "published_at": now_iso,
                })
    except Exception as exc:
        log.debug("Etherscan gas fetch failed: %s", exc)
    # ETH price — USD + BTC, with the timestamp Etherscan attaches.
    try:
        data = _get_json(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": 1,
                "module":  "stats",
                "action":  "ethprice",
                "apikey":  api_key,
            },
        )
        res = data.get("result") or {}
        if isinstance(res, dict) and res.get("ethusd"):
            try:
                usd = float(res.get("ethusd"))
            except (TypeError, ValueError):
                usd = None
            ts = res.get("ethusd_timestamp", "")
            if usd is not None:
                rows.append({
                    "source":        "etherscan_ethprice",
                    "category":      "crypto",
                    "external_id":   f"ETHERSCAN|ethprice|{ts}",
                    "title":         f"ETH price: ${usd} USD / {res.get('ethbtc')} BTC",
                    "url":           "https://etherscan.io/chart/etherprice",
                    "numeric_value": usd,
                    "metadata":      json.dumps(res),
                    "published_at": now_iso,
                })
    except Exception as exc:
        log.debug("Etherscan ethprice fetch failed: %s", exc)
    return rows


def ecb_eurusd() -> list[dict]:
    """ECB SDMX endpoint — last 10 EUR/USD daily observations. The SDMX
    JSON format is wordy; we extract just the observation series."""
    data = _get_json(
        "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A",
        params={"lastNObservations": 10, "format": "jsondata"},
        headers={"Accept": "application/json"},
    )
    try:
        obs = data["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]
        times = [t["id"] for t in data["structure"]["dimensions"]["observation"][0]["values"]]
    except (KeyError, IndexError, TypeError):
        return []
    rows = []
    for idx, vals in obs.items():
        try:
            i = int(idx)
            d = times[i]
            v = float(vals[0])
        except (ValueError, IndexError, TypeError):
            continue
        rows.append({
            "source":       "ecb_eurusd",
            "category":     "macro",
            "external_id":  f"EURUSD|{d}",
            "title":        f"EUR/USD {v} ({d})",
            "url":          "https://data.ecb.europa.eu/",
            "numeric_value": v,
            "metadata":     None,
            "published_at": d,
        })
    return rows


# ── Politics RSS (no auth) ─────────────────────────────────────────────────────

def congress_most_viewed_bills() -> list[dict]:
    return _parse_rss(
        _get_text("https://www.congress.gov/rss/most-viewed-bills.xml"),
        "congress_bills", "politics",
    )


def congress_most_viewed_laws() -> list[dict]:
    # Was public-laws.xml — 404'd as of 2026-05. presented-to-president.xml
    # is the closest still-published feed (bills sent for signature).
    return _parse_rss(
        _get_text("https://www.congress.gov/rss/presented-to-president.xml"),
        "congress_to_president", "politics",
    )


# ── Geopolitics RSS ────────────────────────────────────────────────────────────

def reliefweb_headlines() -> list[dict]:
    """ReliefWeb's RSS endpoints are now CDN-cached and return 202 with
    empty bodies on first hit. Their v1 JSON API still works freely with
    just an `appname` query parameter (no key, no signup)."""
    data = _get_json(
        "https://api.reliefweb.int/v1/reports",
        params={
            "appname":     "sexybot-feeds",
            "limit":       25,
            "sort[]":      "date.created:desc",
            "fields[include][]": ["title", "url_alias", "date", "primary_country.name"],
        },
    )
    rows = []
    for r in data.get("data", []):
        rid = r.get("id")
        f = r.get("fields") or {}
        if not rid:
            continue
        rows.append({
            "source":       "reliefweb",
            "category":     "geopolitics",
            "external_id":  str(rid),
            "title":        f.get("title"),
            "url":          f.get("url_alias"),
            "metadata":     json.dumps({
                "country": (f.get("primary_country") or {}).get("name"),
            }),
            "published_at": (f.get("date") or {}).get("created"),
        })
    return rows


# ── AP News (RSS) ──────────────────────────────────────────────────────────────

def news_ap() -> list[dict]:
    # AP's official RSS endpoints are partially deprecated; Google News
    # site-search is the most reliable mirror at this point.
    return _parse_rss(
        _get_text("https://news.google.com/rss/search",
                  params={"q": "site:apnews.com when:1h"}),
        "news_ap", "news",
    )


# ── Wikipedia trending ─────────────────────────────────────────────────────────

def wikipedia_top_pageviews() -> list[dict]:
    """Yesterday's top 25 pages on en.wikipedia.org. A massive spike on a
    name or event often precedes a Polymarket market repricing — early-
    awareness signal."""
    from datetime import datetime, timedelta, timezone as _tz
    d = (datetime.now(tz=_tz.utc) - timedelta(days=1)).strftime("%Y/%m/%d")
    data = _get_json(
        f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top"
        f"/en.wikipedia.org/all-access/{d}",
    )
    rows = []
    items = (data.get("items") or [{}])[0].get("articles", [])
    for a in items[:25]:
        article = a.get("article")
        if not article or article.startswith("Special:") or article == "Main_Page":
            continue
        rows.append({
            "source":       "wiki_top",
            "category":     "social",
            "external_id":  f"{d}|{article}",
            "title":        article.replace("_", " "),
            "url":          f"https://en.wikipedia.org/wiki/{article}",
            "numeric_value": float(a.get("views") or 0),
            "metadata":     json.dumps({"rank": a.get("rank")}),
            "published_at": d.replace("/", "-"),
        })
    return rows


# ── Box office (entertainment markets) ─────────────────────────────────────────

def box_office_mojo_weekend() -> list[dict]:
    """Scrape Box Office Mojo's weekend chart. Light HTML parsing with
    regex — BeautifulSoup is not in requirements.txt. Failure modes are
    page layout changes; we log and move on."""
    html = _get_text(
        "https://www.boxofficemojo.com/weekend/chart/",
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
        },
    )
    import re
    rows = []
    # Mojo's table: rows have a release URL + title cell, then a gross cell.
    pattern = re.compile(
        r'/release/(?P<rid>rl\d+)/[^"]*"[^>]*>(?P<title>[^<]+)</a>.*?'
        r'\$(?P<gross>[\d,]+)',
        re.DOTALL,
    )
    seen = set()
    for m in pattern.finditer(html):
        rid = m.group("rid")
        if rid in seen:
            continue
        seen.add(rid)
        try:
            gross = float(m.group("gross").replace(",", ""))
        except ValueError:
            continue
        rows.append({
            "source":       "boxoffice_mojo",
            "category":     "entertainment",
            "external_id":  rid,
            "title":        m.group("title").strip(),
            "url":          f"https://www.boxofficemojo.com/release/{rid}/",
            "numeric_value": gross,
            "metadata":     None,
            "published_at": None,
        })
        if len(rows) >= 20:
            break
    return rows


# ── NBA stats API (needs specific headers or 403s) ─────────────────────────────

def nba_scoreboard() -> list[dict]:
    """NBA stats today's scoreboard. The stats.nba.com endpoints fingerprint
    on Referer/Origin headers; a plain User-Agent isn't enough. They also
    have aggressive rate-limiting that produces silent timeouts under load,
    so we give it 30s instead of the default 10s and accept that some
    passes will fail."""
    from datetime import datetime, timezone as _tz
    today = datetime.now(tz=_tz.utc).strftime("%m/%d/%Y")
    data = _get_json(
        "https://stats.nba.com/stats/scoreboardV2",
        params={"DayOffset": 0, "LeagueID": "00", "gameDate": today},
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            ),
            "Origin":  "https://www.nba.com",
            "Referer": "https://www.nba.com/",
            "x-nba-stats-origin": "stats",
            "x-nba-stats-token":  "true",
        },
        timeout=30,
    )
    # resultSets is a typed table — find the GameHeader set
    rows = []
    for rs in data.get("resultSets", []):
        if rs.get("name") != "GameHeader":
            continue
        headers = rs.get("headers", [])
        for r in rs.get("rowSet", []):
            row = dict(zip(headers, r))
            gid = row.get("GAME_ID")
            if not gid:
                continue
            rows.append({
                "source":       "nba_scoreboard",
                "category":     "sports",
                "external_id":  str(gid),
                "title":        f"{row.get('VISITOR_TEAM_ID')} @ {row.get('HOME_TEAM_ID')} ({row.get('GAMECODE')})",
                "url":          f"https://www.nba.com/game/{gid}",
                "metadata":     json.dumps({
                    "status":   row.get("GAME_STATUS_TEXT"),
                    "gamecode": row.get("GAMECODE"),
                    "season":   row.get("SEASON"),
                }),
                "published_at": row.get("GAME_DATE_EST"),
            })
        break
    return rows


# ── YouTube channel RSS (no auth) ──────────────────────────────────────────────

def _youtube_channel(channel_id: str, source_label: str) -> list[dict]:
    return _parse_rss(
        _get_text(f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"),
        source_label, "social", limit=15,
    )


# Channels picked for breadth: macro/markets, news, sports betting, AI/tech.
# YouTube only exposes the latest 15 videos per channel via RSS so each
# call returns at most 15 rows. Adjust this list if you want more / less
# coverage in any vertical.
def youtube_cnbc() -> list[dict]:
    return _youtube_channel("UCrp_UI8XtuYfpiqluWLD7Lw", "yt_cnbc")          # CNBC Television

def youtube_bloomberg() -> list[dict]:
    return _youtube_channel("UCIALMKvObZNtJ6AmdCLP7Lg", "yt_bloomberg")     # Bloomberg

def youtube_espn() -> list[dict]:
    return _youtube_channel("UCiWLfSweyRNmLpgEHekhoAg", "yt_espn")          # ESPN

def youtube_reuters() -> list[dict]:
    return _youtube_channel("UChqUTb7kYRX8-EiaN3XFrSQ", "yt_reuters")       # Reuters

def youtube_ap() -> list[dict]:
    return _youtube_channel("UC52X5wxOL_s5yw0dQk7NtgA", "yt_ap")            # Associated Press


# ── Registry ───────────────────────────────────────────────────────────────────

# (label, fetcher). The label is what shows up in the log line. Order
# matters only for log readability — there's no dependency between feeds.
FEEDS: list[tuple[str, Callable[[], list[dict]]]] = [
    # Cross-market peers
    ("metaculus",          metaculus),
    # News & social (Reddit JSON 403s from DO IPs but works on operator's Mac)
    ("hn_top",             hacker_news_top),
    ("reddit_sportsbook",  lambda: reddit_hot("sportsbook")),
    ("reddit_wallstreetbets", lambda: reddit_hot("wallstreetbets")),
    ("reddit_politics",    lambda: reddit_hot("politics", limit=15)),
    ("reddit_nba",         lambda: reddit_hot("nba")),
    ("reddit_nfl",         lambda: reddit_hot("nfl")),
    ("reddit_soccer",      lambda: reddit_hot("soccer")),
    # Reddit RSS fallback — may work where JSON is blocked
    ("reddit_rss_politics",    lambda: reddit_rss("politics", limit=15)),
    ("reddit_rss_nba",         lambda: reddit_rss("nba")),
    ("reddit_rss_nfl",         lambda: reddit_rss("nfl")),
    ("reddit_rss_sportsbook",  lambda: reddit_rss("sportsbook")),
    # Bluesky search (Reddit/Twitter substitute)
    ("bluesky_polymarket",  bluesky_polymarket),
    ("bluesky_prediction",  bluesky_prediction),
    ("bluesky_macro",       bluesky_macro),
    ("bluesky_elections",   bluesky_elections),
    # GDELT broad news graph
    ("gdelt",              gdelt),
    # News RSS
    ("news_bbc",           news_bbc),
    ("news_nyt",           news_nyt),
    ("news_reuters",       news_reuters),
    ("news_politico",      news_politico),
    ("news_google",        news_google_topstories),
    ("news_ap",            news_ap),
    # Politics RSS
    ("congress_bills",     congress_most_viewed_bills),
    ("congress_laws",      congress_most_viewed_laws),
    # Geopolitics — ReliefWeb removed; their RSS returns 202 empty and the
    # v1/v2 JSON APIs now require an approved appname (free but requires
    # operator signup). Add it back once an appname is registered.
    # Weather / disaster
    ("nws_alerts",         nws_alerts),
    ("usgs_quakes",        usgs_quakes),
    ("nhc_storms",         nhc_storms),
    # Sports — ESPN scoreboards
    ("espn_nfl",           espn_nfl),
    ("espn_nba",           espn_nba),
    ("espn_mlb",           espn_mlb),
    ("espn_nhl",           espn_nhl),
    ("espn_epl",           espn_soccer_eng),
    ("espn_ucl",           espn_soccer_uefa),
    ("mlb_schedule",       mlb_schedule),
    ("nba_scoreboard",     nba_scoreboard),
    # Crypto
    ("coingecko",          coingecko_top),
    ("binance_ticker",     binance_ticker),
    ("defillama_tvl",      defillama_tvl),
    ("mempool_fees",       mempool_fees),
    # Macro / fixed income
    ("treasury_rates",     treasury_avg_interest_rates),
    ("worldbank_us_cpi",         worldbank_us_cpi),
    ("worldbank_us_gdp_growth",  worldbank_us_gdp_growth),
    ("worldbank_world_inflation", worldbank_world_inflation),
    ("ecb_eurusd",         ecb_eurusd),
    # FRED — Fed funds, CPI, payrolls, yield curve, VIX, M2, WTI, gold.
    # No-op if FRED_API_KEY isn't set in env / .env.
    ("fred_macro",         fred_macro_series),
    # BLS — CPI, core CPI, unemployment, payrolls, AHE, PPI. Faster than
    # FRED on release day. No-op if BLS_API_KEY isn't set.
    ("bls_releases",       bls_releases),
    # Etherscan — gas oracle + ETH price. No-op if ETHERSCAN_API_KEY unset.
    ("etherscan_chain",    etherscan_chain),
    # Trending / entertainment
    ("wikipedia_top",      wikipedia_top_pageviews),
    ("boxoffice_mojo",     box_office_mojo_weekend),
    # YouTube channel RSS
    ("yt_cnbc",            youtube_cnbc),
    ("yt_bloomberg",       youtube_bloomberg),
    ("yt_espn",            youtube_espn),
    ("yt_reuters",         youtube_reuters),
    ("yt_ap",              youtube_ap),
]


def run_all() -> list[dict]:
    """Run every registered fetcher and flatten the results into one list
    ready for record_external_feeds. A failure in one source never affects
    the others — _safe catches everything."""
    out: list[dict] = []
    for label, fn in FEEDS:
        out.extend(_safe(fn, label))
    return out
