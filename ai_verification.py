import os
import sqlite3
import time
import json
import logging
import requests
from typing import Optional, Tuple
from datetime import datetime

log = logging.getLogger(__name__)

# Load keys
THE_ODDS_API_KEY = os.getenv("THE_ODDS_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")
CLAUDE_FAST_MODEL = os.getenv("CLAUDE_FAST_MODEL", "claude-haiku-4-5")

# Local cache for The Odds API to save free-tier requests
ODDS_CACHE_FILE = "/tmp/oddsapi_cache.json"

class PretradeVerifier:
    def __init__(self, db_path: str = "/root/polybot/trades.db"):
        self.db_path = db_path
        self._odds_cache = self._load_cache()

    def _load_cache(self) -> dict:
        try:
            if os.path.exists(ODDS_CACHE_FILE):
                with open(ODDS_CACHE_FILE, "r") as f:
                    return json.load(f)
        except Exception as e:
            log.debug("Failed to load odds cache: %s", e)
        return {}

    def _save_cache(self):
        try:
            with open(ODDS_CACHE_FILE, "w") as f:
                json.dump(self._odds_cache, f)
        except Exception as e:
            log.debug("Failed to save odds cache: %s", e)

    def _get_sportsbook_odds(self, sport_key: str) -> list:
        if not THE_ODDS_API_KEY:
            return []
        
        now = time.time()
        # 30 minute cache window
        if sport_key in self._odds_cache:
            entry = self._odds_cache[sport_key]
            if now - entry["ts"] < 1800:
                return entry["data"]

        try:
            url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
            params = {
                "apiKey": THE_ODDS_API_KEY,
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "american"
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            self._odds_cache[sport_key] = {"ts": now, "data": data}
            self._save_cache()
            return data
        except Exception as e:
            log.warning("Failed to fetch odds from Odds API for %s: %s", sport_key, e)
            # Fall back to expired cache if available
            if sport_key in self._odds_cache:
                return self._odds_cache[sport_key]["data"]
        return []

    def _match_sport(self, question: str) -> Optional[str]:
        q_lower = question.lower()
        if "nba" in q_lower or "basketball" in q_lower:
            return "basketball_nba"
        if "nfl" in q_lower or "super bowl" in q_lower:
            return "americanfootball_nfl"
        if "mlb" in q_lower or "baseball" in q_lower:
            return "baseball_mlb"
        if "mls" in q_lower or "soccer" in q_lower:
            return "soccer_usa_mls"
        return None

    def _get_db_macro_context(self) -> str:
        context = []
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.execute("PRAGMA busy_timeout=5000")
            cur = conn.cursor()
            # Fetch latest FRED data
            cur.execute('''
                SELECT source, title, numeric_value, published_at 
                FROM external_feeds 
                WHERE source LIKE 'fred_%'
                GROUP BY source 
                HAVING max(published_at)
                ORDER BY published_at DESC 
                LIMIT 10
            ''')
            fred_rows = cur.fetchall()
            for r in fred_rows:
                context.append(f"FRED | {r[1]} ({r[0]}): {r[2]} (published: {r[3]})")

            # Fetch latest BLS data
            cur.execute('''
                SELECT source, title, numeric_value, published_at 
                FROM external_feeds 
                WHERE source LIKE 'bls_%'
                GROUP BY source 
                HAVING max(published_at)
                ORDER BY published_at DESC 
                LIMIT 10
            ''')
            bls_rows = cur.fetchall()
            for r in bls_rows:
                context.append(f"BLS | {r[1]} ({r[0]}): {r[2]} (published: {r[3]})")

            conn.close()
        except Exception as e:
            log.debug("Error fetching db macro context: %s", e)
        return "\n".join(context)

    def _get_db_news_headlines(self) -> str:
        headlines = []
        try:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.execute("PRAGMA busy_timeout=5000")
            cur = conn.cursor()
            cur.execute('''
                SELECT title, published_at, source 
                FROM external_feeds 
                WHERE source = 'newsapi' OR category = 'news'
                ORDER BY published_at DESC 
                LIMIT 15
            ''')
            rows = cur.fetchall()
            for r in rows:
                headlines.append(f"- [{r[2]} / {r[1]}] {r[0]}")
            conn.close()
        except Exception as e:
            log.debug("Error fetching news context: %s", e)
        return "\n".join(headlines)

    async def verify_trade(self, market, trade_side: str, trade_prob: float, book) -> Tuple[bool, str]:
        """
        Query AI (Anthropic/Gemini) to perform pre-trade vetting.
        Returns: (proceed: bool, reason: str)
        """
        if not ANTHROPIC_API_KEY:
            return True, "Pretrade check skipped: ANTHROPIC_API_KEY missing"

        question = market.question
        category = market.category or ""
        
        # 1. Fetch relevant Sportsbook odds if applicable
        sport_key = self._match_sport(question)
        sports_context = ""
        if sport_key:
            odds_data = self._get_sportsbook_odds(sport_key)
            # Find matching events in odds data
            matches = []
            q_words = set(question.lower().replace("?", "").split())
            for ev in odds_data:
                home = ev.get("home_team", "")
                away = ev.get("away_team", "")
                # Simple matching on team names
                if home.lower() in question.lower() or away.lower() in question.lower():
                    bookmakers = ev.get("bookmakers", [])
                    if bookmakers:
                        # Extract first bookmaker's odds
                        bk = bookmakers[0]
                        markets = bk.get("markets", [])
                        if markets:
                            outcomes = markets[0].get("outcomes", [])
                            odds_str = f"{ev['sport_title']} | {home} vs {away}: "
                            odds_str += ", ".join([f"{o['name']}: American odds {o['price']}" for o in outcomes])
                            matches.append(odds_str)
            if matches:
                sports_context = "\n".join(matches)

        # 2. Fetch latest Macro context if macro/economic market
        macro_context = ""
        if "cpi" in question.lower() or "interest rate" in question.lower() or "fed" in question.lower() or "unemployment" in question.lower():
            macro_context = await asyncio.to_thread(self._get_db_macro_context)

        # 3. Fetch latest News context
        news_context = await asyncio.to_thread(self._get_db_news_headlines)

        # 4. Construct Prompt
        prompt = f"""You are the pre-trade risk management assistant for an automated Polymarket trading bot.
The bot has detected a technical momentum/orderbook signal and is proposing the following trade:

MARKET: {question}
PROPOSED DIRECTION: BUY {trade_side}
TECHNICAL ESTIMATED PROBABILITY: {trade_prob * 100:.1f}%
VAMP (Volume-Weighted Midpoint): {book.vamp}
BEST BID: {book.best_bid} | BEST ASK: {book.best_ask} | OBI (Order Book Imbalance): {book.obi}

Here is the latest external context retrieved from data APIs:
--- SPORTSBOOK ODDS CONTEXT (if matching sport event found) ---
{sports_context or "No matching sportsbook odds found."}

--- MACROECONOMIC DATA CONTEXT (if relevant to macro markets) ---
{macro_context or "No relevant macroeconomic data found."}

--- RECENT TOP NEWS HEADLINES ---
{news_context or "No recent news headlines available."}
---

Your task is to double-check this trade against the external news, sports odds, or economic facts.
You MUST block the trade if:
1. The sportsbook odds strongly contradict the bot's prediction (e.g. bookmaker has YES probability at 30% but bot is trying to buy YES at 70%).
2. The news headlines indicate the market has already resolved or an event has occurred that renders the bot's signal invalid (e.g., candidate dropped out, game finished).
3. The trade is highly speculative and news/macro context suggests significant downside.

Reply ONLY with a JSON object in this format:
{{
  "proceed": true/false,
  "confidence": 0-100,
  "reason": "Clear explanation of your choice"
}}"""

        try:
            if not hasattr(self, "_anthropic_client") or self._anthropic_client is None:
                import anthropic
                self._anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            
            import asyncio
            message = await asyncio.to_thread(
                self._anthropic_client.messages.create,
                model=CLAUDE_FAST_MODEL,
                max_tokens=200,
                temperature=0.0,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            # Parse response
            resp_text = message.content[0].text.strip()
            if resp_text.startswith("```json"):
                resp_text = resp_text.replace("```json", "").replace("```", "").strip()
            res = json.loads(resp_text)
            
            proceed = res.get("proceed", True)
            reason = res.get("reason", "No reason provided")
            log.info("AI PRETRADE VERIFIER RESULT | Proceed: %s | Reason: %s", proceed, reason)
            return proceed, reason
        except Exception as e:
            log.error("AI Pretrade verification failed to execute or parse (failing open): %s", e)
            return True, f"Error running check (failed open): {e}"
