#!/usr/bin/env python3
"""
resolve_brier_backlog.py
Bulk resolve historical predictions in brier_scores and trade records on the VPS database.
Also backfills historical prediction source fields to 'momentum_v2' so that probability
calibration is trained on all historical trades.
"""

import os
import sys
import time
import json
import sqlite3
import urllib.request as urllib_req
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("resolve_brier_backlog")

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "trades.db")

def _gamma_market_outcome(token_id: str) -> int | None:
    url = f"https://gamma-api.polymarket.com/markets?clob_token_ids={token_id}"
    data = None
    
    # Standard request headers
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    
    try:
        req = urllib_req.Request(url, headers=headers)
        with urllib_req.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
    except Exception as e:
        log.warning(f"Urllib Gamma API fetch failed for {token_id[:16]}: {e}")
        # Try a quick curl fallback on the VPS
        try:
            import subprocess
            res = subprocess.run(
                ["curl", "-s", "-m", "10", "-H", f"User-Agent: {headers['User-Agent']}", url],
                capture_output=True, text=True
            )
            if res.returncode == 0 and res.stdout:
                data = json.loads(res.stdout)
        except Exception as e2:
            log.warning(f"Curl fallback also failed for {token_id[:16]}: {e2}")

    if not isinstance(data, list) or not data:
        return None
    m = data[0]
    if not (m.get("closed") or m.get("resolved")):
        return None
    
    prices = m.get("outcomePrices") or m.get("outcome_prices")
    if isinstance(prices, str):
        prices = json.loads(prices)
    ids = m.get("clobTokenIds") or m.get("clob_token_ids")
    if isinstance(ids, str):
        ids = json.loads(ids)
        
    if not prices or not ids or len(prices) < 2 or len(ids) < 2:
        return None
        
    try:
        slot = 0 if str(ids[0]) == str(token_id) else (1 if str(ids[1]) == str(token_id) else None)
        if slot is None:
            return None
        our_p = float(prices[slot])
    except (TypeError, ValueError, IndexError):
        return None
        
    if our_p >= 0.95:
        return 1
    if our_p <= 0.05:
        return 0
    return None

def main():
    db_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_PATH
    log.info(f"Target database: {db_path}")
    
    if not os.path.exists(db_path):
        log.error("Database file does not exist!")
        sys.exit(1)
        
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    
    # 1. Backfill sources
    log.info("Backfilling null/default sources in brier_scores to 'momentum_v2'...")
    cur = conn.execute(
        "UPDATE brier_scores SET source = 'momentum_v2' WHERE source IS NULL OR source = 'default'"
    )
    conn.commit()
    log.info(f"Source backfill complete: {cur.rowcount} row(s) updated.")
    
    # 2. Get unique unresolved tokens from brier_scores
    unresolved_tokens = [
        row[0] for row in conn.execute(
            "SELECT DISTINCT token_id FROM brier_scores WHERE resolved = 0 AND token_id IS NOT NULL AND token_id != ''"
        ).fetchall()
    ]
    log.info(f"Found {len(unresolved_tokens)} unique unresolved token(s) in brier_scores.")
    
    resolved_count = 0
    total_predictions_resolved = 0
    
    for i, token_id in enumerate(unresolved_tokens, 1):
        log.info(f"[{i}/{len(unresolved_tokens)}] Resolving token: {token_id[:16]}...")
        outcome = _gamma_market_outcome(token_id)
        if outcome is not None:
            # Update all predictions for this token_id
            rows = conn.execute(
                "SELECT id, predicted_prob FROM brier_scores WHERE resolved = 0 AND token_id = ?",
                (token_id,)
            ).fetchall()
            
            for row_id, pred_prob in rows:
                try:
                    p = float(pred_prob or 0.0)
                except (TypeError, ValueError):
                    p = 0.5
                brier = (p - outcome) ** 2
                conn.execute(
                    "UPDATE brier_scores SET resolved=1, actual_outcome=?, brier_score=? WHERE id=?",
                    (outcome, round(brier, 6), row_id)
                )
            
            conn.commit()
            resolved_count += 1
            total_predictions_resolved += len(rows)
            log.info(f"Resolved token {token_id[:16]}: outcome={outcome} (settled {len(rows)} predictions)")
        else:
            log.info(f"Token {token_id[:16]} is still active or lookup failed.")
        
        # Rate limit spacing
        time.sleep(0.3)
        
    log.info(f"Resolution batch complete. Resolved {resolved_count} tokens and updated {total_predictions_resolved} predictions in brier_scores.")

    # 3. Get unique unresolved tokens from trades
    unresolved_trades = [
        row[0] for row in conn.execute(
            "SELECT DISTINCT token_id FROM trades WHERE resolved = 0 AND token_id IS NOT NULL AND token_id != ''"
        ).fetchall()
    ]
    log.info(f"Found {len(unresolved_trades)} unique unresolved token(s) in trades.")
    
    trades_resolved_count = 0
    for i, token_id in enumerate(unresolved_trades, 1):
        outcome = _gamma_market_outcome(token_id)
        if outcome is not None:
            # Settle trades
            # Find BUY trade to determine won / realized_pnl
            buy_rows = conn.execute(
                "SELECT id, price, shares FROM trades WHERE token_id = ? AND side = 'BUY'",
                (token_id,)
            ).fetchall()
            
            for buy_id, buy_price, shares in buy_rows:
                # If outcome is 1, YES wins. So payout is $1.00 per share.
                # If outcome is 0, YES loses. So payout is $0.00 per share.
                payout = float(shares) if outcome == 1 else 0.0
                cost = float(buy_price) * float(shares)
                pnl = payout - cost
                won = 1 if outcome == 1 else 0
                conn.execute(
                    "UPDATE trades SET resolved=1, won=?, realized_pnl=?, resolved_at=datetime('now') WHERE id=?",
                    (won, round(pnl, 4), buy_id)
                )
                
            conn.commit()
            trades_resolved_count += 1
            log.info(f"Resolved trades for token {token_id[:16]}: outcome={outcome} (settled {len(buy_rows)} trades)")
        
        time.sleep(0.3)

    log.info(f"Trades resolution complete. Resolved {trades_resolved_count} unique trade markets.")
    conn.close()

if __name__ == "__main__":
    main()
