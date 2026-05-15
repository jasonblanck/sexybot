#!/usr/bin/env python3
"""
feeds_poll.py
One-shot poller — meant to be invoked by cron.

Calls every feed in feeds.py and writes rows into the external_feeds table.
Exits 0 on success, non-zero only if SQLite is unreachable. A failure in
any individual feed is a logged warning, not an error — see feeds._safe.

Install on the VPS (run as the user owning /root/polybot/trades.db):

    */10 * * * * cd /root/polybot && /root/polybot/venv/bin/python feeds_poll.py >> /var/log/sexybot-feeds.log 2>&1

That's 144 passes/day. Most fetchers return new rows on a fraction of those;
deduping happens at the (source, external_id) UNIQUE index.
"""
from __future__ import annotations

import logging
import sys
import time

from feeds import run_all
from observability import record_external_feeds


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    log = logging.getLogger("feeds_poll")

    t0 = time.time()
    rows = run_all()
    fetch_secs = time.time() - t0

    if not rows:
        log.warning("No feed rows returned this pass (network down? all sources 4xx?)")
        return 0

    t1 = time.time()
    inserted = record_external_feeds(rows)
    write_secs = time.time() - t1

    log.info(
        "feeds_poll done | fetched=%d new=%d fetch=%.1fs write=%.2fs total=%.1fs",
        len(rows), inserted, fetch_secs, write_secs, time.time() - t0,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
