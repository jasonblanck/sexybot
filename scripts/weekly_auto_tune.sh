#!/bin/bash
# weekly_auto_tune.sh — runs scripts/auto_tune.py and Telegrams the result.
#
# Install (run once on the VPS):
#   chmod +x /root/polybot/scripts/weekly_auto_tune.sh
#   (crontab -l 2>/dev/null | grep -v weekly_auto_tune;
#    echo "0 6 * * 1 /root/polybot/scripts/weekly_auto_tune.sh >> /var/log/sexybot-auto-tune.log 2>&1") | crontab -
#
# Runs every Monday 06:00 UTC. Reads TELEGRAM_TOKEN + TELEGRAM_CHAT_ID
# from /root/polybot/.env so we do not duplicate secrets.

set -euo pipefail

POLYBOT=/root/polybot
LOG=/var/log/sexybot-auto-tune.log

# Load TELEGRAM creds from .env. Use grep + cut so we do not source the
# whole file (which has shell-unsafe values like wallet keys).
TG_TOKEN=$(grep '^TELEGRAM_TOKEN=' "$POLYBOT/.env" 2>/dev/null | cut -d= -f2- | head -1)
TG_CHAT=$(grep '^TELEGRAM_CHAT_ID=' "$POLYBOT/.env" 2>/dev/null | cut -d= -f2- | head -1)

OUTPUT=$("$POLYBOT/venv/bin/python3" "$POLYBOT/scripts/auto_tune.py" 2>&1 || true)
TS=$(date -u '+%F %T')

# Local log
echo "$TS -------- weekly auto-tune --------" >> "$LOG"
echo "$OUTPUT" >> "$LOG"

# Telegram (only if creds present)
if [ -n "${TG_TOKEN:-}" ] && [ -n "${TG_CHAT:-}" ]; then
    # Telegram messages cap at 4096 chars. Trim if needed.
    BODY="Weekly auto-tune ($TS UTC)
$(echo "$OUTPUT" | head -c 3800)"
    curl -sS --max-time 10 \
        "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
        --data-urlencode "chat_id=${TG_CHAT}" \
        --data-urlencode "text=${BODY}" \
        > /dev/null || echo "$TS WARN: telegram send failed" >> "$LOG"
fi
