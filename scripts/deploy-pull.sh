#!/bin/bash
# VPS-side auto-deploy script. Intended to be run every minute via cron.
#
# Flow:
#   1. Fetch main from GitHub (quiet unless error)
#   2. If HEAD is unchanged, exit silently (no-op)
#   3. If HEAD changed: hard-reset, sync dashboard HTML, reload nginx,
#      restart the sexybot service, log the deploy
#
# No secrets, no SSH key management. The VPS already has git remote auth
# set up (HTTPS token in the remote URL per CLAUDE.md), so `git fetch`
# just works.
#
# Install (one time, on VPS as root):
#
#   cp /root/polybot/scripts/deploy-pull.sh /root/polybot/deploy-pull.sh
#   chmod +x /root/polybot/deploy-pull.sh
#   (crontab -l 2>/dev/null | grep -v deploy-pull;
#    echo "* * * * * /root/polybot/deploy-pull.sh") | crontab -
#
# Logs: /var/log/sexybot-deploy.log

set -u

cd /root/polybot || exit 1

# Fetch latest main; silent on success, error goes to log
git fetch origin main --quiet 2>>/var/log/sexybot-deploy.log

OLD=$(git rev-parse HEAD)
NEW=$(git rev-parse origin/main)

if [ "$OLD" = "$NEW" ]; then
    # Nothing to do.
    exit 0
fi

# Something changed — deploy it.
git reset --hard origin/main >>/var/log/sexybot-deploy.log 2>&1

# Sync dashboard HTML to nginx web root (ok if file doesn't exist)
cp index.html /var/www/html/index.html 2>/dev/null || true

# Reload nginx only if its config still validates
nginx -t >/dev/null 2>&1 && systemctl reload nginx 2>/dev/null || true

# Restart the bot service. If this fails, the bot is down — log loudly.
if ! systemctl restart sexybot; then
    echo "$(date -u '+%F %T'): RESTART FAILED after pull to ${NEW:0:8}" >> /var/log/sexybot-deploy.log
    exit 1
fi

echo "$(date -u '+%F %T'): deployed ${NEW:0:8} from ${OLD:0:8}" >> /var/log/sexybot-deploy.log
