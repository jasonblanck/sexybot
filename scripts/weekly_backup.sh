#!/bin/bash
# weekly_backup.sh — VPS-side weekly backup of sexybot's stateful files.
#
# What it does:
#   1. tar `.env` + `trades.db` from /root/polybot
#   2. gpg-encrypt with AES256 using the passphrase at
#      /root/polybot/.backup-passphrase
#   3. Save the encrypted blob to /var/backups/sexybot/ (rotated, keep 8)
#   4. Mirror the same blob into the private GitHub repo
#      jasonblanck/sexybot-backups (rotated, keep 8 commits worth)
#
# Why both targets:
#   - Local copy on the VPS recovers from bot crashes / DB corruption / .env mistakes — instant retrieval, no decryption needed.
#   - GitHub copy is the offsite leg — survives the VPS being lost.
#     Encrypted at rest, decryptable only with the iCloud-stored
#     passphrase.
#
# Install:
#   cp /root/polybot/scripts/weekly_backup.sh /root/polybot/weekly_backup.sh
#   chmod +x /root/polybot/weekly_backup.sh
#   (crontab -l 2>/dev/null | grep -v weekly_backup;
#    echo "0 5 * * 0 /root/polybot/weekly_backup.sh >> /var/log/sexybot-backup.log 2>&1") | crontab -
#
# Run on demand:
#   /root/polybot/weekly_backup.sh
#
# To restore: see jasonblanck/sexybot-backups/README.md

set -euo pipefail

POLYBOT_DIR="/root/polybot"
LOCAL_BACKUP_DIR="/var/backups/sexybot"
KEEP=8
PASSPHRASE_FILE="${POLYBOT_DIR}/.backup-passphrase"
BACKUP_REPO="jasonblanck/sexybot-backups"

if [ ! -f "${PASSPHRASE_FILE}" ]; then
    echo "$(date -u '+%F %T'): ERROR: passphrase file ${PASSPHRASE_FILE} missing" >&2
    exit 1
fi

TS=$(date -u +%Y%m%d-%H%M)
NAME="sexybot-backup-${TS}.tgz.gpg"
TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

# 1. tar
tar czf "${TMPDIR}/payload.tgz" -C "${POLYBOT_DIR}" .env trades.db

# 2. encrypt
gpg --batch --yes --quiet --symmetric --cipher-algo AES256 \
    --passphrase-file "${PASSPHRASE_FILE}" \
    --output "${TMPDIR}/${NAME}" \
    "${TMPDIR}/payload.tgz"
rm -f "${TMPDIR}/payload.tgz"

# 3. local copy + rotate
mkdir -p "${LOCAL_BACKUP_DIR}"
cp "${TMPDIR}/${NAME}" "${LOCAL_BACKUP_DIR}/"
ls -1t "${LOCAL_BACKUP_DIR}"/sexybot-backup-*.tgz.gpg 2>/dev/null \
    | tail -n +$((KEEP + 1)) | xargs -r rm -f

# 4. GitHub copy. Reuse the GitHub token already configured on the
#    polybot git remote — keeps secrets in one place.
TOKEN=$(grep -oE 'gh[ps]_[A-Za-z0-9]+' "${POLYBOT_DIR}/.git/config" | head -1)
if [ -z "${TOKEN}" ]; then
    echo "$(date -u '+%F %T'): WARN: no GitHub token in ${POLYBOT_DIR}/.git/config — local copy saved, GitHub push skipped" >&2
    echo "$(date -u '+%F %T'): wrote ${LOCAL_BACKUP_DIR}/${NAME} (local only)"
    exit 0
fi

REPO_DIR="${TMPDIR}/sexybot-backups"
git clone --depth 1 --quiet \
    "https://${TOKEN}@github.com/${BACKUP_REPO}.git" "${REPO_DIR}"

cp "${TMPDIR}/${NAME}" "${REPO_DIR}/${NAME}"

cd "${REPO_DIR}"
# Rotate older blobs in the repo too
ls -1t sexybot-backup-*.tgz.gpg 2>/dev/null \
    | tail -n +$((KEEP + 1)) | xargs -r git rm -q -f

git -c user.email=sexybot-backup@noreply.local \
    -c user.name="sexybot backup bot" \
    add -A
# Skip the commit if nothing actually changed (rotation kept the same set)
if git -c user.email=sexybot-backup@noreply.local \
       -c user.name="sexybot backup bot" \
       diff --cached --quiet; then
    echo "$(date -u '+%F %T'): no-op — repo unchanged after rotation"
else
    git -c user.email=sexybot-backup@noreply.local \
        -c user.name="sexybot backup bot" \
        commit -q -m "weekly backup ${TS}"
    git push -q origin main
fi

echo "$(date -u '+%F %T'): backup ${NAME} saved (local + ${BACKUP_REPO})"
