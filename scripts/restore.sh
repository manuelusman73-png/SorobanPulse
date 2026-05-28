#!/usr/bin/env bash
# scripts/restore.sh — Restore the SorobanPulse PostgreSQL database from an encrypted dump.
#
# Usage:
#   ./scripts/restore.sh ./backups/soroban_pulse_20260314T000000Z.dump.gpg
#   ./scripts/restore.sh s3://my-bucket/backups/soroban_pulse_20260314T000000Z.dump.gpg
#
# Environment variables:
#   DATABASE_URL              — PostgreSQL connection string (required)
#   BACKUP_ENCRYPTION_KEY     — GPG symmetric encryption passphrase (required)
#
# WARNING: This will DROP and recreate the target database. All existing data
#          will be permanently lost. Confirm before running in production.
#
# Dependencies: pg_restore, psql, gpg, aws CLI (only for S3 sources)

set -euo pipefail

DATABASE_URL="${DATABASE_URL:?DATABASE_URL must be set}"
BACKUP_ENCRYPTION_KEY="${BACKUP_ENCRYPTION_KEY:?BACKUP_ENCRYPTION_KEY must be set}"
DUMP_SOURCE="${1:?Usage: $0 <dump-file-or-s3-uri>}"
LOCAL_ENCRYPTED="${DUMP_SOURCE}"
LOCAL_DUMP="/tmp/soroban_pulse_restore_$$.dump"

if [[ "${DUMP_SOURCE}" == s3://* ]]; then
    LOCAL_ENCRYPTED="/tmp/soroban_pulse_restore_$$.dump.gpg"
    echo "[restore] Downloading ${DUMP_SOURCE} → ${LOCAL_ENCRYPTED}"
    aws s3 cp "${DUMP_SOURCE}" "${LOCAL_ENCRYPTED}"
    echo "[restore] Download complete"
fi

echo "[restore] Decrypting backup..."
echo "${BACKUP_ENCRYPTION_KEY}" | gpg --decrypt --batch --no-tty --passphrase-fd 0 \
    --output "${LOCAL_DUMP}" "${LOCAL_ENCRYPTED}"
echo "[restore] Decryption complete"

echo "[restore] WARNING: This will overwrite all data in the target database."
read -r -p "[restore] Type 'yes' to continue: " CONFIRM
if [[ "${CONFIRM}" != "yes" ]]; then
    echo "[restore] Aborted."
    rm -f "${LOCAL_DUMP}"
    if [[ "${DUMP_SOURCE}" == s3://* ]]; then
        rm -f "${LOCAL_ENCRYPTED}"
    fi
    exit 1
fi

echo "[restore] Restoring from ${LOCAL_DUMP}"
pg_restore --clean --if-exists --no-owner --no-privileges \
    --dbname "${DATABASE_URL}" "${LOCAL_DUMP}"

echo "[restore] Restore complete"

# Clean up temp files
rm -f "${LOCAL_DUMP}"
if [[ "${DUMP_SOURCE}" == s3://* ]]; then
    rm -f "${LOCAL_ENCRYPTED}"
    echo "[restore] Temp files removed"
fi
