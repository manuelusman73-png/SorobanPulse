#!/usr/bin/env bash
# scripts/backup.sh — Dump the SorobanPulse PostgreSQL database with encryption.
#
# Usage:
#   ./scripts/backup.sh                  # dump to ./backups/
#   BACKUP_DEST=s3://my-bucket/backups ./scripts/backup.sh
#
# Environment variables:
#   DATABASE_URL              — PostgreSQL connection string (required)
#   BACKUP_DEST               — local directory or s3://bucket/prefix (default: ./backups)
#   BACKUP_ENCRYPTION_KEY     — GPG symmetric encryption passphrase (required)
#   KEEP_LOCAL                — keep local dump file after S3 upload (default: false)
#
# Dependencies: pg_dump, gpg, aws CLI (only for S3 uploads)

set -euo pipefail

DATABASE_URL="${DATABASE_URL:?DATABASE_URL must be set}"
BACKUP_ENCRYPTION_KEY="${BACKUP_ENCRYPTION_KEY:?BACKUP_ENCRYPTION_KEY must be set}"
BACKUP_DEST="${BACKUP_DEST:-./backups}"
KEEP_LOCAL="${KEEP_LOCAL:-false}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
FILENAME="soroban_pulse_${TIMESTAMP}.dump"
LOCAL_PATH="/tmp/${FILENAME}"
ENCRYPTED_PATH="${LOCAL_PATH}.gpg"

echo "[backup] Starting pg_dump → ${LOCAL_PATH}"
pg_dump --format=custom --no-password "${DATABASE_URL}" --file "${LOCAL_PATH}"
echo "[backup] Dump complete: $(du -sh "${LOCAL_PATH}" | cut -f1)"

echo "[backup] Encrypting with GPG..."
echo "${BACKUP_ENCRYPTION_KEY}" | gpg --symmetric --cipher-algo AES256 \
    --batch --no-tty --passphrase-fd 0 \
    --output "${ENCRYPTED_PATH}" "${LOCAL_PATH}"
echo "[backup] Encryption complete: $(du -sh "${ENCRYPTED_PATH}" | cut -f1)"

# Remove unencrypted dump
rm -f "${LOCAL_PATH}"

if [[ "${BACKUP_DEST}" == s3://* ]]; then
    echo "[backup] Uploading to ${BACKUP_DEST}/${FILENAME}.gpg"
    aws s3 cp "${ENCRYPTED_PATH}" "${BACKUP_DEST}/${FILENAME}.gpg"
    echo "[backup] Upload complete"
    if [[ "${KEEP_LOCAL}" != "true" ]]; then
        rm -f "${ENCRYPTED_PATH}"
        echo "[backup] Local encrypted file removed"
    fi
else
    mkdir -p "${BACKUP_DEST}"
    mv "${ENCRYPTED_PATH}" "${BACKUP_DEST}/${FILENAME}.gpg"
    echo "[backup] Saved to ${BACKUP_DEST}/${FILENAME}.gpg"
fi

echo "[backup] Done"
