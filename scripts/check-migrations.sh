#!/bin/bash
# Check for duplicate migration timestamps

set -e

MIGRATIONS_DIR="migrations"
DUPLICATES=$(ls "$MIGRATIONS_DIR"/*.sql | grep -v '\.down\.sql$' | sed 's/.*\///' | sed 's/_.*$//' | sort | uniq -d)

if [ -n "$DUPLICATES" ]; then
    echo "ERROR: Found duplicate migration timestamps:"
    echo "$DUPLICATES"
    exit 1
fi

echo "✓ All migration timestamps are unique"
