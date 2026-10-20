#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BUCKET="${S3_BUCKET:-cloud-fundamentals-sales-vishnupriya-2026}"
SOURCE="$ROOT/data/raw/sensor_data_100k.csv"
KEY="predictive-maintenance/raw/sensor_data_100k.csv"

if [[ ! -f "$SOURCE" ]]; then
  echo "ERROR: Source file not found: $SOURCE" >&2
  exit 1
fi

echo "Uploading raw dataset to s3://$BUCKET/$KEY"
aws s3 cp "$SOURCE" "s3://$BUCKET/$KEY" \
  --region ap-south-1 \
  --only-show-errors

TEMP_FILE="$(mktemp)"
trap 'rm -f "$TEMP_FILE"' EXIT

aws s3api get-object \
  --bucket "$BUCKET" \
  --key "$KEY" \
  "$TEMP_FILE" >/dev/null

if cmp -s "$SOURCE" "$TEMP_FILE"; then
  echo "VERIFIED: S3 object matches the local source byte-for-byte."
else
  echo "ERROR: Verification failed; local and S3 contents differ." >&2
  exit 1
fi
