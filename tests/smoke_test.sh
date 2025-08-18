#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:8000}"
SITE_ID="${SITE_ID:-CLL2}"
TS="${TS:-2025-08-01T10:00:00Z}"

echo "-> /health"
curl -fsS "$API_BASE/health" | jq -r '.status' | grep -q '^ok$' || {
  echo "Health check failed"; exit 1; }

echo "-> /predict"
resp="$(curl -fsS -X POST "$API_BASE/predict" \
  -H 'Content-Type: application/json' \
  -d "{\"site_id\":\"$SITE_ID\",\"timestamp\":\"$TS\"}")"

echo "$resp" | jq .
echo "$resp" | jq -e '.pm25_pred | numbers' >/dev/null

echo "PASS"
