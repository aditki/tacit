#!/usr/bin/env bash
set -euo pipefail

SAMPLE_URL="${SAMPLE_URL:-https://storage.googleapis.com/hyperdx/sample.tar.gz}"
OTLP_METRICS_URL="${OTLP_METRICS_URL:-http://localhost:4318/v1/metrics}"
VICTORIAMETRICS_URL="${VICTORIAMETRICS_URL:-http://localhost:8428}"
CACHE_DIR="${CACHE_DIR:-data/demo/clickstack}"
ARCHIVE_PATH="$CACHE_DIR/sample.tar.gz"
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing dependency: $1" >&2
    exit 1
  }
}

need curl
need tar
need python3

mkdir -p "$CACHE_DIR"

if [[ ! -f "$ARCHIVE_PATH" ]]; then
  echo "Downloading the official ClickStack OTLP sample..."
  curl -fL --retry 3 --progress-bar "$SAMPLE_URL" -o "$ARCHIVE_PATH"
else
  echo "Using cached sample: $ARCHIVE_PATH"
fi

if ! tar -tzf "$ARCHIVE_PATH" metrics.json >/dev/null 2>&1; then
  echo "Archive does not contain metrics.json: $ARCHIVE_PATH" >&2
  exit 1
fi

echo "Waiting for the OpenTelemetry HTTP receiver..."
ready=0
for _ in {1..30}; do
  if curl -sS -o /dev/null -X POST "$OTLP_METRICS_URL" \
    -H "Content-Type: application/json" \
    --data-binary '{"resourceMetrics":[]}'; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "$ready" -ne 1 ]]; then
  echo "OpenTelemetry receiver did not become ready: $OTLP_METRICS_URL" >&2
  exit 1
fi

echo "Replaying ClickStack metrics into the local telemetry stack..."
sent=0
while IFS= read -r payload; do
  printf '%s\n' "$payload" | curl -fsS -o /dev/null -X POST "$OTLP_METRICS_URL" \
    -H "Content-Type: application/json" \
    --data-binary @-
  sent=$((sent + 1))
  if (( sent % 100 == 0 )); then
    echo "  sent $sent OTLP batches"
  fi
done < <(python3 "$DEMO_DIR/rebase_otlp_metrics.py" "$ARCHIVE_PATH")

metric_count="$(curl -fsS "$VICTORIAMETRICS_URL/api/v1/label/__name__/values" | python3 -c '
import json
import sys

payload = json.load(sys.stdin)
print(len(payload.get("data", [])))
')"

echo "Imported $sent OTLP batches. VictoriaMetrics exposes $metric_count metric names."
