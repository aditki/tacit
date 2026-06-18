#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_JSON="${DASHBOARD_JSON:-$DEMO_DIR/checkout-service-incident.grafana.json}"
PROMPT="${PROMPT:-checkout-service is in an incident: p95 latency is spiking after deploy, 5xx errors are rising on payment routes, and requests are piling up. Build the dashboard before creating anything noisy.}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing dependency: $1" >&2
    exit 1
  }
}

request_json() {
  local method="$1"
  local path="$2"
  local body="${3:-}"

  if [[ -n "$body" ]]; then
    curl -fsS -X "$method" "$API_URL$path" \
      -H "Content-Type: application/json" \
      --data-binary "$body"
  else
    curl -fsS -X "$method" "$API_URL$path"
  fi
}

pretty() {
  python3 -m json.tool
}

need curl
need python3

echo "DashForge checkout incident demo"
echo "API: $API_URL"
echo

echo "1. Checking DashForge health"
request_json GET "/healthz" | pretty
echo

echo "2. Uploading a known-good checkout incident dashboard for learning"
UPLOAD_PAYLOAD="$(python3 - "$DASHBOARD_JSON" <<'PY'
import json
import sys
from pathlib import Path

dashboard = json.loads(Path(sys.argv[1]).read_text())
print(json.dumps({
    "vendor": "grafana",
    "source_name": "checkout-service-incident.grafana.json",
    "auto_approve": False,
    "dashboard": dashboard,
}))
PY
)"
UPLOAD_RESPONSE="$(request_json POST "/api/v1/learn/dashboard/json" "$UPLOAD_PAYLOAD")"
printf "%s\n" "$UPLOAD_RESPONSE" | pretty
LEARNED_DASHBOARD_UID="$(printf "%s" "$UPLOAD_RESPONSE" | python3 -c 'import json,sys; print(json.load(sys.stdin)["dashboard_uid"])')"
echo

echo "3. Approving the learned dashboard signals"
request_json POST "/api/v1/learn/dashboards/$LEARNED_DASHBOARD_UID/approve?backend=grafana_json" | pretty
echo

echo "4. Listing learned dashboards"
request_json GET "/api/v1/learn/dashboards?limit=5" | pretty
echo

echo "5. Asking DashForge to generate a fresh incident dashboard"
CHART_PAYLOAD="$(python3 - "$PROMPT" <<'PY'
import json
import sys

print(json.dumps({
    "prompt": sys.argv[1],
    "user_id": "demo",
    "channel_id": "checkout-incident-demo",
}))
PY
)"

set +e
CHART_RESPONSE="$(request_json POST "/api/v1/chart" "$CHART_PAYLOAD" 2>&1)"
CHART_STATUS=$?
set -e

if [[ "$CHART_STATUS" -ne 0 ]]; then
  echo "$CHART_RESPONSE"
  echo
  echo "Dashboard generation needs a configured LLM provider/API key."
  echo "The learning flow above still completed, so you can demo the uploaded-dashboard learning UI now."
  exit "$CHART_STATUS"
fi

printf "%s\n" "$CHART_RESPONSE" | pretty
DASHBOARD_UID="$(printf "%s" "$CHART_RESPONSE" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("dashboard_uid",""))')"
echo

if [[ -n "$DASHBOARD_UID" ]]; then
  echo "6. Recording demo feedback to show the improvement loop"
  FEEDBACK_PAYLOAD="$(python3 - "$DASHBOARD_UID" <<'PY'
import json
import sys

print(json.dumps({
    "dashboard_uid": sys.argv[1],
    "symptom_visibility": 5,
    "root_cause_support": 4,
    "noise_level": 4,
    "investigation_speed": 5,
    "overall_useful": True,
    "comment": "Demo review: useful incident surface with latency, errors, saturation, and downstream evidence.",
    "reviewer": "demo",
}))
PY
)"
  request_json POST "/api/v1/feedback" "$FEEDBACK_PAYLOAD" | pretty
  echo

  echo "7. Fetching recent investigation history"
  request_json GET "/api/v1/history?limit=3" | pretty
fi

echo
echo "Demo complete."
