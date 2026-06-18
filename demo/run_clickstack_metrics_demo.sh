#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LEARNING_DASHBOARD="$DEMO_DIR/clickstack-payment-cache.grafana.json"
PROMPT="${PROMPT:-Investigate the ClickStack payment cache saturation incident. Show the Visa validation cache size, payment transaction rate, Redis memory, cache hit ratio, evictions, and client pressure using the real imported telemetry.}"

"$DEMO_DIR/load_clickstack_metrics.sh"

echo
echo "Teaching DashForge the known-good real-telemetry investigation..."
python3 - "$API_URL" "$GRAFANA_URL" "$PROMPT" "$LEARNING_DASHBOARD" <<'PY'
import json
import sys
from pathlib import Path
from urllib.request import Request, urlopen

api_url, grafana_url, prompt, dashboard_path = sys.argv[1:]


def post(path, payload=None):
    body = json.dumps(payload).encode() if payload is not None else b""
    request = Request(
        f"{api_url}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=180) as response:
        return json.load(response)


dashboard = json.loads(Path(dashboard_path).read_text())
learned = post(
    "/api/v1/learn/dashboard/json",
    {
        "vendor": "grafana",
        "source_name": Path(dashboard_path).name,
        "auto_approve": False,
        "dashboard": dashboard,
    },
)
approved = post(
    f"/api/v1/learn/dashboards/{learned['dashboard_uid']}/approve?backend=grafana_json"
)
print(
    json.dumps(
        {
            "learning": {
                "dashboard_uid": learned["dashboard_uid"],
                "status": learned["status"],
                "panel_count": learned["panel_count"],
                "metrics_found": learned["metrics_found"],
            },
            "approval": approved,
        },
        indent=2,
    )
)

print("\nAsking DashForge to investigate the ClickStack checkout incident...")
body = {
    "prompt": prompt,
    "user_id": "demo-real-telemetry",
    "channel_id": "clickstack-metrics",
}
chart = post("/api/v1/chart", body)
print(json.dumps(chart, indent=2))

dashboard_uid = chart.get("dashboard_uid", "")
if not dashboard_uid:
    raise SystemExit("DashForge did not publish a dashboard")

with urlopen(f"{grafana_url}/api/dashboards/uid/{dashboard_uid}", timeout=30) as response:
    generated = json.load(response)["dashboard"]

datasource_uids = {
    (target.get("datasource") or panel.get("datasource") or {}).get("uid", "")
    for panel in generated.get("panels", [])
    for target in panel.get("targets", [])
}
unexpected_uids = datasource_uids - {"real-telemetry"}
if "real-telemetry" not in datasource_uids or unexpected_uids:
    raise SystemExit(
        "Generated dashboard did not use only the real-telemetry datasource: "
        + ", ".join(sorted(datasource_uids))
    )
print("Verified: every generated target uses the real-telemetry datasource.")
PY
