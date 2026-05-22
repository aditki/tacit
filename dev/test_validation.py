"""Quick test: verify query validation works via Grafana proxy."""
import asyncio
import os
import sys
import httpx
from urllib.parse import quote

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

from dashforge.config import settings

async def main():
    c = httpx.AsyncClient(
        base_url=settings.grafana_url,
        headers={"Authorization": f"Bearer {settings.grafana_api_key}", "X-Grafana-Org-Id": "1"},
        timeout=10,
    )
    ds = await c.get("/api/datasources")
    uid = ds.json()[0]["uid"]
    print(f"Datasource UID: {uid}")

    queries = {
        "should_be_empty": 'http_requests_total{service=~".*order-processing.*"}',
        "should_have_data": 'http_requests_total{service="checkout-service"}',
    }

    for label, q in queries.items():
        url = f"/api/datasources/proxy/uid/{uid}/api/v1/query?query={quote(q, safe='')}"
        r = await c.get(url)
        d = r.json()
        count = len(d.get("data", {}).get("result", []))
        print(f"  {label}: {count} series (status={r.status_code})")

    await c.aclose()

asyncio.run(main())
