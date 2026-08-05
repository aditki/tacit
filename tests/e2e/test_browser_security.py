from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

import pytest
from playwright.sync_api import Route, sync_playwright


@pytest.mark.e2e
def test_history_renders_hostile_stored_values_without_executing_them():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    hostile_prompt = '" onmouseover="window.__storedXss=true'
    investigation = {
        "id": "inv-hostile-history",
        "status": "success",
        "prompt": hostile_prompt,
        "path_used": "archetype",
        "panel_count": 1,
        "total_time": 1.25,
        "archetypes": [],
    }
    detail = {
        **investigation,
        "dashboard_url": "javascript:window.__urlXss = true",
        "intent_signals": [],
        "metrics_selected": [],
        "datasource_types": [],
        "generated_queries": [],
        "timings": {},
        "validation_warnings": [],
    }

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/investigations":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"investigations": [investigation]}),
            )
        elif parsed.path == "/api/v1/investigations/inv-hostile-history":
            route.fulfill(status=200, content_type="application/json", body=json.dumps(detail))
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")
        page.evaluate("window.__storedXss = false; window.__urlXss = false")
        page.evaluate("switchTab('history')")

        prompt_cell = page.locator(".prompt-cell")
        prompt_cell.wait_for()
        assert prompt_cell.get_attribute("title") == hostile_prompt
        assert prompt_cell.get_attribute("onmouseover") is None
        assert prompt_cell.text_content() == hostile_prompt
        prompt_cell.hover()
        assert page.evaluate("window.__storedXss") is False
        assert page.locator("[onmouseover]").count() == 0

        page.locator(".investigation-detail-btn").click()
        page.locator(".hist-detail").wait_for()
        assert page.locator(".hist-detail a").count() == 0
        assert page.evaluate("window.__urlXss") is False
        assert page.evaluate("safeExternalUrl('javascript:alert(1)')") == ""
        assert page.evaluate("safeExternalUrl('https://grafana.example/d/checkout')").startswith(
            "https://grafana.example/"
        )
        browser.close()


@pytest.mark.e2e
def test_signal_detail_loads_additional_mapping_pages_without_replacing_prior_rows():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()

    def mapping(pattern: str, mapping_id: int) -> dict[str, object]:
        return {
            "id": mapping_id,
            "metric_pattern": pattern,
            "confidence": 0.9,
            "source_type": "teach",
        }

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/signals":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "signal_types": [
                            {
                                "signal_type": "request_latency",
                                "description": "Request latency",
                                "category": "latency",
                                "unit": "seconds",
                                "mapping_count": 2,
                            }
                        ]
                    }
                ),
            )
        elif parsed.path == "/api/v1/signals/stats":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"signal_types": 1, "metric_mappings": 2}),
            )
        elif parsed.path == "/api/v1/signals/request_latency":
            is_continuation = "cursor=" in parsed.query
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "signal_type": "request_latency",
                        "description": "Request latency",
                        "category": "latency",
                        "unit": "seconds",
                        "mapping_count": 2,
                        "mappings": [
                            (
                                mapping("second_metric_seconds", 2)
                                if is_continuation
                                else mapping("first_metric_seconds", 1)
                            )
                        ],
                        "has_more": not is_continuation,
                        "next_cursor": None if is_continuation else "next-page",
                    }
                ),
            )
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")
        page.evaluate("switchTab('signals')")

        page.locator(".signal-detail-btn").click()
        page.locator(".signal-mappings-more").wait_for()
        assert page.locator(".signal-pattern").all_text_contents() == ["first_metric_seconds"]

        page.locator(".signal-mappings-more").click()
        page.locator(".signal-pattern").nth(1).wait_for()
        assert page.locator(".signal-pattern").all_text_contents() == [
            "first_metric_seconds",
            "second_metric_seconds",
        ]
        assert page.locator(".signal-mappings-more").count() == 0
        browser.close()
