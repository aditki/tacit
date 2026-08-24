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
def test_insights_render_hostile_stored_values_as_text():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()
    hostile_recommendation = '<img id="recommendation-xss" src=x onerror="window.__insightsXss=true">: review'
    hostile_archetype = '<img id="archetype-xss" src=x onerror="window.__insightsXss=true">'

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/feedback/stats":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "total_feedback": 1,
                        "total_dashboards": 1,
                        "useful_rate": 1,
                        "avg_noise_level": 4,
                    }
                ),
            )
        elif parsed.path == "/api/v1/feedback/analysis":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "recommendations": [hostile_recommendation],
                        "per_archetype_quality": [
                            {
                                "archetype": hostile_archetype,
                                "useful_rate": 1,
                                "avg_noise": 4,
                                "avg_symptom": 5,
                                "count": 1,
                            }
                        ],
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
        page.evaluate("window.__insightsXss = false")
        page.evaluate("switchTab('insights')")

        page.locator(".rec-item").first.wait_for()
        assert page.locator("#recommendation-xss, #archetype-xss").count() == 0
        assert page.locator("#stats-content img").count() == 0
        assert page.evaluate("window.__insightsXss") is False
        assert hostile_archetype in page.locator(".rec-item").nth(1).text_content()
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


@pytest.mark.e2e
def test_signal_taxonomy_loads_later_pages_into_display_and_teaching_selector():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()

    def signal(name: str, category: str) -> dict[str, object]:
        return {
            "signal_type": name,
            "description": f"{name} description",
            "category": category,
            "unit": "count",
            "mapping_count": 1,
        }

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/signals":
            continuation = "cursor=" in parsed.query
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps(
                    {
                        "signal_types": [
                            (
                                signal("second_page_signal", "saturation")
                                if continuation
                                else signal("first_page_signal", "latency")
                            )
                        ],
                        "has_more": not continuation,
                        "next_cursor": None if continuation else "taxonomy-page-2",
                    }
                ),
            )
        elif parsed.path == "/api/v1/signals/stats":
            route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"signal_types": 2, "metric_mappings": 2}),
            )
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")
        page.evaluate("switchTab('signals')")

        page.locator(".signal-taxonomy-more").wait_for()
        assert page.locator(".signal-card h3").all_text_contents() == ["first_page_signal"]
        assert page.locator("#teach-signal option").all_text_contents() == ["first_page_signal (latency)"]

        page.locator(".signal-taxonomy-more").click()
        page.get_by_text("second_page_signal", exact=True).wait_for()

        assert page.locator(".signal-card h3").all_text_contents() == [
            "first_page_signal",
            "second_page_signal",
        ]
        assert page.locator("#teach-signal option").all_text_contents() == [
            "first_page_signal (latency)",
            "second_page_signal (saturation)",
        ]
        assert page.locator(".signal-taxonomy-more").count() == 0
        browser.close()


@pytest.mark.e2e
def test_long_lived_browser_views_load_later_tenant_pinned_pages():
    html = (Path(__file__).parents[2] / "tacit" / "static" / "index.html").read_text()

    def investigation(identifier: str, prompt: str) -> dict[str, object]:
        return {
            "id": identifier,
            "status": "success",
            "prompt": prompt,
            "path_used": "archetype",
            "panel_count": 1,
            "total_time": 1,
            "archetypes": [],
        }

    def dashboard(row_id: int, uid: str) -> dict[str, object]:
        return {
            "id": row_id,
            "dashboard_uid": uid,
            "dashboard_title": uid,
            "backend_name": "grafana",
            "status": "pending",
            "panel_count": 1,
            "metrics_found": [],
            "signals_inferred": [],
        }

    def candidate(identifier: str) -> dict[str, object]:
        return {
            "id": identifier,
            "kind": "dependency",
            "proposition": {"subject_ref": "checkout", "predicate": "depends_on", "object_ref": identifier},
            "state": {"eligibility": "candidate"},
            "policy": {},
            "scope": {},
            "corroboration": {},
        }

    def serve(route: Route) -> None:
        parsed = urlparse(route.request.url)
        if parsed.path == "/":
            route.fulfill(status=200, content_type="text/html", body=html)
        elif parsed.path == "/api/v1/investigations":
            continuation = "before_started_at=" in parsed.query
            payload = {
                "investigations": [
                    (
                        investigation("inv-second", "second history page")
                        if continuation
                        else investigation("inv-first", "first history page")
                    )
                ],
                "next_cursor": None if continuation else {"before_started_at": 2.0, "before_id": "inv-first"},
            }
            route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))
        elif parsed.path == "/api/v1/learn/dashboards":
            continuation = "before_created_at=" in parsed.query
            payload = {
                "dashboards": [dashboard(2, "dashboard-second") if continuation else dashboard(1, "dashboard-first")],
                "next_cursor": None if continuation else {"before_created_at": 2.0, "before_id": 1},
            }
            route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))
        elif parsed.path == "/api/v1/knowledge/review-queue":
            continuation = "candidate_cursor=" in parsed.query
            payload = {
                "candidates": [candidate("candidate-second") if continuation else candidate("candidate-first")],
                "candidate_has_more": not continuation,
                "candidate_next_cursor": None if continuation else "candidate-page-2",
                "unresolved_conflicts": [],
                "conflict_has_more": False,
                "conflict_next_cursor": None,
                "attention_items": [],
                "attention_has_more": False,
                "attention_next_cursor": None,
            }
            route.fulfill(status=200, content_type="application/json", body=json.dumps(payload))
        elif parsed.path == "/api/v1/knowledge/status":
            route.fulfill(status=200, content_type="application/json", body=json.dumps({"tenant_id": ""}))
        else:
            route.fulfill(status=404, content_type="application/json", body="{}")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page()
        page.route("**/*", serve)
        page.goto("http://tacit.test/")

        page.evaluate("switchTab('history')")
        page.get_by_text("first history page", exact=True).wait_for()
        page.locator(".history-more").click()
        page.get_by_text("second history page", exact=True).wait_for()
        assert page.locator(".prompt-cell").all_text_contents() == ["first history page", "second history page"]

        page.evaluate("switchTab('learning')")
        page.locator(".ingest-title").first.wait_for()
        page.locator(".dashboard-learning-more").click()
        page.locator(".ingest-title").nth(1).wait_for()
        assert page.locator(".ingest-title").all_text_contents() == ["dashboard-first", "dashboard-second"]

        page.evaluate("switchTab('knowledge')")
        page.locator('[data-candidate-id="candidate-first"]').first.wait_for()
        page.locator(".knowledge-queue-more").click()
        page.locator('[data-candidate-id="candidate-second"]').first.wait_for()
        assert page.locator(".knowledge-review-btn[data-decision='approve']").count() == 2
        assert page.locator(".knowledge-queue-more").count() == 0
        browser.close()
