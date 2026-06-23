from __future__ import annotations

from dashforge.signals.learning_index import build_learning_context_rows, infer_services_for_learning


def test_infer_services_preserves_job_and_pod_labels():
    services = infer_services_for_learning(
        metric="http_requests_total",
        query_text='sum(rate(http_requests_total{job="checkout", pod="checkout-api-7f9d"}[5m]))',
        dashboard_title="Checkout",
        panel_title="Traffic",
        tags=[],
    )

    assert "checkout" in services
    assert "checkout-api-7f9d" in services


def test_learning_context_rows_include_job_and_pod_services():
    rows = build_learning_context_rows(
        dashboard_uid="dash-1",
        backend_name="grafana",
        dashboard_title="Checkout",
        dashboard_tags=[],
        panels=[
            {
                "title": "Traffic",
                "queries": ['sum(rate(http_requests_total{job="checkout", pod="checkout-api-7f9d"}[5m]))'],
                "metrics": ["http_requests_total"],
            }
        ],
        metrics_found=["http_requests_total"],
        signals_inferred=[{"metric": "http_requests_total", "signal_type": "request_rate", "confidence": 0.8}],
        status="approved",
        activated_pairs=None,
    )

    service_column = rows[0][9]
    assert "checkout" in service_column
    assert "checkout-api-7f9d" in service_column
