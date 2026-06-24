from tacit.backends.grafana import _parse_grafana_alert_rule
from tacit.backends.signalfx import _parse_signalfx_detector


def test_grafana_alert_rule_parses_to_common_alert_features():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "A",
            "isPaused": False,
            "labels": {"service": "checkout", "severity": "critical"},
            "annotations": {"__dashboardUid__": "checkout-dashboard", "__panelTitle__": "p95 latency"},
            "data": [
                {
                    "refId": "A",
                    "model": {
                        "expr": (
                            "histogram_quantile(0.95, " 'rate(checkout_latency_seconds_bucket{service="checkout"}[5m]))'
                        )
                    },
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
    )

    assert features.alert_uid == "checkout-latency"
    assert features.backend_name == "grafana"
    assert features.query_language == "promql"
    assert features.metrics_found == ["checkout_latency_seconds_bucket"]
    assert features.service_hints == ["checkout"]
    assert features.dashboard_uid == "checkout-dashboard"


def test_signalfx_detector_parses_to_common_alert_features():
    features = _parse_signalfx_detector(
        {
            "id": "detector-1",
            "name": "Checkout errors high",
            "tags": ["service:checkout"],
            "teams": ["payments"],
            "programText": "A = data('checkout.errors').sum().publish(label='A')",
            "rules": [{"detectLabel": "A above threshold", "severity": "Critical"}],
        },
        backend_name="signalfx",
        realm="us1",
    )

    assert features.alert_uid == "detector-1"
    assert features.backend_name == "signalfx"
    assert features.query_language == "signalflow"
    assert features.metrics_found == ["checkout.errors"]
    assert features.condition == "A above threshold"
    assert features.severity == "Critical"
    assert features.labels == {"team": "payments"}
