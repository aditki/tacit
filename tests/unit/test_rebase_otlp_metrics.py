from demo.rebase_otlp_metrics import _timestamp_values, shift_timestamps


def test_shift_timestamps_preserves_relative_timing_and_other_numbers():
    payload = {
        "resourceMetrics": [
            {
                "scopeMetrics": [
                    {
                        "metrics": [
                            {
                                "sum": {
                                    "dataPoints": [
                                        {
                                            "startTimeUnixNano": "100",
                                            "timeUnixNano": "160",
                                            "asInt": "42",
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }

    shifted = shift_timestamps(payload, 1_000)
    timestamps = list(_timestamp_values(shifted))

    assert timestamps == [1_100, 1_160]
    assert timestamps[1] - timestamps[0] == 60
    assert shifted["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["sum"]["dataPoints"][0]["asInt"] == "42"
