import pytest

from tacit.catalog import select_query_target
from tacit.models.schemas import MetricEntry


def metric(name="requests", uid="p", **kwargs):
    return MetricEntry(
        name=name,
        datasource_uid=uid,
        datasource_name=uid,
        datasource_type="prometheus",
        query_language="promql",
        **kwargs,
    )


def test_all_operands_need_same_exact_owner_and_defaults_do_not_disambiguate():
    catalog = [metric("a", "one", datasource_is_default=True), metric("b", "two")]
    assert select_query_target(catalog, ["a", "b"], "promql").status == "missing"
    assert select_query_target(catalog, ["a", "missing"], "promql").reason == "missing_operand"
    catalog += [metric("a", "two")]
    result = select_query_target(catalog, ["a", "b"], "promql")
    assert result.status == "selected" and result.target.datasource_uid == "two"
    catalog += [metric("b", "one")]
    assert select_query_target(catalog, ["a", "b"], "promql").status == "ambiguous"
    assert select_query_target(catalog, ["a"], "promql", "absent").reason == "explicit_uid_mismatch"


def test_metricless_markers_do_not_select_and_metric_spelling_is_exact():
    assert select_query_target([metric("")], [], "promql").status == "missing"
    assert select_query_target([metric("Requests")], ["requests"], "promql").status == "missing"


@pytest.mark.parametrize(
    "field,values",
    [("unit", ("seconds", "bytes")), ("metric_type", ("gauge", "counter")), ("datasource_name", ("one", "two"))],
)
def test_duplicate_metadata_conflicts_are_ambiguous_and_permutation_invariant(field, values):
    catalog = (
        [metric(**{field: value}) for value in values]
        if field != "datasource_name"
        else [metric().model_copy(update={field: value}) for value in values]
    )
    one = select_query_target(catalog, ["requests"], "promql")
    two = select_query_target(reversed(catalog), ["requests"], "promql")
    assert one == two
    assert one.status == "ambiguous" and one.reason == "metadata_conflict"
    assert one.conflicts


def test_unknown_metadata_merges_and_no_input_mutates():
    a, b = metric(dimensions=["z"]), metric(unit="seconds", dimensions=["a"])
    chosen = select_query_target([a, b], ["requests"], "promql")
    assert chosen.status == "selected"
    chosen.target.datasource_uid = "changed"
    assert chosen.target.datasource_uid == "p"
    assert a.unit == "" and a.dimensions == ["z"]


def test_selection_audit_is_nested_and_copies_results():
    from tacit.catalog import begin_selection_audit, end_selection_audit, record_query_selection, recorded_selections

    chosen = select_query_target([metric()], ["requests"], "promql")
    kwargs = dict(consumer="archetype", subject_ref="a/1/1", required_metrics=["requests"], query_language="promql")
    assert recorded_selections() == ()
    record_query_selection(chosen, **kwargs)
    assert recorded_selections() == ()
    token = begin_selection_audit()
    try:
        record_query_selection(chosen, **kwargs)
        record_query_selection(chosen, **kwargs)
        assert len(recorded_selections()) == 1
        recorded_selections()[0]["required_metrics"].clear()
        assert recorded_selections()[0]["required_metrics"] == ["requests"]
        nested = begin_selection_audit()
        try:
            assert recorded_selections() == ()
        finally:
            end_selection_audit(nested)
        assert len(recorded_selections()) == 1
    finally:
        end_selection_audit(token)
    assert recorded_selections() == ()


def test_operands_exact_cap_and_plus_one():
    from tacit.catalog import AcquisitionCaptureLimitError

    assert select_query_target([], [f"m{i}" for i in range(64)], "promql").status == "missing"
    with pytest.raises(AcquisitionCaptureLimitError, match="selection_operands"):
        select_query_target([], [f"m{i}" for i in range(65)], "promql")
