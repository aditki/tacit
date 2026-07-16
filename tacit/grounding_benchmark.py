"""Grounding Benchmark v1 over Tacit's frozen investigation corpus."""

from __future__ import annotations

import json
from importlib.resources import files
from typing import Any

from tacit.investigation_contract import CausalStatus, InvestigationContract, InvestigationContractAssembler
from tacit.models.schemas import (
    ContextChunk,
    CulpritCandidate,
    CulpritRanking,
    DashboardSpec,
    DashRequest,
    EvidenceObservation,
    EvidenceObservationOutcome,
    EvidenceRequirement,
    EvidenceResolution,
    EvidenceResolutionStatus,
    Intent,
    PanelQuery,
    PanelSpec,
)


def load_grounding_corpus() -> list[dict[str, Any]]:
    resource = files("tacit.data").joinpath("grounding_benchmark_v1.json")
    data = json.loads(resource.read_text(encoding="utf-8"))
    cases: list[dict[str, Any]] = []
    for family in data["adversarial_families"]:
        for index, prompt in enumerate(family["prompts"], start=1):
            entities = family.get("entities", [])
            cases.append(
                {
                    "id": f"{family['id']}-{index:02d}",
                    "kind": family["kind"],
                    "prompt": prompt,
                    "expected_grounding": family["expected_grounding"],
                    "expected_abstained": family["expected_abstained"],
                    "family": family["id"],
                    "fixture": family["fixture"],
                    "entity": entities[index - 1] if index <= len(entities) else "checkout",
                }
            )
    return cases


def load_acceptance_corpus() -> list[dict[str, Any]]:
    resource = files("tacit.data").joinpath("grounding_benchmark_v1.json")
    return json.loads(resource.read_text(encoding="utf-8"))["acceptance_cases"]


def _contract_for_case(case: dict[str, Any]):
    kind = str(case["kind"])
    service = str(case.get("entity") or "checkout")
    fixture = dict(case.get("fixture") or {})
    requirements = [
        EvidenceRequirement(id="er_01", evidence_type="metric", signal_type="latency", service_scope=[service])
    ]
    resolutions = [
        EvidenceResolution(
            requirement_id="er_01",
            status=EvidenceResolutionStatus.RESOLVED,
            reason_code="metadata_match",
            metric="request_latency",
            datasource_uid="prom",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]
    observations = [
        EvidenceObservation(
            requirement_id="er_01",
            outcome=EvidenceObservationOutcome.SUPPORTED_OBSERVATION,
            panel_title="Latency",
            query="request_latency",
            datasource_uid="prom",
            valid_query=True,
            non_empty=True,
            survived=True,
        )
    ]
    candidates = [
        CulpritCandidate(
            rank=1,
            suspect=service,
            suspect_type="service",
            score=0.7,
            supporting_requirement_ids=["er_01"],
        )
    ]
    ranking = CulpritRanking(abstained=False, candidates=candidates, telemetry_status="evidenced")
    context_chunks: list[ContextChunk] = []
    if fixture.get("context_available"):
        context_chunks.append(
            ContextChunk(
                content=f"Operational context is available for {service}, but it contains no live telemetry.",
                source=f"runbook:{service}",
                relevance_score=0.8,
                metadata={"source_type": "runbook", "claim_type": "context_only"},
            )
        )
    if context_entity := fixture.get("context_implicates"):
        context_chunks.append(
            ContextChunk(
                content=f"Historical operational context implicates {context_entity}.",
                source=f"incident-history:{context_entity}",
                relevance_score=0.85,
                metadata={"source_type": "incident_history", "claim_type": "suspect_context"},
            )
        )

    if kind in {"partial", "conflicting", "missing_critical"}:
        requirements.append(EvidenceRequirement(id="er_02", evidence_type="metric", signal_type="errors"))
        resolutions.append(
            EvidenceResolution(
                requirement_id="er_02",
                status=EvidenceResolutionStatus.RESOLVED,
                reason_code="metadata_match",
                metric="request_errors",
            )
        )
        second_outcome = (
            EvidenceObservationOutcome.AMBIGUOUS_EVIDENCE
            if kind == "conflicting"
            else EvidenceObservationOutcome.MISSING_EVIDENCE
        )
        observations.append(
            EvidenceObservation(
                requirement_id="er_02",
                outcome=second_outcome,
                valid_query=True,
                rejection_reason="conflicting_signal" if kind == "conflicting" else "empty_result",
            )
        )
        if kind == "conflicting":
            ranking = ranking.model_copy(
                update={
                    "candidates": [
                        *ranking.candidates,
                        CulpritCandidate(
                            rank=2,
                            suspect="shared-cache",
                            suspect_type="cache",
                            score=0.65,
                            supporting_requirement_ids=["er_02"],
                        ),
                    ]
                }
            )
        elif kind == "missing_critical":
            ranking = CulpritRanking(abstained=True, abstention_reason="missing_critical_observation")
    elif kind in {"contradictory", "context_contradicted"}:
        observations[0] = observations[0].model_copy(update={"outcome": EvidenceObservationOutcome.NEGATIVE_EVIDENCE})
        contradicted_candidate = ranking.candidates[0].model_copy(
            update={"supporting_requirement_ids": [], "contradicting_requirement_ids": ["er_01"]}
        )
        ranking = ranking.model_copy(update={"candidates": [contradicted_candidate]})
        if kind == "context_contradicted":
            ranking = CulpritRanking(abstained=True, abstention_reason="telemetry_contradicts_context")
    elif kind in {"missing", "no_runtime", "failed_query", "unresolved_entity", "unknown_service", "stale_artifact"}:
        reason = {
            "missing": "metric_not_found",
            "no_runtime": "no_runtime_telemetry",
            "failed_query": "query_validation_failed",
            "unresolved_entity": "entity_not_resolved",
            "unknown_service": "insufficient_operational_knowledge",
            "stale_artifact": "no_current_telemetry",
        }[kind]
        resolutions[0] = resolutions[0].model_copy(
            update={"status": EvidenceResolutionStatus.UNRESOLVED, "reason_code": reason, "metric": ""}
        )
        observations[0] = EvidenceObservation(
            requirement_id="er_01",
            outcome=EvidenceObservationOutcome.MISSING_EVIDENCE,
            rejection_reason=reason,
        )
        ranking = CulpritRanking(abstained=True, abstention_reason=reason)
        if kind == "stale_artifact":
            context_chunks = [
                ContextChunk(
                    content="Historical checkout latency note",
                    source="runbook:checkout",
                    relevance_score=0.8,
                    metadata={"stale": True},
                )
            ]
    elif kind in {"negative_control", "no_culprit"}:
        ranking = CulpritRanking(abstained=True, abstention_reason="no_supported_culprit")
    elif kind == "multiple_plausible":
        ranking = ranking.model_copy(
            update={
                "candidates": [
                    *ranking.candidates,
                    CulpritCandidate(
                        rank=2,
                        suspect="shared-cache",
                        suspect_type="cache",
                        score=0.6,
                        supporting_requirement_ids=["er_01"],
                    ),
                ]
            }
        )

    dashboard = DashboardSpec(
        title="Grounding benchmark",
        panels=[
            PanelSpec(
                title="Latency",
                queries=[
                    PanelQuery(
                        expr="request_latency",
                        datasource_uid="prom",
                        validation_status="passed",
                        validation_has_data=observations[0].non_empty,
                    )
                ],
            )
        ],
    )
    return InvestigationContractAssembler().from_pipeline(
        investigation_id=f"benchmark_{case['id']}",
        revision=0,
        parent_revision=None,
        request=DashRequest(prompt=str(case["prompt"]), user_id="grounding-benchmark"),
        intent=Intent(summary=str(case["prompt"]), domain="application", services=[service]),
        dashboard_spec=dashboard,
        evidence_requirements=requirements,
        evidence_resolutions=resolutions,
        evidence_observations=observations,
        culprit_ranking=ranking,
        context_chunks=context_chunks,
        dashboard_url="",
        dashboard_uid="",
    )


def _has_unsafe_abstention_output(contract: InvestigationContract, *, expected_abstain: bool) -> bool:
    if not expected_abstain:
        return False
    conclusion = contract.grounding.maximum_trustworthy_conclusion
    causal_status = conclusion.get("causal_status")
    conclusion_text = str(conclusion.get("text", "")).lower()
    return (
        bool(contract.grounding.unsafe_conclusions)
        or causal_status
        in {
            CausalStatus.PROVEN,
            CausalStatus.SUSPECT_NOT_PROVEN,
        }
        or "leading suspect" in conclusion_text
    )


def run_grounding_benchmark() -> dict[str, Any]:
    cases = load_grounding_corpus()
    results: list[dict[str, Any]] = []
    true_positive = false_positive = false_negative = 0
    unsafe = 0
    insufficient_cases = 0
    correct = 0
    trustworthy = 0
    for case in cases:
        contract = _contract_for_case(case)
        expected_abstain = bool(case["expected_abstained"])
        insufficient_cases += int(expected_abstain)
        actual_abstain = contract.grounding.abstained
        status_correct = contract.grounding.status.value == case["expected_grounding"]
        if status_correct and actual_abstain == expected_abstain:
            correct += 1
        if actual_abstain and expected_abstain:
            true_positive += 1
        elif actual_abstain:
            false_positive += 1
        elif expected_abstain:
            false_negative += 1
        unsafe_assertion = _has_unsafe_abstention_output(contract, expected_abstain=expected_abstain)
        unsafe += int(unsafe_assertion)
        case_passed = status_correct and actual_abstain == expected_abstain and not unsafe_assertion
        trustworthy += int(case_passed)
        results.append(
            {
                "id": case["id"],
                "grounding": contract.grounding.status.value,
                "abstained": actual_abstain,
                "passed": case_passed,
            }
        )
    total = len(cases)
    precision_denominator = true_positive + false_positive
    recall_denominator = true_positive + false_negative
    return {
        "benchmark": "grounding-v1",
        "cases": total,
        "grounding_accuracy": correct / total,
        "abstention_precision": true_positive / precision_denominator if precision_denominator else 1.0,
        "abstention_recall": true_positive / recall_denominator if recall_denominator else 1.0,
        "unsafe_assertion_rate": unsafe / insufficient_cases if insufficient_cases else 0.0,
        "trustworthy_answer_rate": trustworthy / total,
        "passed": correct == total and unsafe == 0,
        "results": results,
    }


def run_acceptance_corpus() -> dict[str, Any]:
    """Round-trip the ten representative contract shapes through JSON."""
    from tacit.investigation_contract import InvestigationContract

    results: list[dict[str, Any]] = []
    for case in load_acceptance_corpus():
        contract = _contract_for_case(case)
        round_tripped = InvestigationContract.model_validate_json(contract.model_dump_json(by_alias=True))
        passed = (
            round_tripped == contract
            and contract.grounding.status.value == case["expected_grounding"]
            and contract.grounding.abstained == case["expected_abstained"]
        )
        results.append({"id": case["id"], "passed": passed})
    return {
        "corpus": "investigation-contract-acceptance-v1",
        "cases": len(results),
        "passed": all(result["passed"] for result in results),
        "results": results,
    }
