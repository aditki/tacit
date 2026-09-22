from __future__ import annotations

import pytest

from tacit.api.app import create_app
from tacit.config import Settings
from tests.http_client import TestClient


class _Repository:
    def __init__(self, exists: bool):
        self.exists = exists

    def get_candidate(self, *_args, **_kwargs):
        return object() if self.exists else None

    def get_correction(self, *_args, **_kwargs):
        return object() if self.exists else None


class _InvalidTransitionService:
    def __init__(self, exists: bool):
        self.repository = _Repository(exists)

    def review_candidate(self, *_args, **_kwargs):
        raise ValueError("candidate is already terminal")

    def review_correction(self, *_args, **_kwargs):
        raise ValueError("correction is already terminal")


@pytest.mark.parametrize("exists,expected_status", [(True, 409), (False, 404)])
def test_candidate_review_distinguishes_invalid_transition_from_missing(
    monkeypatch: pytest.MonkeyPatch,
    exists: bool,
    expected_status: int,
):
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: _InvalidTransitionService(exists))
    client = TestClient(
        create_app(
            runtime_settings=Settings(
                _env_file=None,
                knowledge_permissions="knowledge.read,knowledge.review",
            )
        )
    )

    response = client.post(
        "/api/v1/knowledge/candidates/kc-terminal/review",
        json={"decision": "approve", "reviewer": "operator", "evaluate": False},
    )

    assert response.status_code == expected_status
    assert response.json()["detail"] == "candidate is already terminal"


@pytest.mark.parametrize("exists,expected_status", [(True, 409), (False, 404)])
def test_correction_review_distinguishes_invalid_transition_from_missing(
    monkeypatch: pytest.MonkeyPatch,
    exists: bool,
    expected_status: int,
):
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: _InvalidTransitionService(exists))
    client = TestClient(
        create_app(
            runtime_settings=Settings(
                _env_file=None,
                knowledge_permissions=("knowledge.read,knowledge.review,knowledge.apply,knowledge.override"),
            )
        )
    )

    response = client.post(
        "/api/v1/knowledge/corrections/correction-terminal/review",
        json={"decision": "approve", "reviewer": "operator", "authoritative": False},
    )

    assert response.status_code == expected_status
    assert response.json()["detail"] == "correction is already terminal"
