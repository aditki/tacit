"""Framework-neutral Operational Knowledge authorization policy."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Final

from tacit.errors import SemanticAuthorizationError


class KnowledgeAction(StrEnum):
    """Product actions mapped to their server-side authorization requirements."""

    READ = "read"
    APPROVE = "approve"
    TRUST = "trust"
    REJECT = "reject"
    CORRECT = "correct"
    APPLY = "apply"
    EXPORT = "export"
    OVERRIDE = "override"
    TEACH_SIGNALS = "teach_signals"
    LEARN_ARTIFACTS = "learn_artifacts"


KNOWLEDGE_ACTION_PERMISSIONS: Final[dict[KnowledgeAction, tuple[str, ...]]] = {
    KnowledgeAction.READ: ("knowledge.read",),
    KnowledgeAction.APPROVE: ("knowledge.review",),
    KnowledgeAction.TRUST: ("knowledge.review", "knowledge.trust"),
    KnowledgeAction.REJECT: ("knowledge.reject",),
    KnowledgeAction.CORRECT: ("knowledge.correct",),
    KnowledgeAction.APPLY: ("knowledge.apply",),
    KnowledgeAction.EXPORT: ("knowledge.read", "knowledge.export"),
    KnowledgeAction.OVERRIDE: ("knowledge.override",),
    KnowledgeAction.TEACH_SIGNALS: (
        "knowledge.read",
        "knowledge.review",
        "knowledge.trust",
        "knowledge.apply",
    ),
    KnowledgeAction.LEARN_ARTIFACTS: (
        "knowledge.read",
        "knowledge.review",
        "knowledge.apply",
    ),
}


def enforce_knowledge_action(runtime_settings: Any, action: KnowledgeAction) -> None:
    """Authorize every configured permission required by a semantic action."""
    permissions = {value.strip() for value in str(runtime_settings.knowledge_permissions).split(",") if value.strip()}
    for permission in KNOWLEDGE_ACTION_PERMISSIONS[action]:
        if permission not in permissions:
            raise SemanticAuthorizationError(f"Missing permission: {permission}")
