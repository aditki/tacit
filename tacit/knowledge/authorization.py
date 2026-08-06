"""Framework-neutral Operational Knowledge authorization policy."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Final


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
        "knowledge.review",
        "knowledge.trust",
        "knowledge.apply",
    ),
}


def enforce_knowledge_action(runtime_settings: Any, action: KnowledgeAction) -> None:
    """Authorize every configured permission required by a semantic action."""
    permissions = {value.strip() for value in str(runtime_settings.knowledge_permissions).split(",") if value.strip()}
    for permission in KNOWLEDGE_ACTION_PERMISSIONS[action]:
        if permission not in permissions:
            raise PermissionError(f"Missing permission: {permission}")
