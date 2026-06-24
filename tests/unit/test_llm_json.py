from __future__ import annotations

from dashforge.agents.llm_json import attempt_json_repair, parse_json_with_repair
from dashforge.agents.providers.base import LLMProvider, LLMResult, TokenUsage


class RepairProvider(LLMProvider):
    def __init__(self):
        self.calls = 0

    async def chat_json(self, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> LLMResult:
        self.calls += 1
        return LLMResult('{"fixed": true}', TokenUsage(prompt_tokens=1, completion_tokens=2, total_tokens=3))

    async def chat_text(self, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> LLMResult:
        return LLMResult("")


async def test_parse_json_with_repair_uses_programmatic_repair_first():
    parsed, usage = await parse_json_with_repair(
        RepairProvider(),
        '```json\n{"ok": true,}\n```',
    )

    assert parsed == {"ok": True}
    assert usage.total_tokens == 0


async def test_parse_json_with_repair_uses_provider_repair_when_needed():
    provider = RepairProvider()

    parsed, usage = await parse_json_with_repair(provider, "{not json")

    assert parsed == {"fixed": True}
    assert usage.total_tokens == 3
    assert provider.calls == 1


def test_attempt_json_repair_returns_none_for_unrepairable_text():
    assert attempt_json_repair("{not json") is None
