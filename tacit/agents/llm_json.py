"""JSON parsing and repair helpers for LLM responses."""

from __future__ import annotations

import json
import re
from typing import Any

import structlog

from tacit.agents.llm_errors import LLMParseError, wrap_transient_llm_error
from tacit.agents.providers.base import LLMProvider, TokenUsage

logger = structlog.get_logger()

REPAIR_SYSTEM_PROMPT = """\
You are a JSON repair tool. The following text was intended to be valid JSON \
but has syntax errors. Fix the JSON and return ONLY the corrected JSON object. \
Do not add commentary, markdown fences, or extra text. Preserve all data."""


def strip_trailing_commas(text: str) -> str:
    """Remove trailing commas before } or ] only when outside JSON string literals."""
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == '"':
            out.append(ch)
            i += 1
            while i < n:
                c = text[i]
                out.append(c)
                i += 1
                if c == "\\" and i < n:
                    out.append(text[i])
                    i += 1
                elif c == '"':
                    break
        elif ch == ",":
            j = i + 1
            while j < n and text[j] in " \t\n\r":
                j += 1
            if j < n and text[j] in ("}", "]"):
                i += 1
            else:
                out.append(ch)
                i += 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def attempt_json_repair(raw: str) -> str | None:
    """Try lightweight programmatic fixes for common LLM JSON issues."""
    text = raw.strip()

    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()

    text = strip_trailing_commas(text)

    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        return None


async def parse_json_with_repair(provider: LLMProvider, raw: str) -> tuple[Any, TokenUsage]:
    """Parse LLM JSON, trying deterministic repair and one provider repair call."""
    try:
        return json.loads(raw), TokenUsage()
    except json.JSONDecodeError:
        repaired = attempt_json_repair(raw)
        if repaired is not None:
            logger.info("llm_json_repaired", method="programmatic")
            return json.loads(repaired), TokenUsage()

    logger.warning("llm_json_repair_attempting", raw_preview=raw[:200])
    try:
        repair_result = await provider.chat_json(
            REPAIR_SYSTEM_PROMPT,
            raw[:4000],
            temperature=0.0,
        )
        parsed = json.loads(repair_result.text)
        logger.info("llm_json_repaired", method="llm_reask")
        return parsed, repair_result.usage
    except json.JSONDecodeError as exc:
        logger.error("llm_json_repair_failed", error=str(exc), raw=raw[:300])
        raise LLMParseError(f"LLM returned invalid JSON (repair failed): {exc}") from exc
    except Exception as exc:
        transient = wrap_transient_llm_error(exc)
        if transient is not None:
            logger.warning("llm_repair_transient_error", error=str(exc), exc_type=type(exc).__name__)
            raise transient from exc
        logger.error("llm_json_repair_failed", error=str(exc), raw=raw[:300])
        raise LLMParseError(f"LLM returned invalid JSON (repair failed): {exc}") from exc
