"""Static limits for request data retained by pipeline execution."""

from __future__ import annotations

from tacit.tenancy import MAX_TENANT_LENGTH

DASH_REQUEST_PROMPT_MAX_LENGTH = 2_000
DASH_REQUEST_CHANNEL_ID_MAX_LENGTH = 256
DASH_REQUEST_USER_ID_MAX_LENGTH = 256
DASH_REQUEST_THREAD_TS_MAX_LENGTH = 64

# Account for four bytes per Unicode code point plus a conservative allowance
# for the Pydantic model, its field dictionary, and Python string headers.
DASH_REQUEST_RETAINED_FIXED_OVERHEAD_BYTES = 16 * 1_024
DASH_REQUEST_MAX_RETAINED_BYTES = DASH_REQUEST_RETAINED_FIXED_OVERHEAD_BYTES + 4 * (
    DASH_REQUEST_PROMPT_MAX_LENGTH
    + DASH_REQUEST_CHANNEL_ID_MAX_LENGTH
    + DASH_REQUEST_USER_ID_MAX_LENGTH
    + DASH_REQUEST_THREAD_TS_MAX_LENGTH
    + MAX_TENANT_LENGTH
)


def pipeline_retained_request_memory_bound(*, max_concurrent: int, max_queued: int) -> int:
    """Return the conservative retained-memory ceiling for admitted requests."""
    return (max_concurrent + max_queued) * DASH_REQUEST_MAX_RETAINED_BYTES
