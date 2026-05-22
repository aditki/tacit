"""End-to-end test: sends a prompt through the full pipeline and prints the dashboard URL."""
import asyncio
import sys
import os

# Ensure we load .env from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

from dashforge.models.schemas import DashRequest
from dashforge.pipeline import run_pipeline


PROMPT = (
    "High 5xx error rate on the checkout service in the last 30 minutes. "
    "Users are reporting failed payments."
)


async def main():
    print(f"\n{'='*70}")
    print(f"DashForge E2E Test")
    print(f"{'='*70}")
    print(f"Prompt: {PROMPT}\n")

    request = DashRequest(
        prompt=PROMPT,
        user_id="e2e-test",
        channel_id="test-channel",
    )

    print("Running pipeline...")
    response = await run_pipeline(request)

    print(f"\n{'='*70}")
    print(f"Result:")
    print(f"  Dashboard URL:  {response.dashboard_url}")
    print(f"  Dashboard UID:  {response.dashboard_uid}")
    print(f"  Panel count:    {response.panel_count}")
    print(f"  Summary:        {response.summary}")
    print(f"{'='*70}\n")

    if response.dashboard_url:
        print(f"Open in browser: {response.dashboard_url}")
    else:
        print("No dashboard was created.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
