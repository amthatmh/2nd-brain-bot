from __future__ import annotations

import asyncio
import json
import re
from typing import Any

import anthropic


_JSON_FENCE_RE = re.compile(r"^```(?:json)?|```$", re.MULTILINE)


def _strip_markdown_json(text: str) -> str:
    return _JSON_FENCE_RE.sub("", text).strip()


async def claude_classify(
    client: anthropic.Anthropic,
    model: str,
    prompt: str,
    max_tokens: int,
    retries: int = 3,
) -> dict[str, Any]:
    delay = 0.75
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            msg = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: client.messages.create(
                    model=model,
                    max_tokens=max_tokens,
                    messages=[{"role": "user", "content": prompt}],
                ),
            )
            text = msg.content[0].text if msg.content else "{}"
            return json.loads(_strip_markdown_json(text) or "{}")
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            transient = any(token in str(exc).lower() for token in ("rate", "529", "timeout", "tempor"))
            if not transient or attempt == retries:
                break
            await asyncio.sleep(delay)
            delay *= 2
    raise RuntimeError(f"Claude classification failed after {retries} attempts: {last_error}")
