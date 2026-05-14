"""Shared Anthropic Claude client helpers."""

from __future__ import annotations

import os
import re

import anthropic

_client: anthropic.Anthropic | None = None
_JSON_FENCE_RE = re.compile(r"^```(?:json)?|```$", re.MULTILINE)


def get_claude_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        try:
            from second_brain.config import ANTHROPIC_KEY
        except KeyError:
            ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
        client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        if type(client).__module__.startswith("unittest.mock"):
            return client
        _client = client
    return _client


def strip_json_fences(text: str) -> str:
    """Remove Markdown JSON code fences from a Claude response."""
    return _JSON_FENCE_RE.sub("", text).strip()
