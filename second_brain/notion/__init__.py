from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable

log = logging.getLogger(__name__)


def notion_call(fn: Callable[..., Any], *args: Any, retries: int = 3, backoff: float = 5.0, **kwargs: Any) -> Any:
    """
    Wraps any Notion client call with exponential backoff on 429 rate limits.

    Usage:
        notion_call(notion.databases.query, database_id=..., filter=...)
        notion_call(notion.pages.create, parent=..., properties=...)
    """
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            is_rate_limit = "rate limited" in str(exc).lower() or "429" in str(exc)
            if is_rate_limit and attempt < retries - 1:
                wait = backoff * (2 ** attempt)
                log.warning(
                    "notion_call rate limited (attempt %d/%d), retrying in %.0fs — fn=%s",
                    attempt + 1,
                    retries,
                    wait,
                    getattr(fn, "__name__", "anonymous"),
                )
                time.sleep(wait)
            else:
                log.exception(
                    "notion_call_failed fn=%s err=%s",
                    getattr(fn, "__name__", "anonymous"),
                    exc,
                )
                raise


async def notion_call_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await asyncio.get_running_loop().run_in_executor(
        None, lambda: notion_call(fn, *args, **kwargs)
    )
