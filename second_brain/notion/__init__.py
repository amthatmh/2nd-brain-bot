from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any, Callable

log = logging.getLogger(__name__)


def notion_call(
    fn: Callable[..., Any],
    *args: Any,
    retries: int = 3,
    backoff: float = 1.0,
    max_backoff: float = 8.0,
    **kwargs: Any,
) -> Any:
    """
    Wraps any Notion client call with bounded exponential backoff + jitter for
    transient API failures (429/5xx).

    Usage:
        notion_call(notion.databases.query, database_id=..., filter=...)
        notion_call(notion.pages.create, parent=..., properties=...)
    """
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            msg = str(exc).lower()
            is_transient = any(code in msg for code in ("429", "500", "502", "503", "504")) or "rate limited" in msg
            if is_transient and attempt < retries - 1:
                wait = min(max_backoff, backoff * (2 ** attempt)) + random.uniform(0.05, 0.35)
                log.warning(
                    "notion_call transient error (attempt %d/%d), retrying in %.2fs — fn=%s err=%s",
                    attempt + 1,
                    retries,
                    wait,
                    getattr(fn, "__name__", "anonymous"),
                    exc,
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
