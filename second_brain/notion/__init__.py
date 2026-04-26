from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

log = logging.getLogger(__name__)


def notion_call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001
        log.exception("notion_call_failed fn=%s err=%s", getattr(fn, "__name__", "anonymous"), exc)
        raise


async def notion_call_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await asyncio.get_running_loop().run_in_executor(None, lambda: notion_call(fn, *args, **kwargs))
