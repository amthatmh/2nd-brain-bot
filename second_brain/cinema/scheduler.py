"""Cinema Utility Scheduler handler registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from second_brain.scheduler_manager import UtilitySchedulerManager

log = logging.getLogger(__name__)


async def handle_cinema_sync(bot=None) -> dict:
    """Utility Scheduler job wrapper for cinema log sync."""
    from second_brain.main import run_cinema_sync

    await run_cinema_sync(bot)
    return {"ok": True, "action": "synced"}


def register_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register cinema jobs with the Utility Scheduler Manager."""
    manager.register_handler("cinema_sync", handle_cinema_sync)
    log.info("cinema: registered scheduler handlers (cinema_sync)")
