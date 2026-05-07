"""Cinema Utility Scheduler handler registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from second_brain.monitoring import track_job_execution

if TYPE_CHECKING:
    from second_brain.scheduler_manager import UtilitySchedulerManager

log = logging.getLogger(__name__)


@track_job_execution("cinema_sync")
async def handle_cinema_sync(bot=None) -> dict:
    """Utility Scheduler job wrapper for cinema log sync."""
    from second_brain.main import run_cinema_sync

    return await run_cinema_sync(bot)


def register_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register cinema jobs with the Utility Scheduler Manager."""
    manager.register_handler("cinema_sync", handle_cinema_sync)
    log.info("cinema: registered scheduler handlers (cinema_sync)")
