"""Task and Asana Utility Scheduler handler registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from second_brain.monitoring import track_job_execution

if TYPE_CHECKING:
    from second_brain.scheduler_manager import UtilitySchedulerManager

log = logging.getLogger(__name__)


@track_job_execution("process_pending_programmes")
async def handle_process_pending_programmes(bot=None) -> dict:
    """Utility Scheduler job wrapper for pending CrossFit programme processing."""
    from second_brain.main import process_pending_programmes

    result = await process_pending_programmes(bot)
    return result or {"ok": True, "action": "processed"}


@track_job_execution("asana_sync")
async def handle_asana_sync(bot=None) -> dict:
    """Utility Scheduler job wrapper for Asana sync."""
    from second_brain.main import run_asana_sync

    return await run_asana_sync(bot)


def register_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register task and Asana jobs with the Utility Scheduler Manager."""
    manager.register_handler("process_pending_programmes", handle_process_pending_programmes)
    manager.register_handler("asana_sync", handle_asana_sync)
    log.info("tasks: registered scheduler handlers (process_pending_programmes, asana_sync)")
