"""Daily-log Utility Scheduler handler registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from second_brain.scheduler_manager import UtilitySchedulerManager

log = logging.getLogger(__name__)


async def handle_daily_log_generate(bot=None) -> dict:
    """Utility Scheduler job wrapper for daily log generation."""
    from second_brain.main import generate_daily_log

    await generate_daily_log(bot)
    return {"ok": True, "action": "generated"}


def register_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register daily log jobs with the Utility Scheduler Manager."""
    manager.register_handler("daily_log_generate", handle_daily_log_generate)
    log.info("daily_log: registered scheduler handlers (daily_log_generate)")
