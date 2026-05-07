"""Weather Utility Scheduler handler registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from second_brain.monitoring import track_job_execution

from second_brain import weather as wx

if TYPE_CHECKING:
    from second_brain.scheduler_manager import UtilitySchedulerManager

log = logging.getLogger(__name__)


@track_job_execution("weather_cache_refresh")
async def handle_weather_cache_refresh(bot=None) -> dict:
    """Utility Scheduler job wrapper for refreshing weather cache."""
    result = await wx.fetch_weather_cache(bot)
    return result or {"ok": True, "action": "refreshed"}


def register_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register weather jobs with the Utility Scheduler Manager."""
    manager.register_handler("weather_cache_refresh", handle_weather_cache_refresh)
    log.info("weather: registered scheduler handlers (weather_cache_refresh)")
