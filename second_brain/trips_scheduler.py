"""Trip Utility Scheduler handler registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from second_brain.monitoring import track_job_execution

if TYPE_CHECKING:
    from second_brain.scheduler_manager import UtilitySchedulerManager

log = logging.getLogger(__name__)


@track_job_execution("trip_weather_refresh")
async def handle_trip_weather_refresh(bot=None) -> dict:
    """Utility Scheduler job wrapper for refreshing trip weather."""
    from second_brain.main import handle_trip_weather_refresh as _handle_trip_weather_refresh

    return await _handle_trip_weather_refresh(bot)


def register_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register trip jobs with the Utility Scheduler Manager."""
    manager.register_handler("trip_weather_refresh", handle_trip_weather_refresh)
    log.info("trips: registered scheduler handlers (trip_weather_refresh)")
