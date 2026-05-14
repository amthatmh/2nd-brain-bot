"""Feature Utility Scheduler handler registration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from second_brain import weather as wx
from second_brain.monitoring import track_job_execution

if TYPE_CHECKING:
    from second_brain.scheduler_manager import UtilitySchedulerManager

log = logging.getLogger(__name__)


@track_job_execution("trip_weather_refresh")
async def handle_trip_weather_refresh(bot=None) -> dict:
    """Utility Scheduler job wrapper for refreshing trip weather."""
    from second_brain.main import handle_trip_weather_refresh as _handle_trip_weather_refresh

    return await _handle_trip_weather_refresh(bot)


@track_job_execution("weather_cache_refresh")
async def handle_weather_cache_refresh(bot=None) -> dict:
    """Utility Scheduler job wrapper for refreshing weather cache."""
    result = await wx.fetch_weather_cache(bot)
    return result or {"ok": True, "action": "refreshed"}


@track_job_execution("daily_log_generate")
async def handle_daily_log_generate(bot=None) -> dict:
    """Utility Scheduler job wrapper for daily log generation."""
    from second_brain.main import generate_daily_log

    result = await generate_daily_log(bot)
    return result or {"ok": True, "action": "generated"}


@track_job_execution("cinema_sync")
async def handle_cinema_sync(bot=None) -> dict:
    """Utility Scheduler job wrapper for cinema log sync."""
    from second_brain.main import run_cinema_sync

    return await run_cinema_sync(bot)


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


def register_trips_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register trip jobs with the Utility Scheduler Manager."""
    manager.register_handler("trip_weather_refresh", handle_trip_weather_refresh)
    log.info("trips: registered scheduler handlers (trip_weather_refresh)")


def register_weather_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register weather jobs with the Utility Scheduler Manager."""
    manager.register_handler("weather_cache_refresh", handle_weather_cache_refresh)
    log.info("weather: registered scheduler handlers (weather_cache_refresh)")


def register_daily_log_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register daily log jobs with the Utility Scheduler Manager."""
    manager.register_handler("daily_log_generate", handle_daily_log_generate)
    log.info("daily_log: registered scheduler handlers (daily_log_generate)")


def register_cinema_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register cinema jobs with the Utility Scheduler Manager."""
    manager.register_handler("cinema_sync", handle_cinema_sync)
    log.info("cinema: registered scheduler handlers (cinema_sync)")


def register_tasks_handlers(manager: "UtilitySchedulerManager") -> None:
    """Register task and Asana jobs with the Utility Scheduler Manager."""
    manager.register_handler("process_pending_programmes", handle_process_pending_programmes)
    manager.register_handler("asana_sync", handle_asana_sync)
    log.info("tasks: registered scheduler handlers (process_pending_programmes, asana_sync)")
