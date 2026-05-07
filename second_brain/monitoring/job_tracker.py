"""
Job execution tracking and metrics storage.

Stores job performance data in the Notion ENV DB for baseline calculation and alerting.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from functools import wraps
from statistics import median
from typing import Any, Callable, Optional, TypeVar, cast

from second_brain.notion.env_db import get_env_value, set_env_value
from utils.alert_handlers import alert_job_failure, alert_job_success

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def _parse_int(value: Optional[str], default: int = 0) -> int:
    """Parse integer ENV values defensively."""
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        logger.warning("Invalid integer metric value %r; using %d", value, default)
        return default


def _parse_float(value: Optional[str]) -> Optional[float]:
    """Parse float ENV values defensively."""
    if not value:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        logger.warning("Invalid float metric value %r", value)
        return None


def update_job_metrics(job_key: str, duration: float, status: str) -> None:
    """
    Update job execution metrics in the Notion ENV DB.

    Args:
        job_key: Unique identifier for the job (e.g., "asana_sync").
        duration: Execution time in seconds.
        status: "success" or "failed".
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    set_env_value(f"job_last_run_{job_key}", now)
    set_env_value(f"job_last_duration_{job_key}", str(duration))
    set_env_value(f"job_last_status_{job_key}", status)

    if status == "success":
        set_env_value(f"job_consecutive_fails_{job_key}", "0")
        update_baseline(job_key, duration)
    else:
        current_fails = _parse_int(get_env_value(f"job_consecutive_fails_{job_key}"))
        set_env_value(f"job_consecutive_fails_{job_key}", str(current_fails + 1))

    weekly_execs = _parse_int(get_env_value("weekly_job_executions"))
    set_env_value("weekly_job_executions", str(weekly_execs + 1))

    if status == "failed":
        weekly_fails = _parse_int(get_env_value("weekly_job_failures"))
        set_env_value("weekly_job_failures", str(weekly_fails + 1))


def update_baseline(job_key: str, duration: float) -> None:
    """
    Update rolling baseline as the median of the last 20 successful runs.

    Durations are stored as a comma-separated history in the Notion ENV DB.
    """
    baseline_key = f"job_baseline_durations_{job_key}"
    durations_str = get_env_value(baseline_key) or ""
    durations: list[float] = []

    if durations_str:
        for raw_duration in durations_str.split(","):
            parsed = _parse_float(raw_duration.strip())
            if parsed is not None:
                durations.append(parsed)

    durations.append(duration)
    durations = durations[-20:]

    baseline = median(durations)
    set_env_value(f"job_baseline_duration_{job_key}", str(baseline))
    set_env_value(baseline_key, ",".join(str(d) for d in durations))


def get_baseline_duration(job_key: str) -> Optional[float]:
    """Get baseline duration for a job."""
    return _parse_float(get_env_value(f"job_baseline_duration_{job_key}"))


def get_consecutive_failures(job_key: str) -> int:
    """Get consecutive failure count for a job."""
    return _parse_int(get_env_value(f"job_consecutive_fails_{job_key}"))


def track_job_execution(job_key: str) -> Callable[[F], F]:
    """
    Decorator to automatically track job execution and send alerts.

    This records execution time, stores metrics in the Notion ENV DB, sends
    success/failure alerts, and maintains baseline duration history.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.time()
            try:
                logger.info("[JOB_TRACKER] Starting %s", job_key)
                result = await func(*args, **kwargs)
                duration = time.time() - start

                update_job_metrics(job_key, duration, "success")
                alert_job_success(job_key, duration, result)

                logger.info("[JOB_TRACKER] %s completed in %.2fs", job_key, duration)
                return result
            except Exception as exc:
                duration = time.time() - start
                consecutive = get_consecutive_failures(job_key) + 1

                update_job_metrics(job_key, duration, "failed")
                alert_job_failure(job_key, str(exc), consecutive)

                logger.error("[JOB_TRACKER] %s failed after %.2fs: %s", job_key, duration, exc)
                raise

        @wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.time()
            try:
                logger.info("[JOB_TRACKER] Starting %s", job_key)
                result = func(*args, **kwargs)
                duration = time.time() - start

                update_job_metrics(job_key, duration, "success")
                alert_job_success(job_key, duration, result)

                logger.info("[JOB_TRACKER] %s completed in %.2fs", job_key, duration)
                return result
            except Exception as exc:
                duration = time.time() - start
                consecutive = get_consecutive_failures(job_key) + 1

                update_job_metrics(job_key, duration, "failed")
                alert_job_failure(job_key, str(exc), consecutive)

                logger.error("[JOB_TRACKER] %s failed after %.2fs: %s", job_key, duration, exc)
                raise

        if asyncio.iscoroutinefunction(func):
            return cast(F, async_wrapper)
        return cast(F, sync_wrapper)

    return decorator
