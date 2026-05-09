"""
Job execution tracking with in-memory metrics.
All operational state stored in memory - resets on restart.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from functools import wraps
from statistics import median
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger(__name__)

# In-memory storage (cleared on restart)
_job_metrics: Dict[str, Dict[str, Any]] = {}
_weekly_counters: Dict[str, int] = {"executions": 0, "failures": 0}
_alert_cooldowns: Dict[str, datetime] = {}
_alert_configs: Dict[str, Dict[str, Any]] = {}


_DEFAULT_ALERT_CONFIG: Dict[str, Any] = {
    "alert_on_success": "full",
    "alert_on_failure": "always",
    "alert_on_overlap": True,
    "success_cooldown_hours": 0,
    "failure_cooldown_hours": 6,
    "overlap_cooldown_hours": 6,
    "overlap_threshold_seconds": 180,
}


def load_alert_config(job_key: str, config: Dict[str, Any]) -> None:
    """
    Load alert configuration for a job from Utility Scheduler config.

    Args:
        job_key: Job identifier
        config: Job configuration dict from Utility Scheduler
    """
    _alert_configs[job_key] = {**_DEFAULT_ALERT_CONFIG, **{k: v for k, v in config.items() if v is not None}}
    logger.debug("[JOB_TRACKER] Loaded alert config for %s: %s", job_key, _alert_configs[job_key])


def get_alert_config(job_key: str) -> Dict[str, Any]:
    """Get alert configuration for a job. Returns defaults if not configured."""
    return dict(_alert_configs.get(job_key, _DEFAULT_ALERT_CONFIG))


def update_job_metrics(job_key: str, duration: float, status: str) -> None:
    """
    Update job execution metrics in memory.

    Args:
        job_key: Unique identifier for the job (e.g., "asana_sync")
        duration: Execution time in seconds
        status: "success" or "failed"
    """
    now = datetime.now(timezone.utc)

    # Initialize job entry if doesn't exist
    if job_key not in _job_metrics:
        _job_metrics[job_key] = {
            "last_run": None,
            "last_duration": None,
            "last_status": None,
            "consecutive_fails": 0,
            "duration_history": [],
            "total_runs": 0,
            "total_failures": 0,
        }

    metrics = _job_metrics[job_key]

    # Update metrics
    metrics["last_run"] = now.isoformat()
    metrics["last_duration"] = duration
    metrics["last_status"] = status
    metrics["total_runs"] += 1

    # Update failure tracking
    if status == "success":
        metrics["consecutive_fails"] = 0

        # Add to duration history (keep last 20 for baseline)
        history = metrics["duration_history"]
        history.append(duration)
        metrics["duration_history"] = history[-20:]
    else:
        metrics["consecutive_fails"] += 1
        metrics["total_failures"] += 1

    # Update weekly counters
    _weekly_counters["executions"] += 1
    if status == "failed":
        _weekly_counters["failures"] += 1

    logger.debug(
        "[JOB_TRACKER] Updated metrics for %s: duration=%.2fs, status=%s",
        job_key,
        duration,
        status,
    )


def get_baseline_duration(job_key: str) -> Optional[float]:
    """
    Get baseline duration (median of last 20 runs) for a job.
    Returns None if insufficient data (<5 runs).
    """
    if job_key not in _job_metrics:
        return None

    history = _job_metrics[job_key]["duration_history"]

    # Need at least 5 runs for meaningful baseline
    if len(history) < 5:
        return None

    return median(history)


def get_consecutive_failures(job_key: str) -> int:
    """Get consecutive failure count for a job."""
    if job_key not in _job_metrics:
        return 0
    return _job_metrics[job_key]["consecutive_fails"]


def get_last_run_time(job_key: str) -> Optional[str]:
    """Get last run timestamp for a job (ISO format)."""
    if job_key not in _job_metrics:
        return None
    return _job_metrics[job_key]["last_run"]


def check_alert_cooldown(cooldown_key: str, cooldown_hours: int = 6) -> bool:
    """
    Check if alert is in cooldown period.

    Args:
        cooldown_key: Unique key for this alert type
        cooldown_hours: Hours to wait between alerts

    Returns:
        True if alert can be sent (not in cooldown), False if in cooldown
    """
    if cooldown_key not in _alert_cooldowns:
        return True

    last_alert = _alert_cooldowns[cooldown_key]
    now = datetime.now(timezone.utc)
    hours_since = (now - last_alert).total_seconds() / 3600

    return hours_since >= cooldown_hours


def set_alert_cooldown(cooldown_key: str) -> None:
    """Mark alert as sent, starting cooldown period."""
    _alert_cooldowns[cooldown_key] = datetime.now(timezone.utc)
    logger.debug("[JOB_TRACKER] Set cooldown for %s", cooldown_key)


def get_weekly_metrics() -> Dict[str, Any]:
    """
    Get weekly counters and job performance data.

    Returns:
        Dict with total_executions, total_failures, success_rate, job_performance
    """
    total = _weekly_counters["executions"]
    failures = _weekly_counters["failures"]

    success_rate = 0.0
    if total > 0:
        success_rate = ((total - failures) / total) * 100

    # Build per-job performance summary
    job_performance = []
    for job_key, metrics in _job_metrics.items():
        baseline = get_baseline_duration(job_key)
        last_duration = metrics.get("last_duration")

        if baseline and last_duration:
            # Determine trend
            if last_duration > baseline * 1.2:
                trend = "↑"
            elif last_duration < baseline * 0.8:
                trend = "↓"
            else:
                trend = "→"

            job_performance.append(
                {
                    "job": job_key,
                    "current": last_duration,
                    "baseline": baseline,
                    "trend": trend,
                    "total_runs": metrics["total_runs"],
                    "total_failures": metrics["total_failures"],
                }
            )

    # Sort by most active jobs first
    job_performance.sort(key=lambda x: x["total_runs"], reverse=True)

    return {
        "total_executions": total,
        "total_failures": failures,
        "success_rate": success_rate,
        "job_performance": job_performance,
    }


def reset_weekly_counters() -> None:
    """Reset weekly counters (called after weekly report)."""
    logger.info("[JOB_TRACKER] Resetting weekly counters")
    _weekly_counters["executions"] = 0
    _weekly_counters["failures"] = 0


def get_most_recent_job_time() -> Optional[datetime]:
    """
    Get the most recent job execution time across all jobs.
    Used for scheduler health checks.
    """
    most_recent = None

    for metrics in _job_metrics.values():
        last_run_str = metrics.get("last_run")
        if last_run_str:
            try:
                last_run = datetime.fromisoformat(last_run_str)
                if most_recent is None or last_run > most_recent:
                    most_recent = last_run
            except ValueError:
                pass

    return most_recent


def send_duration_alert_if_slow(job_key: str, baseline: Optional[float], duration: float) -> bool:
    """Alert when a job runs meaningfully slower than its in-memory baseline."""
    if baseline is None:
        return False

    overlap_amount = duration - baseline
    if overlap_amount <= 0:
        return False

    try:
        from utils.alert_handlers import alert_job_overlap

        return alert_job_overlap(job_key, baseline, duration, overlap_amount)
    except Exception as exc:  # noqa: BLE001 - monitoring alerts must never break jobs
        logger.warning("[JOB_TRACKER] Slow-run alert failed for %s: %s", job_key, exc)
        return False


def _alert_on_success(job_key: str, duration: float, result: Any, alert_config: Dict[str, Any]) -> None:
    """Send a success alert according to the job-specific alert configuration."""
    from utils.alert_handlers import alert_job_success

    alert_level = alert_config["alert_on_success"]
    if alert_level == "full":
        alert_job_success(job_key, duration, result)
    elif alert_level == "quiet":
        alert_job_success(job_key, duration, None)


def _alert_on_overlap(job_key: str, baseline: Optional[float], duration: float, alert_config: Dict[str, Any]) -> None:
    """Send an overlap alert when enabled and above the configured threshold."""
    if not alert_config["alert_on_overlap"] or baseline is None or duration <= baseline * 1.5:
        return

    overlap = duration - baseline
    if overlap <= alert_config["overlap_threshold_seconds"]:
        return

    from utils.alert_handlers import alert_job_overlap

    cooldown_key = f"overlap_{job_key}"
    if check_alert_cooldown(cooldown_key, alert_config["overlap_cooldown_hours"]):
        alert_job_overlap(job_key, baseline, duration, overlap)
        set_alert_cooldown(cooldown_key)


def _alert_on_failure(job_key: str, error: Exception, consecutive: int, alert_config: Dict[str, Any]) -> None:
    """Send a failure alert according to the job-specific alert configuration."""
    from utils.alert_handlers import alert_job_failure

    failure_mode = alert_config["alert_on_failure"]
    should_alert = failure_mode == "always" or (failure_mode in {"after_3", "critical_only"} and consecutive >= 3)
    if not should_alert:
        return

    # Critical alerts bypass cooldown so operators always see recurring failures.
    if consecutive >= 3:
        alert_job_failure(job_key, str(error), consecutive)
        return

    cooldown_key = f"failure_{job_key}"
    if check_alert_cooldown(cooldown_key, alert_config["failure_cooldown_hours"]):
        alert_job_failure(job_key, str(error), consecutive)
        set_alert_cooldown(cooldown_key)


def track_job_execution(job_key: str):
    """
    Decorator to track job execution with dynamic alert configuration.

    Alert behavior is loaded from the Utility Scheduler database.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            alert_config = get_alert_config(job_key)
            start = time.time()
            try:
                logger.info("[JOB_TRACKER] Starting %s", job_key)
                result = await func(*args, **kwargs)
                duration = time.time() - start

                baseline = get_baseline_duration(job_key)
                _alert_on_overlap(job_key, baseline, duration, alert_config)
                update_job_metrics(job_key, duration, "success")
                _alert_on_success(job_key, duration, result, alert_config)

                logger.info("[JOB_TRACKER] %s completed in %.2fs", job_key, duration)
                return result

            except Exception as e:
                duration = time.time() - start
                update_job_metrics(job_key, duration, "failed")
                consecutive = get_consecutive_failures(job_key)
                _alert_on_failure(job_key, e, consecutive, alert_config)

                logger.error("[JOB_TRACKER] %s failed after %.2fs: %s", job_key, duration, e)
                raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            alert_config = get_alert_config(job_key)
            start = time.time()
            try:
                logger.info("[JOB_TRACKER] Starting %s", job_key)
                result = func(*args, **kwargs)
                duration = time.time() - start

                baseline = get_baseline_duration(job_key)
                _alert_on_overlap(job_key, baseline, duration, alert_config)
                update_job_metrics(job_key, duration, "success")
                _alert_on_success(job_key, duration, result, alert_config)

                logger.info("[JOB_TRACKER] %s completed in %.2fs", job_key, duration)
                return result

            except Exception as e:
                duration = time.time() - start
                update_job_metrics(job_key, duration, "failed")
                consecutive = get_consecutive_failures(job_key)
                _alert_on_failure(job_key, e, consecutive, alert_config)

                logger.error("[JOB_TRACKER] %s failed after %.2fs: %s", job_key, duration, e)
                raise

        if asyncio.iscoroutinefunction(func):
            async_wrapper._job_tracker_key = job_key  # type: ignore[attr-defined]
            return async_wrapper

        sync_wrapper._job_tracker_key = job_key  # type: ignore[attr-defined]
        return sync_wrapper

    return decorator
