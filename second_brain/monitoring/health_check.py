"""
System health monitoring.
Checks scheduler vitality and generates weekly metrics.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from second_brain.monitoring import get_most_recent_job_time, get_weekly_metrics, reset_weekly_counters
from utils.alert_handlers import alert_scheduler_health, alert_weekly_system_health

logger = logging.getLogger(__name__)


def check_scheduler_health() -> Dict[str, Any]:
    """
    Check if scheduler is healthy by verifying recent job activity.

    Returns:
        Dict with status and details.
    """
    logger.info("[HEALTH_CHECK] Running scheduler health check")

    most_recent = get_most_recent_job_time()

    if most_recent is None:
        # No job data yet, system just started.
        logger.warning("[HEALTH_CHECK] No job execution data found (system just started)")
        return {
            "ok": True,
            "action": "no_data",
            "reason": "System recently started, no job history yet",
        }

    # Check if any job ran in last 15 minutes.
    now = datetime.now(timezone.utc)
    if most_recent.tzinfo is None:
        most_recent = most_recent.replace(tzinfo=timezone.utc)
    minutes_since = (now - most_recent).total_seconds() / 60

    if minutes_since > 15:
        logger.error("[HEALTH_CHECK] Scheduler unhealthy. Last job: %.1f min ago", minutes_since)

        active_jobs = 11  # Known count from the current scheduled job set.

        alert_scheduler_health(
            status="unhealthy",
            last_job_time=most_recent.isoformat(),
            active_jobs=active_jobs,
        )

        return {
            "ok": False,
            "action": "unhealthy",
            "minutes_since_last_job": minutes_since,
        }

    # Healthy - don't alert.
    logger.info("[HEALTH_CHECK] Scheduler healthy. Last job: %.1f min ago", minutes_since)
    return {
        "ok": True,
        "action": "healthy",
        "minutes_since_last_job": minutes_since,
    }


def generate_weekly_system_health() -> Dict[str, Any]:
    """
    Generate weekly system health metrics report.
    Sends alert with week's data, then resets counters.

    Returns:
        Dict with metrics.
    """
    logger.info("[HEALTH_CHECK] Generating weekly system health report")

    metrics = get_weekly_metrics()
    metrics["week_ending"] = datetime.now().strftime("%B %d, %Y")

    alert_weekly_system_health(metrics)
    reset_weekly_counters()

    logger.info("[HEALTH_CHECK] Weekly report complete, counters reset")

    return {
        "ok": True,
        "action": "generated",
        "metrics": metrics,
    }
