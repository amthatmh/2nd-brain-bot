"""Monitoring and alerting infrastructure."""

from .job_tracker import (
    check_alert_cooldown,
    get_baseline_duration,
    get_consecutive_failures,
    get_most_recent_job_time,
    get_weekly_metrics,
    reset_weekly_counters,
    send_duration_alert_if_slow,
    set_alert_cooldown,
    track_job_execution,
)

__all__ = [
    "track_job_execution",
    "get_baseline_duration",
    "get_consecutive_failures",
    "get_weekly_metrics",
    "reset_weekly_counters",
    "get_most_recent_job_time",
    "check_alert_cooldown",
    "set_alert_cooldown",
    "send_duration_alert_if_slow",
]
