"""Operational alert helpers for Second Brain.

The alert_* functions intentionally only format messages and delegate delivery to
send_alert so callers do not need to know Telegram delivery details.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from utils.alerts import send_alert

log = logging.getLogger(__name__)


def _truncate(value: Any, limit: int = 1000) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1]}…"


def alert_startup(version: str, commit: str) -> bool:
    """Send deployment alert on startup"""
    import logging
    from datetime import datetime

    logger = logging.getLogger(__name__)

    logger.info("[ALERT_HANDLER] alert_startup() called")

    message = f"""**Deployment**

Version: {version}
Commit: {commit}
Time: {datetime.now().strftime('%b %d, %I:%M %p %Z')}

All systems operational ✓"""

    result = send_alert(message, level="DEPLOY")
    logger.info(f"[ALERT_HANDLER] alert_startup() returned: {result}")
    return result


def alert_notion_auth_failure(error: str) -> bool:
    return send_alert("*Notion auth failure*\n" f"Error: `{_truncate(error)}`", level="ERROR")


def alert_claude_auth_failure(error: str) -> bool:
    return send_alert("*Claude API/auth failure*\n" f"Error: `{_truncate(error)}`", level="ERROR")


def alert_cinema_sync_complete(
    synced_count: int,
    duplicates_skipped: int,
    duration: float,
    next_run: datetime | str | None = None,
) -> bool:
    """Alert on cinema sync completion."""
    logger = logging.getLogger(__name__)
    logger.info("[ALERT_HANDLER] alert_cinema_sync_complete() called")

    if isinstance(next_run, datetime):
        next_run_text = next_run.strftime("%b %d, %I:%M %p %Z")
    else:
        next_run_text = next_run or "not scheduled"

    message = (
        "*Cinema Sync Completed*\n\n"
        f"✓ {synced_count} new favourites synced\n"
        f"✓ Duplicate guard: {duplicates_skipped} skipped\n\n"
        f"Duration: {duration:.1f}s\n"
        f"Next sync: {next_run_text}"
    )

    result = send_alert(message, level="INFO")
    logger.info("[ALERT_HANDLER] alert_cinema_sync_complete() send_alert returned: %s", result)
    return result


def alert_digest_sent(slot_name: str) -> bool:
    return send_alert("*Digest sent*\n" f"Slot: `{_truncate(slot_name, 120)}`", level="INFO")


def alert_scheduler_event(job_id: str, event_type: str, error: str | None = None) -> bool:
    lines = [
        "*Scheduler event*",
        f"Job: `{_truncate(job_id, 120)}`",
        f"Type: `{_truncate(event_type, 80)}`",
    ]
    if error:
        lines.append(f"Error: `{_truncate(error)}`")
    return send_alert("\n".join(lines), level="WARN")


def alert_weekly_summary(summary: str) -> bool:
    return send_alert("*Weekly summary*\n" f"{_truncate(summary, 3000)}", level="INFO")


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def alert_job_success(job_key: str, duration: float, result: Any = None) -> bool:
    """
    Send INFO alert for successful job completion.

    Args:
        job_key: Job identifier (e.g., "asana_sync")
        duration: Execution time in seconds
        result: Optional dict with job-specific metrics

    Returns:
        True if alert sent successfully
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"[ALERT_HANDLER] alert_job_success() called for {job_key}")

    # Format duration
    if duration < 1:
        duration_str = f"{duration * 1000:.0f}ms"
    elif duration < 60:
        duration_str = f"{duration:.1f}s"
    else:
        mins = int(duration // 60)
        secs = int(duration % 60)
        duration_str = f"{mins}m {secs}s"

    # Build metrics section
    metrics_lines = []
    if result and isinstance(result, dict):
        for key, value in result.items():
            # Format key (convert snake_case to Title Case)
            display_key = key.replace('_', ' ').title()
            metrics_lines.append(f"• {display_key}: {value}")

    metrics_section = "\n".join(metrics_lines) if metrics_lines else ""

    message = f"""**Job Completed: {job_key}**

Duration: {duration_str}
Status: ✓ Success
{metrics_section if metrics_section else ''}"""

    result_sent = send_alert(message, level="INFO")
    logger.info(f"[ALERT_HANDLER] alert_job_success() returned: {result_sent}")
    return result_sent


def alert_job_failure(job_key: str, error: str, consecutive_count: int) -> bool:
    """
    Send WARNING or CRITICAL alert for job failure.

    Args:
        job_key: Job identifier
        error: Error message or exception string
        consecutive_count: Number of consecutive failures

    Returns:
        True if alert sent successfully
    """
    import logging
    logger = logging.getLogger(__name__)

    # Determine severity
    if consecutive_count >= 3:
        level = "CRITICAL"
        emoji = "🚨"
        cooldown_key = None  # No cooldown for CRITICAL
    else:
        level = "WARNING"
        emoji = "⚠️"
        cooldown_key = f"job_failure_{job_key}"

    logger.info(f"[ALERT_HANDLER] alert_job_failure() called: job={job_key}, consecutive={consecutive_count}, level={level}")

    # Truncate error if too long
    if len(error) > 200:
        error = error[:200] + "..."

    message = f"""{emoji} **Job Failed: {job_key}**

Error: {error}
Consecutive failures: {consecutive_count}
Severity: {level}

{"⚠️ This job has failed 3+ times in a row." if consecutive_count >= 3 else ""}"""

    result = send_alert(message, level=level, cooldown_key=cooldown_key)
    logger.info(f"[ALERT_HANDLER] alert_job_failure() returned: {result}")
    return result


def alert_job_overlap(job_key: str, expected_duration: float, actual_duration: float, overlap_amount: float) -> bool:
    """
    Send WARNING alert for job overlap.

    Only fires if overlap > 3 minutes (180 seconds).

    Args:
        job_key: Job identifier
        expected_duration: Baseline duration in seconds
        actual_duration: Actual duration in seconds
        overlap_amount: Seconds over expected

    Returns:
        True if alert sent successfully
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"[ALERT_HANDLER] alert_job_overlap() called: job={job_key}, overlap={overlap_amount:.1f}s")

    # Only alert if overlap >3 minutes
    if overlap_amount < 180:
        logger.info(f"[ALERT_HANDLER] Overlap <3min, skipping alert")
        return False

    # Format durations
    def format_duration(secs):
        if secs < 60:
            return f"{secs:.1f}s"
        mins = int(secs // 60)
        secs_rem = int(secs % 60)
        return f"{mins}m {secs_rem}s"

    message = f"""⚠️ **Job Overlap: {job_key}**

Expected: {format_duration(expected_duration)}
Actual: {format_duration(actual_duration)}
Overlap: {format_duration(overlap_amount)}

Next execution may have been delayed."""

    result = send_alert(message, level="WARNING", cooldown_key=f"job_overlap_{job_key}")
    logger.info(f"[ALERT_HANDLER] alert_job_overlap() returned: {result}")
    return result


def alert_scheduler_health(status: str, last_job_time: Optional[str], active_jobs: int) -> bool:
    """
    Send CRITICAL alert if scheduler appears unhealthy.

    Args:
        status: "healthy" or "unhealthy"
        last_job_time: ISO timestamp of last job
        active_jobs: Number of registered jobs

    Returns:
        True if alert sent successfully
    """
    import logging
    from datetime import datetime
    logger = logging.getLogger(__name__)

    if status == "healthy":
        return True

    logger.info(f"[ALERT_HANDLER] alert_scheduler_health() called: status={status}")

    # Calculate time since last job
    try:
        if last_job_time:
            last_job_dt = datetime.fromisoformat(last_job_time)
            now = datetime.utcnow()
            minutes_since = (now - last_job_dt).total_seconds() / 60
        else:
            minutes_since = "unknown"
    except Exception:
        minutes_since = "unknown"

    message = f"""🚨 **CRITICAL: Scheduler Unhealthy**

Status: {status}
Last job: {minutes_since if isinstance(minutes_since, str) else f'{minutes_since:.1f} min ago'}
Active jobs: {active_jobs}

System automation may be offline."""

    result = send_alert(message, level="CRITICAL")
    logger.info(f"[ALERT_HANDLER] alert_scheduler_health() returned: {result}")
    return result
