"""Health checks for runtime monitoring."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def check_scheduler_health(scheduler: Any) -> dict[str, Any]:
    """Return a side-effect-free health snapshot for an APScheduler instance."""
    if scheduler is None:
        return {"ok": False, "status": "missing", "jobs": 0, "checked_at": datetime.now(timezone.utc).isoformat()}

    running = bool(getattr(scheduler, "running", False))
    try:
        jobs = scheduler.get_jobs()
    except Exception as exc:  # noqa: BLE001 - health check should report, not raise
        return {
            "ok": False,
            "status": "error",
            "error": str(exc),
            "jobs": 0,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    return {
        "ok": running,
        "status": "running" if running else "stopped",
        "jobs": len(jobs),
        "job_ids": [getattr(job, "id", "unknown") for job in jobs],
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
