import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from second_brain.monitoring import job_tracker
import utils.alert_handlers as alert_handlers


@pytest.fixture(autouse=True)
def reset_job_tracker(monkeypatch):
    job_tracker._job_metrics.clear()
    job_tracker._weekly_counters["executions"] = 0
    job_tracker._weekly_counters["failures"] = 0
    job_tracker._alert_cooldowns.clear()
    monkeypatch.setattr(alert_handlers, "alert_job_success", lambda *args, **kwargs: True)
    monkeypatch.setattr(alert_handlers, "alert_job_failure", lambda *args, **kwargs: True)
    yield
    job_tracker._job_metrics.clear()
    job_tracker._weekly_counters["executions"] = 0
    job_tracker._weekly_counters["failures"] = 0
    job_tracker._alert_cooldowns.clear()


def test_update_job_metrics_tracks_success_and_baseline():
    for duration in [3.0, 5.0, 7.0, 9.0, 11.0]:
        job_tracker.update_job_metrics("unit", duration, "success")

    metrics = job_tracker._job_metrics["unit"]
    assert metrics["last_status"] == "success"
    assert metrics["consecutive_fails"] == 0
    assert metrics["duration_history"] == [3.0, 5.0, 7.0, 9.0, 11.0]
    assert job_tracker.get_baseline_duration("unit") == 7.0
    assert job_tracker.get_weekly_metrics()["total_executions"] == 5


def test_baseline_requires_five_successful_runs():
    for duration in [3.0, 5.0, 7.0, 9.0]:
        job_tracker.update_job_metrics("unit", duration, "success")

    assert job_tracker.get_baseline_duration("unit") is None


def test_update_job_metrics_tracks_failures():
    job_tracker.update_job_metrics("unit", 1.0, "failed")
    job_tracker.update_job_metrics("unit", 1.5, "failed")

    metrics = job_tracker._job_metrics["unit"]
    assert metrics["last_status"] == "failed"
    assert metrics["consecutive_fails"] == 2
    assert metrics["total_runs"] == 2
    assert metrics["total_failures"] == 2
    assert job_tracker.get_weekly_metrics()["total_executions"] == 2
    assert job_tracker.get_weekly_metrics()["total_failures"] == 2


def test_weekly_metrics_include_per_job_performance_and_reset():
    for duration in [10.0, 10.0, 10.0, 10.0, 13.0]:
        job_tracker.update_job_metrics("unit", duration, "success")
    job_tracker.update_job_metrics("other", 1.0, "failed")

    weekly = job_tracker.get_weekly_metrics()
    assert weekly["total_executions"] == 6
    assert weekly["total_failures"] == 1
    assert weekly["success_rate"] == pytest.approx(83.3333333333)
    assert weekly["job_performance"] == [
        {
            "job": "unit",
            "current": 13.0,
            "baseline": 10.0,
            "trend": "↑",
            "total_runs": 5,
            "total_failures": 0,
        }
    ]

    job_tracker.reset_weekly_counters()
    assert job_tracker.get_weekly_metrics()["total_executions"] == 0
    assert job_tracker.get_weekly_metrics()["total_failures"] == 0


def test_alert_cooldown_is_in_memory():
    assert job_tracker.check_alert_cooldown("alert") is True

    job_tracker.set_alert_cooldown("alert")
    assert job_tracker.check_alert_cooldown("alert", cooldown_hours=6) is False

    job_tracker._alert_cooldowns["alert"] = datetime.now(timezone.utc) - timedelta(hours=7)
    assert job_tracker.check_alert_cooldown("alert", cooldown_hours=6) is True


def test_get_most_recent_job_time():
    assert job_tracker.get_most_recent_job_time() is None

    job_tracker.update_job_metrics("old", 1.0, "success")
    old_run = job_tracker.get_last_run_time("old")
    assert old_run is not None

    job_tracker.update_job_metrics("new", 2.0, "success")
    most_recent = job_tracker.get_most_recent_job_time()

    assert most_recent is not None
    assert most_recent.isoformat() == job_tracker.get_last_run_time("new")


def test_track_job_execution_wraps_sync_function():
    @job_tracker.track_job_execution("sync_unit")
    def sample(value):
        return {"value": value}

    assert sample(42) == {"value": 42}
    assert job_tracker._job_metrics["sync_unit"]["last_status"] == "success"
    assert job_tracker.get_consecutive_failures("sync_unit") == 0


def test_track_job_execution_wraps_async_function():
    @job_tracker.track_job_execution("async_unit")
    async def sample(value):
        return {"value": value}

    assert asyncio.run(sample(7)) == {"value": 7}
    assert job_tracker._job_metrics["async_unit"]["last_status"] == "success"
    assert job_tracker.get_consecutive_failures("async_unit") == 0


def test_send_duration_alert_if_slow_delegates_overlap_alert(monkeypatch):
    calls = []
    monkeypatch.setattr(
        alert_handlers,
        "alert_job_overlap",
        lambda *args, **kwargs: calls.append((args, kwargs)) or True,
    )

    assert job_tracker.send_duration_alert_if_slow("slow", 10.0, 250.0) is True

    assert calls == [(('slow', 10.0, 250.0, 240.0), {})]


def test_send_duration_alert_if_slow_skips_without_baseline_or_extra_duration(monkeypatch):
    calls = []
    monkeypatch.setattr(
        alert_handlers,
        "alert_job_overlap",
        lambda *args, **kwargs: calls.append((args, kwargs)) or True,
    )

    assert job_tracker.send_duration_alert_if_slow("fast", None, 250.0) is False
    assert job_tracker.send_duration_alert_if_slow("fast", 300.0, 250.0) is False
    assert calls == []
