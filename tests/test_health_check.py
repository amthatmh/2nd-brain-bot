from datetime import datetime, timedelta, timezone

from second_brain.monitoring import health_check, job_tracker


def setup_function():
    job_tracker._job_metrics.clear()
    job_tracker._weekly_counters["executions"] = 0
    job_tracker._weekly_counters["failures"] = 0


def teardown_function():
    job_tracker._job_metrics.clear()
    job_tracker._weekly_counters["executions"] = 0
    job_tracker._weekly_counters["failures"] = 0


def test_check_scheduler_health_returns_no_data_without_history(monkeypatch):
    called = False

    def fake_alert(*_args, **_kwargs):
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(health_check, "alert_scheduler_health", fake_alert)

    result = health_check.check_scheduler_health()

    assert result["ok"] is True
    assert result["action"] == "no_data"
    assert called is False


def test_check_scheduler_health_alerts_when_last_job_is_stale(monkeypatch):
    alerts = []
    stale_time = datetime.now(timezone.utc) - timedelta(minutes=16)
    job_tracker._job_metrics["stale_job"] = {"last_run": stale_time.isoformat()}

    def fake_alert(**kwargs):
        alerts.append(kwargs)
        return True

    monkeypatch.setattr(health_check, "alert_scheduler_health", fake_alert)

    result = health_check.check_scheduler_health()

    assert result["ok"] is False
    assert result["action"] == "unhealthy"
    assert result["minutes_since_last_job"] >= 15
    assert alerts == [
        {
            "status": "unhealthy",
            "last_job_time": stale_time.isoformat(),
            "active_jobs": 11,
        }
    ]


def test_generate_weekly_system_health_sends_report_and_resets(monkeypatch):
    sent_metrics = []
    job_tracker.update_job_metrics("unit", 1.0, "success")
    job_tracker.update_job_metrics("unit", 2.0, "failed")

    def fake_weekly_alert(metrics):
        sent_metrics.append(metrics.copy())
        return True

    monkeypatch.setattr(health_check, "alert_weekly_system_health", fake_weekly_alert)

    result = health_check.generate_weekly_system_health()

    assert result["ok"] is True
    assert result["action"] == "generated"
    assert sent_metrics[0]["total_executions"] == 2
    assert sent_metrics[0]["total_failures"] == 1
    assert "week_ending" in sent_metrics[0]
    assert job_tracker.get_weekly_metrics()["total_executions"] == 0
    assert job_tracker.get_weekly_metrics()["total_failures"] == 0
