from datetime import timezone

from second_brain.scheduler_manager import UtilitySchedulerManager


def _manager() -> UtilitySchedulerManager:
    return UtilitySchedulerManager(
        notion=None,
        db_id="db",
        scheduler=None,
        bot=None,
        chat_id="chat",
        tz=timezone.utc,
    )


def _title(value: str) -> dict:
    return {"title": [{"plain_text": value}]}


def _rich_text(value: str) -> dict:
    return {"rich_text": [{"plain_text": value}]}


def _select(value: str) -> dict:
    return {"select": {"name": value}}


def _checkbox(value: bool) -> dict:
    return {"checkbox": value}


def test_extract_job_config_reads_rich_text_cron_zeroes() -> None:
    config = _manager()._extract_job_config(
        {
            "Job Key": _title("digest_schedule_rebuild"),
            "Trigger Type": _select("cron"),
            "Cron Hour": _rich_text("0"),
            "Cron Minute": _rich_text("0"),
        }
    )

    assert config["cron_hour"] == 0
    assert config["cron_minute"] == 0


def test_extract_job_config_reads_run_on_startup_checkbox() -> None:
    config = _manager()._extract_job_config(
        {
            "Job Key": _title("asana_sync"),
            "Trigger Type": _select("interval"),
            "Run On Startup": _checkbox(True),
        }
    )

    assert config["run_on_start"] is True


def test_extract_job_config_ignores_old_run_on_start_checkbox_name() -> None:
    config = _manager()._extract_job_config(
        {
            "Job Key": _title("asana_sync"),
            "Trigger Type": _select("interval"),
            "Run On Start": _checkbox(True),
        }
    )

    assert config["run_on_start"] is False


def test_build_cron_kwargs_defaults_minute_to_zero_when_hour_is_set() -> None:
    kwargs = UtilitySchedulerManager._build_cron_kwargs(
        {"cron_day_of_week": None, "cron_hour": 6, "cron_minute": None}
    )

    assert kwargs == {"hour": 6, "minute": 0}


def test_extract_job_config_uses_interval_env_fallback_when_notion_is_blank() -> None:
    config = _manager()._extract_job_config(
        {
            "Job Key": _title("asana_sync"),
            "Trigger Type": _select("interval"),
        },
        env_fallbacks={"asana_sync": 60},
    )

    assert config["interval_seconds"] == 60
    assert config["interval_minutes"] is None
    assert config["interval_hours"] is None


class _FakeScheduler:
    timezone = timezone.utc

    def __init__(self) -> None:
        self.calls = []

    def add_job(self, fn, trigger, **kwargs):
        self.calls.append({"fn": fn, "trigger": trigger, "kwargs": kwargs})


def _manager_with_scheduler(scheduler: _FakeScheduler) -> UtilitySchedulerManager:
    return UtilitySchedulerManager(
        notion=None,
        db_id="db",
        scheduler=scheduler,
        bot=None,
        chat_id="chat",
        tz=timezone.utc,
    )


def _interval_config(*, run_on_start: bool) -> dict:
    return {
        "trigger_type": "interval",
        "interval_seconds": 60,
        "interval_minutes": None,
        "interval_hours": None,
        "cron_day_of_week": None,
        "cron_hour": None,
        "cron_minute": None,
        "run_on_start": run_on_start,
        "max_instances": 1,
        "misfire_grace_seconds": 300,
        "coalesce": True,
    }


def test_add_job_sets_next_run_time_when_run_on_start_enabled() -> None:
    scheduler = _FakeScheduler()
    manager = _manager_with_scheduler(scheduler)

    manager._add_job("asana_sync", _interval_config(run_on_start=True), "page-id")

    assert len(scheduler.calls) == 1
    kwargs = scheduler.calls[0]["kwargs"]
    assert "next_run_time" in kwargs
    assert kwargs["next_run_time"].tzinfo == timezone.utc


def test_add_job_omits_next_run_time_when_run_on_start_disabled() -> None:
    scheduler = _FakeScheduler()
    manager = _manager_with_scheduler(scheduler)

    manager._add_job("asana_sync", _interval_config(run_on_start=False), "page-id")

    assert len(scheduler.calls) == 1
    assert "next_run_time" not in scheduler.calls[0]["kwargs"]


def _reset_job_tracker_state() -> None:
    from second_brain.monitoring import job_tracker

    job_tracker._job_metrics.clear()
    job_tracker._weekly_counters["executions"] = 0
    job_tracker._weekly_counters["failures"] = 0
    job_tracker._alert_cooldowns.clear()


class _FakePages:
    def __init__(self) -> None:
        self.updated = []

    def update(self, **kwargs):
        self.updated.append(kwargs)


class _FakeNotion:
    def __init__(self) -> None:
        self.pages = _FakePages()


def test_execute_job_tracks_plain_handler_success(monkeypatch) -> None:
    import asyncio
    import second_brain.scheduler_manager as scheduler_manager
    from second_brain.monitoring import job_tracker

    _reset_job_tracker_state()
    monkeypatch.setattr(scheduler_manager, "alert_job_success", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler_manager, "send_duration_alert_if_slow", lambda *args, **kwargs: False)

    manager = UtilitySchedulerManager(
        notion=_FakeNotion(),
        db_id="db",
        scheduler=None,
        bot=None,
        chat_id="chat",
        tz=timezone.utc,
    )
    manager.register_handler("plain_job", lambda bot: {"ok": True})

    asyncio.run(manager._execute_job("plain_job", "page-id"))

    assert job_tracker._job_metrics["plain_job"]["last_status"] == "success"
    assert job_tracker.get_weekly_metrics()["total_executions"] == 1


def test_execute_job_does_not_double_track_decorated_handler(monkeypatch) -> None:
    import asyncio
    import second_brain.scheduler_manager as scheduler_manager
    from second_brain.monitoring import job_tracker, track_job_execution

    _reset_job_tracker_state()
    monkeypatch.setattr("utils.alert_handlers.alert_job_success", lambda *args, **kwargs: True)
    monkeypatch.setattr("utils.alert_handlers.alert_job_failure", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler_manager, "alert_job_success", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler_manager, "send_duration_alert_if_slow", lambda *args, **kwargs: False)

    @track_job_execution("decorated_job")
    async def decorated(bot):
        return {"ok": True}

    manager = UtilitySchedulerManager(
        notion=_FakeNotion(),
        db_id="db",
        scheduler=None,
        bot=None,
        chat_id="chat",
        tz=timezone.utc,
    )
    manager.register_handler("decorated_job", decorated)

    asyncio.run(manager._execute_job("decorated_job", "page-id"))

    assert job_tracker._job_metrics["decorated_job"]["last_status"] == "success"
    assert job_tracker.get_weekly_metrics()["total_executions"] == 1
