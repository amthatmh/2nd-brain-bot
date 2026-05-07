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
