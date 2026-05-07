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
