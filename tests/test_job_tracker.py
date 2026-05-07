import asyncio

import pytest

from second_brain.monitoring import job_tracker


@pytest.fixture
def env_store(monkeypatch):
    store = {}

    def get_value(key):
        return store.get(key)

    def set_value(key, value):
        store[key] = value
        return True

    monkeypatch.setattr(job_tracker, "get_env_value", get_value)
    monkeypatch.setattr(job_tracker, "set_env_value", set_value)
    monkeypatch.setattr(job_tracker, "alert_job_success", lambda *args, **kwargs: True)
    monkeypatch.setattr(job_tracker, "alert_job_failure", lambda *args, **kwargs: True)
    return store


def test_update_job_metrics_tracks_success_and_baseline(env_store):
    job_tracker.update_job_metrics("unit", 3.0, "success")
    job_tracker.update_job_metrics("unit", 5.0, "success")

    assert env_store["job_last_status_unit"] == "success"
    assert env_store["job_consecutive_fails_unit"] == "0"
    assert env_store["job_baseline_duration_unit"] == "4.0"
    assert env_store["job_baseline_durations_unit"] == "3.0,5.0"
    assert env_store["weekly_job_executions"] == "2"


def test_update_job_metrics_tracks_failures(env_store):
    job_tracker.update_job_metrics("unit", 1.0, "failed")
    job_tracker.update_job_metrics("unit", 1.5, "failed")

    assert env_store["job_last_status_unit"] == "failed"
    assert env_store["job_consecutive_fails_unit"] == "2"
    assert env_store["weekly_job_executions"] == "2"
    assert env_store["weekly_job_failures"] == "2"


def test_track_job_execution_wraps_sync_function(env_store):
    @job_tracker.track_job_execution("sync_unit")
    def sample(value):
        return {"value": value}

    assert sample(42) == {"value": 42}
    assert env_store["job_last_status_sync_unit"] == "success"
    assert env_store["job_consecutive_fails_sync_unit"] == "0"


def test_track_job_execution_wraps_async_function(env_store):
    @job_tracker.track_job_execution("async_unit")
    async def sample(value):
        return {"value": value}

    assert asyncio.run(sample(7)) == {"value": 7}
    assert env_store["job_last_status_async_unit"] == "success"
    assert env_store["job_consecutive_fails_async_unit"] == "0"
