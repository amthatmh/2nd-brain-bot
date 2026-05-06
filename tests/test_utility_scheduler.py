import asyncio
from datetime import datetime, timezone

from second_brain.utility_scheduler import (
    STATUS_DISABLED,
    STATUS_FAILED_VALIDATION,
    STATUS_OK,
    STATUS_UNKNOWN_JOB,
    UtilityJobDefinition,
    apply_utility_job_specs,
    parse_utility_job_rows,
)


class _FakeJob:
    def __init__(self, job_id):
        self.id = job_id
        self.removed = False

    def remove(self):
        self.removed = True


class _FakeScheduler:
    def __init__(self):
        self.calls = []
        self.jobs = {}

    def add_job(self, fn, trigger, **kwargs):
        self.calls.append({"fn": fn, "trigger": trigger, "kwargs": kwargs})
        job = _FakeJob(kwargs["id"])
        self.jobs[kwargs["id"]] = job
        return job

    def get_job(self, job_id):
        job = self.jobs.get(job_id)
        if job and job.removed:
            return None
        return job


class _Recorder:
    def __init__(self):
        self.loaded = []
        self.run_ok = []
        self.run_failed = []

    def mark_loaded(self, spec, status=STATUS_OK, error=""):
        self.loaded.append((spec.job_key, status, error))

    def mark_run_ok(self, spec):
        self.run_ok.append(spec.job_key)

    def mark_run_failed(self, spec, error):
        self.run_failed.append((spec.job_key, error))


def _prop_title(value):
    return {"title": [{"plain_text": value}]}


def _prop_select(value):
    return {"select": {"name": value}}


def _prop_number(value):
    return {"number": value}


def _prop_checkbox(value):
    return {"checkbox": value}


def _row(job_key, **props):
    defaults = {
        "Job Key": _prop_title(job_key),
        "Enabled": _prop_checkbox(True),
        "Trigger Type": _prop_select("cron"),
        "Cron Hour": _prop_number(0),
        "Cron Minute": _prop_number(0),
        "Max Instances": _prop_number(1),
        "Coalesce": _prop_checkbox(True),
        "Misfire Grace Seconds": _prop_number(300),
    }
    defaults.update(props)
    return {"id": f"page-{job_key}", "properties": defaults}


def test_parse_cron_preserves_midnight_zeroes():
    specs = parse_utility_job_rows([_row("digest_schedule_rebuild")])

    assert len(specs) == 1
    assert specs[0].valid
    assert specs[0].trigger_type == "cron"
    assert specs[0].cron_hour == 0
    assert specs[0].cron_minute == 0


def test_parse_interval_requires_exactly_one_interval_field():
    specs = parse_utility_job_rows([
        _row(
            "digest_schedule_refresh",
            **{
                "Trigger Type": _prop_select("interval"),
                "Cron Hour": {},
                "Cron Minute": {},
                "Interval Minutes": _prop_number(10),
            },
        ),
        _row(
            "broken_interval",
            **{
                "Trigger Type": _prop_select("interval"),
                "Cron Hour": {},
                "Cron Minute": {},
            },
        ),
    ])

    assert specs[0].valid
    assert specs[0].interval_minutes == 10
    assert not specs[1].valid
    assert "exactly one interval" in specs[1].validation_error


def test_apply_specs_registers_and_tracks_unknown_disabled_and_invalid_rows():
    scheduler = _FakeScheduler()
    recorder = _Recorder()
    specs = parse_utility_job_rows([
        _row("known_job"),
        _row("unknown_job"),
        _row("disabled_job", **{"Enabled": _prop_checkbox(False)}),
        _row("invalid_job", **{"Cron Hour": {}, "Cron Minute": {}}),
    ])

    stats = apply_utility_job_specs(
        scheduler=scheduler,
        specs=specs,
        registry={"known_job": UtilityJobDefinition(lambda: None)},
        status_recorder=recorder,
        initial_load=True,
        tz=timezone.utc,
        now_fn=lambda _tz: datetime(2026, 5, 6, tzinfo=timezone.utc),
    )

    assert stats["registered"] == 1
    assert stats["unknown"] == 1
    assert stats["disabled"] == 1
    assert stats["invalid"] == 1
    assert scheduler.calls[0]["trigger"] == "cron"
    statuses = {key: status for key, status, _ in recorder.loaded}
    assert statuses["known_job"] == STATUS_OK
    assert statuses["unknown_job"] == STATUS_UNKNOWN_JOB
    assert statuses["disabled_job"] == STATUS_DISABLED
    assert statuses["invalid_job"] == STATUS_FAILED_VALIDATION


def test_apply_specs_does_not_replace_unchanged_jobs_on_reload():
    scheduler = _FakeScheduler()
    recorder = _Recorder()
    specs = parse_utility_job_rows([_row("known_job")])
    registry = {"known_job": UtilityJobDefinition(lambda: None)}

    first = apply_utility_job_specs(
        scheduler=scheduler,
        specs=specs,
        registry=registry,
        status_recorder=recorder,
        initial_load=True,
    )
    second = apply_utility_job_specs(
        scheduler=scheduler,
        specs=specs,
        registry=registry,
        status_recorder=recorder,
        initial_load=False,
    )

    assert first["registered"] == 1
    assert second["unchanged"] == 1
    assert len(scheduler.calls) == 1


def test_tracked_job_updates_run_status():
    scheduler = _FakeScheduler()
    recorder = _Recorder()
    specs = parse_utility_job_rows([_row("known_job")])

    apply_utility_job_specs(
        scheduler=scheduler,
        specs=specs,
        registry={"known_job": UtilityJobDefinition(lambda: "ok")},
        status_recorder=recorder,
        initial_load=True,
    )
    asyncio.run(scheduler.calls[0]["fn"]())

    assert recorder.run_ok == ["known_job"]
