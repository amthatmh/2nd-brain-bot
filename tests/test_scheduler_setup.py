import unittest
from datetime import datetime, timezone

from second_brain.scheduler import register_cinema_jobs, register_core_jobs


class _FakeScheduler:
    def __init__(self):
        self.calls = []

    def add_job(self, fn, trigger, **kwargs):
        self.calls.append({"fn": fn, "trigger": trigger, "kwargs": kwargs})


def _dummy(*_args, **_kwargs):
    return None


class TestSchedulerSetup(unittest.TestCase):
    def test_register_core_jobs_adds_expected_triggers(self):
        scheduler = _FakeScheduler()
        register_core_jobs(
            scheduler=scheduler,
            bot="bot",
            run_recurring_check=_dummy,
            send_daily_digest=_dummy,
            rc_h=7,
            rc_m=0,
            wk_h=8,
            wk_m=15,
            we_h=12,
            we_m=0,
        )
        triggers = [c["trigger"] for c in scheduler.calls]
        self.assertIn("cron", triggers)
        self.assertEqual(triggers.count("cron"), 3)

    def test_register_cinema_jobs_adds_hourly_interval(self):
        scheduler = _FakeScheduler()

        def fixed_now(_tz):
            return datetime(2026, 4, 24, 10, 0, tzinfo=timezone.utc)

        register_cinema_jobs(
            scheduler=scheduler,
            bot="bot",
            run_cinema_sync=_dummy,
            sync_interval_minutes=60,
            tz=timezone.utc,
            now_fn=fixed_now,
        )
        self.assertEqual(len(scheduler.calls), 1)
        self.assertEqual(scheduler.calls[0]["trigger"], "interval")
        self.assertEqual(scheduler.calls[0]["kwargs"]["minutes"], 60)
        self.assertIn("next_run_time", scheduler.calls[0]["kwargs"])


if __name__ == "__main__":
    unittest.main()
