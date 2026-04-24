"""Scheduler registration helpers to keep main startup logic slim."""

from __future__ import annotations

from datetime import timedelta


def register_core_jobs(
    *,
    scheduler,
    bot,
    run_recurring_check,
    send_daily_digest,
    send_sunday_review,
    register_habit_schedules,
    rc_h: int,
    rc_m: int,
    wk_h: int,
    wk_m: int,
    we_h: int,
    we_m: int,
) -> None:
    scheduler.add_job(run_recurring_check, "cron", hour=rc_h, minute=rc_m, args=[bot])
    scheduler.add_job(send_daily_digest, "cron", day_of_week="mon-fri", hour=wk_h, minute=wk_m, args=[bot])
    scheduler.add_job(send_daily_digest, "cron", day_of_week="sat", hour=we_h, minute=we_m, args=[bot])
    scheduler.add_job(send_sunday_review, "cron", day_of_week="sun", hour=we_h, minute=we_m, args=[bot])
    register_habit_schedules(scheduler, bot)


def register_cinema_jobs(
    *,
    scheduler,
    bot,
    run_cinema_sync,
    cinema_sync_hour: int,
    cinema_sync_minute: int,
    sync_buffer_minutes: int,
    tz,
    now_fn,
) -> None:
    scheduler.add_job(
        run_cinema_sync,
        "cron",
        hour=cinema_sync_hour,
        minute=cinema_sync_minute,
        args=[bot],
        id="cinema_sync",
    )
    scheduler.add_job(
        run_cinema_sync,
        "interval",
        minutes=sync_buffer_minutes,
        args=[bot],
        id="cinema_sync_buffer",
        max_instances=1,
        coalesce=True,
        next_run_time=now_fn(tz) + timedelta(minutes=sync_buffer_minutes),
    )
