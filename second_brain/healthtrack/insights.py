"""Weekly health insight generation from the Health Metrics Log."""

from __future__ import annotations

import asyncio
import logging
import statistics
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from second_brain.ai.client import get_claude_client
from second_brain.monitoring import track_job_execution
from second_brain.notion.env_db import set_env_value
from second_brain.notion.properties import (
    date_filter_range,
    extract_date,
    extract_formula,
    extract_multi_select,
    extract_number,
    extract_plain_text,
    get_property_by_name,
    query_all,
)

log = logging.getLogger(__name__)


@dataclass
class WeekStats:
    avg_sleep_min: float | None
    avg_sleep_efficiency: float | None
    avg_deep_pct: float | None
    avg_rem_pct: float | None
    bedtime_stddev_min: float | None
    avg_hrv: float | None
    avg_rhr: float | None
    last_vo2: float | None
    avg_active_energy: float | None
    avg_exercise_min: float | None
    exercise_days: int
    latest_weight: float | None
    days_with_data: int
    daily_readiness: list[tuple[str, float]] = field(default_factory=list)
    daily_exercise_min: list[tuple[str, float]] = field(default_factory=list)


def fetch_health_range(notion, db_id: str, start: date, end: date) -> list[dict]:
    """Fetch health metric rows for an inclusive date range."""
    return query_all(
        notion,
        db_id,
        filter=date_filter_range("Date", start, end),
        sorts=[{"property": "Date", "direction": "ascending"}],
    )


def _safe_avg(values: list[float | None]) -> float | None:
    present = [float(value) for value in values if value is not None]
    if not present:
        return None
    return sum(present) / len(present)


def _row_props(row: dict[str, Any]) -> dict[str, Any]:
    return row.get("properties", {}) or {}


def _row_date(row: dict[str, Any]) -> date | None:
    raw = extract_date(get_property_by_name(_row_props(row), "Date"))
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _num(row: dict[str, Any], prop_name: str) -> float | None:
    prop = get_property_by_name(_row_props(row), prop_name)
    value = extract_number(prop)
    if value is None:
        formula = extract_formula(prop)
        value = formula if isinstance(formula, (int, float)) else None
    return float(value) if value is not None else None


def _latest_number(rows: list[dict], prop_name: str) -> float | None:
    dated_values: list[tuple[date, float]] = []
    fallback: float | None = None
    for row in rows:
        value = _num(row, prop_name)
        if value is None:
            continue
        fallback = value
        row_day = _row_date(row)
        if row_day is not None:
            dated_values.append((row_day, value))
    if dated_values:
        return sorted(dated_values, key=lambda item: item[0])[-1][1]
    return fallback


def _daily_pct(rows: list[dict], numerator_prop: str) -> list[float | None]:
    values: list[float | None] = []
    for row in rows:
        total = _num(row, "Total Sleep (min)")
        numerator = _num(row, numerator_prop)
        if total is None or total <= 0 or numerator is None:
            values.append(None)
        else:
            values.append((numerator / total) * 100)
    return values


def _bedtime_minutes(row: dict[str, Any]) -> float | None:
    raw = extract_date(get_property_by_name(_row_props(row), "Bedtime"))
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    minutes = parsed.hour * 60 + parsed.minute + parsed.second / 60
    if minutes < 12 * 60:
        minutes += 24 * 60
    return minutes


def _bedtime_stddev(rows: list[dict]) -> float | None:
    values = [value for row in rows if (value := _bedtime_minutes(row)) is not None]
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    return statistics.pstdev(values)


def compute_week_stats(rows: list[dict]) -> WeekStats:
    """Aggregate daily health rows into coach-friendly weekly metrics."""
    exercise_values = [_num(row, "Exercise Time (min)") for row in rows]
    daily_readiness: list[tuple[str, float]] = []
    daily_exercise_min: list[tuple[str, float]] = []
    for row in rows:
        row_day = _row_date(row)
        if row_day is None:
            continue

        readiness = _num(row, "Readiness Score")
        if readiness is not None and readiness > 0:
            daily_readiness.append((row_day.isoformat(), readiness))

        exercise_min = _num(row, "Exercise Time (min)")
        if exercise_min is not None and exercise_min > 0:
            daily_exercise_min.append((row_day.isoformat(), exercise_min))

    daily_readiness.sort(key=lambda item: item[0])
    daily_exercise_min.sort(key=lambda item: item[0])

    return WeekStats(
        avg_sleep_min=_safe_avg([_num(row, "Total Sleep (min)") for row in rows]),
        avg_sleep_efficiency=_safe_avg([_num(row, "Sleep Efficiency (%)") for row in rows]),
        avg_deep_pct=_safe_avg(_daily_pct(rows, "Deep Sleep (min)")),
        avg_rem_pct=_safe_avg(_daily_pct(rows, "REM Sleep (min)")),
        bedtime_stddev_min=_bedtime_stddev(rows),
        avg_hrv=_safe_avg([_num(row, "HRV (ms)") for row in rows]),
        avg_rhr=_safe_avg([_num(row, "Resting Heart Rate (bpm)") for row in rows]),
        last_vo2=_latest_number(rows, "VO2 Max"),
        avg_active_energy=_safe_avg([_num(row, "Active Energy (kcal)") for row in rows]),
        avg_exercise_min=_safe_avg(exercise_values),
        exercise_days=sum(1 for value in exercise_values if value is not None and value > 0),
        latest_weight=_latest_number(rows, "Weight (kg)"),
        days_with_data=len(rows),
        daily_readiness=daily_readiness,
        daily_exercise_min=daily_exercise_min,
    )


def _trip_text(row: dict[str, Any], prop_name: str) -> str:
    return extract_plain_text(get_property_by_name(_row_props(row), prop_name))


def get_travel_context(notion, trips_db_id: str, start: date, end: date) -> dict | None:
    """Return overlapping travel context for the week, if configured and present."""
    if not trips_db_id:
        return None

    rows = query_all(
        notion,
        trips_db_id,
        filter={
            "and": [
                {"property": "Departure Date", "date": {"on_or_before": end.isoformat()}},
                {"property": "Return Date", "date": {"on_or_after": start.isoformat()}},
            ]
        },
        sorts=[{"property": "Departure Date", "direction": "ascending"}],
    )
    if not rows:
        return None

    destinations: list[str] = []
    purposes: list[str] = []
    dep_dates: list[str] = []
    ret_dates: list[str] = []
    for row in rows:
        props = _row_props(row)
        destination = (
            _trip_text(row, "Destination(s)")
            or _trip_text(row, "Destination")
            or _trip_text(row, "Name")
        )
        if destination:
            destinations.append(destination)
        purpose_prop = get_property_by_name(props, "Purpose")
        purpose_names = extract_multi_select(purpose_prop)
        if purpose_names:
            purposes.extend(purpose_names)
        dep = extract_date(get_property_by_name(props, "Departure Date"))
        ret = extract_date(get_property_by_name(props, "Return Date"))
        if dep:
            dep_dates.append(dep[:10])
        if ret:
            ret_dates.append(ret[:10])

    return {
        "destinations": ", ".join(dict.fromkeys(destinations)),
        "purpose": ", ".join(dict.fromkeys(purposes)),
        "dep_date": min(dep_dates) if dep_dates else None,
        "ret_date": max(ret_dates) if ret_dates else None,
    }


def _fmt_num(value: float | None, suffix: str = "", digits: int = 0) -> str:
    if value is None:
        return "no data"
    return f"{value:.{digits}f}{suffix}"


def _fmt_hours(minutes: float | None) -> str:
    if minutes is None:
        return "no data"
    return f"{minutes / 60:.1f}h"


def build_health_insight_prompt(
    week_stats: WeekStats,
    baseline_stats: WeekStats,
    week_label: str,
    travel_context: dict | None,
    as_of_date: str,
) -> str:
    """Build the Claude prompt for the weekly Telegram insight."""
    daily_readiness_str = ", ".join(
        f"{day} {value:.1f}" for day, value in week_stats.daily_readiness
    ) or "no data"
    daily_exercise_str = ", ".join(
        f"{day} {value:.0f}m" for day, value in week_stats.daily_exercise_min
    ) or "no data"
    travel_block = ""
    if travel_context:
        destinations = travel_context.get("destinations") or "Travel"
        purpose = travel_context.get("purpose") or "general travel"
        dep = travel_context.get("dep_date") or "unknown departure"
        ret = travel_context.get("ret_date") or "unknown return"
        travel_block = (
            f'\nTRAVEL CONTEXT: "{destinations} trip ({dep}-{ret}), {purpose}. '
            'Account for travel fatigue."\n'
        )

    return f"""You are a warm, encouraging personal health coach writing a weekly check-in for Telegram.
Today is {as_of_date}. You are reviewing {week_label}.

WEEKLY DATA (7-day):
- Sleep: avg {_fmt_hours(week_stats.avg_sleep_min)}, efficiency {_fmt_num(week_stats.avg_sleep_efficiency, "%", 1)}, deep {_fmt_num(week_stats.avg_deep_pct, "%", 1)}, REM {_fmt_num(week_stats.avg_rem_pct, "%", 1)}, bedtime variability {_fmt_num(week_stats.bedtime_stddev_min, " min", 0)} stddev
- Recovery: HRV avg {_fmt_num(week_stats.avg_hrv, " ms", 0)} (28-day baseline: {_fmt_num(baseline_stats.avg_hrv, " ms", 0)}), RHR avg {_fmt_num(week_stats.avg_rhr, " bpm", 0)} (baseline: {_fmt_num(baseline_stats.avg_rhr, " bpm", 0)})
- Training: {week_stats.exercise_days}/7 days active, avg {_fmt_num(week_stats.avg_active_energy, " kcal", 0)} active energy, avg {_fmt_num(week_stats.avg_exercise_min, " min/day", 0)} exercise
- Readiness trend (daily scores Mon→Sun): {daily_readiness_str}
- Workout duration trend (days with exercise): {daily_exercise_str}
- VO2 Max: {_fmt_num(week_stats.last_vo2, "", 1)} (28-day baseline: {_fmt_num(baseline_stats.last_vo2, "", 1)})

28-DAY BASELINE: HRV {_fmt_num(baseline_stats.avg_hrv, " ms", 0)}, RHR {_fmt_num(baseline_stats.avg_rhr, " bpm", 0)}, sleep {_fmt_hours(baseline_stats.avg_sleep_min)} / {_fmt_num(baseline_stats.avg_sleep_efficiency, "%", 1)} efficiency
{travel_block}
Write exactly 8 sections in Markdown. *Bold* key numbers. 250-350 words total:
1. One opening vibe sentence (no header)
2. 💚/🟡/🔴 *Recovery & Readiness* - HRV vs baseline, RHR trend
3. 😴 *Sleep* - duration, efficiency, deep/REM, bedtime consistency
4. 🏃 *Training Load* - active days, energy; VO2 Max only if changed >0.5
5. 📈 *Trending* - 1-2 positive observations vs 28-day baseline
6. ⚠️ *Watch This Week* - 0-2 items (omit section if nothing notable)
7. 🎯 *This Week's Focus* - one concrete actionable recommendation
8. 📊 *Week in Review* - Show readiness score trend across the week (high/low/direction). Note any correlation with workout load or sleep. Skip if fewer than 3 readiness scores.

RULES: Compare only to personal baseline, never population norms. Skip metrics with no data.
If travel present, acknowledge in opening and soften recovery/sleep expectations.
Use plain Markdown (*bold*), not MarkdownV2."""


def call_claude_for_insight(prompt: str, model: str) -> str:
    """Call Claude and return the text response."""
    resp = get_claude_client().messages.create(
        model=model,
        max_tokens=600,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text.strip()


def build_health_profile_text(
    week_stats: WeekStats,
    baseline_stats: WeekStats,
    as_of_date: str,
) -> str:
    """Create a compact factual health snapshot for future context."""
    parts = [f"As of {as_of_date}"]
    if week_stats.avg_hrv is not None:
        if baseline_stats.avg_hrv is not None:
            parts.append(
                f"HRV averages {week_stats.avg_hrv:.0f} ms vs "
                f"{baseline_stats.avg_hrv:.0f} ms baseline"
            )
        else:
            parts.append(f"HRV averages {week_stats.avg_hrv:.0f} ms")
    if week_stats.avg_rhr is not None:
        if baseline_stats.avg_rhr is not None:
            parts.append(
                f"RHR averages {week_stats.avg_rhr:.0f} bpm vs "
                f"{baseline_stats.avg_rhr:.0f} bpm baseline"
            )
        else:
            parts.append(f"RHR averages {week_stats.avg_rhr:.0f} bpm")
    if week_stats.avg_sleep_min is not None:
        parts.append(f"sleep averages {week_stats.avg_sleep_min / 60:.1f}h")
    if week_stats.last_vo2 is not None:
        parts.append(f"VO2 Max latest is {week_stats.last_vo2:.1f}")
    parts.append(f"training frequency is {week_stats.exercise_days}/7 days")
    return "; ".join(parts) + "."


def update_health_profile(text: str) -> bool:
    """Persist the latest health profile to the Notion ENV DB."""
    return set_env_value("HEALTH_PROFILE", text)


def _format_week_label(start: date, end: date) -> str:
    return f"{start.strftime('%b')} {start.day}-{end.strftime('%b')} {end.day}"


def _split_rows_for_windows(
    rows: list[dict],
    *,
    week_start: date,
    week_end: date,
    base_start: date,
) -> tuple[list[dict], list[dict]]:
    week_rows: list[dict] = []
    baseline_rows: list[dict] = []
    baseline_end = week_start - timedelta(days=1)
    for row in rows:
        row_day = _row_date(row)
        if row_day is None:
            continue
        if week_start <= row_day <= week_end:
            week_rows.append(row)
        elif base_start <= row_day <= baseline_end:
            baseline_rows.append(row)
    return week_rows, baseline_rows


async def _send_health_message(bot, chat_id: int | str | None, text: str) -> None:
    if bot is None or chat_id is None:
        return
    await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")


async def generate_weekly_health_insight(
    bot,
    *,
    notion=None,
    metrics_db_id: str | None = None,
    trips_db_id: str | None = None,
    chat_id: int | str | None = None,
    tz=None,
    claude_model: str | None = None,
    today: date | None = None,
) -> dict:
    """Generate and send the weekly health insight Telegram message."""
    if notion is None or metrics_db_id is None or chat_id is None or tz is None or claude_model is None:
        from second_brain.config import CLAUDE_MODEL
        from second_brain.main import MY_CHAT_ID, NOTION_HEALTH_METRICS_DB, NOTION_TRIPS_DB, TZ, notion as main_notion

        notion = notion or main_notion
        metrics_db_id = metrics_db_id if metrics_db_id is not None else NOTION_HEALTH_METRICS_DB
        trips_db_id = trips_db_id if trips_db_id is not None else NOTION_TRIPS_DB
        chat_id = chat_id if chat_id is not None else MY_CHAT_ID
        tz = tz or TZ
        claude_model = claude_model or CLAUDE_MODEL

    if not metrics_db_id:
        await _send_health_message(
            bot,
            chat_id,
            "⚠️ Weekly Health Insight skipped: health metrics database is not configured.",
        )
        return {"ok": False, "reason": "missing_metrics_db", "days_analyzed": 0}

    local_today = today or datetime.now(tz).date()
    week_end = local_today - timedelta(days=1)
    week_start = week_end - timedelta(days=6)
    base_start = week_end - timedelta(days=34)
    week_label = _format_week_label(week_start, week_end)

    rows = await asyncio.to_thread(fetch_health_range, notion, metrics_db_id, base_start, week_end)
    week_rows, baseline_rows = _split_rows_for_windows(
        rows,
        week_start=week_start,
        week_end=week_end,
        base_start=base_start,
    )
    week_stats = compute_week_stats(week_rows)
    baseline_stats = compute_week_stats(baseline_rows)

    if week_stats.days_with_data < 3:
        text = (
            f"⚠️ *Weekly Health Insight — {week_label}*\n\n"
            f"Insufficient health data this week ({week_stats.days_with_data}/7 days). "
            "I'll wait for at least 3 days before generating a coaching summary."
        )
        await _send_health_message(bot, chat_id, text)
        return {
            "ok": False,
            "reason": "insufficient_data",
            "days_analyzed": week_stats.days_with_data,
        }

    travel = await asyncio.to_thread(get_travel_context, notion, trips_db_id or "", week_start, week_end)
    prompt = build_health_insight_prompt(
        week_stats,
        baseline_stats,
        week_label,
        travel,
        local_today.isoformat(),
    )
    insight_text = await asyncio.to_thread(call_claude_for_insight, prompt, claude_model)
    message = f"🏥 *Weekly Health Insight — {week_label}*\n\n{insight_text}"
    await _send_health_message(bot, chat_id, message)

    profile_text = build_health_profile_text(week_stats, baseline_stats, local_today.isoformat())
    profile_updated = await asyncio.to_thread(update_health_profile, profile_text)
    return {
        "ok": True,
        "days_analyzed": week_stats.days_with_data,
        "profile_updated": profile_updated,
    }


@track_job_execution("weekly_health_insight")
async def handle_weekly_health_insight_job(bot=None) -> dict:
    """Utility Scheduler job wrapper for weekly health insight generation."""
    return await generate_weekly_health_insight(bot)
