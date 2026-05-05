from __future__ import annotations
from datetime import datetime, timedelta, date
from second_brain.config import NUMBER_EMOJIS, HORIZON_LABELS, TZ
from second_brain.notion import tasks as notion_tasks
from second_brain import weather as wx


def num_emoji(n: int) -> str:
    return NUMBER_EMOJIS[n - 1] if 1 <= n <= 10 else f"{n}."

def context_emoji(context: str | None) -> str:
    ctx = (context or "").strip().lower()
    if ctx == "💼 work":
        return "💼"
    if ctx == "🏠 personal":
        return "🏠"
    if ctx == "🏃 health":
        return "🏃"
    if ctx == "🤝 hk":
        return "🤝"
    return "📝"

def format_hybrid_digest(tasks: list[dict]) -> tuple[str, list[dict]]:
    """Main digest message in product layout: weather + Today + This Week."""
    del tasks  # counts and sections are always computed fresh
    overdue, today_tasks, this_week, backlog = _get_tasks_by_deadline_horizon()
    _ = backlog
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    lines = [f"☀️ *{date_str}*", format_digest_weather_card(), ""]

    ordered: list[dict] = []
    n = 1
    today_bucket = overdue + today_tasks

    lines.append("📌 *Today*")
    if today_bucket:
        for task in today_bucket:
            lines.append(f"{num_emoji(n)} {task['name']}  {notion_tasks._context_label(task)}")
            ordered.append(task)
            n += 1
    else:
        lines.append("✅ Nothing — all clear!")
    lines.append("")

    lines.append("🗓️ *This Week*")
    if this_week:
        for task in this_week:
            lines.append(f"{num_emoji(n)} {task['name']}  {notion_tasks._context_label(task)}")
            ordered.append(task)
            n += 1
    else:
        lines.append("✅ Nothing — all clear!")

    lines.append("")
    if ordered:
        lines.append("_Reply `done 1`, `done 1,3`, or `done: task name` to complete_")

    return "\n".join(lines).strip(), ordered

def format_daily_digest(
    tasks: list[dict],
    habits: list[dict] | None = None,
    weather_mode: str = "today",
) -> tuple[str, list[dict]]:
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    habits = habits or []
    if not tasks and not habits:
        return f"☀️ *{date_str}*\n\nAll clear — no tasks or habits pending right now! 🎉", []

    today_str = local_today().isoformat()
    overdue = [t for t in tasks if t["deadline"] and t["deadline"] < today_str]
    today_now = [t for t in tasks if t["auto_horizon"] == "🔴 Today" and t not in overdue]
    carryover = [t for t in tasks if t not in overdue and t not in today_now]

    lines, ordered, n = [f"☀️ *{date_str}*"], [], 1
    weather_block = format_weather_block(wx.fetch_weather(weather_mode), label="🌤️")
    lines.append(weather_block or weather_unavailable_digest_line())
    lines.append("")

    if overdue:
        lines.append("🚨 *Overdue*")
        for t in overdue:
            lines.append(f"{num_emoji(n)}{context_emoji(t.get('context'))} {t['name']}")
            ordered.append(t); n += 1
        lines.append("")

    if today_now:
        lines.append("📌 *Today*")
        for t in today_now:
            lines.append(f"{num_emoji(n)}{context_emoji(t.get('context'))} {t['name']}")
            ordered.append(t); n += 1
        lines.append("")

    if carryover:
        lines.append("🔁 *Carry-over (still open)*")
        for t in carryover:
            lines.append(f"{num_emoji(n)}{context_emoji(t.get('context'))} {t['name']} · {t['auto_horizon']}")
            ordered.append(t); n += 1
        lines.append("")

    if ordered:
        lines.append("\n_Reply `done 1`, `done 1,3`, or `done: task name` to mark complete | `cancel` to dismiss_")
    return "\n".join(lines), ordered

def format_week_view(view_type: str) -> tuple[str, list[dict]]:
    """Return the This Week or Backlog expanded view."""
    _, _, this_week, backlog = _get_tasks_by_deadline_horizon()

    if view_type == "week":
        title = "🟠 *This Week (2–7 days)*"
        tasks = this_week
        max_display = None
    elif view_type == "backlog":
        title = "⚪ *Backlog (7+ days)*"
        tasks = backlog
        max_display = 20
    else:
        raise ValueError("view_type must be 'week' or 'backlog'")

    lines = [title]
    if not tasks:
        lines.append("✅ Nothing — all clear!")
        lines.append("")
        lines.append("_Tap items below to adjust urgency 👇_")
        return "\n".join(lines), []

    shown = tasks
    hidden_count = 0
    if max_display is not None and len(tasks) > max_display:
        shown = tasks[:max_display]
        hidden_count = len(tasks) - max_display

    for i, task in enumerate(shown, 1):
        lines.append(f"{num_emoji(i)} {task['name']}  {notion_tasks._context_label(task)}")

    if hidden_count:
        lines.append("")
        lines.append(f"... and {hidden_count} more (view in Notion)")

    lines.append("")
    lines.append("_Tap items below to adjust urgency 👇_")

    return "\n".join(lines), shown

def format_sunday_intro(week_tasks: list[dict], month_tasks: list[dict]) -> tuple[str, list[dict]]:
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    if not week_tasks and not month_tasks:
        return f"🔁 *Weekly Review — {date_str}*\n\nNothing in This Week or This Month — clean slate! 🎉", []
    lines, ordered, n = [f"🔁 *Weekly Review — {date_str}*\n"], [], 1
    if week_tasks:
        lines.append("🟠 *This Week*")
        for t in week_tasks:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t); n += 1
        lines.append("")
    if month_tasks:
        lines.append("🟡 *This Month*")
        for t in month_tasks:
            lines.append(f"{num_emoji(n)} {t['name']}  {t['context']}")
            ordered.append(t); n += 1
    lines.append("\n_Reply `review 1` or `review 1,3` to reassign urgency_")
    return "\n".join(lines), ordered

def format_reminder_snapshot(mode: str = "priority", limit: int = 8) -> str:
    today = local_today()
    today_str = today.isoformat()
    date_str = datetime.now(TZ).strftime("%A, %B %-d")
    all_tasks = notion_tasks.get_all_active_tasks(notion, NOTION_DB_ID)
    overdue = [t for t in all_tasks if t["deadline"] and t["deadline"] < today_str]
    today_tasks = [t for t in all_tasks if t.get("deadline") == today_str and t not in overdue]
    quick_refresh_tasks = notion_tasks.get_quick_refresh_tasks(notion, NOTION_DB_ID, limit=max(limit, 10))
    open_count = len(all_tasks)

    if mode == "all_open":
        ordered = quick_refresh_tasks
        header = f"📋 *To Do (Due ≤ 7 Days) — {date_str}*"
    else:
        ordered = quick_refresh_tasks
        header = f"🔔 *Reminder — {date_str}*"

    lines = []

    if mode == "all_open":
        five_day_cutoff = (today + timedelta(days=5)).isoformat()

        def is_personal(task: dict) -> bool:
            ctx = (task.get("context") or "").lower()
            return "personal" in ctx or "🏠" in ctx

        week_focus = [
            t for t in all_tasks
            if t.get("deadline")
            and today_str <= t["deadline"] <= five_day_cutoff
            and (t.get("auto_horizon") == "🔴 Today" or is_personal(t))
        ]
        week_focus = sorted(
            week_focus,
            key=lambda t: (t.get("deadline") or "9999-12-31", t.get("name", "").lower()),
        )

        if week_focus:
            lines.append("🟠 *This Week*")
            for t in week_focus[:5]:
                lines.append(f"{t['name']} | {t['deadline']}")
            lines.append("")

    lines.extend([
        header,
        "",
        f"Open: *{open_count}*  ·  Overdue: *{len(overdue)}*  ·  Today: *{len(today_tasks)}*",
        "",
    ])

    if not ordered:
        lines.append("✅ No Personal/Work tasks due within the next 7 days.")
    else:
        for idx, task in enumerate(ordered[:limit], start=1):
            deadline = f" · due {task['deadline']}" if task.get("deadline") else ""
            lines.append(f"{num_emoji(idx)} {task['name']}  {task['context']} · {task['auto_horizon']}{deadline}")
        if len(ordered) > limit:
            lines.append(f"\n…and *{len(ordered) - limit}* more.")

    lines.append("\n_You can still type normally to add tasks anytime._")
    return "\n".join(lines)

def format_batch_summary(results: list[dict]) -> str:
    captured   = [r for r in results if r["status"] == "captured"]
    duplicates = [r for r in results if r["status"] == "duplicate"]
    errors     = [r for r in results if r["status"] == "error"]
    lines = []
    if captured:
        groups: dict[tuple, list[dict]] = {}
        for r in captured:
            groups.setdefault((r["horizon_label"], r["context"]), []).append(r)
        lines.append("✅ Captured!")
        for (horizon, ctx), items in groups.items():
            for r in items:
                recur_tag = f"  _{r['recurring']}_" if r.get("recurring", "None") != "None" else ""
                lines.append(f"📝 {r['name']}{recur_tag}")
            lines.append(f"🕐 {horizon}  {ctx}  · _Saved to Notion_")
            lines.append("")
    if duplicates:
        lines.append("⚠️ *Already on your list* (skipped):")
        for r in duplicates:
            dup = r["duplicate"]
            lines.append(f"  · {r['name']}  _{dup.get('auto_horizon','')} {dup.get('context','')}_")
        lines.append("")
    if errors:
        lines.append("❌ *Couldn't capture*:")
        for r in errors:
            lines.append(f"  · {r['name']}")
    return "\n".join(lines).strip()

def format_weather_block(weather: dict | None, label: str = "🌤️") -> str:
    """Format weather payload into digest-friendly text."""
    def fmt_temp_pair(temp_c: float | int) -> tuple[str, str]:
        temp_f = round((float(temp_c) * 9 / 5) + 32)
        temp_c_rounded = round(float(temp_c))
        return (f"{temp_c_rounded}°C", f"{temp_f}°F")

    if not weather:
        return ""
    if "temp_high" in weather and "temp_low" in weather:
        high_c, high_f = fmt_temp_pair(weather["temp_high"])
        low_c, low_f = fmt_temp_pair(weather["temp_low"])
        return (
            f"{label} {weather['condition']} · C: High {high_c} / Low {low_c}\n"
            f"F: High {high_f} / Low {low_f} · 💧{weather.get('precip_chance', 0)}%"
        )
    temp_c, temp_f = fmt_temp_pair(weather["temp"])
    return f"{label} C: {temp_c} · F: {temp_f} ({weather['condition']})"

def format_weather_snapshot() -> str:
    """Compose a richer weather snapshot for quick access."""
    lines = [f"📍 Weather · {wx.current_location}"]
    current = wx.fetch_weather("current")
    if current:
        temp_c = int(round(float(current.get("temp", 0))))
        temp_f = int(round((temp_c * 9 / 5) + 32))
        lines.append(f"🌤️ Now: {temp_c}°C / {temp_f}°F · {current.get('condition', 'Unknown')}")

    daily = wx.fetch_daily_weather(days=5)
    if daily:
        def day_block(title: str, day: dict, icon: str) -> list[str]:
            high_c = int(round(float(day.get("temp_high", 0))))
            low_c = int(round(float(day.get("temp_low", 0))))
            high_f = int(round((high_c * 9 / 5) + 32))
            low_f = int(round((low_c * 9 / 5) + 32))
            uvi = float(day.get("uvi", 0))
            sunscreen = "Recommended if outdoors" if uvi >= 3 else "Usually optional"
            lines = [
                "",
                f"{icon} {title}",
                f"🌥️ {day.get('description', day.get('condition', 'Unknown'))}",
                f"🌡️ High / Low: {high_c}°C / {low_c}°C",
                f"   Imperial: {high_f}°F / {low_f}°F",
                f"💧 Rain: {int(day.get('precip_chance', 0))}%",
            ]
            if _should_show_uv_guidance(uvi, sunrise_iso=day.get("sunrise"), sunset_iso=day.get("sunset")):
                lines.extend([
                    f"🔆 UV: {uvi:.1f} {uvi_level_text(uvi)} {uvi_emoji(uvi)}",
                    f"🧴 Sunscreen: {sunscreen}",
                ])
            return lines

        lines.extend(day_block("Today", daily[0], "📅"))
        if len(daily) > 1:
            lines.extend(day_block("Tomorrow", daily[1], "🌙"))
        lines.extend(["", "📆 Next 5 Days"])
        for day in daily:
            dt = datetime.fromisoformat(day["date"]).strftime("%a")
            high_c = int(day["temp_high"])
            low_c = int(day["temp_low"])
            high_f = int(round((high_c * 9 / 5) + 32))
            low_f = int(round((low_c * 9 / 5) + 32))
            uvi = float(day.get("uvi", 0))
            lines.append(
                f"{dt}  {condition_emoji(day.get('condition', 'Unknown'))} {high_f}°/{low_f}° · "
                f"💧{int(day.get('precip_chance', 0))}% · UV {uvi:.1f} {uvi_emoji(uvi)}"
            )
    elif len(lines) == 1:
        if not wx.OPENWEATHER_KEY:
            lines.append("Weather is unavailable: OPENWEATHER_KEY is missing or invalid.")
        else:
            lines.append("Weather is unavailable. Verify OpenWeather location (try /location) and API key access.")
    return "\n".join(lines)


def condition_emoji(condition: str) -> str:
    mapping = {
        "Clear": "☀️",
        "Clouds": "☁️",
        "Rain": "🌧️",
        "Drizzle": "🌦️",
        "Thunderstorm": "⛈️",
        "Snow": "❄️",
        "Mist": "🌫️",
    }
    return mapping.get((condition or "").strip(), "🌥️")


def uvi_level_text(uvi: float) -> str:
    if uvi <= 2:
        return "Low"
    if uvi <= 5:
        return "Moderate"
    if uvi <= 7:
        return "High"
    if uvi <= 10:
        return "Very High"
    return "Extreme"

def append_location_to_weather_block(weather_block: str, location_label: str) -> str:
    """Attach compact location to the final line of a weather block."""
    if not weather_block:
        return weather_block
    if not location_label:
        return weather_block
    block_lines = weather_block.splitlines()
    block_lines[-1] = f"{block_lines[-1]} · 📍{location_label}"
    return "\n".join(block_lines)

def weather_unavailable_digest_line() -> str:
    """Digest fallback text when weather cannot be rendered."""
    if wx.current_lat is not None and wx.current_lon is not None and wx.current_location:
        return f"🌤️ Weather unavailable for {wx.current_location} — send /weather to retry or /location to update"
    if wx.current_location:
        return f"🌤️ Weather unavailable. Last location: {wx.current_location} — send /location (city/state/country or ZIP)"
    return "🌤️ Weather unavailable — set with /location (city/state/country or ZIP)"

def format_digest_weather_card() -> str:
    """Digest weather card in the compact layout requested by product."""
    daily = wx.fetch_daily_weather(days=1)
    today = daily[0] if daily else None
    if not today:
        # Fallback for environments where One Call 3.0 is unavailable/slow.
        today_basic = wx.fetch_weather("today")
        if today_basic:
            today = {
                "description": today_basic.get("condition", "Unknown"),
                "condition": today_basic.get("condition", "Unknown"),
                "temp_high": today_basic.get("temp_high", 0),
                "temp_low": today_basic.get("temp_low", 0),
                "precip_chance": today_basic.get("precip_chance", 0),
                "uvi": 0.0,
            }
    if not today:
        return weather_unavailable_digest_line()
    current = wx.fetch_weather("current")
    location = digest_location_label() or (wx.current_location or "Unknown location")
    condition = today.get("description", today.get("condition", "Unknown"))
    high_c = int(today.get("temp_high", 0))
    low_c = int(today.get("temp_low", 0))
    high_f = int(round((high_c * 9 / 5) + 32))
    low_f = int(round((low_c * 9 / 5) + 32))
    rain = int(today.get("precip_chance", 0))
    uvi = float(today.get("uvi", 0))
    sunscreen = "Recommended if outdoors" if uvi >= 3 else "Usually optional"
    current_icon = condition_emoji(current.get("condition", "")) if current else condition_emoji(today.get("condition", ""))
    lines = [
        f"📍 {location} · {current_icon} {condition}",
        f"🌡️ {high_c}°C / {low_c}°C",
        f"    {high_f}°F / {low_f}°F",
        f"💧 Rain chance: {rain}%",
    ]
    if _should_show_uv_guidance(uvi, sunrise_iso=today.get("sunrise"), sunset_iso=today.get("sunset")):
        lines.extend([
            f"🔆 UV Index: {uvi:.1f} · {uvi_level_text(uvi)}",
            f"🧴 Sunscreen: {sunscreen}",
        ])
    return "\n".join(lines)


def _should_show_uv_guidance(
    uvi: float,
    now_dt: datetime | None = None,
    sunrise_iso: str | None = None,
    sunset_iso: str | None = None,
) -> bool:
    """Show UV/sunscreen guidance only when UV is meaningful and during daytime."""
    if uvi < 3:
        return False
    now = now_dt or datetime.now(TZ)
    if sunrise_iso and sunset_iso:
        try:
            sunrise = datetime.fromisoformat(sunrise_iso)
            sunset = datetime.fromisoformat(sunset_iso)
            return sunrise <= now <= sunset
        except Exception:
            pass
    return 6 <= now.hour < 18

def digest_location_label() -> str:
    """Compact location label for digest weather line (City, ST or country)."""
    parts = [p.strip() for p in (wx.current_location or "").split(",") if p.strip()]
    if not parts:
        return ""
    if len(parts) >= 3:
        city, state, country = parts[0], parts[1], parts[2]
        country_upper = country.upper()
        if country_upper in {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}:
            us_state_map = {
                "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
                "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
                "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
                "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
                "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
                "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
                "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
                "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
                "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
                "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
            }
            state_clean = state.strip()
            state_abbr = us_state_map.get(state_clean.lower(), state_clean.upper() if len(state_clean) <= 3 else state_clean[:2].upper())
            return f"{city}, {state_abbr}"
        return f"{city}, {country}"
    if len(parts) == 2:
        return f"{parts[0]}, {parts[1]}"
    return parts[0]

def mute_status_text() -> str:
    """Human-friendly mute status line."""
    if is_muted() and mute_until:
        return f"🔕 Digests paused until {mute_until.strftime('%Y-%m-%d %H:%M %Z')}."
    return "🔔 Digests are active."

def uvi_emoji(uvi: float) -> str:
    """WHO UV index colour scale."""
    if uvi <= 2:
        return "🟢"
    if uvi <= 5:
        return "🟡"
    if uvi <= 7:
        return "🟠"
    if uvi <= 10:
        return "🔴"
    return "🟣"
