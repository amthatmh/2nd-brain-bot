from __future__ import annotations

import re


def load_digest_slots(*, rows: list[dict], logger) -> list[dict]:
    """Build normalized digest slots from Digest Selector rows."""
    context_map = {
        "🏠 Personal": "🏠 Personal",
        "💼 Work": "💼 Work",
        "🏃 Health": "🏃 Health",
        "🤝 HK": "🤝 HK",
    }

    def first_text(prop: dict) -> str:
        rich_text = prop.get("rich_text", [])
        if rich_text:
            return (rich_text[0].get("plain_text") or "").strip()
        title = prop.get("title", [])
        if title:
            return (title[0].get("plain_text") or "").strip()
        select = prop.get("select")
        if select and select.get("name"):
            return (select.get("name") or "").strip()
        date_value = prop.get("date") or {}
        if isinstance(date_value, dict) and date_value.get("start"):
            return str(date_value.get("start")).strip()
        return ""

    def normalize_slot_time(raw: str) -> str | None:
        value = (raw or "").strip()
        if not value:
            return None

        iso_match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::\d{2})?", value)
        if iso_match:
            hh = int(iso_match.group(1))
            mm = int(iso_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"
            return None

        ampm_match = re.fullmatch(r"(\d{1,2}):(\d{2})\s*([AaPp][Mm])", value)
        if ampm_match:
            hh = int(ampm_match.group(1))
            mm = int(ampm_match.group(2))
            ampm = ampm_match.group(3).lower()
            if not (1 <= hh <= 12 and 0 <= mm <= 59):
                return None
            hh = (0 if hh == 12 else hh) if ampm == "am" else (12 if hh == 12 else hh + 12)
            return f"{hh:02d}:{mm:02d}"

        dt_match = re.search(r"T(\d{2}):(\d{2})", value)
        if dt_match:
            hh = int(dt_match.group(1))
            mm = int(dt_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"

        internal_match = re.search(r"\b(\d{1,2}):(\d{2})\b", value)
        if internal_match:
            hh = int(internal_match.group(1))
            mm = int(internal_match.group(2))
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"
        return None

    slots: list[dict] = []
    seen_slot_keys: set[tuple[str, bool]] = set()

    for row in rows:
        props = row.get("properties", {})
        slot_time_raw = first_text(props.get("Time", {}))
        slot_time = normalize_slot_time(slot_time_raw)
        if not slot_time:
            logger.warning("Skipping digest selector row with invalid Time=%r", slot_time_raw)
            continue

        ww = props.get("Weekday/Weekend", {}).get("select")
        ww_name = (ww.get("name") if ww else "").strip()
        ww_norm = ww_name.lower()
        is_all = ww_norm in {"all", "every day", "daily", "always"}
        is_weekday_val = ww_norm in {"weekday", "weekdays", "mon-fri"}
        is_weekend_val = ww_norm in {"weekend", "weekends", "sat,sun", "sat/sun"}
        if not (is_all or is_weekday_val or is_weekend_val):
            logger.warning("Skipping digest selector row with invalid Weekday/Weekend=%r", ww_name)
            continue

        weekday_variants = [True, False] if is_all else ([True] if is_weekday_val else [False])
        include_habits = bool(props.get("Habits", {}).get("checkbox", False))
        max_items_raw = props.get("Max Items", {}).get("number")
        max_items = int(max_items_raw) if isinstance(max_items_raw, (int, float)) else None
        contexts = [context_label for prop_name, context_label in context_map.items() if bool(props.get(prop_name, {}).get("checkbox", False))]
        is_signoff = bool(props.get("Signoff", {}).get("checkbox", False))
        include_weather = bool(props.get("Weather", {}).get("checkbox", False))
        include_uvi = bool(props.get("UVI", {}).get("checkbox", False))

        for is_weekday in weekday_variants:
            slot_key = (slot_time, is_weekday)
            if slot_key in seen_slot_keys:
                logger.warning("Skipping duplicate digest selector slot %s (%s)", slot_time, "weekday" if is_weekday else "weekend")
                continue
            seen_slot_keys.add(slot_key)
            slots.append({"time": slot_time, "is_weekday": is_weekday, "include_habits": include_habits, "max_items": max_items, "contexts": contexts, "is_signoff": is_signoff, "include_weather": include_weather, "include_uvi": include_uvi})

    logger.info("Loaded %d digest selector slot(s) from Notion", len(slots))
    return slots


def pending_habits_for_digest(*, habit_cache: dict[str, dict], time_str: str | None, already_logged_today, is_on_pace) -> list[dict]:
    habits = habit_cache.values() if time_str is None else [h for h in habit_cache.values() if h.get("time") == time_str]
    pending: list[dict] = []
    for habit in sorted(habits, key=lambda h: h["sort"]):
        pid = habit["page_id"]
        if already_logged_today(pid) or is_on_pace(habit):
            continue
        pending.append(habit)
    return pending
