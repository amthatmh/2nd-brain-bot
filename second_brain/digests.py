from __future__ import annotations

from datetime import datetime


def manual_digest_config_now(slots: list[dict], now_dt: datetime, is_weekday: bool) -> dict | None:
    """Pick the most recent slot for the provided day type."""
    candidates: list[tuple[int, dict]] = []
    for slot in slots:
        if bool(slot.get("is_weekday")) != is_weekday:
            continue
        try:
            hh, mm = map(int, str(slot.get("time", "")).split(":"))
        except Exception:
            continue
        candidates.append((hh * 60 + mm, slot))

    if not candidates:
        return None

    now_minutes = now_dt.hour * 60 + now_dt.minute
    earlier_or_equal = [item for item in candidates if item[0] <= now_minutes]
    chosen = max(earlier_or_equal, key=lambda x: x[0])[1] if earlier_or_equal else min(candidates, key=lambda x: x[0])[1]
    return {
        "include_habits": bool(chosen.get("include_habits")),
        # Manual Digest requests should always include the weather card; scheduled
        # Digest Selector slots still control weather via get_digest_config().
        "include_weather": True,
        "include_uvi": bool(chosen.get("include_uvi")),
        "include_feel": bool(chosen.get("include_feel")),
        "contexts": chosen.get("contexts"),
        "max_items": chosen.get("max_items"),
    }
