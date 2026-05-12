
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx

from second_brain.config import WEATHER_LOCATION, TZ, CLAUDE_MODEL
from second_brain.config import OPENWEATHER_KEY, WEATHER_LOCATION, TZ, CLAUDE_MODEL
log = logging.getLogger(__name__)
notion = None
NOTION_ENV_DB = os.environ.get("ENV_DB_ID", "").strip()


OPENWEATHER_KEY = os.environ.get("OPENWEATHER_KEY", "") or os.environ.get("OPENWEATHER_API_KEY", "")
def _openweather_key() -> str:
    import os
    return (
        os.environ.get("OPENWEATHER_KEY", "").strip()
        or os.environ.get("OPENWEATHER_API_KEY", "").strip()
    )

def _resolve_state_dir() -> Path:
    override = os.environ.get("BOT_STATE_DIR", "").strip()
    if override:
        state_dir = Path(override).expanduser()
    elif Path("/data").exists():
        state_dir = Path("/data")
    elif Path.home().exists():
        state_dir = Path.home() / ".second_brain_bot"
    else:
        state_dir = Path.cwd()
    state_dir.mkdir(parents=True, exist_ok=True)
    return state_dir


STATE_DIR = _resolve_state_dir()
location_state_file = STATE_DIR / "location_state.json"
location_state_fallback_file = Path(__file__).resolve().parents[1] / ".second_brain_location_state.json"
location_history_file = STATE_DIR / "location_history.json"
location_history_fallback_file = Path(__file__).resolve().parents[1] / ".second_brain_location_history.json"

current_location: str = ""
current_lat: float | None = None
current_lon: float | None = None
weather_cache: dict[str, dict] = {
    "current": {"timestamp": None, "data": None},
    "today": {"timestamp": None, "data": None},
    "tomorrow": {"timestamp": None, "data": None},
    "daily": {"timestamp": None, "data": None},
}


def _location_state_files() -> list[Path]:
    """Return ordered location state file paths (primary first, durable fallback second)."""
    return [location_state_file, location_state_fallback_file]


def _location_history_files() -> list[Path]:
    """Return ordered location history paths (primary first, durable fallback second)."""
    return [location_history_file, location_history_fallback_file]


def save_location_history(raw_text: str) -> None:
    text = (raw_text or "").strip()
    if not text:
        return
    history: list[str] = []
    for file_path in _location_history_files():
        try:
            if file_path.exists():
                payload = json.loads(file_path.read_text() or "[]")
                if isinstance(payload, list):
                    history = [str(item).strip() for item in payload if str(item).strip()]
                    break
        except Exception as e:
            log.warning("Failed reading location history from %s: %s", file_path, e)
    history = [item for item in history if item.lower() != text.lower()]
    history.append(text)
    history = history[-25:]
    raw = json.dumps(history)
    for file_path in _location_history_files():
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(raw)
        except Exception as e:
            log.error("Failed saving location history to %s: %s", file_path, e)


def recover_location_from_history(claude) -> bool:
    history: list[str] = []
    for file_path in _location_history_files():
        try:
            if not file_path.exists():
                continue
            payload = json.loads(file_path.read_text() or "[]")
            if isinstance(payload, list):
                history = [str(item).strip() for item in payload if str(item).strip()]
                if history:
                    break
        except Exception as e:
            log.warning("Failed loading location history from %s: %s", file_path, e)
    for candidate in reversed(history):
        if set_location_smart(candidate, claude):
            log.info("Recovered weather location from history: %s", current_location)
            return True
    return False


def save_location_state(location: str) -> None:
    payload = {"last_weather_location": location}
    raw = json.dumps(payload)
    saved_any = False
    for file_path in _location_state_files():
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(raw)
            saved_any = True
        except Exception as e:
            log.error("Failed saving location state to %s: %s", file_path, e)
    if not saved_any:
        log.error("Failed saving location state to all configured paths")


def load_location_state() -> None:
    global current_location, current_lat, current_lon
    current_location = ""
    current_lat = None
    current_lon = None
    for file_path in _location_state_files():
        try:
            if not file_path.exists():
                continue
            payload = json.loads(file_path.read_text() or "{}")
            current_location = (payload.get("last_weather_location") or "").strip()
            if file_path != location_state_file:
                save_location_state(current_location)
            return
        except Exception as e:
            log.warning("Failed loading location state from %s: %s", file_path, e)


def load_notion_env_location() -> bool:
    global current_location, current_lat, current_lon
    if not NOTION_ENV_DB or notion is None:
        return False
    try:
        results = notion.databases.query(database_id=NOTION_ENV_DB, filter={"property": "Name", "title": {"equals": "Location"}})
        rows = results.get("results", [])
        if not rows:
            return False
        props = rows[0]["properties"]
        value_parts = props.get("Value", {}).get("rich_text", [])
        value = value_parts[0]["text"]["content"].strip() if value_parts else ""
        lat = props.get("Lat", {}).get("number")
        lon = props.get("Lon", {}).get("number")
        if value and lat is not None and lon is not None:
            current_location = value
            current_lat = float(lat)
            current_lon = float(lon)
            log.info("Location loaded from Notion ENV: %s (%.4f, %.4f)", value, lat, lon)
            return True
        return False
    except Exception as e:
        log.warning("load_notion_env_location failed: %s", e)
        return False


def save_notion_env_location(location: str, lat: float, lon: float) -> None:
    if not NOTION_ENV_DB or notion is None:
        return
    try:
        results = notion.databases.query(database_id=NOTION_ENV_DB, filter={"property": "Name", "title": {"equals": "Location"}})
        rows = results.get("results", [])
        props = {"Value": {"rich_text": [{"text": {"content": location}}]}, "Lat": {"number": lat}, "Lon": {"number": lon}}
        if rows:
            notion.pages.update(page_id=rows[0]["id"], properties=props)
        else:
            props["Name"] = {"title": [{"text": {"content": "Location"}}]}
            notion.pages.create(parent={"database_id": NOTION_ENV_DB}, properties=props)
        log.info("Location saved to Notion ENV: %s (%.4f, %.4f)", location, lat, lon)
    except Exception as e:
        log.error("save_notion_env_location failed: %s", e)


def set_location(location: str) -> bool:
    global current_location, current_lat, current_lon
    openweather_key = _openweather_key()
    if not openweather_key:
        return False
    try:
        resp = httpx.get("https://api.openweathermap.org/geo/1.0/direct", params={"q": location, "limit": 1, "appid": openweather_key}, timeout=10)
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return False
        row = rows[0]
        lat = row.get("lat")
        lon = row.get("lon")
        if lat is None or lon is None:
            return False
        display_name = row.get("name") or location
        state = row.get("state")
        country = row.get("country")
        pretty = ", ".join([p for p in [display_name, state, country] if p])
        current_location = pretty
        current_lat = float(lat)
        current_lon = float(lon)
        clear_weather_cache()
        save_location_state(current_location)
        save_notion_env_location(current_location, float(lat), float(lon))
        return True
    except Exception as e:
        log.error("Location geocode failed for %s: %s", location, e)
        return False


def _location_candidates(text: str) -> list[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return []
    candidates: list[str] = [cleaned]
    normalized = re.sub(r"\s+", " ", cleaned)
    if normalized != cleaned:
        candidates.append(normalized)
    phrase_patterns = [r"(?:weather|forecast)\s+(?:for|in|at)\s+(.+)$", r"(?:set|use|change|update)\s+(?:my\s+)?location\s+(?:to|as)\s+(.+)$", r"(?:i(?:'| a)?m|im)\s+in\s+(.+)$", r"(?:for|in|at)\s+(.+)$"]
    for pattern in phrase_patterns:
        m = re.search(pattern, normalized, flags=re.IGNORECASE)
        if m:
            fragment = m.group(1).strip(" .!?")
            if fragment:
                candidates.append(fragment)
    slash_fixed = re.sub(r"\s*/\s*", ", ", normalized)
    if slash_fixed != normalized:
        candidates.append(slash_fixed)
    comma_spaced = re.sub(r"\s*,\s*", ", ", slash_fixed)
    if comma_spaced != slash_fixed:
        candidates.append(comma_spaced)
    no_zip = re.sub(r"\b\d{5}(?:-\d{4})?\b", "", comma_spaced).strip(" ,")
    if no_zip and no_zip != comma_spaced:
        candidates.append(no_zip)
    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", comma_spaced)
    if zip_match:
        candidates.append(zip_match.group(0))
    state_map = {"illinois": "IL", "california": "CA", "new york": "NY", "texas": "TX", "florida": "FL", "washington": "WA", "massachusetts": "MA", "georgia": "GA", "colorado": "CO", "arizona": "AZ"}
    lowered = no_zip.lower()
    for full, abbr in state_map.items():
        if full in lowered:
            candidates.append(re.sub(rf"\b{re.escape(full)}\b", abbr, no_zip, flags=re.IGNORECASE))
    deduped: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        key = c.strip().lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(c.strip())
    return deduped


def normalize_location_with_claude(text: str, claude) -> list[str]:
    prompt = f'''Extract a weather location query from user input.
Input: "{text}"

Return ONLY valid JSON:
{{
  "city": "city name or null",
  "state_code": "2-letter US state code or null",
  "country_code": "2-letter country code or null",
  "postal_code": "postal/zip code or null",
  "normalized_query": "best query for OpenWeather geocoding, e.g. Chicago, IL, US",
  "alternates": ["up to 3 alternate queries"]
}}'''
    try:
        resp = claude.messages.create(model=CLAUDE_MODEL, max_tokens=180, messages=[{"role": "user", "content": prompt}])
        raw = re.sub(r"```(?:json)?|```", "", resp.content[0].text.strip()).strip()
        payload = json.loads(raw)
        candidates = []
        normalized = (payload.get("normalized_query") or "").strip()
        if normalized:
            candidates.append(normalized)
        alt = payload.get("alternates") or []
        if isinstance(alt, list):
            candidates.extend(str(a).strip() for a in alt if str(a).strip())
        city = (payload.get("city") or "").strip()
        state_code = (payload.get("state_code") or "").strip().upper()
        country_code = (payload.get("country_code") or "").strip().upper()
        if city:
            if state_code and country_code:
                candidates.append(f"{city}, {state_code}, {country_code}")
            if state_code:
                candidates.append(f"{city}, {state_code}")
            if country_code:
                candidates.append(f"{city}, {country_code}")
            candidates.append(city)
        merged: list[str] = []
        seen: set[str] = set()
        for c in candidates + _location_candidates(text):
            key = c.strip().lower()
            if key and key not in seen:
                seen.add(key)
                merged.append(c.strip())
        return merged
    except Exception as e:
        log.warning("Claude location normalization failed for %r: %s", text, e)
        return _location_candidates(text)


def set_location_smart(user_text: str, claude) -> bool:
    for query in normalize_location_with_claude(user_text, claude):
        if set_location(query):
            save_location_history(user_text)
            return True
    zip_match = re.search(r"\b\d{5}(?:-\d{4})?\b", user_text or "")
    openweather_key = _openweather_key()
    if zip_match and openweather_key:
        zip_value = zip_match.group(0)
        try:
            resp = httpx.get("https://api.openweathermap.org/geo/1.0/zip", params={"zip": zip_value, "appid": openweather_key}, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
            lat = payload.get("lat")
            lon = payload.get("lon")
            if lat is not None and lon is not None:
                if set_location(f"{payload.get('name') or zip_value}, {payload.get('country') or 'US'}"):
                    save_location_history(user_text)
                    return True
        except Exception as e:
            log.warning("ZIP location fallback failed for %s: %s", zip_value, e)
    return False


def clear_weather_cache() -> None:
    """Clear all weather payloads after the active location changes."""
    for key in weather_cache:
        weather_cache[key] = {"timestamp": None, "data": None}


def fetch_weather(forecast_type: str = "current", force_refresh: bool = False) -> dict | None:
    if forecast_type not in {"current", "today", "tomorrow"}:
        return None
    openweather_key = _openweather_key()
    if not openweather_key:
        return None
    cache_entry = weather_cache.get(forecast_type, {"timestamp": None, "data": None})
    now = datetime.now(TZ)
    ttl = timedelta(hours=24 if forecast_type == "tomorrow" else 3)
    if not force_refresh and cache_entry.get("timestamp") and cache_entry.get("data"):
        if now - cache_entry["timestamp"] <= ttl:
            return cache_entry["data"]
    try:
        if current_lat is None or current_lon is None:
            if not set_location(WEATHER_LOCATION):
                return None
        if forecast_type == "current":
            resp = httpx.get("https://api.openweathermap.org/data/2.5/weather", params={"lat": current_lat, "lon": current_lon, "appid": openweather_key, "units": "metric"}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            sunrise_ts = data.get("sys", {}).get("sunrise")
            sunset_ts = data.get("sys", {}).get("sunset")
            result = {
                "temp": round(data.get("main", {}).get("temp", 0)),
                "feels_like": round(data.get("main", {}).get("feels_like", 0)),
                "condition": (data.get("weather") or [{}])[0].get("main", "Unknown"),
                "precip_chance": int(round((data.get("pop") or 0) * 100)),
                "sunrise": datetime.fromtimestamp(sunrise_ts, timezone.utc).astimezone(TZ).isoformat() if sunrise_ts else None,
                "sunset": datetime.fromtimestamp(sunset_ts, timezone.utc).astimezone(TZ).isoformat() if sunset_ts else None,
            }
        else:
            resp = httpx.get("https://api.openweathermap.org/data/2.5/forecast", params={"lat": current_lat, "lon": current_lon, "appid": openweather_key, "units": "metric"}, timeout=10)
            resp.raise_for_status()
            rows = resp.json().get("list", [])
            target = datetime.now(TZ).date() + timedelta(days=1 if forecast_type == "tomorrow" else 0)
            bucket = []
            for row in rows:
                dt_utc = datetime.fromtimestamp(row["dt"], timezone.utc)
                local_dt = dt_utc.astimezone(TZ)
                if local_dt.date() == target:
                    bucket.append(row)
            if not bucket:
                return None
            highs = [r.get("main", {}).get("temp_max", 0) for r in bucket]
            lows = [r.get("main", {}).get("temp_min", 0) for r in bucket]
            pops = [r.get("pop", 0) for r in bucket]
            conds = [(r.get("weather") or [{}])[0].get("main", "Unknown") for r in bucket]
            mode_condition = max(set(conds), key=conds.count)
            result = {"temp_high": round(max(highs)), "temp_low": round(min(lows)), "condition": mode_condition, "precip_chance": int(round(max(pops) * 100))}
        weather_cache[forecast_type] = {"timestamp": now, "data": result}
        return result
    except Exception as e:
        log.error("Weather fetch failed (%s): %s", forecast_type, e)
        return None


def _forecast_rows_for_coordinates(lat: float, lon: float, *, num_days: int, start_date: date | None = None, end_date: date | None = None) -> list[dict]:
    """Fetch and bucket OpenWeather 5-day/3-hour forecast rows for coordinates."""
    import os as _os
    _key = _os.environ.get("OPENWEATHER_KEY", "").strip() or OPENWEATHER_KEY
    log.info("_forecast_rows_for_coordinates: key_present=%s num_days=%d start=%s end=%s", bool(_key), num_days, start_date, end_date)
    if not _key or num_days <= 0:
        log.warning("_forecast_rows_for_coordinates: early exit key_present=%s num_days=%d", bool(_key), num_days)
        return []
    try:
        resp = httpx.get(
            "https://api.openweathermap.org/data/2.5/forecast",
            params={"lat": lat, "lon": lon, "appid": _key, "units": "metric", "cnt": 40},
            timeout=10,
        )
        resp.raise_for_status()
    except Exception as e:
        log.error("_forecast_rows_for_coordinates: HTTP failed lat=%s lon=%s: %s", lat, lon, e)
        return []
    try:
        rows = resp.json().get("list", [])
        log.info(
            "_forecast_rows_for_coordinates: API returned %d slots for lat=%s lon=%s start=%s end=%s",
            len(rows),
            lat,
            lon,
            start_date,
            end_date,
        )
        today = datetime.now(TZ).date()
        if start_date is None:
            start_date = today
        if end_date is None:
            end_date = start_date + timedelta(days=num_days - 1)
        log.info("_forecast_rows_for_coordinates: filtering for %s to %s", start_date, end_date)

        buckets: dict[date, dict] = {}
        for row in rows:
            dt_utc = datetime.fromtimestamp(row["dt"], timezone.utc)
            local_dt = dt_utc.astimezone(TZ)
            day = local_dt.date()
            if day < start_date or day > end_date:
                continue
            item = buckets.setdefault(day, {"highs": [], "lows": [], "pops": [], "conds": [], "descriptions": []})
            item["highs"].append(row.get("main", {}).get("temp_max", 0))
            item["lows"].append(row.get("main", {}).get("temp_min", 0))
            item["pops"].append(row.get("pop", 0))
            weather_item = (row.get("weather") or [{}])[0]
            item["conds"].append(weather_item.get("main", "Unknown"))
            item["descriptions"].append(weather_item.get("description", "Unknown"))
        log.info("_forecast_rows_for_coordinates: bucketed %d days: %s", len(buckets), sorted(buckets))

        out: list[dict] = []
        for day in sorted(buckets)[:num_days]:
            item = buckets[day]
            conds = item["conds"] or ["Unknown"]
            descriptions = item["descriptions"] or ["Unknown"]
            out.append(
                {
                    "date": day.isoformat(),
                    "label": day.strftime("%a %b %-d"),
                    "temp_high": round(max(item["highs"])) if item["highs"] else None,
                    "temp_low": round(min(item["lows"])) if item["lows"] else None,
                    "precip_chance": int(round(max(item["pops"]) * 100)) if item["pops"] else 0,
                    "condition": max(set(conds), key=conds.count),
                    "description": max(set(descriptions), key=descriptions.count).title(),
                }
            )
        log.info("_forecast_rows_for_coordinates: returning %d day rows", len(out))
        return out
    except Exception as e:
        log.error("_forecast_rows_for_coordinates: processing failed lat=%s lon=%s: %s", lat, lon, e)
        return []


def fetch_multi_day_forecast(num_days: int) -> list[dict] | None:
    """Return up to num_days of forecast rows for the active weather location."""
    openweather_key = _openweather_key()
    if not openweather_key or num_days <= 0:
        return None
    if current_lat is None or current_lon is None:
        if not set_location(WEATHER_LOCATION):
            return None
    try:
        return _forecast_rows_for_coordinates(float(current_lat), float(current_lon), num_days=num_days)
    except Exception as e:
        log.warning("Multi-day forecast fetch failed for %s: %s", current_location or WEATHER_LOCATION, e)
        return None


def fetch_trip_weather_range(departure_date: str, return_date: str, destination: str) -> list[dict]:
    key = _openweather_key()
    if not key:
        log.warning("fetch_trip_weather_range: OPENWEATHER_KEY not set")
        return []
    try:
        start = datetime.fromisoformat(departure_date).date()
        end = datetime.fromisoformat(return_date).date()
    except Exception:
        return []
    if end < start:
        return []
    try:
        geo = httpx.get("https://api.openweathermap.org/geo/1.0/direct", params={"q": destination, "limit": 1, "appid": key}, timeout=10)
        geo.raise_for_status()
        places = geo.json()
        if not places:
            return []
        lat = places[0].get("lat")
        lon = places[0].get("lon")
        if lat is None or lon is None:
            return []
        log.info("fetch_trip_weather_range: geo resolved %s → lat=%s lon=%s", destination, lat, lon)
        log.info("fetch_trip_weather_range: resolved %s → lat=%s lon=%s, calling forecast", destination, lat, lon)
        try:
            result = _forecast_rows_for_coordinates(float(lat), float(lon), num_days=(end - start).days + 1, start_date=start, end_date=end)
            log.info("fetch_trip_weather_range: got %d rows for %s", len(result), destination)
            return result
        except Exception as e:
            log.error("fetch_trip_weather_range: _forecast_rows_for_coordinates failed for %s: %s", destination, e, exc_info=True)
            return []
    except Exception as e:
        log.warning("Trip weather fetch failed for %s: %s", destination, e)
        return []


def fetch_uvi_data() -> dict | None:
    openweather_key = _openweather_key()
    if not openweather_key or current_lat is None or current_lon is None:
        return None
    try:
        resp = httpx.get("https://api.openweathermap.org/data/3.0/onecall", params={"lat": current_lat, "lon": current_lon, "exclude": "minutely,hourly,alerts", "appid": openweather_key}, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        current_uvi = float(data.get("current", {}).get("uvi", 0))
        daily = data.get("daily", [])
        max_uvi = float(daily[0].get("uvi", 0)) if daily else current_uvi
        log.info(f"UVI — current: {current_uvi}, max: {max_uvi}")
        return {"current": current_uvi, "max": max_uvi}
    except Exception as e:
        log.error(f"UVI fetch error: {e}")
        return None


def fetch_daily_weather(days: int = 5, force_refresh: bool = False) -> list[dict]:
    """Return cached daily weather rows (today forward) from the shared One Call pull."""
    openweather_key = _openweather_key()
    if not openweather_key or days <= 0:
        return []
    if current_lat is None or current_lon is None:
        if not set_location(WEATHER_LOCATION):
            return []

    now = datetime.now(TZ)
    cache_entry = weather_cache.get("daily", {"timestamp": None, "data": None})
    cached_rows = cache_entry.get("data") or []
    if (
        not force_refresh
        and cache_entry.get("timestamp")
        and len(cached_rows) >= days
        and now - cache_entry["timestamp"] <= timedelta(hours=3)
    ):
        return cached_rows[:days]

    try:
        resp = httpx.get(
            "https://api.openweathermap.org/data/3.0/onecall",
            params={
                "lat": current_lat,
                "lon": current_lon,
                "exclude": "minutely,hourly,alerts",
                "appid": openweather_key,
                "units": "metric",
            },
            timeout=8,
        )
        resp.raise_for_status()
        daily = resp.json().get("daily", [])[: max(days, 5)]
        rows: list[dict] = []
        for item in daily:
            weather_item = (item.get("weather") or [{}])[0]
            temps = item.get("temp", {})
            rows.append(
                {
                    "date": datetime.fromtimestamp(item.get("dt", 0), timezone.utc).astimezone(TZ).date().isoformat(),
                    "temp_high": round(temps.get("max", 0)),
                    "temp_low": round(temps.get("min", 0)),
                    "condition": weather_item.get("main", "Unknown"),
                    "description": weather_item.get("description", "Unknown").title(),
                    "precip_chance": int(round(float(item.get("pop", 0)) * 100)),
                    "uvi": float(item.get("uvi", 0)),
                    "sunrise": datetime.fromtimestamp(item.get("sunrise", 0), timezone.utc).astimezone(TZ).isoformat() if item.get("sunrise") else None,
                    "sunset": datetime.fromtimestamp(item.get("sunset", 0), timezone.utc).astimezone(TZ).isoformat() if item.get("sunset") else None,
                }
            )
        weather_cache["daily"] = {"timestamp": now, "data": rows}
        return rows[:days]
    except Exception as e:
        log.error("Daily weather fetch failed: %s", e)
        if cached_rows:
            return cached_rows[:days]
        return []


async def fetch_weather_cache(bot) -> None:
    _ = bot
    if not _openweather_key():
        return
    fetch_weather("current", force_refresh=True)
    fetch_weather("today", force_refresh=True)
    fetch_weather("tomorrow", force_refresh=True)
    fetch_daily_weather(days=5, force_refresh=True)


def uvi_emoji(uvi: float) -> str:
    """Backwards-compatible UVI badge helper for legacy formatter call sites."""
    if uvi >= 8:
        return "🔴"
    if uvi >= 6:
        return "🟠"
    if uvi >= 3:
        return "🟡"
    return "🟢"
    log.debug("Weather cache refreshed")
