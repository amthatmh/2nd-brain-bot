"""Entertainment logging helpers and handlers."""

from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher

from second_brain.config import (
    NOTION_CINEMA_LOG_DB,
    NOTION_FAVE_DB,
    NOTION_PERFORMANCE_LOG_DB,
    NOTION_SPORTS_LOG_DB,
)
from second_brain.notion import notion_call, notion_call_async
from second_brain.utils import reply_notion_error


log = logging.getLogger(__name__)

entertainment_schemas: dict[str, dict] = {}
pending_sport_competition_map: dict[int, dict] = {}


def parse_explicit_entertainment_log(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None

    normalized = re.sub(r"^\s*/?log\s+", "log ", raw, flags=re.IGNORECASE)
    m = re.match(r"^log\s+(cinema|movie|film|performance|sports|sport)\s*:?\s*(.+)$", normalized, re.IGNORECASE)
    if not m:
        return None

    raw_log_type, remainder = m.groups()
    log_type = raw_log_type.lower()
    if log_type in ("movie", "film"):
        log_type = "cinema"
    elif log_type == "sports":
        log_type = "sport"

    rest = (remainder or "").strip()
    if not rest:
        return None

    def _extract_favourite_marker(raw_text: str | None) -> tuple[str | None, bool]:
        if not raw_text:
            return None, False
        cleaned = re.sub(
            r"\bmark(?:\s+as)?\s+favou?rite\b",
            "",
            raw_text,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(r"\bfavou?rite\b", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,;-")
        return (cleaned or None), cleaned != (raw_text or "").strip()

    def _normalize_time(hour: str | None, minute: str | None, compact: str | None = None) -> str | None:
        if hour is not None and minute is not None:
            return f"{int(hour):02d}:{minute}"
        if not compact:
            return None
        digits = compact.strip()
        if not digits.isdigit() or len(digits) not in (3, 4):
            return None
        parsed_hour = int(digits[:-2])
        parsed_minute = int(digits[-2:])
        if 0 <= parsed_hour <= 23 and 0 <= parsed_minute <= 59:
            return f"{parsed_hour:02d}:{parsed_minute:02d}"
        return None

    parsed_time = None
    parsed_date = None
    raw_title = rest
    raw_venue = None
    extracted_notes = None
    favourite = False

    if log_type == "cinema":
        rest_without_favourite, favourite = _extract_favourite_marker(rest)
        rest = rest_without_favourite or rest

    match_on_datetime = re.search(
        r"\s+on\s+(\d{4})[/-](\d{1,2})[/-](\d{1,2})(?:\s+(?:at\s+)?([01]?\d|2[0-3]):([0-5]\d))?",
        rest,
        re.IGNORECASE,
    )
    if match_on_datetime:
        parsed_date = (
            f"{int(match_on_datetime.group(1)):04d}-"
            f"{int(match_on_datetime.group(2)):02d}-"
            f"{int(match_on_datetime.group(3)):02d}"
        )
        if match_on_datetime.group(4) and match_on_datetime.group(5):
            parsed_time = _normalize_time(match_on_datetime.group(4), match_on_datetime.group(5))
        tail = rest[match_on_datetime.end():].strip()
        if tail:
            extracted_notes = tail
        rest = rest[: match_on_datetime.start()].strip()
    else:
        match_relative_datetime = re.search(
            r"\s+(today|tomorrow|yesterday)(?:\s+at)?\s+(?:([01]?\d|2[0-3]):([0-5]\d)|(\d{3,4}))\s*$",
            rest,
            re.IGNORECASE,
        )
        if match_relative_datetime:
            relative_day = (match_relative_datetime.group(1) or "").lower()
            parsed_time = _normalize_time(
                match_relative_datetime.group(2),
                match_relative_datetime.group(3),
                match_relative_datetime.group(4),
            )
            if relative_day == "today":
                parsed_date = date.today().isoformat()
            elif relative_day == "tomorrow":
                parsed_date = (date.today() + timedelta(days=1)).isoformat()
            elif relative_day == "yesterday":
                parsed_date = (date.today() - timedelta(days=1)).isoformat()
            rest = rest[: match_relative_datetime.start()].strip()

        match_time = re.search(r"\s+at\s+(?:([01]?\d|2[0-3]):([0-5]\d)|(\d{3,4}))\s*$", rest, re.IGNORECASE)
        if match_time:
            parsed_time = _normalize_time(match_time.group(1), match_time.group(2), match_time.group(3))
            rest = rest[: match_time.start()].strip()

    if not raw_venue:
        title_and_venue = re.match(r"^(?P<title>.+?)\s+at\s+(?P<venue>.+)$", rest, re.IGNORECASE)
        if title_and_venue:
            raw_title = (title_and_venue.group("title") or "").strip()
            raw_venue = (title_and_venue.group("venue") or "").strip()

    title = (raw_title or "").strip().rstrip(":")
    title = re.sub(
        r"^(?:i\s+)?(?:watched|watch|saw|caught|went\s+to|attended)\s+",
        "",
        title,
        flags=re.IGNORECASE,
    ).strip()
    if not title:
        return None

    payload = {
        "type": "entertainment_log",
        "log_type": log_type,
        "title": title,
        "date": date.today().isoformat(),
        "confidence": "high",
    }
    venue = (raw_venue or "").strip()
    if venue:
        payload["venue"] = venue
    if extracted_notes:
        payload["notes"] = extracted_notes
    if favourite:
        payload["favourite"] = True
    if parsed_date and parsed_time:
        payload["date"] = f"{parsed_date}T{parsed_time}:00"
    elif parsed_date:
        payload["date"] = parsed_date
    elif parsed_time and "notes" not in payload:
        payload["notes"] = f"{parsed_time}"
    return payload


# ══════════════════════════════════════════════════════════════════════════════
# RECURRING LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def _inspect_database_schema(notion, db_id: str, label: str) -> dict:
    db = notion_call(notion.databases.retrieve, database_id=db_id)
    properties = db.get("properties", {})
    schema = {name: prop.get("type") for name, prop in properties.items()}
    log.info("Entertainment schema loaded for %s:", label)
    for name, prop_type in schema.items():
        log.info("  - %s: %s", name, prop_type)
    return schema

def _first_prop_by_type(schema: dict, desired_type: str) -> str | None:
    for name, prop_type in schema.items():
        if prop_type == desired_type:
            return name
    return None

def _pick_prop(schema: dict, desired_type: str, candidates: list[str]) -> str | None:
    lowered = {name.lower(): name for name in schema}
    for candidate in candidates:
        if schema.get(candidate) == desired_type:
            return candidate
        alt = lowered.get(candidate.lower())
        if alt and schema.get(alt) == desired_type:
            return alt
    return _first_prop_by_type(schema, desired_type)

def _pick_exact_prop(schema: dict, desired_type: str, candidates: list[str]) -> str | None:
    lowered = {name.lower(): name for name in schema}
    for candidate in candidates:
        if schema.get(candidate) == desired_type:
            return candidate
        alt = lowered.get(candidate.lower())
        if alt and schema.get(alt) == desired_type:
            return alt
    return None

def _title_prop_name(schema: dict) -> str | None:
    return _first_prop_by_type(schema, "title")

def _build_common_entertainment_props(
    schema: dict,
    *,
    title: str,
    when_iso: str | None,
    venue: str | None,
    notes: str | None,
    source_name: str = "📱 Telegram",
) -> dict:
    props: dict = {}
    notes_value = notes
    title_prop = _title_prop_name(schema)
    if title_prop:
        props[title_prop] = {"title": [{"text": {"content": title}}]}

    date_prop = _pick_prop(schema, "date", ["Date", "When", "Datetime", "Watched At"])
    if date_prop and when_iso:
        date_payload = {"start": when_iso}
        if "T" in str(when_iso) and not re.search(r"(Z|[+-]\d{2}:\d{2})$", str(when_iso)):
            date_payload["time_zone"] = os.environ.get("TIMEZONE", "America/Chicago")
        props[date_prop] = {"date": date_payload}

    venue_select_prop = _pick_exact_prop(schema, "select", ["Venue", "Place", "Location"])
    venue_status_prop = _pick_exact_prop(schema, "status", ["Venue", "Place", "Location"])
    venue_rich_text_prop = _pick_exact_prop(schema, "rich_text", ["Venue", "Place", "Location"])
    if venue and venue_select_prop:
        props[venue_select_prop] = {"select": {"name": venue}}
    elif venue and venue_status_prop:
        props[venue_status_prop] = {"status": {"name": venue}}
    elif venue and venue_rich_text_prop:
        props[venue_rich_text_prop] = {"rich_text": [{"text": {"content": venue}}]}
    elif venue:
        notes_value = f"Venue: {venue}" if not notes_value else f"{notes_value}\nVenue: {venue}"

    notes_prop = _pick_prop(schema, "rich_text", ["Notes", "Comment", "Details"])
    if notes_prop and notes_value:
        props[notes_prop] = {"rich_text": [{"text": {"content": notes_value}}]}

    source_prop = _pick_exact_prop(schema, "select", ["Source"])
    if source_prop:
        props[source_prop] = {"select": {"name": source_name}}

    return props

def _safe_create_entertainment_page(notion, schema: dict, db_id: str, props: dict) -> dict:
    try:
        return notion_call(notion.pages.create, parent={"database_id": db_id}, properties=props)
    except Exception as first_error:
        fallback_props = dict(props)
        notes_prop = _pick_prop(schema, "rich_text", ["Notes", "Comment", "Details"])
        fallback_notes: list[str] = []

        def _extract_option_name(prop_payload: dict) -> str | None:
            if "select" in prop_payload:
                return (prop_payload.get("select") or {}).get("name")
            if "status" in prop_payload:
                return (prop_payload.get("status") or {}).get("name")
            return None

        for field in ("Venue", "Place", "Location"):
            prop_name = _pick_exact_prop(schema, "select", [field]) or _pick_exact_prop(schema, "status", [field])
            if prop_name and prop_name in fallback_props:
                extracted = _extract_option_name(fallback_props[prop_name])
                if extracted:
                    fallback_notes.append(f"{field}: {extracted}")
                fallback_props.pop(prop_name, None)

        source_prop = _pick_exact_prop(schema, "select", ["Source"]) or _pick_exact_prop(schema, "status", ["Source"])
        if source_prop and source_prop in fallback_props:
            fallback_props.pop(source_prop, None)

        if fallback_notes and notes_prop:
            existing = ""
            if notes_prop in fallback_props:
                chunks = fallback_props[notes_prop].get("rich_text", [])
                existing = "".join(c.get("text", {}).get("content", "") for c in chunks).strip()
            merged = "\n".join([part for part in [existing, "\n".join(fallback_notes)] if part]).strip()
            if merged:
                fallback_props[notes_prop] = {"rich_text": [{"text": {"content": merged}}]}

        try:
            return notion_call(notion.pages.create, parent={"database_id": db_id}, properties=fallback_props)
        except Exception:
            raise first_error

def _build_sport_competition_props(schema: dict, competition: str) -> dict:
    competition_select_prop = _pick_exact_prop(schema, "select", ["Competition", "League", "Tournament"])
    competition_status_prop = _pick_exact_prop(schema, "status", ["Competition", "League", "Tournament"])
    competition_multi_select_prop = _pick_exact_prop(schema, "multi_select", ["Competition", "League", "Tournament"])
    competition_rich_text_prop = _pick_exact_prop(schema, "rich_text", ["Competition", "League", "Tournament"])
    if competition_select_prop:
        return {competition_select_prop: {"select": {"name": competition}}}
    if competition_status_prop:
        return {competition_status_prop: {"status": {"name": competition}}}
    if competition_multi_select_prop:
        return {competition_multi_select_prop: {"multi_select": [{"name": competition}]}}
    if competition_rich_text_prop:
        return {competition_rich_text_prop: {"rich_text": [{"text": {"content": competition}}]}}
    return {}

def _remember_pending_sport_competition(message, page_id: str) -> None:
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", None)
    if chat_id is None:
        return
    pending_sport_competition_map[chat_id] = {"page_id": page_id}

def _extract_cinema_visit_details(notes: str | None) -> tuple[str | None, int | None]:
    if not notes:
        return None, None

    seat_match = re.search(r"\bseat\s*([A-Za-z0-9-]+)\b", notes, re.IGNORECASE)
    seat = seat_match.group(1).strip().upper() if seat_match else None

    auditorium_match = re.search(r"\bauditorium\s*([A-Za-z0-9-]+)\b", notes, re.IGNORECASE)
    auditorium_value = None
    if auditorium_match:
        num_match = re.search(r"\d+", auditorium_match.group(1))
        if num_match:
            auditorium_value = int(num_match.group(0))
    return seat, auditorium_value

def _normalize_entertainment_datetime(when_iso: str | None, notes: str | None) -> str | None:
    def _date_fragment(raw: str | None) -> str | None:
        if not raw:
            return None
        s = str(raw).strip()
        m = re.search(r"\b(\d{4})[/-](\d{1,2})[/-](\d{1,2})\b", s)
        if not m:
            return None
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    def _time_fragment(raw: str | None) -> str | None:
        if not raw:
            return None
        s = str(raw).strip()
        m = re.search(r"\b([01]?\d|2[0-3]):([0-5]\d)\b", s)
        if not m:
            return None
        return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}:00"

    date_part = _date_fragment(when_iso) or _date_fragment(notes)
    if not date_part:
        return when_iso
    time_part = _time_fragment(when_iso) or _time_fragment(notes)
    if not time_part:
        return date_part
    return f"{date_part}T{time_part}"

def _parse_cinema_inline_context(raw: str | None) -> dict[str, str | None]:
    text = (raw or "").strip()
    if not text:
        return {"title": None, "venue": None, "date": None, "time": None, "tail": None}
    m = re.match(
        r"^(?P<title>.+?)\s+at\s+(?P<venue>.+?)\s+on\s+(?P<date>\d{4}[/-]\d{1,2}[/-]\d{1,2})(?:\s+at\s+(?P<time>\d{1,2}:[0-5]\d))?(?:\s+(?P<tail>.*))?$",
        text,
        flags=re.IGNORECASE,
    )
    if not m:
        return {"title": None, "venue": None, "date": None, "time": None, "tail": None}
    return {
        "title": (m.group("title") or "").strip() or None,
        "venue": (m.group("venue") or "").strip() or None,
        "date": (m.group("date") or "").strip() or None,
        "time": (m.group("time") or "").strip() or None,
        "tail": (m.group("tail") or "").strip() or None,
    }

def _strip_cinema_structured_notes(notes: str | None) -> str | None:
    if not notes:
        return None
    cleaned = re.sub(r"\bseat\s*[A-Za-z0-9-]+\b", "", notes, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bauditorium\s*[A-Za-z0-9-]+\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b([01]?\d|2[0-3]):[0-5]\d\b", "", cleaned)
    cleaned = re.sub(r"\b(?:on|at)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[,\-–|]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None

def _strip_datetime_from_notes(notes: str | None) -> str | None:
    if not notes:
        return None
    cleaned = re.sub(r"\b\d{4}[/-]\d{1,2}[/-]\d{1,2}\b", "", notes, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b([01]?\d|2[0-3]):[0-5]\d\b", "", cleaned)
    cleaned = re.sub(r"\b(?:on|at)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[,\-–|]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None

def _strip_seat_from_notes(notes: str | None) -> str | None:
    if not notes:
        return None
    cleaned = re.sub(r"\bseat\s*[A-Za-z0-9-]+\b", "", notes, flags=re.IGNORECASE)
    cleaned = re.sub(r"[,\-–|]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None

def _find_existing_cinema_venue(notion, title: str, schema: dict) -> str | None:
    title_prop = _title_prop_name(schema)
    venue_prop = _pick_exact_prop(schema, "select", ["Venue", "Place", "Location"]) \
        or _pick_exact_prop(schema, "status", ["Venue", "Place", "Location"]) \
        or _pick_exact_prop(schema, "rich_text", ["Venue", "Place", "Location"])
    if not title_prop or not venue_prop:
        return None

    rows = notion_call(notion.databases.query, database_id=NOTION_CINEMA_LOG_DB).get("results", [])
    target = title.strip().lower()
    for row in rows:
        props = row.get("properties", {})
        title_arr = props.get(title_prop, {}).get("title", [])
        row_title = "".join(chunk.get("plain_text", "") for chunk in title_arr).strip().lower()
        if row_title != target:
            continue
        venue_obj = props.get(venue_prop, {})
        venue_type = venue_obj.get("type")
        if venue_type == "select":
            name = (venue_obj.get("select") or {}).get("name")
            if name:
                return name
        if venue_type == "status":
            name = (venue_obj.get("status") or {}).get("name")
            if name:
                return name
        if venue_type == "rich_text":
            chunks = venue_obj.get("rich_text", [])
            value = "".join(c.get("plain_text", "") for c in chunks).strip()
            if value:
                return value
    return None

def _resolve_known_cinema_venue(notion, venue: str | None, schema: dict) -> str | None:
    if not venue:
        return None
    title_prop = _title_prop_name(schema)
    venue_prop = _pick_exact_prop(schema, "select", ["Venue", "Place", "Location"]) \
        or _pick_exact_prop(schema, "status", ["Venue", "Place", "Location"]) \
        or _pick_exact_prop(schema, "rich_text", ["Venue", "Place", "Location"])
    if not title_prop or not venue_prop:
        return venue

    rows = notion_call(notion.databases.query, database_id=NOTION_CINEMA_LOG_DB).get("results", [])
    incoming = venue.strip().lower()
    for row in rows:
        venue_obj = row.get("properties", {}).get(venue_prop, {})
        venue_type = venue_obj.get("type")
        existing = None
        if venue_type == "select":
            existing = (venue_obj.get("select") or {}).get("name")
        elif venue_type == "status":
            existing = (venue_obj.get("status") or {}).get("name")
        elif venue_type == "rich_text":
            chunks = venue_obj.get("rich_text", [])
            existing = "".join(c.get("plain_text", "") for c in chunks).strip()
        if not existing:
            continue
        e = existing.strip().lower()
        if incoming == e or incoming in e or e in incoming:
            return existing
    return venue

def _query_title_values(notion, db_id: str, title_prop_name: str) -> list[dict]:
    rows = notion_call(notion.databases.query, database_id=db_id).get("results", [])
    values: list[dict] = []
    for row in rows:
        title_arr = row.get("properties", {}).get(title_prop_name, {}).get("title", [])
        name = "".join(chunk.get("plain_text", "") for chunk in title_arr).strip()
        if name:
            values.append({"page_id": row["id"], "name": name})
    return values

def _ensure_entertainment_schema(notion, key: str, label: str, db_id: str | None) -> dict | None:
    schema = entertainment_schemas.get(key)
    if schema:
        return schema
    if not db_id:
        return None
    try:
        schema = _inspect_database_schema(notion, db_id, label)
    except Exception as e:
        log.error("Failed to lazily load entertainment schema key=%s label=%s err=%s", key, label, e)
        return None
    entertainment_schemas[key] = schema
    return schema

def create_entertainment_log_entry(notion, payload: dict) -> tuple[str, bool]:
    log_type = payload.get("log_type")
    title = (payload.get("title") or "").strip()
    if not title:
        raise ValueError("Entertainment log missing title")

    when_iso = payload.get("date") or date.today().isoformat()
    venue = payload.get("venue")
    notes = payload.get("notes")
    favourite = bool(payload.get("favourite"))

    if log_type == "cinema":
        schema = _ensure_entertainment_schema(notion, "cinema", "🍿 Cinema Log", NOTION_CINEMA_LOG_DB)
        if not schema:
            raise ValueError("Cinema schema is unavailable")
        parsed_inline = _parse_cinema_inline_context(title)
        if parsed_inline.get("title"):
            title = parsed_inline["title"]
        if not venue and parsed_inline.get("venue"):
            venue = parsed_inline["venue"]
        if parsed_inline.get("tail"):
            notes = f"{parsed_inline['tail']}" if not notes else f"{notes}, {parsed_inline['tail']}"
        elif not notes and parsed_inline.get("time"):
            notes = parsed_inline["time"]

        datetime_hint = " ".join(part for part in [title, venue, notes, parsed_inline.get("time")] if part)
        when_iso = _normalize_entertainment_datetime(when_iso, datetime_hint)
        venue = _resolve_known_cinema_venue(notion, venue, schema)
        seat, auditorium = _extract_cinema_visit_details(notes)
        if not venue:
            venue = _find_existing_cinema_venue(notion, title, schema)
        cleaned_notes = None if (seat or auditorium is not None) else _strip_cinema_structured_notes(notes)
        props = _build_common_entertainment_props(schema, title=title, when_iso=when_iso, venue=venue, notes=notes)
        if cleaned_notes:
            notes_prop = _pick_prop(schema, "rich_text", ["Notes", "Comment", "Details"])
            if notes_prop:
                props[notes_prop] = {"rich_text": [{"text": {"content": cleaned_notes}}]}
        else:
            notes_prop = _pick_prop(schema, "rich_text", ["Notes", "Comment", "Details"])
            if notes_prop and notes_prop in props:
                props.pop(notes_prop, None)
        seat_select_prop = _pick_exact_prop(schema, "select", ["Seat"])
        seat_rich_text_prop = _pick_exact_prop(schema, "rich_text", ["Seat"])
        if seat and seat_select_prop:
            props[seat_select_prop] = {"select": {"name": seat}}
        elif seat and seat_rich_text_prop:
            props[seat_rich_text_prop] = {"rich_text": [{"text": {"content": seat}}]}

        auditorium_number_prop = _pick_exact_prop(schema, "number", ["Auditorium"])
        auditorium_rich_text_prop = _pick_exact_prop(schema, "rich_text", ["Auditorium"])
        if auditorium is not None and auditorium_number_prop:
            props[auditorium_number_prop] = {"number": auditorium}
        elif auditorium is not None and auditorium_rich_text_prop:
            props[auditorium_rich_text_prop] = {"rich_text": [{"text": {"content": str(auditorium)}}]}

        favourite_prop = _pick_exact_prop(schema, "checkbox", ["Favourite", "Favorite"])
        if favourite_prop:
            props[favourite_prop] = {"checkbox": favourite}
        page = _safe_create_entertainment_page(notion, schema, NOTION_CINEMA_LOG_DB, props)

        if favourite and NOTION_FAVE_DB and entertainment_schemas.get("favourite_films"):
            fav_schema = entertainment_schemas["favourite_films"]
            fav_title_prop = _title_prop_name(fav_schema)
            if fav_title_prop:
                existing = _query_title_values(notion, NOTION_FAVE_DB, fav_title_prop)
                if not notion_tasks.fuzzy_match(title, existing):
                    fav_props = _build_common_entertainment_props(
                        fav_schema,
                        title=title,
                        when_iso=when_iso,
                        venue=venue,
                        notes=notes,
                    )
                    notion_call(
                        notion.pages.create,
                        parent={"database_id": NOTION_FAVE_DB},
                        properties=fav_props,
                    )
        return page["id"], favourite

    if log_type == "performance":
        schema = _ensure_entertainment_schema(notion, "performances", "🎟️ Performances Viewings", NOTION_PERFORMANCE_LOG_DB)
        if not schema:
            raise ValueError("Performances schema is unavailable")
        datetime_hint = " ".join(part for part in [title, venue, notes] if part)
        when_iso = _normalize_entertainment_datetime(when_iso, datetime_hint)
        notes = _strip_datetime_from_notes(notes)
        props = _build_common_entertainment_props(schema, title=title, when_iso=when_iso, venue=venue, notes=notes)
        page = _safe_create_entertainment_page(notion, schema, NOTION_PERFORMANCE_LOG_DB, props)
        return page["id"], False

    if log_type == "sport":
        schema = _ensure_entertainment_schema(notion, "sports", "🏅 Sports Log", NOTION_SPORTS_LOG_DB)
        if not schema:
            raise ValueError("Sports schema is unavailable")
        datetime_hint = " ".join(part for part in [title, venue, notes] if part)
        when_iso = _normalize_entertainment_datetime(when_iso, datetime_hint)
        notes = _strip_datetime_from_notes(notes)
        seat, _ = _extract_cinema_visit_details(notes)
        cleaned_notes = _strip_seat_from_notes(notes) if seat else notes
        props = _build_common_entertainment_props(schema, title=title, when_iso=when_iso, venue=venue, notes=notes)
        if seat:
            notes_prop = _pick_prop(schema, "rich_text", ["Notes", "Comment", "Details"])
            if cleaned_notes and notes_prop:
                props[notes_prop] = {"rich_text": [{"text": {"content": cleaned_notes}}]}
            elif notes_prop and notes_prop in props:
                props.pop(notes_prop, None)

            seat_select_prop = _pick_exact_prop(schema, "select", ["Seat"])
            seat_status_prop = _pick_exact_prop(schema, "status", ["Seat"])
            seat_rich_text_prop = _pick_exact_prop(schema, "rich_text", ["Seat"])
            if seat_select_prop:
                props[seat_select_prop] = {"select": {"name": seat}}
            elif seat_status_prop:
                props[seat_status_prop] = {"status": {"name": seat}}
            elif seat_rich_text_prop:
                props[seat_rich_text_prop] = {"rich_text": [{"text": {"content": seat}}]}
        page = _safe_create_entertainment_page(notion, schema, NOTION_SPORTS_LOG_DB, props)
        return page["id"], False

    raise ValueError(f"Unknown entertainment log type: {log_type}")

async def handle_entertainment_log(notion, message, payload: dict) -> None:
    entry_id, fav_saved = create_entertainment_log_entry(notion, payload)
    title = payload.get("title", "Untitled")
    log_type = payload.get("log_type", "cinema")
    venue = payload.get("venue")
    notes = payload.get("notes")
    when_iso = payload.get("date") or date.today().isoformat()

    summary_lines = [
        f"✅ Logged to { {'cinema': 'Cinema', 'performance': 'Performance', 'sport': 'Sports'}.get(log_type, 'Entertainment') }",
        "",
        f"🎫 {title}",
        f"📅 {when_iso}",
    ]
    if venue:
        summary_lines.append(f"📍 {venue}")
    if notes:
        summary_lines.append(f"📝 {notes}")
    if fav_saved and log_type == "cinema":
        summary_lines.append("🎞️ Added to Favourite Films")
    summary_lines.append("")
    summary_lines.append("_Saved to Notion_")
    await message.reply_text("\n".join(summary_lines), parse_mode="Markdown")
    if log_type == "sport":
        _remember_pending_sport_competition(message, entry_id)
        await message.reply_text("🏆 Logged to Sports Log. Which competition should I set for this one?")
    log.info("Entertainment logged type=%s title=%s page_id=%s", log_type, title, entry_id)

def _entertainment_save_error_text(err: Exception, payload: dict | None = None) -> str:
    text = str(err or "")
    log_type = (payload or {}).get("log_type")
    if "Performances schema is unavailable" in text:
        return (
            "⚠️ I couldn't save that performance log because the Performances DB isn't configured.\n"
            "Set `NOTION_PERFORMANCE_LOG_DB`."
        )
    if "Cinema schema is unavailable" in text:
        return "⚠️ I couldn't save that cinema log because `NOTION_CINEMA_LOG_DB` isn't configured."
    if "Sports schema is unavailable" in text:
        return "⚠️ I couldn't save that sports log because `NOTION_SPORTS_LOG_DB` isn't configured."
    return "⚠️ I couldn't save that entertainment log to Notion."

def _entertainment_db_meta(log_type: str | None) -> tuple[str | None, str | None, str | None]:
    if log_type == "cinema":
        return "cinema", "🍿 Cinema Log", NOTION_CINEMA_LOG_DB
    if log_type == "performance":
        return "performances", "🎟️ Performances Viewings", NOTION_PERFORMANCE_LOG_DB
    if log_type == "sport":
        return "sports", "🏅 Sports Log", NOTION_SPORTS_LOG_DB
    return None, None, None

def _normalize_venue_text(text: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()

def _best_known_venue_match(raw_venue: str, candidates: list[str]) -> str | None:
    incoming = _normalize_venue_text(raw_venue)
    if not incoming:
        return None
    best_name = None
    best_score = 0.0
    for candidate in candidates:
        candidate_norm = _normalize_venue_text(candidate)
        if not candidate_norm:
            continue
        if incoming == candidate_norm:
            return None
        if incoming in candidate_norm or candidate_norm in incoming:
            score = 0.95
        else:
            score = SequenceMatcher(None, incoming, candidate_norm).ratio()
        if score > best_score:
            best_score = score
            best_name = candidate
    if best_name and best_score >= 0.62:
        return best_name
    return None

def _known_venues_for_log_type(notion, log_type: str | None) -> list[str]:
    schema_key, label, db_id = _entertainment_db_meta(log_type)
    if not (schema_key and label and db_id):
        return []
    schema = _ensure_entertainment_schema(notion, schema_key, label, db_id)
    if not schema:
        return []
    venue_prop = _pick_exact_prop(schema, "select", ["Venue", "Place", "Location"]) \
        or _pick_exact_prop(schema, "status", ["Venue", "Place", "Location"]) \
        or _pick_exact_prop(schema, "rich_text", ["Venue", "Place", "Location"])
    if not venue_prop:
        return []
    rows = notion_call(notion.databases.query, database_id=db_id).get("results", [])
    seen: set[str] = set()
    values: list[str] = []
    for row in rows:
        venue_obj = row.get("properties", {}).get(venue_prop, {})
        venue_type = venue_obj.get("type")
        name = None
        if venue_type == "select":
            name = (venue_obj.get("select") or {}).get("name")
        elif venue_type == "status":
            name = (venue_obj.get("status") or {}).get("name")
        elif venue_type == "rich_text":
            chunks = venue_obj.get("rich_text", [])
            name = "".join(c.get("plain_text", "") for c in chunks).strip()
        if not name:
            continue
        key = name.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(name.strip())
    return values

def _suggest_known_venue(notion, payload: dict) -> tuple[str | None, str | None]:
    raw_venue = ((payload or {}).get("venue") or "").strip()
    if not raw_venue:
        return None, None
    suggested = _best_known_venue_match(raw_venue, _known_venues_for_log_type(notion, (payload or {}).get("log_type")))
    if not suggested:
        return None, None
    if suggested.strip().lower() == raw_venue.lower():
        return None, None
    return raw_venue, suggested

async def _maybe_prompt_explicit_venue(notion, message, payload: dict, raw_text: str) -> bool:
    original, suggested = _suggest_known_venue(notion, payload)
    if not (original and suggested):
        return False
    adjusted_payload = dict(payload)
    adjusted_payload["venue"] = suggested
    payload.update(adjusted_payload)
    return False


# ══════════════════════════════════════════════════════════════════════════════
# MESSAGE FORMATTERS
# ══════════════════════════════════════════════════════════════════════════════

def load_entertainment_schemas(notion) -> None:
    global entertainment_schemas
    entertainment_schemas = {}
    targets = [
        ("cinema", "🍿 Cinema Log", NOTION_CINEMA_LOG_DB),
        ("performances", "🎟️ Performances Viewings", NOTION_PERFORMANCE_LOG_DB),
        ("sports", "🏟️ Sports Log", NOTION_SPORTS_LOG_DB),
        ("favourite_films", "🎞️ Favourite Films", NOTION_FAVE_DB),
    ]
    for key, label, db_id in targets:
        if not db_id:
            log.warning("Entertainment schema skipped for %s (missing DB id)", label)
            continue
        try:
            entertainment_schemas[key] = _inspect_database_schema(notion, db_id, label)
        except Exception as exc:  # noqa: BLE001
            log.error("Failed to inspect %s schema: %s", label, exc)
