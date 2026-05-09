from __future__ import annotations
import asyncio
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from second_brain.notion import notion_call
from .nlp import fuzzy_match_movements, normalize_movement_name
import logging

log = logging.getLogger(__name__)


def _title(props, key="Name"):
    try:return props[key]["title"][0]["plain_text"]
    except Exception:return ""


def infer_primary_patterns(movement_name: str) -> list[str]:
    """Infer Movements DB Primary Pattern values from a movement name.

    The Notion property is a multi-select, but Phase 1 defaults to one strong
    primary pattern so newly created movements avoid blank metadata.
    """
    name = (movement_name or "").lower()
    rules = [
        ("Olympic", ["clean", "snatch", "jerk", "olympic"]),
        ("Squat", ["squat", "lunge", "step-up", "step up", "wall ball", "thruster"]),
        ("Hinge", ["deadlift", "hinge", "kettlebell swing", "kb swing", "good morning"]),
        ("Push", ["press", "push-up", "push up", "dip", "bench", "wall walk", "handstand"]),
        ("Pull", ["pull-up", "pull up", "chin-up", "chin up", "row", "muscle-up", "muscle up", "rope climb"]),
        ("Core", ["toes to bar", "v-up", "v up", "sit-up", "sit up", "hollow", "plank", "gdh"]),
        ("Monostructural", ["run", "bike", "ski", "double under", "single under", "burpee", "box jump"]),
        ("Carry", ["carry", "farmer", "sandbag", "yoke"]),
    ]
    for pattern, needles in rules:
        if any(needle in name for needle in needles):
            return [pattern]
    return ["Other"]

# existing functions unchanged ...
def find_movement_by_name(notion, movements_db_id: str, name: str):
    res = notion_call(notion.databases.query, database_id=movements_db_id).get("results", [])
    needle = (name or "").strip().lower()
    exact = []
    fuzzy = []
    for r in res:
        nm = _title(r.get("properties", {}))
        if nm.lower() == needle: exact.append({"page_id": r["id"], "name": nm})
        elif needle and needle in nm.lower(): fuzzy.append({"page_id": r["id"], "name": nm})
    return (exact or fuzzy or [None])[0]

def get_or_create_movement(notion, movements_db_id: str, name: str) -> str:
    found = find_movement_by_name(notion, movements_db_id, name)
    if found:
        return found["page_id"]
    primary_patterns = infer_primary_patterns(name)
    properties = {
        "Name": {"title": [{"text": {"content": name}}]},
        "Category": {"multi_select": [{"name": "Compound"}]},
        "Primary Pattern": {"multi_select": [{"name": pattern} for pattern in primary_patterns]},
    }
    page = notion_call(notion.pages.create, parent={"database_id": movements_db_id}, properties=properties)
    return page["id"]



def _extract_unique_movement_names(parsed: dict) -> set[str]:
    names: set[str] = set()
    for track_row in (parsed or {}).get("tracks", []):
        for day_row in track_row.get("days", []):
            for m in (day_row.get("section_b") or {}).get("movements") or []:
                if m:
                    names.add(m)
            for m in (day_row.get("section_c") or {}).get("movements") or []:
                if m:
                    names.add(m)
    return names


def _plain_rich_text(props: dict, key: str) -> str:
    return "".join(x.get("plain_text", "") for x in props.get(key, {}).get("rich_text", []) or [])


def _load_movement_cache_sync(notion, movements_db_id: str) -> dict[str, str]:
    if not notion or not movements_db_id:
        return {}
    cache: dict[str, str] = {}
    start_cursor = None
    while True:
        kwargs = {"database_id": movements_db_id, "page_size": 100}
        if start_cursor:
            kwargs["start_cursor"] = start_cursor
        res = notion_call(notion.databases.query, **kwargs)
        for page in res.get("results", []):
            name = _title(page.get("properties", {}))
            if name:
                cache[name] = page.get("id")
        if not res.get("has_more"):
            break
        start_cursor = res.get("next_cursor")
        if not start_cursor:
            break
    return cache


def _run_fuzzy_match_sync(names: list[str], cache: dict[str, str]):
    if not names or not cache:
        return []
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(fuzzy_match_movements(names, cache))
    # This save path is synchronous; in the unlikely event it is invoked from an
    # active loop, use a local exact/contains fallback rather than nesting loops.
    del running_loop
    out = []
    normalized_cache = {normalize_movement_name(name): name for name in cache}
    for name in names:
        normalized = normalize_movement_name(name)
        matched = normalized_cache.get(normalized)
        if matched:
            out.append((name, matched, 1.0))
            continue
        contained = next((orig for norm, orig in normalized_cache.items() if normalized and normalized in norm), None)
        out.append((name, contained, 0.75 if contained else 0.0))
    return out


def _movement_names_from_text(section_text: str, movement_cache: dict[str, str]) -> list[str]:
    """Find cache movements mentioned in section text, then preserve order."""
    normalized_text = f" {normalize_movement_name(section_text or '')} "
    found: list[str] = []
    for movement_name in movement_cache:
        normalized_name = normalize_movement_name(movement_name)
        if not normalized_name:
            continue
        if f" {normalized_name} " in normalized_text and movement_name not in found:
            found.append(movement_name)
    return sorted(found, key=lambda name: (section_text.lower().find(name.lower()), name))


def _resolve_section_movements(section: dict, movement_cache: dict[str, str], notion, movements_db_id: str) -> list[str]:
    section = section or {}
    candidates: list[str] = []
    for name in section.get("movements") or []:
        if name and name not in candidates:
            candidates.append(name)
    for name in _movement_names_from_text(section.get("description") or "", movement_cache):
        if name not in candidates:
            candidates.append(name)

    resolved: list[str] = []
    for extracted_name, matched_name, score in _run_fuzzy_match_sync(candidates, movement_cache):
        if matched_name and score >= 0.70:
            mid = movement_cache.get(matched_name)
            if mid and mid not in resolved:
                resolved.append(mid)
            continue
        if extracted_name and movements_db_id:
            mid = get_or_create_movement(notion, movements_db_id, extracted_name)
            movement_cache.setdefault(extracted_name, mid)
            if mid not in resolved:
                resolved.append(mid)
    return resolved


def _week_start_from_label(label: str | None) -> str:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", label or "")
    if match:
        return match.group(1)
    return this_monday()


def _cycle_number_from_name(name: str) -> int | None:
    match = re.search(r"\bcycle\s*#?\s*(\d+)\b", name or "", re.I)
    return int(match.group(1)) if match else None


def _get_open_cycle_metadata(notion, cycles_db_id: str, program_db_id: str) -> tuple[str | None, int]:
    """Return (open_cycle_page_id, next_week_number), without creating cycles."""
    if not cycles_db_id or not program_db_id:
        log.warning("[PARSER] No open cycle found — Week set to 1, Cycle # left blank")
        return None, 1
    try:
        open_cycles = notion_call(
            notion.databases.query,
            database_id=cycles_db_id,
            filter={"property": "End Date", "date": {"is_empty": True}},
            page_size=1,
        ).get("results", [])
        if not open_cycles:
            log.warning("[PARSER] No open cycle found — Week set to 1, Cycle # left blank")
            return None, 1

        cycle_id = open_cycles[0]["id"]
        existing = notion_call(
            notion.databases.query,
            database_id=program_db_id,
            filter={"property": "Cycle #", "relation": {"contains": cycle_id}},
            page_size=100,
        ).get("results", [])
        return cycle_id, len(existing) + 1
    except Exception as e:
        log.warning("cycle metadata lookup failed: %s", e)
        log.warning("[PARSER] No open cycle found — Week set to 1, Cycle # left blank")
        return None, 1


def _get_or_create_cycle_metadata(notion, cycles_db_id: str, program_db_id: str, monday_iso: str) -> tuple[str | None, int | None]:
    del monday_iso
    return _get_open_cycle_metadata(notion, cycles_db_id, program_db_id)


def _weekly_program_metadata_props(cycle_id: str | None, week_number: int | None, monday_iso: str) -> dict:
    props: dict = {"Start Date": {"date": {"start": monday_iso}}}
    if cycle_id:
        props["Cycle #"] = {"relation": [{"id": cycle_id}]}
    if week_number is not None:
        props["Week"] = {"number": week_number}
    return props




def _rich_text_chunks(text: str, limit: int = 1900) -> list[dict]:
    """Split text into Notion rich_text block array respecting per-block limit."""
    if not text:
        return [{"text": {"content": ""}}]
    chunks = []
    for i in range(0, len(text), limit):
        chunks.append({"text": {"content": text[i:i + limit]}})
    return chunks


def this_monday() -> str:
    today = date.today()
    return (today - timedelta(days=today.weekday())).isoformat()


def infer_section_b_type(section_b: dict) -> str:
    section_b = section_b or {}
    desc = (section_b.get("description") or "")
    if section_b.get("is_strength_test"):
        return "Strength Test"
    lower = desc.lower()
    if "emom" in lower:
        return "EMOM"
    if "intervals" in lower:
        return "Intervals"
    if "sets of" in lower:
        return "Volume"
    if "1rm" in lower or "max" in lower:
        return "Strength Test"
    if "skill" in lower or "practice" in lower:
        return "Skill"
    if "every" in lower:
        return "Intervals"
    return "Volume"



def infer_section_c_format(section_c: dict) -> str | None:
    desc = ((section_c or {}).get("description") or "").lower()
    if "partner" in desc and "amrap" in desc:
        return "Partner AMRAP"
    if "amrap" in desc:
        return "AMRAP"
    if "for time" in desc or "time cap" in desc:
        return "For Time"
    if "emom" in desc or "every minute" in desc:
        return "EMOM"
    if "chipper" in desc:
        return "Chipper"
    if "intervals" in desc or "work:" in desc or re.search(r"\bevery\s+\d+", desc):
        return "Intervals"
    return None



_DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
_TRACK_NAMES = ("Performance", "Fitness", "Hyrox")


def _header_line_pattern(names: tuple[str, ...]) -> re.Pattern:
    choices = "|".join(re.escape(name) for name in names)
    return re.compile(rf"(?im)^\s*[*_`#>\-\s]*(?P<name>{choices})(?=[\s*_`:#>\-–—)]|$)[^\n]*$", re.IGNORECASE)


def _split_by_headers(text: str, names: tuple[str, ...]) -> list[tuple[str, str]]:
    pattern = _header_line_pattern(names)
    matches = list(pattern.finditer(text or ""))
    blocks: list[tuple[str, str]] = []
    for idx, match in enumerate(matches):
        raw_name = match.group("name").lower()
        canonical = next(name for name in names if name.lower() == raw_name)
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        blocks.append((canonical, text[start:end].strip()))
    return blocks


def _strip_section_label(text: str) -> str:
    return re.sub(r"(?im)^\s*(?:section\s*)?[BC]\s*[.:)\-–—]?\s*", "", text or "", count=1).strip()


def _extract_training_notes(block: str) -> tuple[str, str]:
    match = re.search(r"(?ims)^\s*training\s+notes\s*:\s*(?P<notes>.*)$", block or "")
    if not match:
        return block or "", ""
    return (block[:match.start()].rstrip(), match.group("notes").strip())


def _extract_sections(block: str) -> tuple[str, str, str]:
    body, notes = _extract_training_notes(block or "")
    marker = re.compile(r"(?im)^\s*(?:section\s*)?(?P<section>[BC])\s*[.:)\-–—]\s*")
    matches = list(marker.finditer(body))
    section_text: dict[str, str] = {"B": "", "C": ""}
    for idx, match in enumerate(matches):
        key = match.group("section").upper()
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(body)
        section_text[key] = _strip_section_label(body[start:end])
    if not matches:
        section_text["C"] = body.strip()
    return section_text["B"], section_text["C"], notes


def parse_weekly_program_text(full_text: str, week_label: str | None = None) -> dict:
    """Parse raw Weekly Programs text into tracks/days using day and track headers."""
    tracks_by_name: dict[str, list[dict]] = {track: [] for track in _TRACK_NAMES}
    for day, day_block in _split_by_headers(full_text or "", _DAY_NAMES):
        track_blocks = _split_by_headers(day_block, _TRACK_NAMES)
        if not track_blocks:
            continue
        for track, track_block in track_blocks:
            section_b, section_c, training_notes = _extract_sections(track_block)
            tracks_by_name[track].append({
                "day": day,
                "section_b": {"description": section_b, "movements": []} if section_b else {},
                "section_c": {"description": section_c, "movements": [], "is_partner": "partner" in section_c.lower()} if section_c else {},
                "training_notes": training_notes,
            })
    tracks = [{"track": track, "days": days} for track, days in tracks_by_name.items() if days]
    if not tracks:
        raise ValueError("No day/track workout blocks found in Full Program")
    return {"week_label": week_label or f"Week of {this_monday()}", "tracks": tracks}

def get_current_week_programme(notion, program_db_id: str):
    res = notion_call(notion.databases.query, database_id=program_db_id, sorts=[{"timestamp": "created_time", "direction": "descending"}], page_size=1).get("results", [])
    if not res: return None
    p=res[0]; props=p.get("properties",{})
    full = props.get("Full Program", {}).get("rich_text", [])
    full_text = "".join(x.get("plain_text", "") for x in full)
    parsed = None
    try: parsed = json.loads(full_text)
    except Exception: parsed = None
    return {"page_id": p["id"], "name": _title(props), "full_program": full_text, "week_label": _title(props), "days_parsed": parsed}

def save_programme(notion, program_db_id: str, workout_days_db_id: str, movements_db_id: str, parsed: dict, full_text: str, cycles_db_id: str | None = None) -> str:
    if "tracks" not in parsed and "days" in parsed:
        parsed = {
            "week_label": parsed.get("week_label"),
            "tracks": [{"track": "Performance", "days": parsed.get("days", [])}],
        }

    week_label = parsed.get("week_label") or "Week"
    monday_iso = _week_start_from_label(week_label)
    cycle_id, week_number = _get_or_create_cycle_metadata(notion, cycles_db_id or os.getenv("NOTION_CYCLES_DB", ""), program_db_id, monday_iso)

    try:
        parent = notion_call(
            notion.pages.create,
            parent={"database_id": program_db_id},
            properties={
                "Name": {"title": [{"text": {"content": week_label}}]},
                "Full Program": {"rich_text": _rich_text_chunks(full_text)},
                **_weekly_program_metadata_props(cycle_id, week_number, monday_iso),
            },
        )
        parent_page_id = parent["id"]
        log.info("save_programme: created parent row %s", parent_page_id)
    except Exception as e:
        log.error("save_programme: failed to create Weekly Programs row: %s", e)
        raise

    movement_cache: dict[str, str] = _load_movement_cache_sync(notion, movements_db_id) if movements_db_id else {}
    program_movement_ids: set[str] = set()
    if movements_db_id:
        all_movement_names = _extract_unique_movement_names(parsed)

        log.info("save_programme: resolving %d unique movements", len(all_movement_names))
        for movement_name in all_movement_names:
            try:
                ids = _resolve_section_movements({"description": movement_name, "movements": [movement_name]}, movement_cache, notion, movements_db_id)
                program_movement_ids.update(ids)
                if ids and movement_name not in movement_cache:
                    movement_cache[movement_name] = ids[0]
            except Exception as e:
                log.warning("save_programme: could not resolve movement '%s': %s", movement_name, e)

    if program_movement_ids:
        try:
            notion_call(notion.pages.update, page_id=parent_page_id, properties={"Movements": {"relation": [{"id": mid} for mid in sorted(program_movement_ids)]}})
        except Exception as e:
            log.warning("save_programme: could not update parent movements: %s", e)

    days_created = 0
    for track_row in parsed.get("tracks", []):
        track = track_row.get("track") or "Performance"
        for day_row in track_row.get("days", []):
            day = day_row.get("day") or "Monday"
            section_b = day_row.get("section_b") or {}
            section_c = day_row.get("section_c") or {}
            training_notes = day_row.get("training_notes") or ""
            b_desc = section_b.get("description") or ""
            c_desc = section_c.get("description") or ""

            b_ids = _resolve_section_movements(section_b, movement_cache, notion, movements_db_id) if movements_db_id else []
            c_ids = _resolve_section_movements(section_c, movement_cache, notion, movements_db_id) if movements_db_id else []
            program_movement_ids.update(b_ids)
            program_movement_ids.update(c_ids)

            if not workout_days_db_id:
                continue

            props: dict = {
                "Name": {"title": [{"text": {"content": f"{day} — {track} — {week_label}"}}]},
                "Day": {"select": {"name": day}},
                "Track": {"select": {"name": track}},
                "Week": {"relation": [{"id": parent_page_id}]},
                "Week Of": {"date": {"start": monday_iso}},
                "Is Partner": {"checkbox": bool(section_c.get("is_partner"))},
            }
            if b_desc:
                props["Section B"] = {"rich_text": _rich_text_chunks(b_desc)}
                props["Section B Type"] = {"select": {"name": infer_section_b_type(section_b)}}
            if b_ids:
                props["Section B Movements"] = {"relation": [{"id": mid} for mid in b_ids]}
            if c_desc:
                props["Section C"] = {"rich_text": _rich_text_chunks(c_desc)}
            c_format = section_c.get("format") or infer_section_c_format(section_c)
            if c_format:
                props["Section C Format"] = {"select": {"name": c_format}}
            if c_ids:
                props["Section C Movements"] = {"relation": [{"id": mid} for mid in c_ids]}
            if section_c.get("duration_mins") is not None:
                props["Duration Mins"] = {"number": section_c["duration_mins"]}
            if section_c.get("time_cap_mins") is not None:
                props["Time Cap Mins"] = {"number": section_c["time_cap_mins"]}
            if training_notes:
                props["Training Notes"] = {"rich_text": _rich_text_chunks(training_notes)}

            try:
                notion_call(notion.pages.create, parent={"database_id": workout_days_db_id}, properties=props)
                days_created += 1
                log.info("save_programme: created day row %s / %s", track, day)
            except Exception as e:
                log.error("save_programme: failed to create row %s/%s: %s", track, day, e)
                log.error("save_programme: failed props keys: %s", list(props.keys()))

    if program_movement_ids:
        try:
            notion_call(notion.pages.update, page_id=parent_page_id, properties={"Movements": {"relation": [{"id": mid} for mid in sorted(program_movement_ids)]}})
        except Exception as e:
            log.warning("save_programme: could not update final parent movements: %s", e)

    log.info("save_programme: complete — %d day rows created", days_created)
    return parent_page_id


def save_programme_from_notion_row(
    notion,
    parent_page_id: str,
    workout_days_db_id: str,
    movements_db_id: str,
    parsed: dict,
    program_db_id: str | None = None,
    cycles_db_id: str | None = None,
) -> int:
    """
    Like save_programme() but writes Workout Days rows linked to an
    existing Weekly Programs page. Returns count of day rows created.
    """
    if "tracks" not in parsed and "days" in parsed:
        parsed = {
            "week_label": parsed.get("week_label"),
            "tracks": [{"track": "Performance", "days": parsed.get("days", [])}],
        }

    week_label = parsed.get("week_label") or "Week"
    monday_iso = _week_start_from_label(week_label)
    program_db_id = program_db_id or os.getenv("NOTION_WEEKLY_PROGRAMS_DB") or os.getenv("NOTION_WORKOUT_PROGRAM_DB", "")
    cycle_id, week_number = _get_or_create_cycle_metadata(
        notion,
        cycles_db_id or os.getenv("NOTION_CYCLES_DB", ""),
        program_db_id,
        monday_iso,
    ) if program_db_id else (None, None)

    try:
        notion_call(
            notion.pages.update,
            page_id=parent_page_id,
            properties={"Name": {"title": [{"text": {"content": week_label}}]}, **_weekly_program_metadata_props(cycle_id, week_number, monday_iso)},
        )
    except Exception as e:
        log.warning("save_programme_from_notion_row: could not update parent name: %s", e)

    movement_cache: dict[str, str] = _load_movement_cache_sync(notion, movements_db_id) if movements_db_id else {}
    program_movement_ids: set[str] = set()
    all_names = _extract_unique_movement_names(parsed)
    if movements_db_id:
        for name in all_names:
            try:
                ids = _resolve_section_movements({"description": name, "movements": [name]}, movement_cache, notion, movements_db_id)
                program_movement_ids.update(ids)
                if ids and name not in movement_cache:
                    movement_cache[name] = ids[0]
            except Exception as e:
                log.warning("save_programme_from_notion_row: movement '%s' failed: %s", name, e)

    # keep Weekly Programs metadata in sync (aggregate movement relation)
    try:
        parent_props = {"Name": {"title": [{"text": {"content": week_label}}]}, **_weekly_program_metadata_props(cycle_id, week_number, monday_iso)}
        if program_movement_ids:
            parent_props["Movements"] = {"relation": [{"id": mid} for mid in sorted(program_movement_ids)]}
            parent_props["Movement Summary"] = {"rich_text": _rich_text_chunks(", ".join(sorted(all_names)[:25]))}
        notion_call(notion.pages.update, page_id=parent_page_id, properties=parent_props)
    except Exception as e:
        log.warning("save_programme_from_notion_row: could not update parent metadata: %s", e)

    days_created = 0
    for track_row in parsed.get("tracks", []):
        track = track_row.get("track") or "Performance"
        for day_row in track_row.get("days", []):
            day = day_row.get("day") or "Monday"
            section_b = day_row.get("section_b") or {}
            section_c = day_row.get("section_c") or {}
            training_notes = day_row.get("training_notes") or ""
            b_desc = section_b.get("description") or ""
            c_desc = section_c.get("description") or ""

            b_ids = _resolve_section_movements(section_b, movement_cache, notion, movements_db_id) if movements_db_id else []
            c_ids = _resolve_section_movements(section_c, movement_cache, notion, movements_db_id) if movements_db_id else []
            program_movement_ids.update(b_ids)
            program_movement_ids.update(c_ids)

            if not workout_days_db_id:
                continue

            props: dict = {
                "Name": {"title": [{"text": {"content": f"{day} — {track} — {week_label}"}}]},
                "Day": {"select": {"name": day}},
                "Track": {"select": {"name": track}},
                "Week": {"relation": [{"id": parent_page_id}]},
                "Week Of": {"date": {"start": monday_iso}},
                "Is Partner": {"checkbox": bool(section_c.get("is_partner"))},
            }
            if b_desc:
                props["Section B"] = {"rich_text": _rich_text_chunks(b_desc)}
                props["Section B Type"] = {"select": {"name": infer_section_b_type(section_b)}}
            if b_ids:
                props["Section B Movements"] = {"relation": [{"id": mid} for mid in b_ids]}
            if c_desc:
                props["Section C"] = {"rich_text": _rich_text_chunks(c_desc)}
            c_format = section_c.get("format") or infer_section_c_format(section_c)
            if c_format:
                props["Section C Format"] = {"select": {"name": c_format}}
            if c_ids:
                props["Section C Movements"] = {"relation": [{"id": mid} for mid in c_ids]}
            if section_c.get("duration_mins") is not None:
                props["Duration Mins"] = {"number": section_c["duration_mins"]}
            if section_c.get("time_cap_mins") is not None:
                props["Time Cap Mins"] = {"number": section_c["time_cap_mins"]}
            if training_notes:
                props["Training Notes"] = {"rich_text": _rich_text_chunks(training_notes)}

            try:
                notion_call(notion.pages.create, parent={"database_id": workout_days_db_id}, properties=props)
                days_created += 1
                log.info("save_programme_from_notion_row: created %s / %s", track, day)
            except Exception as e:
                log.error("save_programme_from_notion_row: failed %s/%s: %s", track, day, e)
                log.error("save_programme_from_notion_row: props keys: %s", list(props.keys()))

    if program_movement_ids:
        try:
            notion_call(
                notion.pages.update,
                page_id=parent_page_id,
                properties={"Movements": {"relation": [{"id": mid} for mid in sorted(program_movement_ids)]}},
            )
        except Exception as e:
            log.warning("save_programme_from_notion_row: could not update final parent movements: %s", e)

    log.info("save_programme_from_notion_row: complete — %d rows", days_created)
    return days_created


def validate_workout_days_db(notion, workout_days_db_id: str) -> list[str]:
    """
    Validate that the Workout Days DB is writable and has expected schema.
    Returns list of problems. Empty = OK.
    """
    problems = []
    if not workout_days_db_id:
        problems.append("NOTION_WORKOUT_DAYS_DB is not set")
        return problems
    try:
        db = notion_call(notion.databases.retrieve, database_id=workout_days_db_id)
        props = db.get("properties", {})
        required = ["Name", "Day", "Track", "Week", "Week Of", "Section B", "Section C", "Is Partner"]
        for required_name in required:
            if required_name not in props:
                problems.append(f"Missing property: '{required_name}'")
    except Exception as e:
        problems.append(f"Cannot retrieve Workout Days DB: {e}")
    return problems

def create_strength_log(notion, workout_log_db_id, movement_page_id, movement_name, load_lbs, effort_sets, effort_reps, is_max_attempt, weekly_program_page_id, cycle_page_id, readiness, workout_date=None, effort_scheme=None, load_kg=None):
    """Create a Section B strength/accessory log in Workout Log v2.

    Readiness is intentionally ignored in Phase 1 because readiness now lives
    in the Daily Readiness database.
    """
    del readiness
    del movement_name
    del is_max_attempt
    del cycle_page_id
    del effort_scheme
    del load_kg
    workout_date = workout_date or datetime.now(timezone.utc).date().isoformat()
    movement_ids = movement_page_id if isinstance(movement_page_id, list) else [movement_page_id]
    props = {
        "Name": {"title": [{"text": {"content": f"{workout_date} — Strength"}}]},
        "Date": {"date": {"start": workout_date}},
        "effort_sets": {"number": effort_sets} if effort_sets is not None else None,
        "effort_reps": {"number": effort_reps} if effort_reps is not None else None,
        "load_lbs": {"number": load_lbs} if load_lbs is not None else None,
        "Movement": {"relation": [{"id": mid} for mid in movement_ids if mid]},
        "weekly_program_ref": {"relation": [{"id": weekly_program_page_id}]} if weekly_program_page_id else None,
    }
    props = {key: value for key, value in props.items() if value is not None}
    page = notion_call(notion.pages.create, parent={"database_id": workout_log_db_id}, properties=props)
    return page["id"]


def notion_query_wod_log_by_date(notion, wod_log_db_id: str, workout_date: str, wod_format: str | None = None) -> list[dict]:
    """Return WOD log entries matching a workout date and optional format."""
    filters = [{"property": "Date", "date": {"equals": workout_date}}]
    if wod_format:
        filters.append({"property": "Format", "select": {"equals": wod_format}})
    query_filter = filters[0] if len(filters) == 1 else {"and": filters}
    return notion_call(
        notion.databases.query,
        database_id=wod_log_db_id,
        filter=query_filter,
        page_size=1,
    ).get("results", [])


def create_wod_log(notion, wod_log_db_id, wod_format, duration_mins, time_cap_mins, result_type, result_seconds, result_rounds, result_reps, rx_scaled, scaling_notes, is_partner, wod_name, movement_page_ids, weekly_program_page_id, readiness, workout_date=None, workout_structure=None):
    """Create a Section C WOD log in the dedicated WOD Log database.

    Readiness is intentionally ignored in Phase 1 because readiness now lives
    in the Daily Readiness database.
    """
    # Older callers passed the workout date in the readiness slot while readiness
    # was intentionally ignored. Preserve that behavior, but prefer the explicit
    # workout_date argument for new callers.
    if workout_date is None and isinstance(readiness, str):
        workout_date = readiness
    workout_date = workout_date or datetime.now(timezone.utc).date().isoformat()
    props = {
        "Name": {"title": [{"text": {"content": f"{(wod_name or wod_format)} — {workout_date}"}}]},
        "Date": {"date": {"start": workout_date}},
        "Format": {"select": {"name": wod_format}},
        "Result Type": {"select": {"name": result_type}},
        "Rx / Scaled": {"select": {"name": rx_scaled}},
        "Partner?": {"checkbox": bool(is_partner)},
    }
    if duration_mins is not None:
        props["Duration Mins"] = {"number": duration_mins}
    if time_cap_mins is not None:
        props["Time Cap (mins)"] = {"number": time_cap_mins}
    if result_seconds is not None:
        props["Result (seconds)"] = {"number": result_seconds}
    if result_rounds is not None:
        props["Result (rounds)"] = {"number": result_rounds}
    if result_reps is not None:
        props["Result (reps)"] = {"number": result_reps}
    if movement_page_ids:
        props["Movements"] = {"relation": [{"id": mid} for mid in movement_page_ids if mid]}
    if weekly_program_page_id:
        props["Weekly Program"] = {"relation": [{"id": weekly_program_page_id}]}
    if wod_name:
        props["WOD Name"] = {"rich_text": [{"text": {"content": str(wod_name)}}]}
    if workout_structure:
        props["Workout Structure"] = {"rich_text": [{"text": {"content": str(workout_structure)}}]}
    if scaling_notes:
        props["Scaling Notes"] = {"rich_text": [{"text": {"content": str(scaling_notes)}}]}
    page = notion_call(notion.pages.create, parent={"database_id": wod_log_db_id}, properties=props)
    return page["id"]


def upsert_training_log_feel(notion, daily_readiness_db_id: str, rating, workout_date: str | None = None) -> str | None:
    """Upsert standalone workout feel onto the Daily Readiness entry for a date."""
    workout_date = workout_date or datetime.now(timezone.utc).date().isoformat()
    if not daily_readiness_db_id:
        raise ValueError("NOTION_DAILY_READINESS_DB is not configured")
    properties = {
        "Workout Feel": {"select": {"name": str(rating)}},
    }
    results = notion_call(
        notion.databases.query,
        database_id=daily_readiness_db_id,
        filter={"property": "Date", "date": {"equals": workout_date}},
        page_size=1,
    ).get("results", [])
    if results:
        page_id = results[0]["id"]
        notion_call(notion.pages.update, page_id=page_id, properties=properties)
        return page_id
    page = notion_call(
        notion.pages.create,
        parent={"database_id": daily_readiness_db_id},
        properties={
            "Name": {"title": [{"text": {"content": f"Workout Feel — {workout_date}"}}]},
            "Date": {"date": {"start": workout_date}},
            **properties,
        },
    )
    return page.get("id")


def _relation_names(notion, relation_items: list[dict]) -> list[str]:
    names: list[str] = []
    for rel in relation_items or []:
        page_id = rel.get("id")
        if not page_id:
            continue
        try:
            page = notion_call(notion.pages.retrieve, page_id=page_id)
            name = _title(page.get("properties", {}))
            if name:
                names.append(name)
        except Exception as e:
            log.warning("could not retrieve related movement %s: %s", page_id, e)
    return names


def get_movement_details(notion, movement_page_id: str) -> dict:
    page = notion_call(notion.pages.retrieve, page_id=movement_page_id)
    props = page.get("properties", {})
    return {
        "page_id": movement_page_id,
        "name": _title(props),
        "scaling_notes": _plain_rich_text(props, "Scaling Notes"),
        "antagonist_movements": _relation_names(notion, props.get("Antagonist Movements", {}).get("relation", [])),
        "complementary_movements": _relation_names(notion, props.get("Complementary Movements", {}).get("relation", [])),
    }


def fuzzy_find_movement_details(notion, movements_db_id: str, movement_name: str) -> dict | None:
    cache = _load_movement_cache_sync(notion, movements_db_id)
    matches = _run_fuzzy_match_sync([movement_name], cache)
    if not matches:
        return None
    _extracted, matched_name, score = matches[0]
    if not matched_name or score < 0.70:
        return None
    return get_movement_details(notion, cache[matched_name])


def format_movement_sub_details(details: dict) -> str:
    if not details:
        return "No movement match found."
    comp = ", ".join(details.get("complementary_movements") or []) or "None set"
    ant = ", ".join(details.get("antagonist_movements") or []) or "None set"
    scaling = details.get("scaling_notes") or "None set"
    return (
        f"🎯 *{details.get('name') or 'Movement'}*\n"
        f"📝 Scaling: {scaling}\n"
        f"🔄 Subs/Complements: {comp}\n"
        f"⬅️ Antagonists: {ant}"
    )

def query_subs(notion, subs_db_id, movements_db_id, movement_name, sub_type):
    m=find_movement_by_name(notion,movements_db_id,movement_name)
    if not m: return []
    res=notion_call(notion.databases.query,database_id=subs_db_id).get("results",[])
    out=[]
    for r in res:
        p=r.get("properties",{})
        rel=[x.get("id") for x in p.get("Movement",{}).get("relation",[])]
        typ=(p.get("Type",{}).get("select") or {}).get("name")
        if m["page_id"] in rel and typ==sub_type:
            out.append({"name":_title(p),"alt_movement":"","difficulty":((p.get("Difficulty",{}).get("select") or {}).get("name") or ""),"equipment_needed":"","rationale":""})
    return out


def get_progressions_for_movement(notion, progressions_db_id, movement_page_id) -> list[dict]:
    res = notion_call(notion.databases.query, database_id=progressions_db_id, page_size=100).get("results", [])
    out = []
    for row in res:
        props = row.get("properties", {})
        rel = [x.get("id") for x in props.get("Target Movement", {}).get("relation", [])]
        if movement_page_id not in rel:
            continue
        out.append({
            "page_id": row.get("id"),
            "name": _title(props),
            "order": props.get("Order", {}).get("number") or 0,
            "is_current_level": bool(props.get("Is My Current Level", {}).get("checkbox")),
            "notes": "".join(x.get("plain_text", "") for x in props.get("Notes", {}).get("rich_text", [])),
        })
    return sorted(out, key=lambda x: x.get("order", 0))


def get_movement_category(notion, movements_db_id, movement_page_id) -> str:
    del movements_db_id
    page = notion_call(notion.pages.retrieve, page_id=movement_page_id)
    props = page.get("properties", {})
    category = props.get("Category", {})
    multi_select = category.get("multi_select") or []
    if multi_select:
        return multi_select[0].get("name") or ""
    return ((category.get("select") or {}).get("name") or "")


def set_current_level(notion, progressions_db_id, movement_page_id, new_current_page_id):
    steps = get_progressions_for_movement(notion, progressions_db_id, movement_page_id)
    for step in steps:
        notion_call(
            notion.pages.update,
            page_id=step["page_id"],
            properties={"Is My Current Level": {"checkbox": step["page_id"] == new_current_page_id}},
        )
