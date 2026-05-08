from __future__ import annotations
import json
import re
from datetime import date, datetime, timedelta, timezone
from second_brain.notion import notion_call
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
        "Category": {"select": {"name": "Compound"}},
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
    if "EMOM" in desc:
        return "EMOM"
    if "Every" in desc:
        return "Intervals"
    if "skill" in desc.lower():
        return "Skill"
    return "Volume"


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

def save_programme(notion, program_db_id: str, workout_days_db_id: str, movements_db_id: str, parsed: dict, full_text: str) -> str:
    if "tracks" not in parsed and "days" in parsed:
        parsed = {
            "week_label": parsed.get("week_label"),
            "tracks": [{"track": "Performance", "days": parsed.get("days", [])}],
        }

    week_label = parsed.get("week_label") or "Week"
    monday_iso = this_monday()

    try:
        parent = notion_call(
            notion.pages.create,
            parent={"database_id": program_db_id},
            properties={
                "Name": {"title": [{"text": {"content": week_label}}]},
                "Full Program": {"rich_text": _rich_text_chunks(full_text)},
            },
        )
        parent_page_id = parent["id"]
        log.info("save_programme: created parent row %s", parent_page_id)
    except Exception as e:
        log.error("save_programme: failed to create Weekly Programs row: %s", e)
        raise

    movement_cache: dict[str, str] = {}
    if movements_db_id:
        all_movement_names = _extract_unique_movement_names(parsed)

        log.info("save_programme: resolving %d unique movements", len(all_movement_names))
        for movement_name in all_movement_names:
            try:
                movement_cache[movement_name] = get_or_create_movement(notion, movements_db_id, movement_name)
            except Exception as e:
                log.warning("save_programme: could not resolve movement '%s': %s", movement_name, e)

    if movement_cache:
        try:
            notion_call(notion.pages.update, page_id=parent_page_id, properties={"Movements": {"relation": [{"id": mid} for mid in movement_cache.values()]}})
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

            b_ids = [movement_cache[m] for m in (section_b.get("movements") or []) if m and m in movement_cache]
            c_ids = [movement_cache[m] for m in (section_c.get("movements") or []) if m and m in movement_cache]

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
            if section_c.get("format"):
                props["Section C Format"] = {"select": {"name": section_c["format"]}}
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

    log.info("save_programme: complete — %d day rows created", days_created)
    return parent_page_id


def save_programme_from_notion_row(
    notion,
    parent_page_id: str,
    workout_days_db_id: str,
    movements_db_id: str,
    parsed: dict,
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
    monday_iso = this_monday()
    cycle_match = re.search(r"cycle\\s*#?\\s*(\\d+)", week_label, re.IGNORECASE)
    cycle_num = int(cycle_match.group(1)) if cycle_match else None

    try:
        notion_call(
            notion.pages.update,
            page_id=parent_page_id,
            properties={"Name": {"title": [{"text": {"content": week_label}}]}},
        )
    except Exception as e:
        log.warning("save_programme_from_notion_row: could not update parent name: %s", e)

    movement_cache: dict[str, str] = {}
    all_names = _extract_unique_movement_names(parsed)
    if movements_db_id:
        for name in all_names:
            try:
                movement_cache[name] = get_or_create_movement(notion, movements_db_id, name)
            except Exception as e:
                log.warning("save_programme_from_notion_row: movement '%s' failed: %s", name, e)

    # keep Weekly Programs metadata in sync (aggregate movement relation)
    try:
        parent_props = {"Name": {"title": [{"text": {"content": week_label}}]}}
        if cycle_num is not None:
            parent_props["Cycle #"] = {"number": cycle_num}
        if movement_cache:
            parent_props["Movements"] = {"relation": [{"id": mid} for mid in movement_cache.values()]}
            parent_props["Movement Summary"] = {"rich_text": _rich_text_chunks(", ".join(sorted(movement_cache.keys())[:25]))}
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

            b_ids = [movement_cache[m] for m in (section_b.get("movements") or []) if m and m in movement_cache]
            c_ids = [movement_cache[m] for m in (section_c.get("movements") or []) if m and m in movement_cache]

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
            if section_c.get("format"):
                props["Section C Format"] = {"select": {"name": section_c["format"]}}
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

def get_previous_best(notion, prs_db_id, movement_page_id, reps):
    res=notion_call(notion.databases.query,database_id=prs_db_id,page_size=50).get("results",[])
    best=None
    for r in res:
        p=r.get("properties",{})
        rs=p.get("Reps",{}).get("number")
        rel=[x.get("id") for x in p.get("Movement",{}).get("relation",[])]
        wt=p.get("Weight (lbs)",{}).get("number")
        if rs==reps and movement_page_id in rel and wt is not None and (best is None or wt>best["weight_lbs"]):
            best={"page_id":r["id"],"weight_lbs":wt,"date":(p.get("Date",{}).get("date") or {}).get("start")}
    return best

def create_pr_entry(notion, prs_db_id, cycles_db_id, movement_page_id, movement_name, weight_lbs, reps, previous_best_lbs, notes):
    props={"Name":{"title":[{"text":{"content":f"{movement_name} {reps}RM — {datetime.now(timezone.utc).date().isoformat()}"}}]},"Date":{"date":{"start":datetime.now(timezone.utc).date().isoformat()}},"Movement":{"relation":[{"id":movement_page_id}]},"Weight (lbs)":{"number":weight_lbs},"Previous Best (lbs)":{"number":previous_best_lbs or 0},"Reps":{"number":reps},"Rep Format":{"rich_text":[{"text":{"content":f"{reps}RM"}}]},"Notes":{"rich_text":[{"text":{"content":notes or ""}}]}}
    page=notion_call(notion.pages.create,parent={"database_id":prs_db_id},properties=props); return page["id"]

def create_strength_log(notion, workout_log_db_id, movement_page_id, movement_name, load_lbs, effort_sets, effort_reps, is_max_attempt, weekly_program_page_id, cycle_page_id, readiness, workout_date=None, effort_scheme=None):
    """Create a Section B strength/accessory log in Workout Log v2.

    Readiness is intentionally ignored in Phase 1 because readiness now lives
    in the Daily Readiness database.
    """
    del readiness
    today = workout_date or datetime.now(timezone.utc).date().isoformat()
    movement_ids = movement_page_id if isinstance(movement_page_id, list) else [movement_page_id]
    props = {
        "Name": {"title": [{"text": {"content": f"{movement_name} — {today}"}}]},
        "Date": {"date": {"start": today}},
        "Movement": {"relation": [{"id": mid} for mid in movement_ids if mid]},
        "load_lbs": {"number": load_lbs},
        "effort_sets": {"number": effort_sets},
        "effort_reps": {"number": effort_reps},
        "is_max_attempt": {"checkbox": bool(is_max_attempt)},
    }
    if effort_scheme:
        props["effort_scheme"] = {"rich_text": [{"text": {"content": effort_scheme}}]}
    if weekly_program_page_id:
        props["weekly_program_ref"] = {"relation": [{"id": weekly_program_page_id}]}
    if cycle_page_id:
        props["Cycle"] = {"relation": [{"id": cycle_page_id}]}
    page = notion_call(notion.pages.create, parent={"database_id": workout_log_db_id}, properties=props)
    return page["id"]


def create_wod_log(notion, wod_log_db_id, wod_format, duration_mins, time_cap_mins, result_type, result_seconds, result_rounds, result_reps, rx_scaled, scaling_notes, is_partner, wod_name, movement_page_ids, weekly_program_page_id, readiness, workout_structure=None):
    """Create a Section C WOD log in the dedicated WOD Log database.

    Readiness is intentionally ignored in Phase 1 because readiness now lives
    in the Daily Readiness database.
    """
    del readiness
    today = datetime.now(timezone.utc).date().isoformat()
    props = {
        "Name": {"title": [{"text": {"content": f"{(wod_name or wod_format)} — {today}"}}]},
        "Date": {"date": {"start": today}},
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
    return ((props.get("Category", {}).get("select") or {}).get("name") or "")


def set_current_level(notion, progressions_db_id, movement_page_id, new_current_page_id):
    steps = get_progressions_for_movement(notion, progressions_db_id, movement_page_id)
    for step in steps:
        notion_call(
            notion.pages.update,
            page_id=step["page_id"],
            properties={"Is My Current Level": {"checkbox": step["page_id"] == new_current_page_id}},
        )
