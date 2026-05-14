from __future__ import annotations
import asyncio
import json
import os
import re
from datetime import date, datetime, timedelta
from second_brain.notion import notion_call
from second_brain.notion.properties import (
    query_all,
    rich_text_prop,
    title_prop,
)
from second_brain.utils import local_today
from .nlp import fuzzy_match_movements, normalize_movement_name
import logging

log = logging.getLogger(__name__)

MOVEMENT_BLOCKLIST_PATTERNS = [
    r"^clean[\s-]?up",
    r"^warm[\s-]?up",
    r"^\d{1,2}:\d{2}",
    r"^rest\b", r"^recovery\b",
    r"^prioritize\b", r"^practice\b",
    r"^aim\b", r"^avoid\b", r"^build\b",
    r"^expect\b", r"^scale\b", r"^substitute\b",
    r"^stagger\b", r"^roughly\b",
    r"^training\s+notes?\b",
    r"^you\s+\w+", r"^this\s+(emom|amrap|wod|workout|will|is)\b",
    r"^there\s+is\b", r"^get\s+off\b", r"^run\s+together\b",
    r"^split\s+the\b", r"^each\s+movement\b",
    r"^the\s+\w+", r"^if\s+you\b", r"^it\s+may\b",
    r"^as\s+the\b", r"^hang\s+from\b", r"^but\s+allows?\b",
    r"^this\s+workout\b", r"^other\b", r"^rep\s+scheme\b",
]


def is_valid_movement_candidate(name: str) -> bool:
    s = (name or "").strip()
    if not s:
        return False
    for pattern in MOVEMENT_BLOCKLIST_PATTERNS:
        if re.search(pattern, s, re.IGNORECASE):
            return False
    if len(s.split()) >= 5:
        return False
    if re.search(r"[,\.](?!\d)", s):
        return False
    return True


def load_movement_library(notion, movements_db_id: str) -> dict[str, str]:
    """Load all movements + aliases from NOTION_MOVEMENTS_DB."""
    cache: dict[str, str] = {}
    for page in query_all(notion, movements_db_id, page_size=100):
        props = page.get("properties", {})
        name = "".join(c.get("plain_text", "") for c in props.get("Name", {}).get("title", [])).strip()
        if name:
            cache[name.lower()] = page["id"]
        aliases_text = "".join(c.get("plain_text", "") for c in props.get("Aliases", {}).get("rich_text", [])).strip()
        for alias in re.split(r"[,;]+", aliases_text):
            alias = alias.strip().lower()
            if alias:
                cache[alias] = page["id"]
    log.info("load_movement_library: %d entries", len(cache))
    return cache


def match_movement(name: str, movement_cache: dict[str, str], threshold: int = 80) -> str | None:
    """Match name against loaded library. Returns page_id or None. Never creates pages."""
    from rapidfuzz import process, fuzz
    if not name or not movement_cache:
        return None
    key = name.strip().lower()
    lowered = {str(k).lower(): v for k, v in movement_cache.items()}
    if key in lowered:
        return lowered[key]
    simple_key = re.sub(r"[^a-z0-9 ]+", " ", key)
    simple_key = re.sub(r"\s+", " ", simple_key).strip()
    for candidate, page_id in lowered.items():
        simple_candidate = re.sub(r"[^a-z0-9 ]+", " ", candidate)
        simple_candidate = re.sub(r"\s+", " ", simple_candidate).strip()
        singular_candidate = re.sub(r"\b(\w+)s\b", r"\1", simple_candidate)
        if simple_key and (simple_key in simple_candidate or simple_key in singular_candidate):
            return page_id
    result = process.extractOne(key, lowered.keys(), scorer=fuzz.token_sort_ratio, score_cutoff=threshold)
    if result:
        matched_key, score, _ = result
        log.debug("match_movement: '%s'→'%s' score=%d", name, matched_key, score)
        return lowered[matched_key]
    log.debug("match_movement: no match for '%s'", name)
    return None


def get_available_tracks_today(notion, workout_days_db_id: str) -> list[dict]:
    """Return list of {track, page_id} dicts for today's Workout Days rows."""
    if not workout_days_db_id:
        return []
    today = local_today()
    day_name = today.strftime("%A")
    monday = (today - timedelta(days=today.weekday())).isoformat()
    try:
        results = notion_call(
            notion.databases.query,
            database_id=workout_days_db_id,
            filter={"and": [
                {"property": "Day", "select": {"equals": day_name}},
                {"property": "Week Of", "date": {"equals": monday}},
            ]},
            page_size=5,
        ).get("results", [])
    except Exception as e:
        log.warning("get_available_tracks_today: %s", e)
        return []
    out = []
    for r in results:
        track = (r["properties"].get("Track", {}).get("select") or {}).get("name")
        if track:
            out.append({"track": track, "page_id": r["id"]})
    return out

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
        "Name": title_prop(name),
        "Category": {"multi_select": [{"name": "Compound"}]},
        "Primary Pattern": {"multi_select": [{"name": pattern} for pattern in primary_patterns]},
    }
    page = notion_call(notion.pages.create, parent={"database_id": movements_db_id}, properties=properties)
    return page["id"]



MOVEMENT_BLOCKLIST = {
    "accumulate",
    "build",
    "cool down",
    "cooldown",
    "effort",
    "every",
    "flow",
    "for quality",
    "for time",
    "interval",
    "minutes",
    "practice",
    "rest",
    "round",
    "rounds",
    "section",
    "section b",
    "section c",
    "strategy",
    "time cap",
    "warm up",
    "warm-up",
    "work",
}


MOVEMENT_ALIAS_MAP = [
    (r"russian\s+(kb|kettlebell)\s+swing", "Kettlebell Swing"),
    (r"american\s+(kb|kettlebell)\s+swing", "American Kettlebell Swing"),
    (r"(s/?a|single[\s-]arm)\s+(db|dumbbell)\s+overhead\s+(walking\s+)?lunge", "Overhead Carry"),
    (r"(db|dumbbell)\s+overhead\s+(walking\s+)?lunge", "Overhead Carry"),
    (r"(db|dumbbell)\s+goblet\s+(walking\s+)?lunge", "Lunge"),
    (r"double\s+(kb|kettlebell)\s+hang\s+clean[\s\w]*jerk", "Kettlebell Clean"),
    (r"hang\s+clean\s+and\s+jerk", "Clean & Jerk"),
    (r"box\s+jump\s+over", "Box Jump"),
    (r"burpee\s+broad\s+jump", "Burpee"),
    (r"line[\s-]facing\s+burpee", "Burpee"),
    (r"rope\s+climb", "Pull-Up"),
    (r"lying\s+to\s+stand\s+rope\s+(climb|pull)", "Ring Row"),
    (r"(db|dumbbell)\s+push\s+press", "Dumbbell Push Press"),
    (r"(kb|kettlebell)\s+hang\s+clean", "Kettlebell Clean"),
    (r"farmer[\s']*s?\s+(carry|walk)", "Farmer's Carry"),
    (r"assault\s+bike", "Assault Bike"),
    (r"ski\s+(erg|meters?)", "SkiErg"),
    (r"handstand\s+push[\s-]?ups?", "Handstand Push-Up"),
    (r"kneeling\s+push[\s-]?ups?", "Kneeling Push-Up"),
    (r"toes[\s-]?to[\s-]?bar", "Toes-to-Bar"),
    (r"\bt2b\b", "Toes-to-Bar"),
    (r"push[\s-]?ups?(?!\s+press)", "Push-Up"),
    (r"pull[\s-]?ups?", "Pull-Up"),
    (r"push\s*/\s*power\s+jerk", "Push Jerk"),
    (r"wall\s+ball", "Wall Ball"),
    (r"wall\s+walk", "Wall Walk"),
    (r"air\s+squat", "Air Squat"),
    (r"ring\s+row", "Ring Row"),
    (r"box\s+jump", "Box Jump"),
    (r"double[\s-]?under", "Double-Under"),
    (r"single[\s-]?under", "Single-Under"),
    (r"hanging\s+knee\s+raise", "Toes-to-Bar"),
    (r"kb\s+swing", "Kettlebell Swing"),
    (r"power\s+jerk", "Power Jerk"),
    (r"push\s+jerk", "Push Jerk"),
    (r"push\s+press", "Push Press"),
]


def normalise_movement_name(raw: str) -> list[str]:
    s = (raw or "").strip()
    compact = re.sub(r"\s+", " ", s.lower()).strip(" :-–—")
    if not compact or compact in MOVEMENT_BLOCKLIST:
        return []
    if re.fullmatch(r"(?:amrap|emom|tabata|chipper|for time|rest|work)(?:\s+\d+)?", compact):
        return []
    if re.fullmatch(r"\d+(?::\d{2})?(?:\s*(?:min|mins|minutes|sec|seconds|m|meters?|cal|cals|calories))?", compact):
        return []

    slash_parts = re.split(r"\s*/\s*", s)
    if len(slash_parts) > 1 and all(re.search(r"[a-zA-Z]{3,}", p) for p in slash_parts):
        if len(slash_parts) == 2:
            right_words = slash_parts[1].split()
            left_words = slash_parts[0].split()
            if len(left_words) == 1 and len(right_words) > 1:
                slash_parts[0] = f"{slash_parts[0]} {' '.join(right_words[1:])}"
        out: list[str] = []
        for part in slash_parts:
            out.extend(normalise_movement_name(part))
        return out

    s = re.sub(r"\s*\(.*?\)\s*$", "", s).strip()
    s = re.sub(r"^\d[\d/'\"\.]*\s*(meter|m|cal|calories|foot|feet)?\s*", "", s, flags=re.IGNORECASE).strip()

    if re.match(r"^\w[\w\s]+—\s*\d{1,2}:\d{2}-\d{1,2}:\d{2}", s):
        return []

    for pattern, canonical in MOVEMENT_ALIAS_MAP:
        if re.search(pattern, s, re.IGNORECASE):
            return [canonical]

    s = re.sub(
        r"^(double|single|s/a|sa|db|dumbbell|kb|kettlebell|barbell|"
        r"banded|weighted|strict|kipping|touch\s*n?\s*go|tng)\s+",
        "", s, flags=re.IGNORECASE,
    ).strip()
    s = re.sub(
        r"(?i)\b(swings|jumps|dips|lunges|squats|rows|burpees|climbs)\b",
        lambda m: m.group(0).rstrip("s"), s,
    )
    compact = re.sub(r"\s+", " ", s.lower()).strip(" :-–—")
    if compact in MOVEMENT_BLOCKLIST or len(compact) <= 1:
        return []
    return [s] if s else []


def get_movement_load_type(notion, page_id: str) -> str:
    try:
        page = notion_call(notion.pages.retrieve, page_id=page_id)
        lt = page["properties"].get("Load Type", {}).get("select", {})
        return lt.get("name", "External Load")
    except Exception:
        return "External Load"

def _extract_unique_movement_names(parsed: dict) -> set[str]:
    names: set[str] = set()
    for track_row in (parsed or {}).get("tracks", []):
        for day_row in track_row.get("days", []):
            for section_key in ("section_b", "section_c"):
                for raw in (day_row.get(section_key) or {}).get("movements") or []:
                    for canonical in normalise_movement_name(raw):
                        if canonical:
                            names.add(canonical)
    return names


def _plain_rich_text(props: dict, key: str) -> str:
    return "".join(x.get("plain_text", "") for x in props.get(key, {}).get("rich_text", []) or [])


def _load_movement_cache_sync(notion, movements_db_id: str) -> dict[str, str]:
    if not notion or not movements_db_id:
        return {}
    return load_movement_library(notion, movements_db_id)


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


def _resolve_section_movements(section: dict, movement_cache: dict[str, str], notion=None, movements_db_id: str = "") -> list[str]:
    del notion, movements_db_id
    section = section or {}
    candidates: list[str] = []
    for name in section.get("movements") or []:
        for canonical in normalise_movement_name(name):
            if canonical and canonical not in candidates:
                candidates.append(canonical)
    for name in _movement_names_from_text(section.get("description") or "", movement_cache):
        for canonical in normalise_movement_name(name):
            if canonical and canonical not in candidates:
                candidates.append(canonical)

    resolved: list[str] = []
    for raw_name in candidates:
        for canonical in normalise_movement_name(raw_name):
            if not canonical or not is_valid_movement_candidate(canonical):
                log.debug("programme: blocked '%s'", canonical)
                continue
            mid = match_movement(canonical, movement_cache)
            if mid:
                if mid not in resolved:
                    resolved.append(mid)
            else:
                log.info("programme: no library match for '%s' — skipped", canonical)
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
    """Legacy no-op: Weekly Programs now use plain Cycle/Week numbers."""
    del notion, cycles_db_id, program_db_id
    return None, 1


def _get_or_create_cycle_metadata(notion, cycles_db_id: str, program_db_id: str, monday_iso: str) -> tuple[str | None, int | None]:
    del monday_iso
    return _get_open_cycle_metadata(notion, cycles_db_id, program_db_id)


def _weekly_program_metadata_props(cycle_id: str | None, week_number: int | None, monday_iso: str) -> dict:
    del cycle_id
    props: dict = {"Start Date": {"date": {"start": monday_iso}}}
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
    today = local_today()
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


def _extract_candidate_movements_from_section(description: str) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    skip_re = re.compile(r"\b(?:every|minutes?|training notes|goal|rest|sets?|rounds?|time cap|amrap|emom|for time)\b", re.I)
    for raw_line in (description or "").splitlines():
        line = raw_line.strip().lstrip("•- ").strip()
        if not line or skip_re.search(line):
            continue
        for piece in re.split(r",|;|\+", line):
            for canonical in normalise_movement_name(piece):
                if not canonical or len(canonical) < 3:
                    continue
                key = canonical.lower()
                if key not in seen:
                    seen.add(key)
                    names.append(canonical)
    return names


def _extract_training_notes(block: str) -> tuple[str, str]:
    match = re.search(r"(?ims)^\s*training\s+notes\s*:\s*(?P<notes>.*)$", block or "")
    if not match:
        return block or "", ""
    return (block[:match.start()].rstrip(), match.group("notes").strip())


def _extract_sections(block: str) -> tuple[str, str, str]:
    lines = []
    time_marker = re.compile(r"^\s*\w[\w\s]+—\s*\d{1,2}:\d{2}-\d{1,2}:\d{2}\s*$")
    for line in (block or "").splitlines():
        if time_marker.match(line):
            continue
        lines.append(line)
    body = "\n".join(lines)
    marker = re.compile(r"(?im)^\s*[*_`]*(?:section\s*)?(?P<section>[BC])[*_`]*[\.)]\s+")
    matches = list(marker.finditer(body))
    section_text: dict[str, str] = {"B": "", "C": ""}
    for idx, match in enumerate(matches):
        key = match.group("section").upper()
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(body)
        section_text[key] = _strip_section_label(body[start:end])
    if not matches:
        section_text["C"] = body.strip()
    return section_text["B"], section_text["C"], ""


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
                "section_b": {"description": section_b, "movements": _extract_candidate_movements_from_section(section_b)} if section_b else {},
                "section_c": {"description": section_c, "movements": _extract_candidate_movements_from_section(section_c), "is_partner": "partner" in section_c.lower()} if section_c else {},
                "training_notes": training_notes,
            })
    tracks = [{"track": track, "days": days} for track, days in tracks_by_name.items() if days]
    if not tracks:
        raise ValueError("No day/track workout blocks found in Full Program")
    return {"week_label": f"Week of {this_monday()}", "tracks": tracks}

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

def save_programme(notion, program_db_id: str, workout_days_db_id: str, movements_db_id: str, parsed: dict, full_text: str, cycles_db_id: str | None = None, movement_cache: dict | None = None) -> str:
    if "tracks" not in parsed and "days" in parsed:
        parsed = {
            "week_label": parsed.get("week_label"),
            "tracks": [{"track": "Performance", "days": parsed.get("days", [])}],
        }

    monday_iso = this_monday()
    week_label = f"Week of {monday_iso}"
    cycle_id, week_number = _get_or_create_cycle_metadata(notion, cycles_db_id or os.getenv("NOTION_CYCLES_DB", ""), program_db_id, monday_iso)

    try:
        parent = notion_call(
            notion.pages.create,
            parent={"database_id": program_db_id},
            properties={
                "Name": title_prop(week_label),
                "Full Program": {"rich_text": _rich_text_chunks(full_text)},
                **_weekly_program_metadata_props(cycle_id, week_number, monday_iso),
            },
        )
        parent_page_id = parent["id"]
        log.info("save_programme: created parent row %s", parent_page_id)
    except Exception as e:
        log.error("save_programme: failed to create Weekly Programs row: %s", e)
        raise

    movement_cache: dict[str, str] = dict(movement_cache or (_load_movement_cache_sync(notion, movements_db_id) if movements_db_id else {}))
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
                "Name": title_prop(f"{day} — {track} — {week_label}"),
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
    movement_cache: dict | None = None,
) -> dict:
    """
    Like save_programme() but writes Workout Days rows linked to an
    existing Weekly Programs page. Returns day row count and unique movement
    page IDs written to Section B/C movement relations.
    """
    if "tracks" not in parsed and "days" in parsed:
        parsed = {
            "week_label": parsed.get("week_label"),
            "tracks": [{"track": "Performance", "days": parsed.get("days", [])}],
        }

    monday_iso = this_monday()
    week_label = f"Week of {monday_iso}"
    del program_db_id, cycles_db_id

    try:
        notion_call(
            notion.pages.update,
            page_id=parent_page_id,
            properties={
                "Name": title_prop(week_label),
                "Start Date": {"date": {"start": monday_iso}},
            },
        )
    except Exception as e:
        log.warning("save_programme_from_notion_row: could not update parent name: %s", e)

    movement_cache: dict[str, str] = dict(movement_cache or (_load_movement_cache_sync(notion, movements_db_id) if movements_db_id else {}))
    program_movement_ids: set[str] = set()
    day_movement_ids: set[str] = set()
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
        parent_props = {"Name": title_prop(week_label)}
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
                "Name": title_prop(f"{day} — {track} — {week_label}"),
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
                day_movement_ids.update(b_ids)
                day_movement_ids.update(c_ids)
                days_created += 1
                log.info("save_programme_from_notion_row: created %s / %s", track, day)
            except Exception as e:
                log.error("save_programme_from_notion_row: failed %s/%s: %s", track, day, e)
                log.error("save_programme_from_notion_row: props keys: %s", list(props.keys()))

    all_movement_ids = list({
        movement_cache[canonical]
        for track_row in parsed.get("tracks", [])
        for day_row in track_row.get("days", [])
        for section_key in ("section_b", "section_c")
        for raw in (day_row.get(section_key) or {}).get("movements") or []
        for canonical in normalise_movement_name(raw)
        if canonical and canonical in movement_cache
    }) or sorted(day_movement_ids or program_movement_ids)

    if all_movement_ids:
        try:
            notion_call(
                notion.pages.update,
                page_id=parent_page_id,
                properties={"Movements": {"relation": [{"id": mid} for mid in all_movement_ids]}},
            )
            log.info(
                "save_programme_from_notion_row: wrote %d movements to Weekly Programs",
                len(all_movement_ids),
            )
        except Exception as e:
            log.warning("movements rollup failed (non-fatal): %s", e)

    log.info("save_programme_from_notion_row: complete — %d rows", days_created)
    return {"days_created": days_created, "movement_ids": all_movement_ids}


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

# NOTION FORMULA FIX REQUIRED (manual change in Notion):
# calc_1rm_brzycki formula should be:
#   if(prop("effort_reps") > 0 and prop("effort_reps") < 37
#      and prop("load_lbs") > 0,
#      prop("load_lbs") * 36 / (37 - prop("effort_reps")),
#      0)
# calc_1rm_epley formula should be:
#   if(prop("effort_reps") > 0 and prop("effort_reps") <= 30
#      and prop("load_lbs") > 0,
#      prop("load_lbs") * (1 + prop("effort_reps") / 30.0),
#      0)
def create_strength_log(notion, workout_log_db_id, movement_page_id, movement_name, load_lbs, effort_sets, effort_reps, is_max_attempt, weekly_program_page_id, cycle_page_id, readiness, workout_date=None, effort_scheme=None, load_kg=None, raw_log: str = "", workout_day_id: str | None = None):
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
    workout_date = workout_date or local_today().isoformat()
    movement_ids = movement_page_id if isinstance(movement_page_id, list) else [movement_page_id]
    props = {
        "Name": title_prop(f"{workout_date} — Strength"),
        "Date": {"date": {"start": workout_date}},
        "effort_sets": {"number": effort_sets} if effort_sets is not None else None,
        "effort_reps": {"number": effort_reps} if effort_reps is not None else None,
        "load_lbs": {"number": load_lbs} if load_lbs is not None else None,
        "Movement": {"relation": [{"id": mid} for mid in movement_ids if mid]},
        "weekly_program_ref": {"relation": [{"id": weekly_program_page_id}]} if weekly_program_page_id else None,
    }
    props = {key: value for key, value in props.items() if value is not None}
    if raw_log:
        props["Log"] = {"rich_text": _rich_text_chunks(raw_log)}
    if workout_day_id:
        props["Workout Structure"] = {"relation": [{"id": workout_day_id}]}
    page = notion_call(notion.pages.create, parent={"database_id": workout_log_db_id}, properties=props)
    return page["id"]


def get_today_workout_structure(notion, workout_days_db_id: str) -> str:
    """
    Query Workout Days DB for today's row and return a formatted string
    combining Section B and Section C descriptions.
    Returns empty string if nothing found.
    """
    if not notion or not workout_days_db_id:
        return ""

    today = local_today()
    day_name = today.strftime("%A")
    monday = (today - timedelta(days=today.weekday())).isoformat()

    try:
        results = notion_call(
            notion.databases.query,
            database_id=workout_days_db_id,
            filter={
                "and": [
                    {"property": "Day", "select": {"equals": day_name}},
                    {"property": "Week Of", "date": {"equals": monday}},
                ]
            },
            page_size=3,
        ).get("results", [])
    except Exception as e:
        log.warning("get_today_workout_structure: query failed: %s", e)
        return ""

    if not results:
        return ""

    row = next(
        (
            r for r in results
            if (r.get("properties", {}).get("Track", {}).get("select") or {}).get("name") == "Performance"
        ),
        results[0],
    )
    props = row.get("properties", {})

    def _rt(field):
        return "".join(
            chunk.get("plain_text", "")
            for chunk in (props.get(field, {}).get("rich_text") or [])
        ).strip()

    b = _rt("Section B")
    c = _rt("Section C")

    parts = []
    if b:
        parts.append(f"Section B:\n{b}")
    if c:
        parts.append(f"Section C:\n{c}")

    return "\n\n".join(parts)


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


def create_wod_log(notion, wod_log_db_id, wod_format, duration_mins, time_cap_mins, result_type, result_seconds, result_rounds, result_reps, rx_scaled, scaling_notes, is_partner, wod_name, movement_page_ids, weekly_program_page_id, readiness, workout_date=None, workout_structure: str = "", raw_log: str = "", workout_day_id: str | None = None):
    """Create a Section C WOD log in the dedicated WOD Log database.

    Readiness is intentionally ignored in Phase 1 because readiness now lives
    in the Daily Readiness database.
    """
    # Older callers passed the workout date in the readiness slot while readiness
    # was intentionally ignored. Preserve that behavior, but prefer the explicit
    # workout_date argument for new callers.
    if workout_date is None and isinstance(readiness, str):
        workout_date = readiness
    workout_date = workout_date or local_today().isoformat()
    props = {
        "Name": title_prop(f"{(wod_name or wod_format)} — {workout_date}"),
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
        props["WOD Name"] = rich_text_prop(str(wod_name))
    if raw_log:
        props["Log"] = {"rich_text": _rich_text_chunks(raw_log)}
    if workout_day_id:
        props["Workout Structure"] = {"relation": [{"id": workout_day_id}]}
    if scaling_notes:
        props["Scaling Notes"] = rich_text_prop(str(scaling_notes))
    page = notion_call(notion.pages.create, parent={"database_id": wod_log_db_id}, properties=props)
    return page["id"]



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
