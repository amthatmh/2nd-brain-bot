"""
Asana <-> Notion reconciler.

Design:
- Asana is source of truth for task lifecycle (creates, deletions).
- Field edits flow bi-directionally using last-write-wins by timestamp.
- Notion-side `Last Synced At` prevents sync-induced feedback loops.
- Hybrid cache holds only {asana_gid -> notion_page_id} mappings.
- Dynamic 90-day completion window (self-tuning, no env var needed).
- Writable from Notion back to Asana: name, due date, completion.
- Context is Notion-owned after initial creation.

v9.1 changes:
- Single source of truth for the "Asana" Source label via ASANA_SOURCE_LABEL.
- New validate_notion_schema() helper for fail-loud startup validation.

v9.2 changes:
- Fix my_tasks mode: deprecated /users/me/tasks endpoint replaced with the
  modern User Task List flow (lookup UTL GID per workspace, then query it).
- New required parameter `asana_workspace_gid` for my_tasks mode.
- Module-level cache of UTL GIDs keyed by workspace, so we only look up
  the User Task List once per process.
"""

import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib import parse, request

log = logging.getLogger(__name__)

ASANA_BASE_URL = "https://app.asana.com/api/1.0"

# Fields pulled from Asana every cycle. modified_at is critical for LWW.
ASANA_OPT_FIELDS = (
    "gid,name,completed,completed_at,due_on,modified_at,"
    "permalink_url,notes,memberships.section.name,workspace.gid"
)

# How far back we pull completed tasks from Asana. Self-tuning window:
# tasks completed more than this many days ago are filtered at the source.
COMPLETED_WINDOW_DAYS = 90

# Cache safety-net: force a full rebuild every N seconds regardless of misses.
CACHE_FULL_REBUILD_SECONDS = 600  # 10 minutes

# ── Single source of truth for the Notion `Source` select option that marks
#    an Asana-sourced row. MUST match the exact label (incl. emoji) in Notion.
#    Change here → propagates to query filter AND new-row creation.
ASANA_SOURCE_LABEL = "🔗 Asana"

# ── Required Notion schema for the To-Do DB used by this reconciler.
#    Used by validate_notion_schema(). property_name → expected Notion type.
REQUIRED_NOTION_PROPERTIES: dict[str, str] = {
    "Name":              "title",
    "Source":            "select",
    "Deadline":          "date",
    "Done":              "checkbox",
    "Asana Task ID":     "rich_text",
    "Asana URL":         "url",
    "Asana Modified At": "rich_text",
    "Last Synced At":    "date",
}

# Required `Source` select options (the reconciler writes these names).
REQUIRED_SOURCE_OPTIONS: set[str] = {ASANA_SOURCE_LABEL}


class AsanaSyncError(Exception):
    pass


# ═══════════════════════════════════════════════════════════════════════════
# SCHEMA VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

def validate_notion_schema(notion, database_id: str) -> list[str]:
    """
    Verify the To-Do DB has every property the reconciler writes to,
    with the right type, and every required `Source` select option.

    Returns a list of human-readable problems. Empty list = schema healthy.
    Designed to be called once at startup so failures surface BEFORE the
    sync job starts hammering the API every N seconds.

    Never raises; on Notion API error returns a single "could not validate"
    entry so the caller can decide whether to proceed.
    """
    problems: list[str] = []
    try:
        db = notion.databases.retrieve(database_id=database_id)
    except Exception as e:
        return [f"Could not retrieve Notion DB {database_id}: {e}"]

    props = db.get("properties", {}) or {}

    # 1) Required properties exist with correct type
    for prop_name, expected_type in REQUIRED_NOTION_PROPERTIES.items():
        prop = props.get(prop_name)
        if prop is None:
            problems.append(f"Missing property: '{prop_name}' (expected {expected_type})")
            continue
        actual_type = prop.get("type")
        if actual_type != expected_type:
            problems.append(
                f"Property '{prop_name}' has wrong type: got '{actual_type}', expected '{expected_type}'"
            )

    # 2) `Source` select must contain every label the reconciler writes
    source_prop = props.get("Source") or {}
    if source_prop.get("type") == "select":
        existing_options = {
            opt.get("name") for opt in source_prop.get("select", {}).get("options", [])
        }
        for required_option in REQUIRED_SOURCE_OPTIONS:
            if required_option not in existing_options:
                pretty_existing = ", ".join(sorted(o for o in existing_options if o)) or "(none)"
                problems.append(
                    f"`Source` select is missing option '{required_option}'. "
                    f"Existing options: {pretty_existing}"
                )

    return problems


# ═══════════════════════════════════════════════════════════════════════════
# GID ↔ PAGE_ID CACHE
# ═══════════════════════════════════════════════════════════════════════════

class GidCache:
    """
    Thread-safe cache of {asana_gid -> notion_page_id} mappings.
    Does NOT cache row contents — only the patch-bay map.
    """

    def __init__(self) -> None:
        self._map: dict[str, str] = {}
        self._last_full_rebuild: datetime | None = None
        self._lock = threading.Lock()

    def get(self, gid: str) -> str | None:
        with self._lock:
            return self._map.get(gid)

    def has(self, gid: str) -> bool:
        with self._lock:
            return gid in self._map

    def all_gids(self) -> set[str]:
        with self._lock:
            return set(self._map.keys())

    def set(self, gid: str, page_id: str) -> None:
        with self._lock:
            self._map[gid] = page_id

    def drop(self, gid: str) -> None:
        with self._lock:
            self._map.pop(gid, None)

    def stale(self) -> bool:
        """True if we've never rebuilt or haven't rebuilt in a while."""
        with self._lock:
            if self._last_full_rebuild is None:
                return True
            elapsed = (datetime.now(timezone.utc) - self._last_full_rebuild).total_seconds()
            return elapsed > CACHE_FULL_REBUILD_SECONDS

    def rebuild(self, notion, database_id: str) -> None:
        """Full rebuild from Notion. Call on startup, periodically, and on miss."""
        fresh: dict[str, str] = {}
        cursor: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "database_id": database_id,
                "filter": {"property": "Source", "select": {"equals": ASANA_SOURCE_LABEL}},
                "page_size": 100,
            }
            if cursor:
                kwargs["start_cursor"] = cursor
            result = notion.databases.query(**kwargs)
            for page in result.get("results", []):
                gid = _read_rich_text(page, "Asana Task ID")
                if gid:
                    fresh[gid] = page["id"]
            if not result.get("has_more"):
                break
            cursor = result.get("next_cursor")

        with self._lock:
            self._map = fresh
            self._last_full_rebuild = datetime.now(timezone.utc)
        log.info("GID cache rebuilt: %d mappings", len(fresh))


# Module-level singleton — shared across all reconcile() calls in the process.
_cache = GidCache()


# ═══════════════════════════════════════════════════════════════════════════
# USER TASK LIST GID CACHE
# ═══════════════════════════════════════════════════════════════════════════
# Asana deprecated /users/me/tasks. Modern flow: each user has a per-workspace
# "User Task List" (UTL) — you GET its GID once, then query that UTL's tasks.
# We cache {workspace_gid -> utl_gid} for the process lifetime; the UTL ID
# never changes for a given (user, workspace) pair, so this is safe forever.
# ═══════════════════════════════════════════════════════════════════════════

_utl_cache: dict[str, str] = {}
_utl_cache_lock = threading.Lock()


def _get_user_task_list_gid(workspace_gid: str, token: str) -> str:
    """
    Look up (and cache) the current user's User Task List GID for a given workspace.
    Raises AsanaSyncError if Asana refuses or returns no UTL.
    """
    with _utl_cache_lock:
        cached = _utl_cache.get(workspace_gid)
        if cached:
            return cached

    try:
        payload = _asana_request(
            "/users/me/user_task_list",
            token,
            query={"workspace": workspace_gid, "opt_fields": "gid"},
        )
    except Exception as e:
        raise AsanaSyncError(
            f"Could not look up User Task List for workspace {workspace_gid}: {e}"
        ) from e

    data = payload.get("data") or {}
    utl_gid = data.get("gid")
    if not utl_gid:
        raise AsanaSyncError(
            f"Asana returned no User Task List for workspace {workspace_gid}. "
            "Check that ASANA_WORKSPACE_GID is correct and the PAT owner has access."
        )

    with _utl_cache_lock:
        _utl_cache[workspace_gid] = utl_gid
    log.info("Resolved User Task List GID %s for workspace %s", utl_gid, workspace_gid)
    return utl_gid


# ═══════════════════════════════════════════════════════════════════════════
# ASANA HTTP LAYER
# ═══════════════════════════════════════════════════════════════════════════

def _asana_request(
    path: str,
    token: str,
    method: str = "GET",
    query: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    query_str = f"?{parse.urlencode(query)}" if query else ""
    url = f"{ASANA_BASE_URL}{path}{query_str}"

    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=data, headers=headers, method=method)
    with request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _asana_paginated(path: str, token: str, base_query: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    offset = None
    while True:
        query = dict(base_query)
        query["limit"] = 100
        if offset:
            query["offset"] = offset
        payload = _asana_request(path, token, query=query)
        results.extend(payload.get("data", []))
        next_page = payload.get("next_page")
        if not next_page or not next_page.get("offset"):
            break
        offset = next_page["offset"]
    return results


def _dynamic_completed_since() -> str:
    """ISO timestamp for (now - 90 days). Self-tuning window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=COMPLETED_WINDOW_DAYS)
    return cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")


def _asana_fetch_tasks(
    token: str,
    source_mode: str,
    project_gid: str,
    workspace_gid: str,
) -> list[dict[str, Any]]:
    """
    Pull tasks from Asana based on source_mode:
    - 'project'  → all tasks in ASANA_PROJECT_GID
    - 'my_tasks' → all tasks in the current user's User Task List for the
                   given workspace (modern replacement for the deprecated
                   /users/me/tasks endpoint)
    """
    base = {
        "completed_since": _dynamic_completed_since(),
        "opt_fields": ASANA_OPT_FIELDS,
    }

    if source_mode == "my_tasks":
        utl_gid = _get_user_task_list_gid(workspace_gid, token)
        return _asana_paginated(f"/user_task_lists/{utl_gid}/tasks", token, base)

    base["project"] = project_gid
    return _asana_paginated("/tasks", token, base)


def _asana_update_task(gid: str, token: str, fields: dict[str, Any]) -> dict[str, Any]:
    payload = _asana_request(
        f"/tasks/{gid}",
        token,
        method="PUT",
        query={"opt_fields": ASANA_OPT_FIELDS},
        body={"data": fields},
    )
    return payload.get("data", {})


# ═══════════════════════════════════════════════════════════════════════════
# NOTION PROPERTY HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _notion_title(value: str) -> dict[str, Any]:
    return {"title": [{"text": {"content": value or ""}}]}


def _notion_rich_text(value: str | None) -> dict[str, Any]:
    if not value:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": value}}]}


def _notion_date(value: str | None) -> dict[str, Any]:
    if not value:
        return {"date": None}
    return {"date": {"start": value}}


def _read_title(page: dict[str, Any], prop: str) -> str:
    parts = page["properties"].get(prop, {}).get("title", []) or []
    return "".join(p.get("plain_text", "") for p in parts)


def _read_rich_text(page: dict[str, Any], prop: str) -> str:
    parts = page["properties"].get(prop, {}).get("rich_text", []) or []
    return "".join(p.get("plain_text", "") for p in parts)


def _read_date(page: dict[str, Any], prop: str) -> str | None:
    d = page["properties"].get(prop, {}).get("date")
    return d.get("start") if d else None


def _read_checkbox(page: dict[str, Any], prop: str) -> bool:
    return bool(page["properties"].get(prop, {}).get("checkbox"))


# ═══════════════════════════════════════════════════════════════════════════
# TIME HANDLING
# ═══════════════════════════════════════════════════════════════════════════

def _parse_iso(value: str | None) -> datetime | None:
    """Parse ISO-8601 timestamp as tz-aware UTC, or None."""
    if not value:
        return None
    cleaned = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════════════════
# CONTEXT MAPPING — only used when creating a fresh Notion row
# ═══════════════════════════════════════════════════════════════════════════

def _infer_context_from_asana(task: dict[str, Any]) -> str:
    memberships = task.get("memberships") or []
    section_name = ""
    if memberships:
        section = memberships[0].get("section") or {}
        section_name = (section.get("name") or "").lower()
    if any(k in section_name for k in ["health", "fitness", "workout"]):
        return "🏃 Health"
    if any(k in section_name for k in ["home", "personal", "family"]):
        return "🏠 Personal"
    if any(k in section_name for k in ["collab", "client", "team"]):
        return "🤝 Collab"
    return "💼 Work"


# ═══════════════════════════════════════════════════════════════════════════
# PROPERTY BUILDERS
# ═══════════════════════════════════════════════════════════════════════════

def _build_notion_create_props(task: dict[str, Any]) -> dict[str, Any]:
    """Properties for a brand-new Notion row mirroring an Asana task."""
    return {
        "Name":              _notion_title(task.get("name") or "Untitled Asana Task"),
        "Deadline":          _notion_date(task.get("due_on")),
        "Context":           {"select": {"name": _infer_context_from_asana(task)}},
        "Source":            {"select": {"name": ASANA_SOURCE_LABEL}},
        "Done":              {"checkbox": bool(task.get("completed"))},
        "Recurring":         {"select": {"name": "None"}},
        "Asana Task ID":     _notion_rich_text(task["gid"]),
        "Asana URL":         {"url": task.get("permalink_url")},
        "Asana Modified At": _notion_rich_text(task.get("modified_at")),
        "Last Synced At":    _notion_date(_now_iso()),
    }


def _asana_to_notion_update_props(task: dict[str, Any]) -> dict[str, Any]:
    """Updates pushed Asana → Notion. Deliberately omits Context (Notion-owned)."""
    return {
        "Name":              _notion_title(task.get("name") or "Untitled Asana Task"),
        "Deadline":          _notion_date(task.get("due_on")),
        "Done":              {"checkbox": bool(task.get("completed"))},
        "Asana URL":         {"url": task.get("permalink_url")},
        "Asana Modified At": _notion_rich_text(task.get("modified_at")),
        "Last Synced At":    _notion_date(_now_iso()),
    }


def _notion_to_asana_fields(page: dict[str, Any]) -> dict[str, Any]:
    """Only the three fields writable from Notion to Asana."""
    return {
        "name":      _read_title(page, "Name") or "Untitled",
        "due_on":    _read_date(page, "Deadline"),
        "completed": _read_checkbox(page, "Done"),
    }


def _notion_matches_asana_writable_fields(notion_page: dict[str, Any], asana_task: dict[str, Any]) -> bool:
    """
    Compare only fields that are writable Notion -> Asana.
    This avoids false N->A writes caused by coarse `Last Synced At` date-only values.
    """
    notion_fields = _notion_to_asana_fields(notion_page)
    asana_fields = {
        "name": asana_task.get("name") or "Untitled",
        "due_on": asana_task.get("due_on"),
        "completed": bool(asana_task.get("completed")),
    }
    return notion_fields == asana_fields


def _stamp_notion_after_reverse_write(page_id: str, asana_modified_at: str | None, notion) -> None:
    """Stamp Notion row after pushing Notion→Asana so next cycle doesn't re-trigger."""
    notion.pages.update(
        page_id=page_id,
        properties={
            "Asana Modified At": _notion_rich_text(asana_modified_at),
            "Last Synced At":    _notion_date(_now_iso()),
        },
    )


# ═══════════════════════════════════════════════════════════════════════════
# RECONCILIATION DECISION
# ═══════════════════════════════════════════════════════════════════════════

def _classify(asana_task: dict[str, Any], notion_page: dict[str, Any]) -> str:
    """Return 'skip', 'asana_to_notion', or 'notion_to_asana'."""
    last_synced = _parse_iso(_read_date(notion_page, "Last Synced At"))
    notion_asana_mod = _parse_iso(_read_rich_text(notion_page, "Asana Modified At"))
    asana_mod   = _parse_iso(asana_task.get("modified_at"))
    notion_mod  = _parse_iso(notion_page.get("last_edited_time"))

    # Never synced → treat as fresh Asana→Notion
    if last_synced is None:
        return "asana_to_notion"

    baseline_asana_mod = notion_asana_mod or last_synced
    asana_changed = asana_mod is not None and (
        baseline_asana_mod is None or asana_mod > baseline_asana_mod
    )
    notion_changed = not _notion_matches_asana_writable_fields(notion_page, asana_task)

    if not asana_changed and not notion_changed:
        return "skip"
    if asana_changed and not notion_changed:
        return "asana_to_notion"
    if notion_changed and not asana_changed:
        return "notion_to_asana"

    # Both changed — last-write-wins
    if asana_mod and notion_mod and asana_mod >= notion_mod:
        return "asana_to_notion"
    return "notion_to_asana"


# ═══════════════════════════════════════════════════════════════════════════
# MAIN RECONCILE ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def reconcile(
    *,
    notion,
    notion_db_id: str,
    asana_token: str,
    asana_project_gid: str = "",
    asana_workspace_gid: str = "",
    source_mode: str = "project",
) -> dict[str, int]:
    """
    Run one reconciliation cycle. Returns stats dict.
    Safe to call repeatedly; holds no state between calls except the
    module-level caches (GidCache and User Task List GID cache).
    """
    source_mode = (source_mode or "project").strip().lower()

    if not asana_token:
        raise AsanaSyncError("ASANA_PAT is missing")
    if source_mode not in {"project", "my_tasks"}:
        raise AsanaSyncError("ASANA_SYNC_SOURCE must be 'project' or 'my_tasks'")
    if source_mode == "project" and not asana_project_gid:
        raise AsanaSyncError("ASANA_PROJECT_GID is missing for source_mode=project")
    if source_mode == "my_tasks" and not asana_workspace_gid:
        raise AsanaSyncError("ASANA_WORKSPACE_GID is missing for source_mode=my_tasks")

    stats = {"created": 0, "a2n": 0, "n2a": 0, "deleted": 0, "skipped": 0}

    # ── Cache trigger 1 & 2: rebuild on startup or periodic safety net ──
    if _cache.stale():
        _cache.rebuild(notion, notion_db_id)

    # ── Pull Asana side ──
    asana_tasks  = _asana_fetch_tasks(
        asana_token, source_mode, asana_project_gid, asana_workspace_gid
    )
    asana_by_gid = {t["gid"]: t for t in asana_tasks if t.get("gid")}

    # ── Cache trigger 3: rebuild on detected miss ──
    asana_gids   = set(asana_by_gid.keys())
    cached_gids  = _cache.all_gids()
    unknown_gids = asana_gids - cached_gids
    if unknown_gids:
        log.info("Cache miss for %d GIDs, rebuilding", len(unknown_gids))
        _cache.rebuild(notion, notion_db_id)
        cached_gids = _cache.all_gids()

    # ── Process tasks present in Asana ──
    for gid, task in asana_by_gid.items():
        page_id = _cache.get(gid)

        if page_id is None:
            # Genuinely new — doesn't exist in Notion. Create.
            try:
                new_page = notion.pages.create(
                    parent={"database_id": notion_db_id},
                    properties=_build_notion_create_props(task),
                )
                _cache.set(gid, new_page["id"])  # Cache trigger 4: update on create
                stats["created"] += 1
            except Exception:
                log.exception("Failed to create Notion page for Asana GID %s", gid)
            continue

        # Exists in both — retrieve fresh Notion state and reconcile
        try:
            notion_page = notion.pages.retrieve(page_id=page_id)
        except Exception:
            log.exception("Failed to retrieve Notion page %s, invalidating cache entry", page_id)
            _cache.drop(gid)
            continue

        if notion_page.get("archived"):
            # Notion row was archived out-of-band — drop from cache, skip.
            _cache.drop(gid)
            continue

        decision = _classify(task, notion_page)
        if decision == "skip":
            stats["skipped"] += 1

        elif decision == "asana_to_notion":
            try:
                notion.pages.update(page_id=page_id, properties=_asana_to_notion_update_props(task))
                stats["a2n"] += 1
            except Exception:
                log.exception("Failed A→N update for GID %s", gid)

        elif decision == "notion_to_asana":
            try:
                fields = _notion_to_asana_fields(notion_page)
                refreshed = _asana_update_task(gid, asana_token, fields)
                _stamp_notion_after_reverse_write(page_id, refreshed.get("modified_at"), notion)
                stats["n2a"] += 1
            except Exception:
                log.exception("Failed N→A update for GID %s", gid)

    # ── Handle deletions: in cache but not in Asana → archive Notion row ──
    orphaned_gids = cached_gids - asana_gids
    for gid in orphaned_gids:
        page_id = _cache.get(gid)
        if not page_id:
            continue
        try:
            notion.pages.update(page_id=page_id, archived=True)
            _cache.drop(gid)  # Cache trigger 4: update on delete
            stats["deleted"] += 1
        except Exception:
            log.exception("Failed to archive orphaned Notion page %s", page_id)

    return stats
