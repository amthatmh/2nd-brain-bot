import json
import logging
import os
from datetime import date, timedelta
from typing import Any
from urllib import parse, request

log = logging.getLogger(__name__)

ASANA_BASE_URL = "https://app.asana.com/api/1.0"


class AsanaSyncError(Exception):
    pass


def _asana_request(path: str, token: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
    query_str = f"?{parse.urlencode(query)}" if query else ""
    url = f"{ASANA_BASE_URL}{path}{query_str}"

    req = request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        method="GET",
    )
    with request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def _asana_tasks_for_project(project_gid: str, token: str) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    offset = None

    while True:
        query: dict[str, Any] = {
            "project": project_gid,
            "limit": 100,
            "completed_since": os.environ.get("ASANA_COMPLETED_SINCE", "1970-01-01T00:00:00Z"),
            "opt_fields": "gid,name,completed,due_on,permalink_url,notes,memberships.section.name",
        }
        if offset:
            query["offset"] = offset

        payload = _asana_request("/tasks", token, query=query)
        tasks.extend(payload.get("data", []))

        next_page = payload.get("next_page")
        if not next_page or not next_page.get("offset"):
            break
        offset = next_page["offset"]

    return tasks


def _asana_tasks_for_me(token: str) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    offset = None

    while True:
        query: dict[str, Any] = {
            "limit": 100,
            "completed_since": os.environ.get("ASANA_COMPLETED_SINCE", "1970-01-01T00:00:00Z"),
            "opt_fields": "gid,name,completed,due_on,permalink_url,notes,memberships.section.name",
        }
        if offset:
            query["offset"] = offset

        payload = _asana_request("/users/me/tasks", token, query=query)
        tasks.extend(payload.get("data", []))

        next_page = payload.get("next_page")
        if not next_page or not next_page.get("offset"):
            break
        offset = next_page["offset"]

    return tasks


def _notion_prop(title: str) -> dict[str, Any]:
    return {"title": [{"text": {"content": title}}]}


def _notion_rich_text(value: str) -> dict[str, Any]:
    return {"rich_text": [{"text": {"content": value}}]}


def _notion_date(value: str | None) -> dict[str, Any]:
    if not value:
        return {"date": None}
    return {"date": {"start": value}}


def _horizon_deadline(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def _map_context(task: dict[str, Any]) -> str:
    section_name = ((task.get("memberships") or [{}])[0].get("section") or {}).get("name", "").lower()
    if any(k in section_name for k in ["health", "fitness", "workout"]):
        return "🏃 Health"
    if any(k in section_name for k in ["home", "personal", "family"]):
        return "🏠 Personal"
    if any(k in section_name for k in ["collab", "client", "team"]):
        return "🤝 Collab"
    return "💼 Work"


def _find_notion_task_by_asana_id(notion, database_id: str, asana_gid: str) -> dict[str, Any] | None:
    result = notion.databases.query(
        database_id=database_id,
        filter={"property": "Asana Task ID", "rich_text": {"equals": asana_gid}},
        page_size=1,
    )
    rows = result.get("results", [])
    return rows[0] if rows else None


def _build_notion_properties(task: dict[str, Any]) -> dict[str, Any]:
    title = task.get("name") or "Untitled Asana Task"
    due_on = task.get("due_on")
    completed = bool(task.get("completed"))

    return {
        "Name": _notion_prop(title),
        "Deadline": _notion_date(due_on or _horizon_deadline(7)),
        "Context": {"select": {"name": _map_context(task)}},
        "Source": {"select": {"name": "🟣 Asana"}},
        "Done": {"checkbox": completed},
        "Recurring": {"select": {"name": "None"}},
        "Asana Task ID": _notion_rich_text(task["gid"]),
        "Asana URL": {"url": task.get("permalink_url")},
    }


def sync_asana_project_to_notion(
    *,
    notion,
    notion_db_id: str,
    asana_token: str,
    asana_project_gid: str,
    source_mode: str = "project",
) -> tuple[int, int]:
    if not asana_token:
        raise AsanaSyncError("ASANA_PAT is missing")
    if source_mode == "project" and not asana_project_gid:
        raise AsanaSyncError("ASANA_PROJECT_GID is missing")

    if source_mode == "my_tasks":
        tasks = _asana_tasks_for_me(asana_token)
    else:
        tasks = _asana_tasks_for_project(asana_project_gid, asana_token)
    created = 0
    updated = 0

    for task in tasks:
        if not task.get("gid"):
            continue

        properties = _build_notion_properties(task)
        existing = _find_notion_task_by_asana_id(notion, notion_db_id, task["gid"])

        if existing:
            notion.pages.update(page_id=existing["id"], properties=properties)
            updated += 1
        else:
            notion.pages.create(parent={"database_id": notion_db_id}, properties=properties)
            created += 1

    return created, updated
