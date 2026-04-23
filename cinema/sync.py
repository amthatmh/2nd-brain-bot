"""Cinema Log daily sync helpers."""

from __future__ import annotations

from datetime import date

import httpx

TMDB_BASE = "https://api.themoviedb.org/3"


def _plain_text(prop: dict) -> str:
    chunks = prop.get("title") or prop.get("rich_text") or []
    return "".join(chunk.get("plain_text", "") for chunk in chunks).strip()


async def _search_tmdb_url(title: str, tmdb_api_key: str | None) -> str | None:
    return await _search_tmdb_url_with_client(title, tmdb_api_key, client=None)


async def _search_tmdb_url_with_client(
    title: str,
    tmdb_api_key: str | None,
    client: httpx.AsyncClient | None,
) -> str | None:
    if not tmdb_api_key or not title:
        return None

    if client is not None:
        for media_type in ("movie", "tv"):
            resp = await client.get(
                f"{TMDB_BASE}/search/{media_type}",
                params={"api_key": tmdb_api_key, "query": title, "page": 1},
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                tmdb_id = results[0].get("id")
                if tmdb_id:
                    return f"https://www.themoviedb.org/{media_type}/{tmdb_id}"
        return None

    async with httpx.AsyncClient(timeout=12) as owned_client:
        return await _search_tmdb_url_with_client(title, tmdb_api_key, owned_client)


def _favourite_exists(notion, fave_db_id: str, title: str) -> bool:
    result = notion.databases.query(
        database_id=fave_db_id,
        filter={"property": "Title", "title": {"equals": title}},
        page_size=1,
    )
    return bool(result.get("results"))


async def sync_cinema_log_to_notion(
    *,
    notion,
    cinema_db_id: str,
    fave_db_id: str,
    tmdb_api_key: str | None = None,
) -> dict[str, int]:
    """Sync unsynced cinema entries and optionally promote favourites."""
    stats = {
        "new_entries": 0,
        "tmdb_found": 0,
        "tmdb_missing": 0,
        "added_to_fave": 0,
    }

    query_filter: dict
    if tmdb_api_key:
        query_filter = {
            "or": [
                {"property": "Last Synced", "date": {"is_empty": True}},
                {"property": "TMDB URL", "url": {"is_empty": True}},
            ]
        }
    else:
        query_filter = {"property": "Last Synced", "date": {"is_empty": True}}

    rows: list[dict] = []
    cursor = None
    while True:
        q = {
            "database_id": cinema_db_id,
            "filter": query_filter,
            "page_size": 100,
        }
        if cursor:
            q["start_cursor"] = cursor
        response = notion.databases.query(**q)
        rows.extend(response.get("results", []))
        if not response.get("has_more"):
            break
        cursor = response.get("next_cursor")

    stats["new_entries"] = len(rows)

    async with httpx.AsyncClient(timeout=12) as client:
        for row in rows:
            props = row.get("properties", {})
            title = _plain_text(props.get("Film", {}))
            tmdb_prop = props.get("TMDB URL", {}).get("url")
            favourite = props.get("Favourite", {}).get("checkbox", False)

            update_props: dict = {}
            tmdb_url = tmdb_prop
            if not tmdb_url:
                tmdb_url = await _search_tmdb_url_with_client(title, tmdb_api_key, client)
                if tmdb_url:
                    update_props["TMDB URL"] = {"url": tmdb_url}
                    stats["tmdb_found"] += 1
                else:
                    stats["tmdb_missing"] += 1

            if favourite and title and not _favourite_exists(notion, fave_db_id, title):
                notion.pages.create(
                    parent={"database_id": fave_db_id},
                    properties={"Title": {"title": [{"text": {"content": title}}]}},
                )
                stats["added_to_fave"] += 1

            update_props["Last Synced"] = {"date": {"start": date.today().isoformat()}}
            notion.pages.update(page_id=row["id"], properties=update_props)

    return stats
