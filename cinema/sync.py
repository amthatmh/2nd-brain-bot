"""Cinema Log daily sync helpers."""

from __future__ import annotations

from datetime import date
import re

import httpx

TMDB_BASE = "https://api.themoviedb.org/3"

def _title_search_candidates(title: str) -> list[str]:
    """Generate progressively simpler TMDB search candidates for noisy titles."""
    clean = " ".join((title or "").split()).strip()
    if not clean:
        return []

    candidates: list[str] = [clean]

    no_parens = re.sub(r"\s*\([^)]*\)", "", clean).strip()
    if no_parens and no_parens not in candidates:
        candidates.append(no_parens)

    no_year = re.sub(r"\s*(19|20)\d{2}$", "", no_parens or clean).strip(" -:/")
    if no_year and no_year not in candidates:
        candidates.append(no_year)

    primary_segment = re.split(r"[:\-–—|]", no_year or clean, maxsplit=1)[0].strip()
    if primary_segment and primary_segment not in candidates:
        candidates.append(primary_segment)

    return candidates


def _plain_text(prop: dict) -> str:
    chunks = prop.get("title") or prop.get("rich_text") or []
    return "".join(chunk.get("plain_text", "") for chunk in chunks).strip()


def _normalize_title(value: str) -> str:
    return " ".join((value or "").casefold().split())


def _extract_title(props: dict) -> str:
    """
    Resolve the cinema title from common Notion title property names.

    Some workspaces use "Film" while others keep the default "Name"/"Title".
    We also fall back to any property whose type is "title".
    """
    for key in ("Film", "Title", "Name"):
        title = _plain_text(props.get(key, {}))
        if title:
            return title

    for prop in props.values():
        if isinstance(prop, dict) and prop.get("type") == "title":
            title = _plain_text(prop)
            if title:
                return title
    return ""


def _parse_row_year(props: dict) -> int | None:
    value = (props.get("Date", {}).get("date") or {}).get("start")
    if isinstance(value, str) and len(value) >= 4 and value[:4].isdigit():
        return int(value[:4])
    return None


def _build_cinema_query_filter(tmdb_api_key: str | None) -> dict:
    """
    Build the Notion filter for cinema sync.

    We always process rows that were never synced or were synced before today,
    so operators can verify the job is running daily via the Last Synced column.
    With a TMDB key, we also keep retrying rows that are still missing TMDB URL.
    """
    base_conditions = [
        {"property": "Last Synced", "date": {"is_empty": True}},
        {"property": "Last Synced", "date": {"before": date.today().isoformat()}},
        {"property": "Favourite", "checkbox": {"equals": True}},
    ]
    if tmdb_api_key:
        base_conditions.append({"property": "TMDB URL", "url": {"is_empty": True}})
    return {"or": base_conditions}


async def _search_tmdb_url(title: str, tmdb_api_key: str | None) -> str | None:
    return await _search_tmdb_url_with_client(title, tmdb_api_key, client=None)


async def _search_tmdb_url_with_client(
    title: str,
    tmdb_api_key: str | None,
    client: httpx.AsyncClient | None,
    preferred_media_type: str | None = None,
) -> str | None:
    if not tmdb_api_key or not title:
        return None

    search_titles = _title_search_candidates(title)

    if client is not None:
        media_types = ("movie", "tv")
        if preferred_media_type in {"movie", "tv"}:
            media_types = (preferred_media_type, "tv" if preferred_media_type == "movie" else "movie")
        for query_title in search_titles:
            for media_type in media_types:
                resp = await client.get(
                    f"{TMDB_BASE}/search/{media_type}",
                    params={"api_key": tmdb_api_key, "query": query_title, "page": 1},
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


def _detect_favourite_db_fields(notion, fave_db_id: str | None) -> dict[str, str | None]:
    fields = {
        "title_prop": "Title",
        "year_prop": None,
        "year_type": None,
        "category_prop": None,
        "category_type": None,
    }
    if not fave_db_id:
        return fields

    schema = notion.databases.retrieve(database_id=fave_db_id)
    properties = schema.get("properties", {})
    title_prop = None
    for name, prop in properties.items():
        if (prop or {}).get("type") == "title":
            title_prop = name
            break
    if title_prop:
        fields["title_prop"] = title_prop

    year_prop = properties.get("Year", {})
    if year_prop:
        year_type = year_prop.get("type")
        if year_type in {"select", "number", "rich_text", "date"}:
            fields["year_prop"] = "Year"
            fields["year_type"] = year_type

    category_prop = properties.get("Category", {})
    if category_prop:
        category_type = category_prop.get("type")
        if category_type in {"select", "multi_select"}:
            fields["category_prop"] = "Category"
            fields["category_type"] = category_type

    return fields


def _load_existing_favourites(notion, fave_db_id: str | None, title_prop_name: str) -> set[str]:
    """
    Build an in-memory set of favourite titles once per sync run.

    This avoids one Notion query per row (N+1 pattern), which can become a
    bottleneck for larger sync batches and increases the risk of rate limits.
    """
    favourites: set[str] = set()
    if not fave_db_id:
        return favourites

    cursor = None
    while True:
        query = {
            "database_id": fave_db_id,
            "page_size": 100,
        }
        if cursor:
            query["start_cursor"] = cursor
        response = notion.databases.query(**query)
        for row in response.get("results", []):
            title = _plain_text(row.get("properties", {}).get(title_prop_name, {}))
            if title:
                favourites.add(_normalize_title(title))
        if not response.get("has_more"):
            break
        cursor = response.get("next_cursor")
    return favourites


def _preferred_media_type(props: dict) -> str | None:
    value = (props.get("Type", {}).get("select", {}) or {}).get("name", "").strip().lower()
    if value == "film":
        return "movie"
    if value == "series":
        return "tv"
    return None


async def sync_cinema_log_to_notion(
    *,
    notion,
    cinema_db_id: str,
    fave_db_id: str | None = None,
    tmdb_api_key: str | None = None,
) -> dict[str, int]:
    """Sync unsynced cinema entries and optionally promote favourites."""
    stats = {
        "new_entries": 0,
        "tmdb_found": 0,
        "tmdb_missing": 0,
        "added_to_fave": 0,
    }

    query_filter = _build_cinema_query_filter(tmdb_api_key)

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
    fave_fields = _detect_favourite_db_fields(notion, fave_db_id)
    existing_favourites = _load_existing_favourites(
        notion,
        fave_db_id,
        title_prop_name=fave_fields["title_prop"] or "Title",
    )

    async with httpx.AsyncClient(timeout=12) as client:
        for row in rows:
            props = row.get("properties", {})
            title = _extract_title(props)
            tmdb_prop = props.get("TMDB URL", {}).get("url")
            favourite = props.get("Favourite", {}).get("checkbox", False)

            update_props: dict = {}
            tmdb_url = tmdb_prop
            if not tmdb_url:
                tmdb_url = await _search_tmdb_url_with_client(
                    title,
                    tmdb_api_key,
                    client,
                    preferred_media_type=_preferred_media_type(props),
                )
                if tmdb_url:
                    update_props["TMDB URL"] = {"url": tmdb_url}
                    stats["tmdb_found"] += 1
                else:
                    stats["tmdb_missing"] += 1

            normalized_title = _normalize_title(title)
            if fave_db_id and favourite and normalized_title and normalized_title not in existing_favourites:
                favourite_props = {
                    fave_fields["title_prop"] or "Title": {
                        "title": [{"text": {"content": title}}],
                    }
                }
                row_year = _parse_row_year(props)
                if row_year and fave_fields["year_prop"]:
                    if fave_fields["year_type"] == "number":
                        favourite_props[fave_fields["year_prop"]] = {"number": row_year}
                    elif fave_fields["year_type"] == "select":
                        favourite_props[fave_fields["year_prop"]] = {"select": {"name": str(row_year)}}
                    elif fave_fields["year_type"] == "rich_text":
                        favourite_props[fave_fields["year_prop"]] = {
                            "rich_text": [{"text": {"content": str(row_year)}}]
                        }
                    elif fave_fields["year_type"] == "date":
                        favourite_props[fave_fields["year_prop"]] = {
                            "date": {"start": f"{row_year}-01-01"}
                        }

                if fave_fields["category_prop"]:
                    if fave_fields["category_type"] == "select":
                        favourite_props[fave_fields["category_prop"]] = {"select": {"name": "Film"}}
                    elif fave_fields["category_type"] == "multi_select":
                        favourite_props[fave_fields["category_prop"]] = {
                            "multi_select": [{"name": "Film"}]
                        }

                notion.pages.create(
                    parent={"database_id": fave_db_id},
                    properties=favourite_props,
                )
                existing_favourites.add(normalized_title)
                stats["added_to_fave"] += 1

            update_props["Last Synced"] = {"date": {"start": date.today().isoformat()}}
            notion.pages.update(page_id=row["id"], properties=update_props)

    return stats
