"""Cinema Log TMDB sync helpers."""

from __future__ import annotations

from datetime import date
import re
from difflib import SequenceMatcher

import httpx
import logging

from second_brain.cinema.config import FAVE_DB_ID, TMDB_API_KEY
log = logging.getLogger(__name__)

from second_brain.notion.properties import (
    query_all,
    rich_text_prop,
    title_prop,
)

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_MOVIE_URL_BASE = "https://www.themoviedb.org/movie"

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


def _resolve_cinema_title_property(notion, cinema_db_id: str) -> str:
    schema = notion.databases.retrieve(database_id=cinema_db_id)
    properties = schema.get("properties", {})
    if "Film" in properties and (properties.get("Film") or {}).get("type") == "title":
        return "Film"

    for key in ("Title", "Name"):
        if key in properties and (properties.get(key) or {}).get("type") == "title":
            return key

    for name, prop in properties.items():
        if (prop or {}).get("type") == "title":
            return name
    return "Film"


def _build_cinema_query_filter(title_property: str) -> dict:
    """Query entries that need TMDB URL backfill."""
    return {
        "and": [
            {"property": "TMDB URL", "url": {"is_empty": True}},
            {"property": title_property, "title": {"is_not_empty": True}},
        ]
    }


def _build_tmdb_movie_url(tmdb_movie_id: int | str) -> str:
    return f"{TMDB_MOVIE_URL_BASE}/{tmdb_movie_id}"


def _release_year(result: dict) -> int | None:
    release_date = (result or {}).get("release_date")
    if isinstance(release_date, str) and len(release_date) >= 4 and release_date[:4].isdigit():
        return int(release_date[:4])
    return None


def _result_titles(result: dict) -> list[str]:
    """Return all title variants for a TMDB result, including original_title."""
    titles: list[str] = []
    for key in ("title", "original_title"):
        value = str((result or {}).get(key) or "").strip()
        if value and value not in titles:
            titles.append(value)
    return titles


def _is_cjk_heavy(text: str) -> bool:
    """Return True if the title is predominantly CJK characters."""
    if not text:
        return False
    cjk_count = sum(
        1 for ch in text
        if (
            "\u4e00" <= ch <= "\u9fff"    # CJK Unified Ideographs
            or "\u3400" <= ch <= "\u4dbf"  # CJK Extension A
            or "\u3040" <= ch <= "\u30ff"  # Hiragana + Katakana
            or "\uac00" <= ch <= "\ud7af"  # Korean Hangul
        )
    )
    return cjk_count / max(len(text.replace(" ", "")), 1) > 0.3


def _movie_match_score(result: dict, wanted_title: str, wanted_year: int | None) -> float:
    titles = _result_titles(result)
    if not titles:
        return -1.0

    wanted = _normalize_title(wanted_title)
    best_title_score = -1.0

    for title in titles:
        candidate = _normalize_title(title)
        exact = 1.0 if candidate == wanted else 0.0
        contains = 1.0 if (wanted and (wanted in candidate or candidate in wanted)) else 0.0
        near = SequenceMatcher(None, candidate, wanted).ratio()
        best_title_score = max(best_title_score, (exact * 1000.0) + (contains * 200.0) + (near * 100.0))

    title_score = best_title_score

    year_score = 0.0
    candidate_year = _release_year(result)
    if wanted_year and candidate_year:
        year_gap = abs(candidate_year - wanted_year)
        if year_gap == 0:
            year_score = 40.0
        elif year_gap == 1:
            year_score = 24.0
        elif year_gap == 2:
            year_score = 12.0
        elif year_gap <= 5:
            year_score = 5.0

    popularity = float((result or {}).get("popularity") or 0.0)
    vote_count = float((result or {}).get("vote_count") or 0.0)
    return title_score + year_score + min(popularity, 50.0) + min(vote_count / 100.0, 25.0)


def _select_best_tmdb_movie_match(results: list[dict], title: str, row_year: int | None) -> dict | None:
    if not results:
        return None

    ranked = sorted(
        results,
        key=lambda r: _movie_match_score(r, title, row_year),
        reverse=True,
    )
    best = ranked[0]
    confidence = _movie_match_score(best, title, row_year)

    # CJK/non-Latin titles score lower because SequenceMatcher on unicode
    # characters against a translated English title gives poor similarity.
    # Use a lower threshold so exact original_title matches aren't discarded.
    threshold = 30.0 if _is_cjk_heavy(title) else 60.0

    if confidence < threshold:
        return None
    return best


async def _search_tmdb_url(title: str, tmdb_api_key: str | None) -> str | None:
    return await _search_tmdb_url_with_client(title, tmdb_api_key, row_year=None, client=None)


async def _search_tmdb_url_with_client(
    title: str,
    tmdb_api_key: str | None,
    row_year: int | None,
    client: httpx.AsyncClient | None,
) -> str | None:
    if not tmdb_api_key or not title:
        return None

    search_titles = _title_search_candidates(title)

    if client is not None:
        for query_title in search_titles:
            resp = await client.get(
                f"{TMDB_BASE}/search/movie",
                params={"api_key": tmdb_api_key, "query": query_title, "page": 1},
            )
            resp.raise_for_status()
            results = resp.json().get("results", [])
            best = _select_best_tmdb_movie_match(results, title=title, row_year=row_year)
            if best and best.get("id"):
                return _build_tmdb_movie_url(best["id"])
        return None

    async with httpx.AsyncClient(timeout=12) as owned_client:
        return await _search_tmdb_url_with_client(
            title=title,
            tmdb_api_key=tmdb_api_key,
            row_year=row_year,
            client=owned_client,
        )


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
    """
    favourites: set[str] = set()
    if not fave_db_id:
        return favourites

    for row in query_all(notion, fave_db_id, page_size=100):
        title = _plain_text(row.get("properties", {}).get(title_prop_name, {}))
        if title:
            favourites.add(_normalize_title(title))
    return favourites


def _preferred_media_type(props: dict) -> str | None:
    value = (props.get("Type", {}).get("select", {}) or {}).get("name", "").strip().lower()
    if value == "film":
        return "movie"
    if value == "series":
        return "tv"
    return None


def _tmdb_id_from_props(props: dict) -> str:
    tmdb_prop = props.get("TMDB ID", {})
    if not isinstance(tmdb_prop, dict):
        return ""

    rich_text = tmdb_prop.get("rich_text", [])
    if rich_text:
        tmdb_id = "".join(chunk.get("plain_text", "") for chunk in rich_text).strip()
        if tmdb_id:
            return tmdb_id

    number_value = tmdb_prop.get("number")
    if number_value is not None:
        tmdb_id = str(int(number_value)) if isinstance(number_value, float) else str(number_value)
        return tmdb_id.strip()

    title_value = tmdb_prop.get("title", [])
    if title_value:
        tmdb_id = "".join(chunk.get("plain_text", "") for chunk in title_value).strip()
        if tmdb_id:
            return tmdb_id

    return ""


def _tmdb_media_slug_from_props(props: dict) -> str | None:
    preferred = _preferred_media_type(props)
    if preferred in {"movie", "tv"}:
        return preferred

    category = (props.get("Category", {}).get("select", {}) or {}).get("name", "").strip().lower()
    if category == "film":
        return "movie"
    if category in {"series", "tv", "tv series"}:
        return "tv"

    categories = props.get("Category", {}).get("multi_select", []) or []
    normalized = {
        (item or {}).get("name", "").strip().lower()
        for item in categories
        if isinstance(item, dict)
    }
    if "film" in normalized:
        return "movie"
    if {"series", "tv", "tv series"} & normalized:
        return "tv"

    return None


async def sync_cinema_log_to_notion(
    *,
    notion,
    cinema_db_id: str,
    fave_db_id: str | None = None,
    tmdb_api_key: str | None = None,
    force: bool = False,
) -> dict[str, int]:
    """Sync cinema entries missing TMDB URL and optionally promote favourites."""
    stats = {
        "scanned": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
        "tmdb_found": 0,
        "tmdb_missing": 0,
        "added_to_fave": 0,
    }

    title_property = _resolve_cinema_title_property(notion, cinema_db_id)
    query_filter = _build_cinema_query_filter(title_property)

    rows = query_all(notion, cinema_db_id, filter=query_filter, page_size=100)

    stats["scanned"] = len(rows)
    fave_fields = _detect_favourite_db_fields(notion, fave_db_id)
    existing_favourites = _load_existing_favourites(
        notion,
        fave_db_id,
        title_prop_name=fave_fields["title_prop"] or "Title",
    )

    async with httpx.AsyncClient(timeout=12) as client:
        await _sync_rows(
            notion=notion,
            rows=rows,
            fave_db_id=fave_db_id,
            tmdb_api_key=tmdb_api_key,
            force=force,
            stats=stats,
            fave_fields=fave_fields,
            existing_favourites=existing_favourites,
            client=client,
        )

    return stats


async def sync_single_cinema_entry(
    *,
    notion,
    page_id: str,
    fave_db_id: str | None = None,
    tmdb_api_key: str | None = None,
    force: bool = False,
) -> dict[str, int]:
    row = notion.pages.retrieve(page_id=page_id)
    stats = {"scanned": 1, "updated": 0, "skipped": 0, "failed": 0, "tmdb_found": 0, "tmdb_missing": 0, "added_to_fave": 0}
    fave_fields = _detect_favourite_db_fields(notion, fave_db_id)
    existing_favourites = _load_existing_favourites(
        notion,
        fave_db_id,
        title_prop_name=fave_fields["title_prop"] or "Title",
    )
    async with httpx.AsyncClient(timeout=12) as client:
        await _sync_rows(
            notion=notion,
            rows=[row],
            fave_db_id=fave_db_id,
            tmdb_api_key=tmdb_api_key,
            force=force,
            stats=stats,
            fave_fields=fave_fields,
            existing_favourites=existing_favourites,
            client=client,
        )
    return stats


async def _sync_rows(
    *,
    notion,
    rows: list[dict],
    fave_db_id: str | None,
    tmdb_api_key: str | None,
    force: bool,
    stats: dict[str, int],
    fave_fields: dict[str, str | None],
    existing_favourites: set[str],
    client: httpx.AsyncClient,
) -> None:
    for row in rows:
        props = row.get("properties", {})
        title = _extract_title(props)
        tmdb_prop = props.get("TMDB URL", {}).get("url")
        favourite = props.get("Favourite", {}).get("checkbox", False)
        manual_source = ((props.get("Source", {}).get("select") or {}).get("name") or "").strip()

        update_props: dict = {}
        tmdb_url = tmdb_prop
        if tmdb_url and not force:
            stats["skipped"] += 1
        elif manual_source == "✏️ Manual" and tmdb_url and not force:
            stats["skipped"] += 1
        else:
            try:
                tmdb_id = _tmdb_id_from_props(props)
                tmdb_media_slug = _tmdb_media_slug_from_props(props)
                if tmdb_id and tmdb_media_slug == "movie":
                    tmdb_url = _build_tmdb_movie_url(tmdb_id)
                else:
                    tmdb_url = await _search_tmdb_url_with_client(
                        title=title,
                        tmdb_api_key=tmdb_api_key,
                        row_year=_parse_row_year(props),
                        client=client,
                    )
                if tmdb_url:
                    if tmdb_prop != tmdb_url:
                        update_props["TMDB URL"] = {"url": tmdb_url}
                    stats["tmdb_found"] += 1
                    stats["updated"] += 1 if "TMDB URL" in update_props else 0
                else:
                    stats["tmdb_missing"] += 1
            except Exception:
                stats["failed"] += 1

        normalized_title = _normalize_title(title)
        if fave_db_id and favourite and normalized_title and normalized_title not in existing_favourites:
            favourite_props = {
                fave_fields["title_prop"] or "Title": title_prop(title)
            }
            row_year = _parse_row_year(props)
            if row_year and fave_fields["year_prop"]:
                if fave_fields["year_type"] == "number":
                    favourite_props[fave_fields["year_prop"]] = {"number": row_year}
                elif fave_fields["year_type"] == "select":
                    favourite_props[fave_fields["year_prop"]] = {"select": {"name": str(row_year)}}
                elif fave_fields["year_type"] == "rich_text":
                    favourite_props[fave_fields["year_prop"]] = rich_text_prop(str(row_year))
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

        if update_props:
            notion.pages.update(page_id=row["id"], properties=update_props)


async def run_cinema_sync(notion, bot, *, cinema_log_db: str, chat_id: int, force: bool = False) -> dict[str, int | str]:
    """Background sync for Cinema Log → Favourite Shows."""
    if not cinema_log_db:
        return {
            "scanned": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 0,
            "tmdb_found": 0,
            "tmdb_missing": 0,
            "added_to_fave": 0,
            "action": "disabled",
        }

    try:
        stats = await sync_cinema_log_to_notion(
            notion=notion,
            cinema_db_id=cinema_log_db,
            fave_db_id=FAVE_DB_ID,
            tmdb_api_key=TMDB_API_KEY,
            force=force,
        )
        log.info(
            "Cinema sync: scanned=%s, updated=%s, skipped=%s, failed=%s, tmdb_found=%s, tmdb_missing=%s, added_to_fave=%s",
            stats["scanned"],
            stats["updated"],
            stats["skipped"],
            stats["failed"],
            stats["tmdb_found"],
            stats["tmdb_missing"],
            stats["added_to_fave"],
        )
        return {**stats, "action": "synced"}
    except Exception as e:
        log.exception("Cinema sync failed: %s", e)
        try:
            await bot.send_message(chat_id=chat_id, text="🚨 Cinema sync crashed.\n" f"Error: {e}")
        except Exception:
            pass
        return {
            "scanned": 0,
            "updated": 0,
            "skipped": 0,
            "failed": 1,
            "tmdb_found": 0,
            "tmdb_missing": 0,
            "added_to_fave": 0,
            "action": "error",
            "reason": str(e),
        }
