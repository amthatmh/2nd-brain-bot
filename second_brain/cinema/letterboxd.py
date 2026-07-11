"""Letterboxd RSS → Cinema Log poller.

Pulls the member's public diary RSS feed and creates Cinema/Movie Log rows for
new watches (Source = 🎬 Letterboxd). Location detail (Venue/Seat/Auditorium)
never comes from Letterboxd — it stays private in Notion and is filled in via the
Telegram "🎬 Cinema / 🏠 Home" follow-up prompt.

Design notes:
- Dedup is two-layered: a guid watermark stored in the ENV DB (fast, skips items
  already processed) plus a check against existing Notion rows by (tmdbID,
  watchedDate) so the one-time backfill can't be re-created or re-prompted.
- memberRating is absent when a watch is unrated -> leave Notion Rating empty.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)

TMDB_MOVIE_URL_BASE = "https://www.themoviedb.org/movie"
_LB_NS = {"letterboxd": "https://letterboxd.com", "tmdb": "https://themoviedb.org"}

# Letterboxd half-star rating (0.5–5.0) -> Notion −3..3 select value (string).
# Mirrors the agreed conversion chart; unrated -> None (leave empty, ≠ 0).
_LB_STAR_TO_NOTION = {
    "0.5": "-3", "1.0": "-2", "1.5": "-2", "2.0": "-1", "2.5": "0",
    "3.0": "0", "3.5": "1", "4.0": "2", "4.5": "2", "5.0": "3",
}


def lb_rating_to_notion(member_rating: str | None) -> str | None:
    """Map a Letterboxd memberRating (e.g. "3.0") to a Notion Rating option."""
    if not member_rating:
        return None
    key = member_rating.strip()
    if key not in _LB_STAR_TO_NOTION and key.replace(".0", "").isdigit():
        key = f"{float(key):.1f}"
    return _LB_STAR_TO_NOTION.get(key)


@dataclass(frozen=True)
class DiaryEntry:
    guid: str
    tmdb_id: str
    title: str
    year: str | None
    watched_date: str  # YYYY-MM-DD
    member_rating: str | None
    rewatch: bool

    @property
    def tmdb_url(self) -> str | None:
        return f"{TMDB_MOVIE_URL_BASE}/{self.tmdb_id}" if self.tmdb_id else None


def _text(item: ET.Element, tag: str) -> str | None:
    el = item.find(tag, _LB_NS)
    return el.text.strip() if el is not None and el.text else None


def parse_diary_feed(xml_bytes: bytes | str) -> list[DiaryEntry]:
    """Parse a Letterboxd RSS feed into diary entries (skips non-diary items)."""
    root = ET.fromstring(xml_bytes)
    entries: list[DiaryEntry] = []
    for item in root.iter("item"):
        guid = _text(item, "guid")
        watched = _text(item, "letterboxd:watchedDate")
        # Only diary watches have watchedDate + a guid like "letterboxd-watch-...".
        if not guid or not watched or "letterboxd-watch-" not in guid:
            continue
        entries.append(
            DiaryEntry(
                guid=guid,
                tmdb_id=_text(item, "tmdb:movieId") or "",
                title=_text(item, "letterboxd:filmTitle") or "",
                year=_text(item, "letterboxd:filmYear"),
                watched_date=watched,
                member_rating=_text(item, "letterboxd:memberRating"),
                rewatch=(_text(item, "letterboxd:rewatch") or "").lower() == "yes",
            )
        )
    return entries


async def fetch_diary_feed(rss_url: str, client: httpx.AsyncClient | None = None) -> list[DiaryEntry]:
    if client is not None:
        resp = await client.get(rss_url)
        resp.raise_for_status()
        return parse_diary_feed(resp.content)
    async with httpx.AsyncClient(timeout=12, follow_redirects=True) as owned:
        return await fetch_diary_feed(rss_url, client=owned)


# ── Watermark (processed guids) stored in the Notion ENV DB ───────────────────

SEEN_GUIDS_ENV_NAME = "LETTERBOXD_SEEN_GUIDS"
_MAX_SEEN_GUIDS = 300
SOURCE_NAME = "🎬 Letterboxd"


def _env_row(notion, env_db_id: str, name: str) -> tuple[str | None, str]:
    """Return (page_id, value) for an ENV DB row by Name; ("", "") if absent."""
    res = notion.databases.query(
        database_id=env_db_id, filter={"property": "Name", "title": {"equals": name}}
    )
    rows = res.get("results", [])
    if not rows:
        return None, ""
    props = rows[0].get("properties", {})
    value = "".join(p.get("plain_text", "") for p in props.get("Value", {}).get("rich_text", []))
    return rows[0]["id"], value


def _load_seen_guids(notion, env_db_id: str) -> set[str]:
    _, value = _env_row(notion, env_db_id, SEEN_GUIDS_ENV_NAME)
    return {g for g in (value or "").split(",") if g}


def _save_seen_guids(notion, env_db_id: str, guids: list[str]) -> None:
    value = ",".join(guids[-_MAX_SEEN_GUIDS:])
    page_id, _ = _env_row(notion, env_db_id, SEEN_GUIDS_ENV_NAME)
    props = {"Value": {"rich_text": [{"text": {"content": value}}]}}
    if page_id:
        notion.pages.update(page_id=page_id, properties=props)
    else:
        notion.pages.create(
            parent={"database_id": env_db_id},
            properties={"Name": {"title": [{"text": {"content": SEEN_GUIDS_ENV_NAME}}]}, **props},
        )


def _notion_has_watch(notion, cinema_db_id: str, entry: DiaryEntry) -> bool:
    """Safety dedup: does a row already exist for this film + watched date?"""
    if not entry.tmdb_url:
        return False
    res = notion.databases.query(
        database_id=cinema_db_id,
        filter={
            "and": [
                {"property": "TMDB URL", "url": {"equals": entry.tmdb_url}},
                {"property": "Date", "date": {"equals": entry.watched_date}},
            ]
        },
        page_size=1,
    )
    return bool(res.get("results"))


def _create_cinema_row(notion, cinema_db_id: str, entry: DiaryEntry) -> str:
    props: dict = {
        "Film": {"title": [{"text": {"content": entry.title}}]},
        "Date": {"date": {"start": entry.watched_date}},
        "Source": {"select": {"name": SOURCE_NAME}},
    }
    if entry.tmdb_url:
        props["TMDB URL"] = {"url": entry.tmdb_url}
    rating = lb_rating_to_notion(entry.member_rating)
    if rating is not None:
        props["Rating"] = {"select": {"name": rating}}
    page = notion.pages.create(parent={"database_id": cinema_db_id}, properties=props)
    return page["id"]


async def poll_letterboxd(
    *,
    notion,
    cinema_db_id: str,
    env_db_id: str,
    rss_url: str,
    client: httpx.AsyncClient | None = None,
) -> dict:
    """Pull new Letterboxd diary watches into the Cinema Log.

    First run baselines the current feed (no rows created, no prompts) so the
    one-time backfill isn't re-prompted. Returns stats plus `new_items` — the
    rows created this run, for the Telegram Cinema/Home follow-up.
    """
    if not (rss_url and cinema_db_id and env_db_id):
        return {"action": "disabled", "created": 0, "new_items": []}

    entries = await fetch_diary_feed(rss_url, client)
    all_guids = [e.guid for e in entries]
    seen = _load_seen_guids(notion, env_db_id)

    if not seen:
        _save_seen_guids(notion, env_db_id, all_guids)
        return {"action": "baselined", "feed": len(entries), "created": 0, "new_items": []}

    new_items: list[dict] = []
    for entry in entries:
        if entry.guid in seen:
            continue
        try:
            if _notion_has_watch(notion, cinema_db_id, entry):
                continue
            page_id = _create_cinema_row(notion, cinema_db_id, entry)
            new_items.append(
                {
                    "page_id": page_id,
                    "title": entry.title,
                    "year": entry.year,
                    "watched_date": entry.watched_date,
                    "rewatch": entry.rewatch,
                }
            )
        except Exception:
            log.exception("letterboxd: failed to ingest guid=%s title=%s", entry.guid, entry.title)

    _save_seen_guids(notion, env_db_id, sorted(seen | set(all_guids)))
    return {
        "action": "polled",
        "feed": len(entries),
        "created": len(new_items),
        "new_items": new_items,
    }
