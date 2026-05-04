from __future__ import annotations

import logging
import re
from datetime import date

import httpx

from second_brain import keyboards as kb

log = logging.getLogger(__name__)
from second_brain.config import (
    NOTION_PHOTO_DB,
    NOTION_WANTSLIST_V2_DB,
    NOTION_WATCHLIST_DB,
    TMDB_API_KEY,
    TMDB_BASE,
)

pending_wantslist_map: dict[str, dict] = {}
pending_photo_map: dict[str, dict] = {}
pending_tmdb_map: dict[str, list[dict]] = {}
_v10_counter = 0
_tmdb_http_client: httpx.AsyncClient | None = None


async def tmdb_search(title: str, media_type: str = "multi") -> list[dict]:
    if not TMDB_API_KEY:
        return []
    try:
        client = _get_tmdb_http_client()
        resp = await client.get(
            f"{TMDB_BASE}/search/{media_type}",
            params={"api_key": TMDB_API_KEY, "query": title, "page": 1},
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])[:5]

        candidates: list[dict] = []
        for r in results:
            mtype = r.get("media_type") or media_type
            if mtype not in ("tv", "movie"):
                continue
            candidate = {
                "tmdb_id": str(r.get("id", "")),
                "title": r.get("name") or r.get("title") or title,
                "media_type": mtype,
                "year": (r.get("first_air_date") or r.get("release_date") or "")[:4],
                "seasons": None,
                "episodes": None,
                "runtime": None,
            }
            if mtype == "tv":
                try:
                    det = await client.get(
                        f"{TMDB_BASE}/tv/{r['id']}",
                        params={"api_key": TMDB_API_KEY},
                    )
                    det.raise_for_status()
                    d = det.json()
                    candidate["seasons"] = d.get("number_of_seasons")
                    candidate["episodes"] = d.get("number_of_episodes")
                    rt = d.get("episode_run_time") or []
                    candidate["runtime"] = rt[0] if rt else None
                except Exception as e:
                    log.warning("TMDB TV detail lookup failed for id=%s: %s", r.get("id"), e)
            candidates.append(candidate)
        return candidates
    except Exception as e:
        log.warning("TMDB search failed for '%s': %s", title, e)
        return []


def _notion_type_from_tmdb(media_type: str) -> str:
    return {"tv": "Series", "movie": "Film"}.get(media_type, "Series")


def _tmdb_media_slug(media_type: str) -> str:
    normalized = (media_type or "").strip().lower()
    if normalized in {"film", "movie"}:
        return "movie"
    if normalized in {"series", "tv", "tv series", "anime", "documentary"}:
        return "tv"
    return ""


def _get_tmdb_http_client() -> httpx.AsyncClient:
    global _tmdb_http_client
    if _tmdb_http_client is None:
        _tmdb_http_client = httpx.AsyncClient(timeout=8.0)
    return _tmdb_http_client


def create_watchlist_entry(
    notion,
    title: str,
    media_type: str = "Series",
    tmdb_id: str = "",
    seasons: int | None = None,
    episodes: int | None = None,
    runtime: int | None = None,
) -> str:
    tmdb_url = ""
    tmdb_id_str = str(tmdb_id).strip() if tmdb_id is not None else ""
    if tmdb_id_str:
        media_slug = _tmdb_media_slug(media_type)
        if media_slug:
            tmdb_url = f"https://www.themoviedb.org/{media_slug}/{tmdb_id_str}"
    props: dict = {
        "Title": {"title": [{"text": {"content": title}}]},
        "Type": {"select": {"name": media_type}},
        "Status": {"select": {"name": "Queued"}},
        "Source": {"select": {"name": "📱 Telegram"}},
        "Added": {"date": {"start": date.today().isoformat()}},
    }
    if tmdb_id_str:
        props["TMDB ID"] = {"rich_text": [{"text": {"content": tmdb_id_str}}]}
    if tmdb_url:
        props["TMDB URL"] = {"url": tmdb_url}
    if seasons is not None:
        props["Seasons"] = {"number": seasons}
    if episodes is not None:
        props["Episodes"] = {"number": episodes}
    if runtime is not None:
        props["Runtime (mins/ep)"] = {"number": runtime}

    page = notion.pages.create(parent={"database_id": NOTION_WATCHLIST_DB}, properties=props)
    return page["id"]


def watchlist_duplicate(notion, title: str) -> bool:
    results = notion.databases.query(
        database_id=NOTION_WATCHLIST_DB,
        filter={"property": "Title", "title": {"equals": title}},
    )
    return len(results.get("results", [])) > 0


def create_wantslist_entry(
    notion,
    item: str,
    category: str = "Other",
    priority: str = "Medium",
    est_cost: float | None = None,
    url: str | None = None,
    notes: str | None = None,
) -> str:
    props: dict = {
        "Item": {"title": [{"text": {"content": item}}]},
        "Category": {"select": {"name": category}},
        "Priority": {"select": {"name": priority}},
        "Status": {"select": {"name": "Wanted"}},
        "Source": {"select": {"name": "📱 Telegram"}},
    }
    if est_cost is not None:
        props["Est. Cost"] = {"number": est_cost}
    if url:
        props["userDefined:URL"] = {"url": url}
    if notes:
        props["Notes"] = {"rich_text": [{"text": {"content": notes}}]}

    page = notion.pages.create(parent={"database_id": NOTION_WANTSLIST_V2_DB}, properties=props)
    return page["id"]


def create_photo_entry(
    notion,
    subject: str,
    location: str | None = None,
    season: str | None = None,
    time_of_day: str | None = None,
    notes: str | None = None,
) -> str:
    props: dict = {
        "Subject": {"title": [{"text": {"content": subject}}]},
        "Status": {"select": {"name": "Wishlist"}},
        "Source": {"select": {"name": "📱 Telegram"}},
    }
    if location:
        props["Location"] = {"rich_text": [{"text": {"content": location}}]}
    if season:
        props["Season"] = {"select": {"name": season}}
    if time_of_day:
        props["Time of Day"] = {"select": {"name": time_of_day}}
    if notes:
        props["Notes"] = {"rich_text": [{"text": {"content": notes}}]}

    page = notion.pages.create(parent={"database_id": NOTION_PHOTO_DB}, properties=props)
    return page["id"]


def _save_watchlist_from_candidate(notion, c: dict, fallback_title: str) -> str:
    return create_watchlist_entry(
        notion,
        title=c.get("title") or fallback_title,
        media_type=_notion_type_from_tmdb(c.get("media_type", "tv")),
        tmdb_id=c.get("tmdb_id", ""),
        seasons=c.get("seasons"),
        episodes=c.get("episodes"),
        runtime=c.get("runtime"),
    )


async def handle_watchlist_intent(notion, message, title: str, media_type: str) -> None:
    global _v10_counter
    if not NOTION_WATCHLIST_DB:
        await message.reply_text("📺 Watchlist isn't configured yet — NOTION_WATCHLIST_DB missing.")
        return
    if watchlist_duplicate(notion, title):
        await message.reply_text(f"📺 *{title}* is already on your watchlist!", parse_mode="Markdown")
        return

    thinking = await message.reply_text("📺 Searching TMDB...")
    candidates = await tmdb_search(
        title,
        media_type="tv" if media_type == "Series" else "movie" if media_type == "Film" else "multi",
    )

    if not candidates:
        create_watchlist_entry(notion, title, media_type=media_type)
        await thinking.edit_text(
            f"📺 Added to watchlist!\n\n*{title}* · {media_type}\n_No TMDB metadata found — saved title only_",
            parse_mode="Markdown",
        )
        return

    if len(candidates) == 1:
        c = candidates[0]
        _save_watchlist_from_candidate(notion, c, title)
        seasons_str = f" · {c['seasons']} seasons" if c.get("seasons") else ""
        episodes_str = f" · {c['episodes']} eps" if c.get("episodes") else ""
        runtime_str = f" · {c['runtime']} min/ep" if c.get("runtime") else ""
        await thinking.edit_text(
            f"📺 Added to watchlist!\n\n*{c['title']}* ({c['year']}) · {_notion_type_from_tmdb(c['media_type'])}"
            f"{seasons_str}{episodes_str}{runtime_str}\n_Saved to Notion_",
            parse_mode="Markdown",
        )
        return

    key = str(_v10_counter)
    _v10_counter += 1
    pending_tmdb_map[key] = candidates
    await thinking.edit_text(
        f"📺 Found a few matches for *{title}* — which one?",
        parse_mode="Markdown",
        reply_markup=kb.tmdb_candidates_keyboard(key, candidates, _notion_type_from_tmdb),
    )


async def handle_wantslist_intent(message, item: str, category: str) -> None:
    global _v10_counter
    if not NOTION_WANTSLIST_V2_DB:
        await message.reply_text("🎁 Wantslist isn't configured yet — NOTION_WANTSLIST_V2_DB missing.")
        return
    key = str(_v10_counter)
    _v10_counter += 1
    pending_wantslist_map[key] = {"item": item, "category": category}
    await message.reply_text(
        f"🎁 Save *{item}* to your Wantslist?\n_Category: {category}_",
        parse_mode="Markdown",
        reply_markup=kb.wantslist_confirm_keyboard(key),
    )


async def handle_photo_intent(notion, message, subject: str) -> None:
    global _v10_counter
    if not NOTION_PHOTO_DB:
        await message.reply_text("📷 Photo Bucketlist isn't configured yet — NOTION_PHOTO_DB missing.")
        return

    key = str(_v10_counter)
    _v10_counter += 1
    pending_photo_map[key] = {"subject": subject}
    await message.reply_text(
        f"📷 *{subject}* added to your photo bucketlist!\n\n"
        "_Optionally reply with location and/or best season — e.g. `Kyoto, Autumn` — "
        "or just ignore this and fill it in Notion later._\n\n"
        f"_Reference: `photo_key:{key}`_",
        parse_mode="Markdown",
    )
    page_id = create_photo_entry(notion, subject)
    pending_photo_map[key]["page_id"] = page_id


def _parse_photo_followup(text: str) -> tuple[str | None, str | None, str | None]:
    seasons = {"spring", "summer", "autumn", "fall", "winter", "any"}
    season_map = {"fall": "Autumn"}
    times = {"golden hour", "blue hour", "midday", "night", "any"}
    time_labels = {
        "golden hour": "Golden Hour",
        "blue hour": "Blue Hour",
        "midday": "Midday",
        "night": "Night",
        "any": "Any",
    }
    parts = [p.strip() for p in re.split(r"[,/|·]+", text) if p.strip()]
    location, season, time_of_day = None, None, None
    for part in parts:
        lower = part.lower()
        if lower in seasons:
            season = season_map.get(lower, lower.capitalize())
        elif lower in times:
            time_of_day = time_labels[lower]
        elif not location:
            location = part
    return location, season, time_of_day


async def handle_photo_followup(notion, message, text: str) -> bool:
    key = None
    if message.reply_to_message:
        replied = message.reply_to_message.text or ""
        m = re.search(r"photo_key:(\w+)", replied)
        if m:
            key = m.group(1)

    if key and key in pending_photo_map:
        entry = pending_photo_map[key]
        page_id = entry.get("page_id")
        if not page_id:
            return False
        location, season, time_of_day = _parse_photo_followup(text)
        props: dict = {}
        if location:
            props["Location"] = {"rich_text": [{"text": {"content": location}}]}
        if season:
            props["Season"] = {"select": {"name": season}}
        if time_of_day:
            props["Time of Day"] = {"select": {"name": time_of_day}}
        if props:
            notion.pages.update(page_id=page_id, properties=props)
            parts = []
            if location:
                parts.append(f"📍 {location}")
            if season:
                parts.append(f"🗓️ {season}")
            if time_of_day:
                parts.append(f"🕐 {time_of_day}")
            await message.reply_text(f"📷 Updated: {' · '.join(parts)}\n_Saved to Notion_", parse_mode="Markdown")
            del pending_photo_map[key]
            return True
    return False
