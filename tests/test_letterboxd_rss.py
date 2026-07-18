"""Tests for the Letterboxd RSS diary parser and rating conversion."""

import asyncio

from second_brain.cinema import letterboxd as lb
from second_brain.cinema.letterboxd import (
    DiaryEntry,
    lb_rating_to_notion,
    parse_diary_feed,
    poll_letterboxd,
)

SAMPLE_FEED = """<?xml version='1.0' encoding='utf-8'?>
<rss version="2.0" xmlns:letterboxd="https://letterboxd.com" xmlns:tmdb="https://themoviedb.org">
  <channel>
    <title>Letterboxd - atmh</title>
    <item>
      <title>Mercy, 2026 - ★★★</title>
      <guid isPermaLink="false">letterboxd-watch-1285580267</guid>
      <letterboxd:watchedDate>2026-04-18</letterboxd:watchedDate>
      <letterboxd:rewatch>No</letterboxd:rewatch>
      <letterboxd:filmTitle>Mercy</letterboxd:filmTitle>
      <letterboxd:filmYear>2026</letterboxd:filmYear>
      <letterboxd:memberRating>3.0</letterboxd:memberRating>
      <tmdb:movieId>1236153</tmdb:movieId>
    </item>
    <item>
      <title>Wicked: For Good, 2025</title>
      <guid isPermaLink="false">letterboxd-watch-1083677283</guid>
      <letterboxd:watchedDate>2025-11-23</letterboxd:watchedDate>
      <letterboxd:rewatch>Yes</letterboxd:rewatch>
      <letterboxd:filmTitle>Wicked: For Good</letterboxd:filmTitle>
      <letterboxd:filmYear>2025</letterboxd:filmYear>
      <tmdb:movieId>967941</tmdb:movieId>
    </item>
  </channel>
</rss>"""


def test_parse_extracts_diary_fields():
    entries = parse_diary_feed(SAMPLE_FEED)
    assert len(entries) == 2

    mercy = entries[0]
    assert mercy.guid == "letterboxd-watch-1285580267"
    assert mercy.tmdb_id == "1236153"
    assert mercy.title == "Mercy"
    assert mercy.year == "2026"
    assert mercy.watched_date == "2026-04-18"
    assert mercy.member_rating == "3.0"
    assert mercy.rewatch is False
    assert mercy.tmdb_url == "https://www.themoviedb.org/movie/1236153"


def test_parse_handles_unrated_and_rewatch():
    wicked = parse_diary_feed(SAMPLE_FEED)[1]
    assert wicked.member_rating is None
    assert wicked.rewatch is True


def test_rating_conversion_chart():
    assert lb_rating_to_notion("0.5") == "-3"
    assert lb_rating_to_notion("2.0") == "-1"
    assert lb_rating_to_notion("2.5") == "0"
    assert lb_rating_to_notion("3.0") == "0"
    assert lb_rating_to_notion("5.0") == "3"
    # unrated -> empty (not 0)
    assert lb_rating_to_notion(None) is None
    # integer-style input still maps
    assert lb_rating_to_notion("4") == "2"


def test_reviewed_watch_is_included():
    # Adding review text flips the guid from letterboxd-watch-* to
    # letterboxd-review-*; it's still a diary watch and must be ingested.
    feed = SAMPLE_FEED.replace("letterboxd-watch-1285580267", "letterboxd-review-999")
    entries = parse_diary_feed(feed)
    assert [e.title for e in entries] == ["Mercy", "Wicked: For Good"]
    assert entries[0].guid == "letterboxd-review-999"


def test_reviewed_watch_of_synced_row_is_not_duplicated(monkeypatch):
    # A watch synced as letterboxd-watch-* later gains a review (new guid);
    # the (TMDB URL, Date) dedup must silently skip it.
    feed = SAMPLE_FEED.replace("letterboxd-watch-1285580267", "letterboxd-review-999")
    entries = parse_diary_feed(feed)
    monkeypatch.setattr(lb, "fetch_diary_feed", lambda *a, **k: _async(entries))
    notion = FakeNotion(
        seen_value="letterboxd-watch-1285580267,letterboxd-watch-1083677283",
        existing={("https://www.themoviedb.org/movie/1236153", "2026-04-18")},
    )
    result = _poll(notion)
    assert notion.created == []
    assert result["new_items"] == []


def test_non_diary_items_are_skipped():
    feed = SAMPLE_FEED.replace("letterboxd-watch-1285580267", "letterboxd-review-999")
    feed = feed.replace("<letterboxd:watchedDate>2026-04-18</letterboxd:watchedDate>", "")
    # First item now lacks watchedDate + watch guid -> skipped; only Wicked remains.
    entries = parse_diary_feed(feed)
    assert [e.title for e in entries] == ["Wicked: For Good"]


class FakeNotion:
    """Minimal Notion stand-in: ENV KV row + a set of existing (tmdb_url, date)."""

    def __init__(self, seen_value="", existing=()):
        self._env_value = seen_value
        self._existing = set(existing)
        self.created = []
        self.databases = self._DB(self)
        self.pages = self._Pages(self)

    class _DB:
        def __init__(self, outer):
            self.o = outer

        def query(self, database_id=None, filter=None, page_size=None):
            prop = (filter or {}).get("property")
            if prop == "Name":  # ENV watermark lookup
                if self.o._env_value:
                    return {"results": [{"id": "env-row", "properties": {
                        "Value": {"rich_text": [{"plain_text": self.o._env_value}]}}}]}
                return {"results": []}
            # cinema queries: filter is {"and": [TMDB URL, Date]}; the Date
            # clause is "equals" for dedup, "before" for the rewatch check.
            clauses = (filter or {}).get("and", [])
            url = next((c["url"]["equals"] for c in clauses if c["property"] == "TMDB URL"), None)
            date = next((c["date"] for c in clauses if c["property"] == "Date"), {})
            if "before" in date:
                hit = any(u == url and d < date["before"] for u, d in self.o._existing)
            else:
                hit = (url, date.get("equals")) in self.o._existing
            return {"results": [{"id": "x"}] if hit else []}

    class _Pages:
        def __init__(self, outer):
            self.o = outer

        def create(self, parent=None, properties=None):
            if "Value" in (properties or {}):  # ENV watermark row
                self.o._env_value = properties["Value"]["rich_text"][0]["text"]["content"]
                return {"id": "env-row"}
            title = properties["Film"]["title"][0]["text"]["content"]
            self.o.created.append(title)
            return {"id": f"page-{title}"}

        def update(self, page_id=None, properties=None):
            if "Value" in (properties or {}):
                self.o._env_value = properties["Value"]["rich_text"][0]["text"]["content"]


def _poll(notion):
    return asyncio.run(poll_letterboxd(
        notion=notion, cinema_db_id="cine", env_db_id="env", rss_url="x",
        client=None,
    ))


def test_first_run_baselines_without_creating(monkeypatch):
    entries = parse_diary_feed(SAMPLE_FEED)
    monkeypatch.setattr(lb, "fetch_diary_feed", lambda *a, **k: _async(entries))
    notion = FakeNotion(seen_value="")  # no watermark yet
    result = _poll(notion)
    assert result["action"] == "baselined"
    assert notion.created == []
    # watermark now holds both guids
    assert "letterboxd-watch-1285580267" in notion._env_value


def test_new_watch_creates_row_and_returns_item(monkeypatch):
    entries = parse_diary_feed(SAMPLE_FEED)
    monkeypatch.setattr(lb, "fetch_diary_feed", lambda *a, **k: _async(entries))
    # Watermark already has Wicked; Mercy is new.
    notion = FakeNotion(seen_value="letterboxd-watch-1083677283")
    result = _poll(notion)
    assert result["action"] == "polled"
    assert notion.created == ["Mercy"]
    assert [i["title"] for i in result["new_items"]] == ["Mercy"]


def test_rewatch_derived_from_cinema_log_not_feed_flag(monkeypatch):
    # Feed claims Mercy is a rewatch (import-polluted flag), but the Cinema
    # Log has no earlier watch -> not a rewatch.
    feed = SAMPLE_FEED.replace(
        "<letterboxd:rewatch>No</letterboxd:rewatch>",
        "<letterboxd:rewatch>Yes</letterboxd:rewatch>",
    )
    entries = parse_diary_feed(feed)
    assert entries[0].rewatch is True  # raw feed flag
    monkeypatch.setattr(lb, "fetch_diary_feed", lambda *a, **k: _async(entries))
    notion = FakeNotion(seen_value="letterboxd-watch-1083677283")
    result = _poll(notion)
    assert [i["rewatch"] for i in result["new_items"]] == [False]


def test_rewatch_true_when_earlier_watch_in_cinema_log(monkeypatch):
    # Feed says No, but an earlier watch of the same film exists -> rewatch.
    entries = parse_diary_feed(SAMPLE_FEED)
    assert entries[0].rewatch is False
    monkeypatch.setattr(lb, "fetch_diary_feed", lambda *a, **k: _async(entries))
    notion = FakeNotion(
        seen_value="letterboxd-watch-1083677283",
        existing={("https://www.themoviedb.org/movie/1236153", "2025-01-01")},
    )
    result = _poll(notion)
    assert notion.created == ["Mercy"]
    assert [i["rewatch"] for i in result["new_items"]] == [True]


def test_existing_notion_row_is_not_duplicated(monkeypatch):
    entries = parse_diary_feed(SAMPLE_FEED)
    monkeypatch.setattr(lb, "fetch_diary_feed", lambda *a, **k: _async(entries))
    # Mercy new to watermark, but already in Notion for that date -> skip.
    notion = FakeNotion(
        seen_value="letterboxd-watch-1083677283",
        existing={("https://www.themoviedb.org/movie/1236153", "2026-04-18")},
    )
    result = _poll(notion)
    assert notion.created == []
    assert result["new_items"] == []


async def _async(value):
    return value
