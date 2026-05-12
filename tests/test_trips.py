import os

import asyncio
from datetime import date, timedelta

os.environ.setdefault("TELEGRAM_TOKEN", "test-token")
os.environ.setdefault("NOTION_TOKEN", "test-token")
os.environ.setdefault("NOTION_DB_ID", "test-db")
os.environ.setdefault("MY_CHAT_ID", "1")
os.environ.setdefault("TELEGRAM_CHAT_ID", "1")
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
os.environ.setdefault("NOTION_HABIT_DB", "test-db")
os.environ.setdefault("NOTION_LOG_DB", "test-db")
os.environ.setdefault("NOTION_NOTES_DB", "test-db")
os.environ.setdefault("NOTION_DIGEST_SELECTOR_DB", "test-db")

from second_brain import trips


class _Message:
    def __init__(self):
        self.sent = []

    async def reply_text(self, text):
        self.sent.append(text)


class _Query:
    def __init__(self):
        self.message = _Message()


def _trip_map(departure_date="2026-05-14", return_date="2026-05-17"):
    return {
        "0": {
            "destinations": ["Nashville TN"],
            "departure_date": departure_date,
            "return_date": return_date,
            "duration_label": "2-3 Days",
            "purpose_list": ["Work"],
            "field_work_types": ["Site Survey"],
            "multiple_sites": True,
            "checked_luggage": False,
        }
    }


def test_execute_trip_saves_to_notion(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "c57f9edb406d4368b32d23f0ea2a0c66")
    query = _Query()
    created = {}

    class _NotionPages:
        def create(self, **kwargs):
            created.update(kwargs)
            return {"id": "trip-1"}

    class _NotionDatabases:
        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Trip": {"type": "title"},
                    "Departure Date": {"type": "date"},
                    "Return Date": {"type": "date"},
                    "Destination(s)": {"type": "rich_text"},
                    "Duration": {"type": "select"},
                    "Purpose": {"type": "multi_select"},
                    "Field Work": {"type": "rich_text"},
                    "Multiple Sites": {"type": "checkbox"},
                    "Checked Luggage": {"type": "checkbox"},
                    "Reminder Sent": {"type": "checkbox"},
                }
            }

    notion = type("Notion", (), {"pages": _NotionPages(), "databases": _NotionDatabases()})()
    flag = {"value": False}

    asyncio.run(trips.execute_trip(
        "0",
        query,
        notion=notion,
        claude=None,
        trip_map=_trip_map(),
        set_awaiting_packing_feedback=lambda value: flag.update(value=value),
        fetch_weather=lambda _: None,
    ))

    assert created["parent"]["database_id"] == "c57f9edb-406d-4368-b32d-23f0ea2a0c66"
    assert created["properties"]["Trip"]["title"][0]["text"]["content"].startswith("Nashville TN")
    assert created["properties"]["Purpose"] == {"multi_select": [{"name": "Work"}]}
    assert created["properties"]["Field Work"]["rich_text"][0]["text"]["content"] == "Site Survey"
    assert created["properties"]["Reminder Sent"] == {"checkbox": False}
    assert flag["value"] is False
    assert any("Trip saved to Notion" in msg for msg in query.message.sent)
    assert any("Packing checklist added" in msg for msg in query.message.sent)


def test_execute_trip_appends_native_packing_blocks(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "c57f9edb406d4368b32d23f0ea2a0c66")
    monkeypatch.setattr(trips, "NOTION_PACKING_ITEMS_DB", "packing-db")
    query = _Query()
    calls = {"pages": [], "queries": [], "appends": []}
    flag = {"value": False}

    class _NotionPages:
        def create(self, **kwargs):
            calls["pages"].append(kwargs)
            return {"id": "trip-1"}

    class _NotionDatabases:
        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Trip": {"type": "title"},
                    "Departure Date": {"type": "date"},
                    "Return Date": {"type": "date"},
                    "Destination(s)": {"type": "rich_text"},
                    "Duration": {"type": "select"},
                    "Purpose": {"type": "multi_select"},
                    "Field Work": {"type": "multi_select"},
                    "Multiple Sites": {"type": "checkbox"},
                    "Checked Luggage": {"type": "checkbox"},
                    "Reminder Sent": {"type": "checkbox"},
                }
            }

        def query(self, **kwargs):
            calls["queries"].append(kwargs)
            return {
                "has_more": False,
                "results": [
                    {
                        "properties": {
                            "Item": {"type": "title", "title": [{"plain_text": "Passport"}]},
                            "Category": {"type": "select", "select": {"name": "Documents"}},
                            "Always": {"type": "checkbox", "checkbox": True},
                            "Field Work": {"type": "multi_select", "multi_select": []},
                        }
                    },
                    {
                        "properties": {
                            "Item": {"type": "title", "title": [{"plain_text": "Sound level meter"}]},
                            "Category": {"type": "select", "select": {"name": "Field Gear"}},
                            "Always": {"type": "checkbox", "checkbox": False},
                            "Field Work": {"type": "multi_select", "multi_select": [{"name": "Site Survey"}]},
                        }
                    },
                    {
                        "properties": {
                            "Item": {"type": "title", "title": [{"plain_text": "Beach towel"}]},
                            "Category": {"type": "select", "select": {"name": "Personal"}},
                            "Always": {"type": "checkbox", "checkbox": False},
                            "Field Work": {"type": "multi_select", "multi_select": [{"name": "Personal"}]},
                        }
                    },
                ],
            }

    class _NotionChildren:
        def append(self, **kwargs):
            calls["appends"].append(kwargs)

    class _NotionBlocks:
        children = _NotionChildren()

    notion = type("Notion", (), {"pages": _NotionPages(), "databases": _NotionDatabases(), "blocks": _NotionBlocks()})()

    asyncio.run(trips.execute_trip(
        "0",
        query,
        notion=notion,
        claude=None,
        trip_map=_trip_map(),
        set_awaiting_packing_feedback=lambda value: flag.update(value=value),
        fetch_weather=lambda _: None,
    ))

    assert len(calls["pages"]) == 1
    assert calls["pages"][0]["parent"] == {"database_id": "c57f9edb-406d-4368-b32d-23f0ea2a0c66"}
    assert calls["queries"] == [{"database_id": "packing-db"}]
    assert calls["appends"][0]["block_id"] == "trip-1"
    assert [block["type"] for block in calls["appends"][0]["children"]] == ["heading_2", "to_do", "heading_2", "to_do"]
    assert flag["value"] is False
    assert query.message.sent == ["✅ Trip saved to Notion. Packing checklist added (2 items)."]



def test_parse_trip_message_falls_back_when_nlp_unavailable():
    parsed = trips.parse_trip_message("work and personal trip to Nashville TN, May 14-17", claude=None)

    assert parsed["destinations"] == ["Nashville TN"]
    assert parsed["purpose_list"] == ["Work", "Personal"]
    assert parsed["departure_date"] is None
    assert parsed["return_date"] is None


def test_normalize_notion_database_id():
    assert trips._normalize_notion_database_id("c57f9edbf406d4368b32d23f0ea2a0c66") == ""
    assert trips._normalize_notion_database_id("c57f9edb406d4368b32d23f0ea2a0c66") == "c57f9edb-406d-4368-b32d-23f0ea2a0c66"
    assert trips._normalize_notion_database_id("bad-id") == ""


def test_execute_trip_uses_field_work_types_when_present(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "c57f9edb406d4368b32d23f0ea2a0c66")
    query = _Query()
    created = {}

    class _NotionPages:
        def create(self, **kwargs):
            created.update(kwargs)
            return {"id": "trip-1"}

    class _NotionDatabases:
        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Trip": {"type": "title"},
                    "Departure Date": {"type": "date"},
                    "Return Date": {"type": "date"},
                    "Destination(s)": {"type": "rich_text"},
                    "Purpose": {"type": "multi_select"},
                    "Field Work Types": {"type": "rich_text"},
                }
            }

    notion = type("Notion", (), {"pages": _NotionPages(), "databases": _NotionDatabases()})()

    asyncio.run(trips.execute_trip(
        "0",
        query,
        notion=notion,
        claude=None,
        trip_map=_trip_map(),
        set_awaiting_packing_feedback=lambda value: None,
        fetch_weather=lambda _: None,
    ))

    assert "Field Work Types" in created["properties"]
    assert created["properties"]["Field Work Types"]["rich_text"][0]["text"]["content"] == "Site Survey"


def test_execute_trip_maps_weather_flags_to_multi_select(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_TRIPS_DB", "c57f9edb406d4368b32d23f0ea2a0c66")
    query = _Query()
    created = {}

    class _NotionPages:
        def create(self, **kwargs):
            created.update(kwargs)
            return {"id": "trip-1"}

    class _NotionDatabases:
        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Trip": {"type": "title"},
                    "Departure Date": {"type": "date"},
                    "Return Date": {"type": "date"},
                    "Destination(s)": {"type": "rich_text"},
                    "Purpose": {"type": "multi_select"},
                    "Weather Flags": {"type": "multi_select"},
                    "Weather Summary": {"type": "rich_text"},
                }
            }

    notion = type("Notion", (), {"pages": _NotionPages(), "databases": _NotionDatabases()})()

    asyncio.run(trips.execute_trip(
        "0",
        query,
        notion=notion,
        claude=None,
        trip_map=_trip_map(departure_date="2026-05-08", return_date="2026-05-10"),
        set_awaiting_packing_feedback=lambda value: None,
        fetch_weather=lambda bucket: {"condition": "Rain", "temp_high": 31, "temp_low": 4, "precip_chance": 80} if bucket == "today" else None,
    ))

    tags = [item["name"] for item in created["properties"]["Weather Flags"]["multi_select"]]
    assert set(tags) == {"Rain", "Hot", "Cold"}


def test_trip_weather_summary_uses_trip_date_range():
    summary, flags = trips._build_trip_weather_summary(
        "2026-05-08",
        "2026-05-10",
        "Nashville, TN",
        fetch_weather=lambda _: {"condition": "Rain", "temp_high": 31, "temp_low": 4, "precip_chance": 80},
        fetch_trip_weather_range=lambda dep, ret, dest: [
            {"label": "Fri May 8", "condition": "Clear", "temp_high": 24, "temp_low": 14, "precip_chance": 0},
            {"label": "Sat May 9", "condition": "Rain", "temp_high": 20, "temp_low": 10, "precip_chance": 70},
        ],
    )
    assert "Fri May 8" in summary
    assert "Sat May 9" in summary
    assert "Rain" in flags


def test_trip_weather_summary_uses_placeholder_outside_five_day_window():
    departure = (date.today() + timedelta(days=6)).isoformat()
    return_date = (date.today() + timedelta(days=9)).isoformat()
    summary, flags = trips._build_trip_weather_summary(
        departure,
        return_date,
        "Nashville, TN",
        fetch_weather=lambda _: {"condition": "Rain", "temp_high": 31, "temp_low": 4, "precip_chance": 80},
        fetch_trip_weather_range=lambda *_: [
            {"label": "Thu May 14", "condition": "Rain", "temp_high": 20, "temp_low": 10, "precip_chance": 70},
        ],
    )

    assert summary == trips.WEATHER_PLACEHOLDER_SUMMARY
    assert flags == []


def test_refresh_upcoming_trip_weather_updates_rows():
    updated_pages = []
    query_calls = []

    class _NotionDatabases:
        def query(self, **kwargs):
            query_calls.append(kwargs)
            return {
                "results": [
                    {
                        "id": "page-1",
                        "properties": {
                            "Departure Date": {"date": {"start": "2026-05-06"}},
                            "Return Date": {"date": {"start": "2026-05-08"}},
                            "Destination(s)": {"rich_text": [{"plain_text": "Nashville, TN"}]},
                            "Weather Flags": {"type": "multi_select"},
                            "Weather Summary": {"type": "rich_text"},
                        },
                    }
                ]
            }

        def retrieve(self, **kwargs):
            return {
                "properties": {
                    "Weather Flags": {"type": "multi_select"},
                    "Weather Summary": {"type": "rich_text"},
                }
            }

    class _NotionPages:
        def update(self, **kwargs):
            updated_pages.append(kwargs)

    notion = type("Notion", (), {"databases": _NotionDatabases(), "pages": _NotionPages()})()
    count = trips.refresh_upcoming_trip_weather(
        notion,
        "c57f9edb406d4368b32d23f0ea2a0c66",
        fetch_trip_weather_range=lambda *_: [{"label": "Thu May 6", "condition": "Rain", "temp_high": 30, "temp_low": 10, "precip_chance": 80}],
        lookahead_days=5,
    )
    assert count == 1
    assert updated_pages
    assert query_calls[0]["filter"]["and"][-1] == {
        "property": "Weather Summary",
        "rich_text": {"equals": trips.WEATHER_PLACEHOLDER_SUMMARY},
    }


def test_build_packing_blocks_queries_all_items_and_returns_native_blocks(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_PACKING_ITEMS_DB", "packing-db")
    calls = {"queries": []}

    class _NotionDatabases:
        def query(self, **kwargs):
            calls["queries"].append(kwargs)
            if len(calls["queries"]) == 1:
                return {
                    "has_more": True,
                    "next_cursor": "cursor-2",
                    "results": [
                        {
                            "properties": {
                                "Item": {"type": "title", "title": [{"plain_text": "Sound level meter"}]},
                                "Category": {"type": "select", "select": {"name": "Field Gear"}},
                                "Always": {"type": "checkbox", "checkbox": False},
                                "Field Work": {"type": "multi_select", "multi_select": [{"name": "Noise Measurements"}]},
                            }
                        },
                        {
                            "properties": {
                                "Item": {"type": "title", "title": [{"plain_text": "Passport"}]},
                                "Category": {"type": "rich_text", "rich_text": [{"plain_text": "Documents"}]},
                                "Always": {"type": "checkbox", "checkbox": True},
                                "Field Work": {"type": "multi_select", "multi_select": []},
                            }
                        },
                    ],
                }
            return {
                "has_more": False,
                "results": [
                    {
                        "properties": {
                            "Item": {"type": "title", "title": [{"plain_text": "Beach towel"}]},
                            "Category": {"type": "select", "select": {"name": "Personal"}},
                            "Always": {"type": "checkbox", "checkbox": False},
                            "Field Work": {"type": "multi_select", "multi_select": [{"name": "Personal"}]},
                        }
                    },
                    {
                        "properties": {
                            "Item": {"type": "title", "title": [{"plain_text": "Belt"}]},
                            "Category": {"type": "select", "select": {"name": "Clothing"}},
                            "Always": {"type": "checkbox", "checkbox": True},
                            "Field Work": {"type": "multi_select", "multi_select": []},
                        }
                    },
                ],
            }

    notion = type("Notion", (), {"databases": _NotionDatabases()})()

    blocks = trips.build_packing_blocks({"field_work_types": ["noise measurements"]}, notion)

    assert calls["queries"] == [
        {"database_id": "packing-db"},
        {"database_id": "packing-db", "start_cursor": "cursor-2"},
    ]
    assert [block["type"] for block in blocks] == ["heading_2", "to_do", "heading_2", "to_do", "heading_2", "to_do"]
    assert blocks[0]["heading_2"]["rich_text"][0]["text"]["content"] == "Clothing"
    assert blocks[1]["to_do"]["rich_text"][0]["text"]["content"] == "Belt"
    assert blocks[2]["heading_2"]["rich_text"][0]["text"]["content"] == "Documents"
    assert blocks[3]["to_do"]["rich_text"][0]["text"]["content"] == "Passport"
    assert blocks[4]["heading_2"]["rich_text"][0]["text"]["content"] == "Field Gear"
    assert blocks[5]["to_do"]["rich_text"][0]["text"]["content"] == "Sound level meter"
    assert all(block.get("to_do", {}).get("checked") is False for block in blocks if block["type"] == "to_do")


def test_build_packing_blocks_skips_when_db_unset(monkeypatch, caplog):
    monkeypatch.setattr(trips, "NOTION_PACKING_ITEMS_DB", "")

    class _NotionDatabases:
        def query(self, **kwargs):
            raise AssertionError("Packing DB should not be queried")

    notion = type("Notion", (), {"databases": _NotionDatabases()})()

    blocks = trips.build_packing_blocks({"field_work_types": ["Noise Measurements"]}, notion)

    assert blocks == []
    assert "NOTION_PACKING_ITEMS_DB is not set" in caplog.text


def test_build_packing_blocks_returns_empty_for_no_matching_items(monkeypatch):
    monkeypatch.setattr(trips, "NOTION_PACKING_ITEMS_DB", "packing-db")

    class _NotionDatabases:
        def query(self, **kwargs):
            return {
                "has_more": False,
                "results": [
                    {
                        "properties": {
                            "Item": {"type": "title", "title": [{"plain_text": "Tripod"}]},
                            "Category": {"type": "select", "select": {"name": "Field Gear"}},
                            "Always": {"type": "checkbox", "checkbox": False},
                            "Field Work": {"type": "multi_select", "multi_select": [{"name": "Vibration Measurements"}]},
                        }
                    }
                ],
            }

    notion = type("Notion", (), {"databases": _NotionDatabases()})()

    blocks = trips.build_packing_blocks({"field_work_types": ["None"]}, notion)

    assert blocks == []
