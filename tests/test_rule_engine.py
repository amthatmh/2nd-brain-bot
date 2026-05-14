import asyncio

from second_brain.rules.engine import RuleEngine
from second_brain.rules import entertainment_rules


class _Databases:
    def __init__(self, existing=None):
        self.existing = existing or []
        self.queries = []

    def query(self, **kwargs):
        self.queries.append(kwargs)
        return {"results": self.existing}


class _Pages:
    def __init__(self):
        self.created = []

    def create(self, **kwargs):
        self.created.append(kwargs)
        return {"id": "fav-1"}


class _Notion:
    def __init__(self, existing=None):
        self.databases = _Databases(existing)
        self.pages = _Pages()


def test_entertainment_rules_validate():
    valid, errors = entertainment_rules.validate_rules()
    assert valid is True
    assert errors == []


def test_rule_engine_creates_favourite_for_cinema_favourite():
    notion = _Notion()
    engine = RuleEngine(notion)
    assert engine.startup() is True

    results = asyncio.run(engine.execute_on_save(
        "cinema_log",
        {"Title": "Oppenheimer", "DateWatched": "2026-05-14", "Favourite": True},
        {"favourite_films": "fav-db"},
    ))

    assert results == [{"rule_id": "cinema_to_favourite", "success": True, "message": "Action 'create_entry' executed"}]
    assert notion.pages.created[0]["parent"] == {"database_id": "fav-db"}
    assert notion.pages.created[0]["properties"]["Title"]["title"][0]["text"]["content"] == "Oppenheimer"
    assert notion.pages.created[0]["properties"]["DateWatched"] == {"date": {"start": "2026-05-14"}}


def test_rule_engine_skips_when_not_favourite():
    notion = _Notion()
    engine = RuleEngine(notion)
    assert engine.startup() is True

    results = asyncio.run(engine.execute_on_save(
        "cinema_log",
        {"Title": "Oppenheimer", "DateWatched": "2026-05-14", "Favourite": False},
        {"favourite_films": "fav-db"},
    ))

    assert results == [{"rule_id": "cinema_to_favourite", "success": False, "message": "Condition not met"}]
    assert notion.pages.created == []


def test_rule_engine_deduplicates_favourites():
    notion = _Notion(existing=[{"id": "existing"}])
    engine = RuleEngine(notion)
    assert engine.startup() is True

    results = asyncio.run(engine.execute_on_save(
        "cinema_log",
        {"Title": "Oppenheimer", "DateWatched": "2026-05-14", "Favourite": True},
        {"favourite_films": "fav-db"},
    ))

    assert results == [{"rule_id": "cinema_to_favourite", "success": True, "message": "Action 'create_entry' executed"}]
    assert notion.pages.created == []
