import unittest
from unittest.mock import patch

from cinema.sync import sync_cinema_log_to_notion


class _FakeDatabases:
    def __init__(self):
        self.cinema_pages = []
        self.fave_titles = set()
        self.cinema_query_calls = 0

    def query(self, **kwargs):
        database_id = kwargs["database_id"]
        if database_id == "cinema_db":
            idx = self.cinema_query_calls
            self.cinema_query_calls += 1
            if idx < len(self.cinema_pages):
                return self.cinema_pages[idx]
            return {"results": [], "has_more": False, "next_cursor": None}

        if database_id == "fave_db":
            # Full-table scan path used by _load_existing_favourites
            if "filter" not in kwargs:
                return {
                    "results": [
                        {
                            "id": f"fave_{idx}",
                            "properties": {"Title": {"title": [{"plain_text": title}]}},
                        }
                        for idx, title in enumerate(sorted(self.fave_titles))
                    ],
                    "has_more": False,
                    "next_cursor": None,
                }

            # Legacy targeted lookup (kept for compatibility in tests)
            title = kwargs["filter"]["title"]["equals"]
            found = title in self.fave_titles
            return {"results": ([{"id": "existing"}] if found else [])}

        raise AssertionError(f"Unexpected database_id: {database_id}")


class _FakePages:
    def __init__(self):
        self.created = []
        self.updated = []

    def create(self, **kwargs):
        self.created.append(kwargs)
        return {"id": "new_page_id"}

    def update(self, **kwargs):
        self.updated.append(kwargs)
        return {"id": kwargs["page_id"]}


class _FakeNotion:
    def __init__(self):
        self.databases = _FakeDatabases()
        self.pages = _FakePages()


class TestCinemaSyncIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_backfills_tmdb_and_marks_synced(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_1",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Interstellar"}]},
                            "TMDB URL": {"url": None},
                            "Favourite": {"checkbox": False},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            }
        ]

        with patch("cinema.sync._search_tmdb_url_with_client", return_value="https://www.themoviedb.org/movie/157336"):
            stats = await sync_cinema_log_to_notion(
                notion=notion,
                cinema_db_id="cinema_db",
                fave_db_id="fave_db",
                tmdb_api_key="tmdb_key",
            )

        self.assertEqual(stats["new_entries"], 1)
        self.assertEqual(stats["tmdb_found"], 1)
        self.assertEqual(stats["tmdb_missing"], 0)
        self.assertEqual(len(notion.pages.updated), 1)
        updated_props = notion.pages.updated[0]["properties"]
        self.assertIn("TMDB URL", updated_props)
        self.assertIn("Last Synced", updated_props)

    async def test_favourite_duplicate_is_not_created(self):
        notion = _FakeNotion()
        notion.databases.fave_titles.add("Severance")
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_2",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Severance"}]},
                            "TMDB URL": {"url": "https://www.themoviedb.org/tv/95396"},
                            "Favourite": {"checkbox": True},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            }
        ]

        stats = await sync_cinema_log_to_notion(
            notion=notion,
            cinema_db_id="cinema_db",
            fave_db_id="fave_db",
            tmdb_api_key="tmdb_key",
        )

        self.assertEqual(stats["added_to_fave"], 0)
        self.assertEqual(len(notion.pages.created), 0)
        self.assertEqual(len(notion.pages.updated), 1)

    async def test_paginates_cinema_rows(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_a",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Dune"}]},
                            "TMDB URL": {"url": None},
                            "Favourite": {"checkbox": False},
                        },
                    }
                ],
                "has_more": True,
                "next_cursor": "cursor_1",
            },
            {
                "results": [
                    {
                        "id": "row_b",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Arrival"}]},
                            "TMDB URL": {"url": None},
                            "Favourite": {"checkbox": False},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            },
        ]

        with patch("cinema.sync._search_tmdb_url_with_client", return_value=None):
            stats = await sync_cinema_log_to_notion(
                notion=notion,
                cinema_db_id="cinema_db",
                fave_db_id="fave_db",
                tmdb_api_key="tmdb_key",
            )

        self.assertEqual(stats["new_entries"], 2)
        self.assertEqual(stats["tmdb_missing"], 2)
        self.assertEqual(len(notion.pages.updated), 2)

    async def test_backfills_tmdb_when_title_property_is_name(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_name_1",
                        "properties": {
                            "Name": {"type": "title", "title": [{"plain_text": "The Matrix"}]},
                            "TMDB URL": {"url": None},
                            "Favourite": {"checkbox": False},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            }
        ]

        with patch("cinema.sync._search_tmdb_url_with_client", return_value="https://www.themoviedb.org/movie/603"):
            stats = await sync_cinema_log_to_notion(
                notion=notion,
                cinema_db_id="cinema_db",
                fave_db_id="fave_db",
                tmdb_api_key="tmdb_key",
            )

        self.assertEqual(stats["tmdb_found"], 1)
        self.assertEqual(stats["tmdb_missing"], 0)
        updated_props = notion.pages.updated[0]["properties"]
        self.assertEqual(updated_props["TMDB URL"]["url"], "https://www.themoviedb.org/movie/603")


if __name__ == "__main__":
    unittest.main()
