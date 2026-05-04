import unittest
from unittest.mock import patch

from second_brain.cinema.sync import sync_cinema_log_to_notion


class _FakeDatabases:
    def __init__(self):
        self.cinema_pages = []
        self.fave_titles = set()
        self.fave_title_prop_name = "Title"
        self.fave_year_prop_type = "number"
        self.fave_category_prop_type = "select"
        self.cinema_query_calls = 0

    def retrieve(self, **kwargs):
        database_id = kwargs["database_id"]
        if database_id == "cinema_db":
            return {
                "properties": {
                    "Film": {"type": "title"},
                    "TMDB URL": {"type": "url"},
                }
            }
        if database_id != "fave_db":
            raise AssertionError(f"Unexpected database_id: {database_id}")
        return {
            "properties": {
                self.fave_title_prop_name: {"type": "title"},
                "Year": {"type": self.fave_year_prop_type},
                "Category": {"type": self.fave_category_prop_type},
            }
        }

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
                            "properties": {
                                self.fave_title_prop_name: {
                                    "title": [{"plain_text": title}]
                                }
                            },
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

        with patch("second_brain.cinema.sync._search_tmdb_url_with_client", return_value="https://www.themoviedb.org/movie/157336"):
            stats = await sync_cinema_log_to_notion(
                notion=notion,
                cinema_db_id="cinema_db",
                fave_db_id="fave_db",
                tmdb_api_key="tmdb_key",
            )

        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(stats["tmdb_found"], 1)
        self.assertEqual(stats["tmdb_missing"], 0)
        self.assertEqual(len(notion.pages.updated), 1)
        updated_props = notion.pages.updated[0]["properties"]
        self.assertIn("TMDB URL", updated_props)

    async def test_favourite_checked_creates_new_favourite_row(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_new_fave",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Dune Part Two"}]},
                            "TMDB URL": {"url": "https://www.themoviedb.org/movie/693134"},
                            "Favourite": {"checkbox": True},
                            "Date": {"date": {"start": "2025-03-15"}},
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

        self.assertEqual(stats["added_to_fave"], 1)
        self.assertEqual(len(notion.pages.created), 1)
        created_props = notion.pages.created[0]["properties"]
        self.assertEqual(
            created_props["Title"]["title"][0]["text"]["content"],
            "Dune Part Two",
        )

    async def test_favourite_checked_existing_match_does_not_create_duplicate(self):
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
        self.assertEqual(len(notion.pages.updated), 0)
        self.assertEqual(stats["skipped"], 1)

    async def test_favourite_db_title_property_name_works(self):
        notion = _FakeNotion()
        notion.databases.fave_title_prop_name = "Name"
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_name_fave",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Arrival"}]},
                            "TMDB URL": {"url": "https://www.themoviedb.org/movie/329865"},
                            "Favourite": {"checkbox": True},
                            "Date": {"date": {"start": "2024-10-01"}},
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

        self.assertEqual(stats["added_to_fave"], 1)
        created_props = notion.pages.created[0]["properties"]
        self.assertIn("Name", created_props)
        self.assertEqual(created_props["Name"]["title"][0]["text"]["content"], "Arrival")

    async def test_created_favourite_sets_category_to_film(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_category",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Inception"}]},
                            "TMDB URL": {"url": "https://www.themoviedb.org/movie/27205"},
                            "Favourite": {"checkbox": True},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            }
        ]

        await sync_cinema_log_to_notion(
            notion=notion,
            cinema_db_id="cinema_db",
            fave_db_id="fave_db",
            tmdb_api_key="tmdb_key",
        )

        created_props = notion.pages.created[0]["properties"]
        self.assertEqual(created_props["Category"]["select"]["name"], "Film")

    async def test_created_favourite_sets_year_from_cinema_date(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_year",
                        "properties": {
                            "Film": {"title": [{"plain_text": "The Matrix"}]},
                            "TMDB URL": {"url": "https://www.themoviedb.org/movie/603"},
                            "Favourite": {"checkbox": True},
                            "Date": {"date": {"start": "2021-12-09"}},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            }
        ]

        await sync_cinema_log_to_notion(
            notion=notion,
            cinema_db_id="cinema_db",
            fave_db_id="fave_db",
            tmdb_api_key="tmdb_key",
        )

        created_props = notion.pages.created[0]["properties"]
        self.assertEqual(created_props["Year"]["number"], 2021)

    async def test_non_favourite_rows_do_not_create_favourite_rows(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_not_fave",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Blade Runner"}]},
                            "TMDB URL": {"url": "https://www.themoviedb.org/movie/78"},
                            "Favourite": {"checkbox": False},
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

    async def test_favourite_row_synced_today_still_promotes_to_favourites(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_fave_today",
                        "properties": {
                            "Film": {"title": [{"plain_text": "The Drama"}]},
                            "TMDB URL": {"url": "https://www.themoviedb.org/movie/111"},
                            "Favourite": {"checkbox": True},
                            "Date": {"date": {"start": "2026-04-30"}},
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

        self.assertEqual(stats["added_to_fave"], 1)
        self.assertEqual(len(notion.pages.created), 1)
        created_props = notion.pages.created[0]["properties"]
        self.assertEqual(created_props["Title"]["title"][0]["text"]["content"], "The Drama")
        self.assertEqual(created_props["Category"]["select"]["name"], "Film")

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

        with patch("second_brain.cinema.sync._search_tmdb_url_with_client", return_value=None):
            stats = await sync_cinema_log_to_notion(
                notion=notion,
                cinema_db_id="cinema_db",
                fave_db_id="fave_db",
                tmdb_api_key="tmdb_key",
            )

        self.assertEqual(stats["scanned"], 2)
        self.assertEqual(stats["tmdb_missing"], 2)
        self.assertEqual(stats["failed"], 0)
        self.assertEqual(len(notion.pages.updated), 0)

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

        with patch("second_brain.cinema.sync._search_tmdb_url_with_client", return_value="https://www.themoviedb.org/movie/603"):
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

    async def test_builds_tmdb_url_from_tmdb_id_and_type_without_search(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_tmdb_id",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Severance"}]},
                            "Type": {"select": {"name": "Film"}},
                            "TMDB ID": {"rich_text": [{"plain_text": "95396"}]},
                            "TMDB URL": {"url": None},
                            "Favourite": {"checkbox": False},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            }
        ]

        with patch("second_brain.cinema.sync._search_tmdb_url_with_client") as search_mock:
            stats = await sync_cinema_log_to_notion(
                notion=notion,
                cinema_db_id="cinema_db",
                fave_db_id="fave_db",
                tmdb_api_key="tmdb_key",
            )

        self.assertEqual(stats["tmdb_found"], 1)
        self.assertEqual(stats["tmdb_missing"], 0)
        search_mock.assert_not_called()
        updated_props = notion.pages.updated[0]["properties"]
        self.assertEqual(updated_props["TMDB URL"]["url"], "https://www.themoviedb.org/movie/95396")

    async def test_sync_runs_without_fave_db(self):
        notion = _FakeNotion()
        notion.databases.cinema_pages = [
            {
                "results": [
                    {
                        "id": "row_no_fave",
                        "properties": {
                            "Film": {"title": [{"plain_text": "Inception"}]},
                            "TMDB URL": {"url": None},
                            "Favourite": {"checkbox": True},
                        },
                    }
                ],
                "has_more": False,
                "next_cursor": None,
            }
        ]

        with patch("second_brain.cinema.sync._search_tmdb_url_with_client", return_value="https://www.themoviedb.org/movie/27205"):
            stats = await sync_cinema_log_to_notion(
                notion=notion,
                cinema_db_id="cinema_db",
                fave_db_id="",
                tmdb_api_key="tmdb_key",
            )

        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(stats["tmdb_found"], 1)
        self.assertEqual(stats["added_to_fave"], 0)
        self.assertEqual(len(notion.pages.created), 0)
        self.assertEqual(len(notion.pages.updated), 1)


if __name__ == "__main__":
    unittest.main()
