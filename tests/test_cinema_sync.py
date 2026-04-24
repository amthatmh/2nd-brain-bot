import unittest

from cinema.sync import _build_cinema_query_filter, _load_existing_favourites, _plain_text


class TestCinemaSyncHelpers(unittest.TestCase):
    def test_plain_text_supports_title(self):
        prop = {"title": [{"plain_text": "Inter"}, {"plain_text": "stellar"}]}
        self.assertEqual(_plain_text(prop), "Interstellar")

    def test_plain_text_supports_rich_text(self):
        prop = {"rich_text": [{"plain_text": "Dune"}]}
        self.assertEqual(_plain_text(prop), "Dune")

    def test_filter_with_tmdb_key_backfills_missing_urls(self):
        filter_obj = _build_cinema_query_filter("abc123")
        self.assertIn("or", filter_obj)
        self.assertEqual(len(filter_obj["or"]), 2)

    def test_filter_without_tmdb_key_targets_unsynced_only(self):
        filter_obj = _build_cinema_query_filter("")
        self.assertEqual(
            filter_obj,
            {"property": "Last Synced", "date": {"is_empty": True}},
        )

    def test_load_existing_favourites_handles_pagination(self):
        test_case = self

        class _FakeDatabases:
            def __init__(self):
                self.calls = 0

            def query(self, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return {
                        "results": [
                            {"properties": {"Title": {"title": [{"plain_text": "Dune"}]}}},
                        ],
                        "has_more": True,
                        "next_cursor": "cursor-1",
                    }
                test_case.assertEqual(kwargs.get("start_cursor"), "cursor-1")
                return {
                    "results": [
                        {"properties": {"Title": {"title": [{"plain_text": "Arrival"}]}}},
                    ],
                    "has_more": False,
                }

        class _FakeNotion:
            def __init__(self):
                self.databases = _FakeDatabases()

        favourites = _load_existing_favourites(_FakeNotion(), "fake-db")
        self.assertEqual(favourites, {"Dune", "Arrival"})


if __name__ == "__main__":
    unittest.main()
