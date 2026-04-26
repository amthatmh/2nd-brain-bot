from datetime import date
import unittest

from cinema.sync import (
    _build_cinema_query_filter,
    _load_existing_favourites,
    _plain_text,
    _preferred_media_type,
    _title_search_candidates,
)


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
        self.assertEqual(len(filter_obj["or"]), 3)
        self.assertIn({"property": "TMDB URL", "url": {"is_empty": True}}, filter_obj["or"])

    def test_filter_without_tmdb_key_targets_unsynced_or_stale_rows(self):
        filter_obj = _build_cinema_query_filter("")
        self.assertEqual(
            filter_obj,
            {
                "or": [
                    {"property": "Last Synced", "date": {"is_empty": True}},
                    {"property": "Last Synced", "date": {"before": date.today().isoformat()}},
                ]
            },
        )


    def test_title_search_candidates_strips_common_noise(self):
        candidates = _title_search_candidates("Dune: Part Two (2024)")
        self.assertEqual(candidates, ["Dune: Part Two (2024)", "Dune: Part Two", "Dune"])

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

    def test_preferred_media_type_maps_film_and_series(self):
        self.assertEqual(_preferred_media_type({"Type": {"select": {"name": "Film"}}}), "movie")
        self.assertEqual(_preferred_media_type({"Type": {"select": {"name": "Series"}}}), "tv")
        self.assertIsNone(_preferred_media_type({"Type": {"select": {"name": "Documentary"}}}))


if __name__ == "__main__":
    unittest.main()
