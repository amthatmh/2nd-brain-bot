import unittest

from cinema.sync import (
    _build_tmdb_movie_url,
    _build_cinema_query_filter,
    _resolve_cinema_title_property,
    _detect_favourite_db_fields,
    _load_existing_favourites,
    _select_best_tmdb_movie_match,
    _normalize_title,
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

    def test_filter_targets_rows_missing_tmdb_and_with_title(self):
        filter_obj = _build_cinema_query_filter("Film")
        self.assertEqual(
            filter_obj,
            {
                "and": [
                    {"property": "TMDB URL", "url": {"is_empty": True}},
                    {"property": "Film", "title": {"is_not_empty": True}},
                ]
            },
        )

    def test_resolve_title_property_prefers_film(self):
        class _FakeDatabases:
            def retrieve(self, **kwargs):
                return {
                    "properties": {
                        "Film": {"type": "title"},
                        "Name": {"type": "title"},
                    }
                }

        class _FakeNotion:
            def __init__(self):
                self.databases = _FakeDatabases()

        self.assertEqual(_resolve_cinema_title_property(_FakeNotion(), "cinema_db"), "Film")

    def test_build_tmdb_movie_url(self):
        self.assertEqual(_build_tmdb_movie_url(603), "https://www.themoviedb.org/movie/603")

    def test_match_selection_prefers_title_and_year(self):
        results = [
            {"id": 1, "title": "Dune", "release_date": "1984-12-14", "popularity": 20.0, "vote_count": 1200},
            {"id": 2, "title": "Dune: Part Two", "release_date": "2024-03-01", "popularity": 80.0, "vote_count": 8000},
        ]
        best = _select_best_tmdb_movie_match(results, title="Dune: Part Two", row_year=2024)
        self.assertIsNotNone(best)
        self.assertEqual(best["id"], 2)

    def test_match_selection_returns_none_for_low_confidence(self):
        results = [{"id": 1, "title": "Completely Different", "release_date": "2000-01-01"}]
        best = _select_best_tmdb_movie_match(results, title="Interstellar", row_year=2014)
        self.assertIsNone(best)


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
                            {"properties": {"Name": {"title": [{"plain_text": "Dune"}]}}},
                        ],
                        "has_more": True,
                        "next_cursor": "cursor-1",
                    }
                test_case.assertEqual(kwargs.get("start_cursor"), "cursor-1")
                return {
                    "results": [
                        {"properties": {"Name": {"title": [{"plain_text": "Arrival"}]}}},
                    ],
                    "has_more": False,
                }

        class _FakeNotion:
            def __init__(self):
                self.databases = _FakeDatabases()

        favourites = _load_existing_favourites(_FakeNotion(), "fake-db", "Name")
        self.assertEqual(favourites, {"dune", "arrival"})

    def test_detect_favourite_db_fields_supports_name_title(self):
        class _FakeDatabases:
            def retrieve(self, **kwargs):
                self.kwargs = kwargs
                return {
                    "properties": {
                        "Name": {"type": "title"},
                        "Year": {"type": "number"},
                        "Category": {"type": "select"},
                    }
                }

        class _FakeNotion:
            def __init__(self):
                self.databases = _FakeDatabases()

        fields = _detect_favourite_db_fields(_FakeNotion(), "fave_db")
        self.assertEqual(fields["title_prop"], "Name")
        self.assertEqual(fields["year_prop"], "Year")
        self.assertEqual(fields["year_type"], "number")
        self.assertEqual(fields["category_prop"], "Category")
        self.assertEqual(fields["category_type"], "select")

    def test_normalize_title(self):
        self.assertEqual(_normalize_title("  The   Matrix "), "the matrix")

    def test_preferred_media_type_maps_film_and_series(self):
        self.assertEqual(_preferred_media_type({"Type": {"select": {"name": "Film"}}}), "movie")
        self.assertEqual(_preferred_media_type({"Type": {"select": {"name": "Series"}}}), "tv")
        self.assertIsNone(_preferred_media_type({"Type": {"select": {"name": "Documentary"}}}))


if __name__ == "__main__":
    unittest.main()
