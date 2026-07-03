import io
import json
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

import second_brain.asana.sync as asana_sync
from second_brain.asana.sync import AsanaSyncError, _asana_request


def _mock_urlopen_with_json(payload: dict):
    cm = MagicMock()
    cm.__enter__.return_value.read.return_value = json.dumps(payload).encode("utf-8")
    cm.__exit__.return_value = False
    return cm


class TestAsanaRequestRetries(unittest.TestCase):
    @patch("second_brain.asana.sync.time.sleep", return_value=None)
    @patch("second_brain.asana.sync.random.uniform", return_value=0.0)
    def test_retries_on_429_then_succeeds(self, *_mocks):
        transient = HTTPError(
            url="https://example.com",
            code=429,
            msg="rate limited",
            hdrs=None,
            fp=io.BytesIO(b""),
        )
        success_cm = _mock_urlopen_with_json({"data": {"gid": "123"}})

        with patch("second_brain.asana.sync.request.urlopen", side_effect=[transient, success_cm]) as mocked:
            with patch.object(asana_sync, "ASANA_MAX_RETRIES", 3):
                payload = _asana_request("/tasks", token="abc")

        self.assertEqual(payload, {"data": {"gid": "123"}})
        self.assertEqual(mocked.call_count, 2)

    def test_non_retryable_http_error_is_raised(self):
        bad_request = HTTPError(
            url="https://example.com",
            code=400,
            msg="bad request",
            hdrs=None,
            fp=io.BytesIO(b""),
        )
        with patch("second_brain.asana.sync.request.urlopen", side_effect=bad_request):
            with self.assertRaises(HTTPError):
                _asana_request("/tasks", token="abc")

    @patch("second_brain.asana.sync.time.sleep", return_value=None)
    @patch("second_brain.asana.sync.random.uniform", return_value=0.0)
    def test_exhausted_retries_raise_asana_sync_error(self, *_mocks):
        net_err = URLError("temporary failure")
        with patch("second_brain.asana.sync.request.urlopen", side_effect=net_err):
            with patch.object(asana_sync, "ASANA_MAX_RETRIES", 2):
                with self.assertRaises(AsanaSyncError):
                    _asana_request("/tasks", token="abc")


def _page(gid: str, *, page_id: str, done: bool = False, archived: bool = False):
    return {
        "id": page_id,
        "archived": archived,
        "properties": {
            "Asana Task ID": {"rich_text": [{"plain_text": gid}]},
            "Done": {"checkbox": done},
        },
    }


class TestReassignmentSweep(unittest.TestCase):
    """A task no longer assigned to the PAT owner should be archived in Notion."""

    def setUp(self):
        # Isolate module-level caches between tests.
        asana_sync._cache._map = {}
        asana_sync._cache._last_full_rebuild = None
        asana_sync._me_gid_cache.clear()

    def _run(self, *, feed_tasks, active_pages, assignee_results):
        notion = MagicMock()

        with patch.object(asana_sync, "_asana_fetch_tasks", return_value=feed_tasks), \
             patch.object(asana_sync, "_active_gid_page_map", return_value=active_pages), \
             patch.object(asana_sync, "_get_me_gid", return_value="me-gid"), \
             patch.object(asana_sync._cache, "rebuild", return_value=None), \
             patch.object(asana_sync, "_asana_fetch_task_assignee", side_effect=assignee_results):
            stats = asana_sync.reconcile(
                notion=notion,
                notion_db_id="db",
                asana_token="tok",
                asana_workspace_gid="123",
                source_mode="my_tasks",
            )
        return notion, stats

    def test_reassigned_task_is_archived(self):
        # Task gid=1 is in Notion (active) but gone from the feed; now assigned
        # to someone else → should be archived.
        notion, stats = self._run(
            feed_tasks=[],
            active_pages={"1": _page("1", page_id="p1")},
            assignee_results=["other-person-gid"],
        )
        self.assertEqual(stats["reassigned"], 1)
        notion.pages.update.assert_called_once_with(page_id="p1", archived=True)

    def test_unassigned_task_is_archived(self):
        notion, stats = self._run(
            feed_tasks=[],
            active_pages={"1": _page("1", page_id="p1")},
            assignee_results=[None],
        )
        self.assertEqual(stats["reassigned"], 1)
        notion.pages.update.assert_called_once_with(page_id="p1", archived=True)

    def test_still_mine_but_missing_from_feed_is_left_alone(self):
        # Transiently absent from the feed but still assigned to us → never archive.
        notion, stats = self._run(
            feed_tasks=[],
            active_pages={"1": _page("1", page_id="p1")},
            assignee_results=["me-gid"],
        )
        self.assertEqual(stats["reassigned"], 0)
        notion.pages.update.assert_not_called()

    def test_deleted_task_is_left_alone(self):
        # 404 / transient error must not trigger archival.
        notion, stats = self._run(
            feed_tasks=[],
            active_pages={"1": _page("1", page_id="p1")},
            assignee_results=[AsanaSyncError("404 not found")],
        )
        self.assertEqual(stats["reassigned"], 0)
        notion.pages.update.assert_not_called()

    def test_task_still_in_feed_is_not_swept(self):
        # Present in the feed → handled by the main loop, never reassignment-swept.
        notion, stats = self._run(
            feed_tasks=[{"gid": "1", "name": "x", "modified_at": "2026-01-01T00:00:00Z"}],
            active_pages={"1": _page("1", page_id="p1")},
            assignee_results=[],
        )
        self.assertEqual(stats["reassigned"], 0)


if __name__ == "__main__":
    unittest.main()
