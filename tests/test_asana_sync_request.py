import io
import json
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

import asana_sync
from asana_sync import AsanaSyncError, _asana_request


def _mock_urlopen_with_json(payload: dict):
    cm = MagicMock()
    cm.__enter__.return_value.read.return_value = json.dumps(payload).encode("utf-8")
    cm.__exit__.return_value = False
    return cm


class TestAsanaRequestRetries(unittest.TestCase):
    @patch("asana_sync.time.sleep", return_value=None)
    @patch("asana_sync.random.uniform", return_value=0.0)
    def test_retries_on_429_then_succeeds(self, *_mocks):
        transient = HTTPError(
            url="https://example.com",
            code=429,
            msg="rate limited",
            hdrs=None,
            fp=io.BytesIO(b""),
        )
        success_cm = _mock_urlopen_with_json({"data": {"gid": "123"}})

        with patch("asana_sync.request.urlopen", side_effect=[transient, success_cm]) as mocked:
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
        with patch("asana_sync.request.urlopen", side_effect=bad_request):
            with self.assertRaises(HTTPError):
                _asana_request("/tasks", token="abc")

    @patch("asana_sync.time.sleep", return_value=None)
    @patch("asana_sync.random.uniform", return_value=0.0)
    def test_exhausted_retries_raise_asana_sync_error(self, *_mocks):
        net_err = URLError("temporary failure")
        with patch("asana_sync.request.urlopen", side_effect=net_err):
            with patch.object(asana_sync, "ASANA_MAX_RETRIES", 2):
                with self.assertRaises(AsanaSyncError):
                    _asana_request("/tasks", token="abc")


if __name__ == "__main__":
    unittest.main()
