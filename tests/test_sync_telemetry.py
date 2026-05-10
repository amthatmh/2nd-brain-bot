import unittest

from second_brain.sync_telemetry import format_sync_status_message, init_sync_status


class TestSyncTelemetry(unittest.TestCase):
    def test_init_sync_status_shape(self):
        status = init_sync_status()
        self.assertIn("asana", status)
        self.assertIn("cinema", status)
        self.assertIsNone(status["asana"]["ok"])

    def test_format_sync_status_message_includes_errors_and_stats(self):
        status = init_sync_status()
        status["asana"].update(
            {
                "ok": True,
                "last_run": "2026-04-24 10:00:00 UTC",
                "stats": {"a2n": 2, "n2a": 1},
            }
        )
        status["cinema"].update(
            {
                "ok": False,
                "last_run": "2026-04-24 10:05:00 UTC",
                "error": "tmdb timeout",
            }
        )
        msg = format_sync_status_message(status)
        self.assertIn("Asana", msg)
        self.assertIn("Cinema", msg)
        self.assertIn("a2n", msg)
        self.assertIn("tmdb timeout", msg)


if __name__ == "__main__":
    unittest.main()
