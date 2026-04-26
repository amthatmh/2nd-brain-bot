import unittest
from datetime import datetime

from notes_flow import (
    create_note_payload,
    note_topics_keyboard,
    ordered_topics,
)


class TestNotesFlow(unittest.TestCase):
    def test_ordered_topics_uses_recency_desc(self):
        topics = ["Code", "Ideas", "Personal"]
        recency = {
            "Ideas": datetime(2026, 4, 25, 10, 0, 0),
            "Code": datetime(2026, 4, 26, 8, 0, 0),
        }

        ordered = ordered_topics(topics, recency)

        self.assertEqual(ordered, ["Code", "Ideas", "Personal"])

    def test_note_topics_keyboard_batches_two_per_row_and_has_none_option(self):
        keyboard = note_topics_keyboard("k1", ["Code", "Ideas", "Health"])

        rows = keyboard.inline_keyboard
        self.assertEqual(len(rows), 3)
        self.assertEqual([button.text for button in rows[0]], ["Code", "Ideas"])
        self.assertEqual([button.callback_data for button in rows[0]], ["note_topic:k1:0", "note_topic:k1:1"])
        self.assertEqual([button.text for button in rows[1]], ["Health"])
        self.assertEqual(rows[1][0].callback_data, "note_topic:k1:2")
        self.assertEqual(rows[2][0].callback_data, "note_topic:k1:none")

    def test_create_note_payload_sets_link_type_and_chunks_content(self):
        long_text = "Check this out https://example.com/post), then review notes.\n" + ("A" * 2100)

        payload = create_note_payload(long_text, topic="Ideas")

        self.assertEqual(payload["Type"]["select"]["name"], "🔗 Link/Article")
        self.assertEqual(payload["Link"]["url"], "https://example.com/post")
        self.assertEqual(payload["Topic"]["multi_select"][0]["name"], "Ideas")
        self.assertEqual(len(payload["Content"]["rich_text"]), 2)
        self.assertEqual(payload["Title"]["title"][0]["type"], "text")
        self.assertEqual(payload["Content"]["rich_text"][0]["type"], "text")


if __name__ == "__main__":
    unittest.main()
