import unittest

from second_brain.notion.habits import extract_habit_frequency


class TestExtractHabitFrequency(unittest.TestCase):
    def test_prefers_frequency_per_week_number(self):
        props = {
            "Frequency Per Week": {"type": "number", "number": 4},
            "Frequency": {"type": "select", "select": {"name": "2x/week"}},
        }
        self.assertEqual(extract_habit_frequency(props), 4)

    def test_reads_frequency_from_select_text(self):
        props = {
            "Frequency": {"type": "select", "select": {"name": "3x/week"}},
        }
        self.assertEqual(extract_habit_frequency(props), 3)

    def test_reads_frequency_from_label_fallback(self):
        props = {
            "Frequency Label": {"type": "rich_text", "rich_text": [{"plain_text": "5 per week"}]},
        }
        self.assertEqual(extract_habit_frequency(props), 5)

    def test_returns_none_for_missing_frequency(self):
        props = {
            "Frequency": {"type": "select", "select": {"name": "As needed"}},
        }
        self.assertIsNone(extract_habit_frequency(props))


if __name__ == "__main__":
    unittest.main()
