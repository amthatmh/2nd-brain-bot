import unittest

from second_brain.palette import parse_done_numbers_command


class TestDoneNumbersParser(unittest.TestCase):
    def test_parses_plain_number(self):
        self.assertEqual(parse_done_numbers_command("Complete 3"), [3])

    def test_parses_keycap_number_emoji(self):
        self.assertEqual(parse_done_numbers_command("Complete 3️⃣"), [3])


if __name__ == "__main__":
    unittest.main()
