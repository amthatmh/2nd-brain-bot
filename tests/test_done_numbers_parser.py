import ast
import pathlib
import re
import unittest


def _load_parse_done_numbers_command():
    src = pathlib.Path("second_brain/main.py").read_text()
    tree = ast.parse(src)
    fn_node = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "parse_done_numbers_command"
    )
    fn_src = ast.get_source_segment(src, fn_node)
    namespace = {"re": re}
    exec(fn_src, namespace)
    return namespace["parse_done_numbers_command"]


class TestDoneNumbersParser(unittest.TestCase):
    def setUp(self):
        self.parse_done_numbers_command = _load_parse_done_numbers_command()

    def test_parses_plain_number(self):
        self.assertEqual(self.parse_done_numbers_command("Complete 3"), [3])

    def test_parses_keycap_number_emoji(self):
        self.assertEqual(self.parse_done_numbers_command("Complete 3️⃣"), [3])


if __name__ == "__main__":
    unittest.main()
