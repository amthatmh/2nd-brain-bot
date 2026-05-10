import ast
import pathlib
import unittest


class TestMainDuplicateFunctions(unittest.TestCase):
    def test_duplicate_functions_gone(self):
        src = pathlib.Path("second_brain/main.py").read_text()
        tree = ast.parse(src)
        names = [n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        dupes = sorted(name for name in set(names) if names.count(name) > 1)
        # No duplicate function definitions should remain in main.py.
        self.assertEqual(dupes, [])


if __name__ == "__main__":
    unittest.main()
