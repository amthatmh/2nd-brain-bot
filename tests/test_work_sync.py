"""Tests for scripts/sync_work_context.py and second_brain/work_sync/sync.py."""

import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import tempfile

from second_brain.work_sync.sync import (
    _blocks_to_md,
    _render_rich_text,
    name_to_slug,
    readme_filename,
    run_sync,
)


class TestNameToSlug(unittest.TestCase):
    def test_simple(self):
        self.assertEqual(name_to_slug("My Skill"), "my-skill")

    def test_special_chars(self):
        self.assertEqual(name_to_slug("Data & Analytics!"), "data-analytics")

    def test_already_slug(self):
        self.assertEqual(name_to_slug("code-review"), "code-review")

    def test_leading_trailing(self):
        self.assertEqual(name_to_slug("  Hello World  "), "hello-world")

    def test_numbers(self):
        self.assertEqual(name_to_slug("Q4 2025 Review"), "q4-2025-review")


class TestReadmeFilename(unittest.TestCase):
    def test_work_context_becomes_readme(self):
        self.assertEqual(readme_filename("Work Context"), "README.md")

    def test_other_name(self):
        self.assertEqual(readme_filename("Getting Started"), "Getting_Started.md")

    def test_spaces_become_underscores(self):
        self.assertEqual(readme_filename("Team Norms"), "Team_Norms.md")

    def test_work_context_with_surrounding_spaces(self):
        self.assertEqual(readme_filename("  Work Context  "), "README.md")


class TestRenderRichText(unittest.TestCase):
    def test_plain(self):
        parts = [{"plain_text": "hello", "annotations": {}}]
        self.assertEqual(_render_rich_text(parts), "hello")

    def test_bold(self):
        parts = [{"plain_text": "hi", "annotations": {"bold": True}}]
        self.assertEqual(_render_rich_text(parts), "**hi**")

    def test_italic(self):
        parts = [{"plain_text": "em", "annotations": {"italic": True}}]
        self.assertEqual(_render_rich_text(parts), "*em*")

    def test_code(self):
        parts = [{"plain_text": "x", "annotations": {"code": True}}]
        self.assertEqual(_render_rich_text(parts), "`x`")

    def test_link(self):
        parts = [{"plain_text": "click", "annotations": {}, "href": "https://example.com"}]
        self.assertEqual(_render_rich_text(parts), "[click](https://example.com)")

    def test_multiple(self):
        parts = [
            {"plain_text": "A", "annotations": {}},
            {"plain_text": "B", "annotations": {"bold": True}},
        ]
        self.assertEqual(_render_rich_text(parts), "A**B**")


class TestBlocksToMd(unittest.TestCase):
    def _block(self, btype, text, **kwargs):
        ann = {"bold": False, "italic": False, "code": False}
        return {
            "type": btype,
            btype: {
                "rich_text": [{"plain_text": text, "annotations": ann}],
                **kwargs,
            },
            "children": [],
        }

    def test_heading1(self):
        block = self._block("heading_1", "Title")
        self.assertEqual(_blocks_to_md([block]), ["# Title"])

    def test_heading2(self):
        block = self._block("heading_2", "Sub")
        self.assertEqual(_blocks_to_md([block]), ["## Sub"])

    def test_heading3(self):
        block = self._block("heading_3", "Leaf")
        self.assertEqual(_blocks_to_md([block]), ["### Leaf"])

    def test_paragraph(self):
        block = self._block("paragraph", "Hello world")
        self.assertEqual(_blocks_to_md([block]), ["Hello world"])

    def test_empty_paragraph(self):
        block = self._block("paragraph", "")
        self.assertEqual(_blocks_to_md([block]), [""])

    def test_bullet(self):
        block = self._block("bulleted_list_item", "Item")
        self.assertEqual(_blocks_to_md([block]), ["- Item"])

    def test_numbered(self):
        block = self._block("numbered_list_item", "Step")
        self.assertEqual(_blocks_to_md([block]), ["1. Step"])

    def test_todo_unchecked(self):
        block = self._block("to_do", "Task", checked=False)
        self.assertEqual(_blocks_to_md([block]), ["- [ ] Task"])

    def test_todo_checked(self):
        block = self._block("to_do", "Done", checked=True)
        self.assertEqual(_blocks_to_md([block]), ["- [x] Done"])

    def test_quote(self):
        block = self._block("quote", "Wisdom")
        self.assertEqual(_blocks_to_md([block]), ["> Wisdom"])

    def test_divider(self):
        block = {"type": "divider", "divider": {}, "children": []}
        self.assertEqual(_blocks_to_md([block]), ["---"])

    def test_callout_with_emoji(self):
        block = {
            "type": "callout",
            "callout": {
                "rich_text": [{"plain_text": "Note", "annotations": {}}],
                "icon": {"type": "emoji", "emoji": "💡"},
            },
            "children": [],
        }
        self.assertEqual(_blocks_to_md([block]), ["💡 Note"])

    def test_callout_no_emoji(self):
        block = {
            "type": "callout",
            "callout": {
                "rich_text": [{"plain_text": "Note", "annotations": {}}],
                "icon": {"type": "external", "external": {"url": "..."}},
            },
            "children": [],
        }
        self.assertEqual(_blocks_to_md([block]), ["Note"])

    def test_code_block(self):
        block = {
            "type": "code",
            "code": {
                "rich_text": [{"plain_text": "x = 1", "annotations": {}}],
                "language": "python",
            },
            "children": [],
        }
        self.assertEqual(_blocks_to_md([block]), ["```python", "x = 1", "```"])

    def test_nested_bullet(self):
        child = self._block("bulleted_list_item", "Child")
        parent = self._block("bulleted_list_item", "Parent")
        parent["children"] = [child]
        lines = _blocks_to_md([parent])
        self.assertEqual(lines, ["- Parent", "  - Child"])


class TestRunSync(unittest.TestCase):
    def _make_row(self, name, row_type, description="", page_id="page1"):
        return {
            "id": page_id,
            "properties": {
                "Name": {"title": [{"plain_text": name}]},
                "Type": {"select": {"name": row_type}},
                "Description": {"rich_text": [{"plain_text": description}]},
                "Active": {"checkbox": True},
            },
        }

    def _make_notion(self, rows, blocks=None):
        blocks = blocks or []
        notion = MagicMock()
        notion.databases.query.return_value = {"results": rows, "has_more": False}
        notion.blocks.children.list.return_value = {
            "results": blocks,
            "has_more": False,
        }
        return notion

    def test_readme_work_context_writes_readme_md(self):
        row = self._make_row("Work Context", "README")
        notion = self._make_notion([row])
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_sync(out=tmp, notion=notion)
            self.assertIn("README.md", summary["written"])
            self.assertTrue((Path(tmp) / "README.md").exists())

    def test_readme_other_name_writes_named_file(self):
        row = self._make_row("Team Norms", "README")
        notion = self._make_notion([row])
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_sync(out=tmp, notion=notion)
            self.assertIn("Team_Norms.md", summary["written"])

    def test_skill_writes_to_skills_subdir(self):
        row = self._make_row("Code Review", "Skill", description="Review code")
        notion = self._make_notion([row])
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_sync(out=tmp, notion=notion)
            self.assertIn("skills/code-review/SKILL.md", summary["written"])
            dest = Path(tmp) / "skills" / "code-review" / "SKILL.md"
            self.assertTrue(dest.exists())
            content = dest.read_text()
            self.assertIn("name: code-review", content)
            self.assertIn("description: Review code", content)

    def test_dry_run_writes_nothing(self):
        row = self._make_row("Work Context", "README")
        notion = self._make_notion([row])
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_sync(out=tmp, notion=notion, dry_run=True)
            self.assertIn("README.md", summary["written"])
            self.assertFalse((Path(tmp) / "README.md").exists())

    def test_unknown_type_skipped(self):
        row = self._make_row("Mystery", "Unknown")
        notion = self._make_notion([row])
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_sync(out=tmp, notion=notion)
            self.assertIn("Mystery", summary["skipped"])
            self.assertEqual(summary["written"], [])

    def test_blocks_rendered_into_file(self):
        row = self._make_row("Work Context", "README")
        block = {
            "type": "paragraph",
            "paragraph": {"rich_text": [{"plain_text": "Hello", "annotations": {}}]},
            "has_children": False,
        }
        notion = self._make_notion([row], blocks=[block])
        with tempfile.TemporaryDirectory() as tmp:
            run_sync(out=tmp, notion=notion)
            content = (Path(tmp) / "README.md").read_text()
            self.assertIn("Hello", content)


if __name__ == "__main__":
    unittest.main()
