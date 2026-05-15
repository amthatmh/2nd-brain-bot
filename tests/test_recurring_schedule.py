import asyncio
import importlib
import os
import sys
from datetime import date
from unittest.mock import MagicMock, patch

REQUIRED_ENV = {
    "TELEGRAM_TOKEN": "x",
    "TELEGRAM_CHAT_ID": "1",
    "ANTHROPIC_API_KEY": "x",
    "NOTION_TOKEN": "x",
    "NOTION_DB_ID": "x",
    "NOTION_HABIT_DB": "x",
    "NOTION_LOG_DB": "x",
    "NOTION_STREAK_DB": "x",
    "NOTION_CINEMA_LOG_DB": "x",
    "NOTION_NOTES_DB": "x",
    "NOTION_DIGEST_SELECTOR_DB": "x",
}


def load_main_module():
    sys.modules.pop("second_brain.main", None)
    with patch.dict(os.environ, REQUIRED_ENV, clear=False), patch(
        "notion_client.Client", return_value=MagicMock()
    ), patch("anthropic.Anthropic", return_value=MagicMock()):
        return importlib.import_module("second_brain.main")


def test_monthly_supports_any_ordinal_day():
    main = load_main_module()
    template = {
        "recurring": "🗓️ Monthly",
        "repeat_day": "4th",
        "last_generated": None,
    }

    assert main.notion_tasks.should_spawn_today(template, date(2026, 4, 4))
    assert not main.notion_tasks.should_spawn_today(template, date(2026, 4, 5))


def test_monthly_31st_rolls_to_month_end_for_short_months():
    main = load_main_module()
    template = {
        "recurring": "🗓️ Monthly",
        "repeat_day": "31st",
        "last_generated": None,
    }

    assert main.notion_tasks.should_spawn_today(template, date(2026, 4, 30))
    assert not main.notion_tasks.should_spawn_today(template, date(2026, 4, 29))


def test_quarterly_uses_anchor_month_and_repeat_day():
    main = load_main_module()
    template = {
        "recurring": "📆 Quarterly",
        "repeat_day": "4th",
        "last_generated": "2026-01-04",
        "deadline": "2026-01-04",
    }

    assert main.notion_tasks.should_spawn_today(template, date(2026, 4, 4))
    assert not main.notion_tasks.should_spawn_today(template, date(2026, 5, 4))


def test_quarterly_last_day_rolls_to_month_end():
    main = load_main_module()
    template = {
        "recurring": "📆 Quarterly",
        "repeat_day": "31st",
        "last_generated": "2026-01-31",
        "deadline": "2026-01-31",
    }

    assert main.notion_tasks.should_spawn_today(template, date(2026, 4, 30))
    assert not main.notion_tasks.should_spawn_today(template, date(2026, 4, 29))


def test_next_repeat_day_date_monthly_uses_future_ordinal_day():
    main = load_main_module()
    target = main.next_repeat_day_date("🗓️ Monthly", "4th", date(2026, 4, 1))
    assert target == date(2026, 4, 4)


def test_next_repeat_day_date_monthly_rolls_to_next_month_after_day_passes():
    main = load_main_module()
    target = main.next_repeat_day_date("🗓️ Monthly", "4th", date(2026, 4, 5))
    assert target == date(2026, 5, 4)


def test_spawn_recurring_instance_uses_source_parameter():
    main = load_main_module()
    notion = MagicMock()
    notion.pages.create.return_value = {"id": "instance-1"}
    template = {
        "page_id": "template-1",
        "name": "Water plants",
        "context": "🏠 Personal",
        "recurring": "📅 Weekly",
        "repeat_day": None,
        "deadline": None,
        "recurrence_pattern": None,
    }

    page_id = main.notion_tasks.spawn_recurring_instance(
        notion,
        "todo-db",
        template,
        next_deadline=date(2026, 5, 16),
        source="📱 Telegram",
    )

    assert page_id == "instance-1"
    created_props = notion.pages.create.call_args.kwargs["properties"]
    assert created_props["Source"] == {"select": {"name": "📱 Telegram"}}


def test_telegram_recurring_template_first_instance_passes_telegram_source():
    main = load_main_module()
    main.notion.pages.create.return_value = {"id": "template-1"}

    with patch.object(
        main.notion_tasks, "spawn_recurring_instance", return_value="instance-1"
    ) as spawn:
        main._create_recurring_task_template_and_first_instance(
            "Water plants",
            "🏠 Personal",
            "📅 Weekly",
            None,
        )

    assert spawn.call_args.kwargs["source"] == "📱 Telegram"


def test_generate_next_recurring_instances_preserves_template_source():
    async def run():
        main = load_main_module()
        main.notion.databases.query.return_value = {
            "results": [
                {
                    "id": "completed-1",
                    "properties": {
                        "Recurring Parent ID": {
                            "rich_text": [{"text": {"content": "template-1"}}]
                        },
                        "Deadline": {"date": {"start": "2026-05-15"}},
                    },
                }
            ]
        }
        main.notion.pages.retrieve.return_value = {
            "properties": {
                "Name": {"title": [{"text": {"content": "Water plants"}}]},
                "Context": {"select": {"name": "🏠 Personal"}},
                "Recurring": {"select": {"name": "📅 Weekly"}},
                "Source": {"select": {"name": "📱 Telegram"}},
                "Repeat Day": {"select": None},
                "Deadline": {"date": None},
                "Recurrence Pattern": {"rich_text": []},
            }
        }

        with patch.object(
            main.notion_tasks, "spawn_recurring_instance", return_value="instance-1"
        ) as spawn:
            await main.generate_next_recurring_instances(bot=None)

        assert spawn.call_args.kwargs["source"] == "📱 Telegram"

    asyncio.run(run())
