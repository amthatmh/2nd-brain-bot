import asyncio
from types import SimpleNamespace

from second_brain.handlers.commands import CommandHandlers


class Msg:
    def __init__(self):
        self.calls = []

    async def reply_text(self, text, **kwargs):
        self.calls.append((text, kwargs))


def test_handle_done_command_no_items_replies_complete():
    async def run():
        msg = Msg()
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=1), message=msg)
        deps = {
            'MY_CHAT_ID': 1,
            'habit_cache': {},
            'already_logged_today': lambda _pid: False,
            'notion_tasks': SimpleNamespace(get_today_and_overdue_tasks=lambda notion, db: []),
            'notion': object(),
            'NOTION_DB_ID': 'db',
            'kb': SimpleNamespace(habit_buttons=lambda a,b: None),
            'done_picker_map': {},
            'done_picker_keyboard': lambda key, page=0: None,
            'next_done_picker_key': lambda: 0,
            'send_quick_reminder': None,
        }
        h = CommandHandlers(deps)
        await h.handle_done_command(update, None)
        assert 'Everything done' in msg.calls[0][0]
    asyncio.run(run())
