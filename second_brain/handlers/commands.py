from __future__ import annotations

from datetime import datetime


class CommandHandlers:
    def __init__(self, deps: dict):
        self.d = deps

    async def handle_done_command(self, update, context):
        if update.effective_chat.id != self.d['MY_CHAT_ID']:
            return
        pending_habits = [
            h for h in sorted(self.d['habit_cache'].values(), key=lambda x: x['sort'])
            if not self.d['already_logged_today'](h['page_id'])
        ]
        tasks = self.d['notion_tasks'].get_today_and_overdue_tasks(self.d['notion'], self.d['NOTION_DB_ID'])
        if not pending_habits and not tasks:
            await update.message.reply_text('✅ Everything done for today — nothing left to log!')
            return
        if pending_habits:
            await update.message.reply_text('🏃 *Which habit did you complete?*', parse_mode='Markdown', reply_markup=self.d['kb'].habit_buttons(pending_habits, 'manual'))
        if tasks:
            key = str(self.d['next_done_picker_key']())
            self.d['done_picker_map'][key] = tasks
            await update.message.reply_text('✅ *Which task did you finish?*', parse_mode='Markdown', reply_markup=self.d['done_picker_keyboard'](key, page=0))

    async def handle_remind_command(self, update, context):
        if update.effective_chat.id != self.d['MY_CHAT_ID']:
            return
        await self.d['send_quick_reminder'](update.message, mode='priority')
