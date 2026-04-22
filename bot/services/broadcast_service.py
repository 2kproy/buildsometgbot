from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from aiogram.types import FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup

from bot.config import Config
from bot.services.hybrid_storage import HybridStorage

logger = logging.getLogger(__name__)


class BroadcastService:
    def __init__(self, storage: HybridStorage, bot: Bot, config: Config) -> None:
        self.storage = storage
        self.bot = bot
        self.config = config
        self.scheduler = AsyncIOScheduler(timezone=config.tz)
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self.scheduler.add_job(self.run_due_jobs, "interval", seconds=10, id="broadcast_due_jobs", replace_existing=True)
        self.scheduler.start()
        self._running = True

    async def stop(self) -> None:
        if self._running:
            self.scheduler.shutdown(wait=False)
            self._running = False

    async def run_due_jobs(self) -> None:
        try:
            due = await self.storage.claim_due_broadcasts(limit=self.config.broadcast_batch_size)
            for job in due:
                await self.run_job(int(job["id"]), int(job["broadcast_id"]))
        except Exception:
            logger.exception("Failed to process due broadcasts")

    async def run_job(self, job_id: int, broadcast_id: int) -> None:
        bc = await self.storage.get_broadcast(broadcast_id)
        if not bc:
            await self.storage.mark_broadcast_done(broadcast_id, job_id, "failed", {"error": "broadcast_not_found"})
            return
        await self.storage.mark_broadcast_running(broadcast_id)
        payload = bc.get("payload") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        recipients = await self.storage.list_recipients()
        delivered = 0
        failed = 0
        skipped = 0

        for uid in recipients:
            is_new = await self.storage.dedup_broadcast_recipient(broadcast_id, uid)
            if not is_new:
                skipped += 1
                continue
            try:
                await self._send_payload(uid, payload)
                delivered += 1
                await self.storage.add_broadcast_event(broadcast_id, uid, "delivered")
            except Exception as e:
                failed += 1
                await self.storage.add_broadcast_event(broadcast_id, uid, "failed", str(e))
            await asyncio.sleep(max(0.0, 1.0 / max(1.0, self.config.broadcast_rps)))

        report = {
            "targeted": len(recipients),
            "delivered": delivered,
            "failed": failed,
            "skipped": skipped,
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
        final_status = "completed" if failed == 0 else "failed"
        await self.storage.mark_broadcast_done(broadcast_id, job_id, final_status, report)

    async def _send_payload(self, user_id: int, payload: dict[str, Any]) -> None:
        text = str(payload.get("text", "") or "")
        media = payload.get("media")
        buttons = payload.get("buttons") or []
        keyboard = self._build_keyboard(buttons)

        if isinstance(media, dict) and media.get("type") and media.get("file_id"):
            m_type = media["type"]
            fid = media["file_id"]
            if m_type == "photo":
                await self.bot.send_photo(user_id, fid, caption=text, reply_markup=keyboard)
                return
            if m_type == "video":
                await self.bot.send_video(user_id, fid, caption=text, reply_markup=keyboard)
                return
            if m_type == "document":
                await self.bot.send_document(user_id, fid, caption=text, reply_markup=keyboard)
                return
            if m_type == "animation":
                await self.bot.send_animation(user_id, fid, caption=text, reply_markup=keyboard)
                return
            if m_type == "audio":
                await self.bot.send_audio(user_id, fid, caption=text, reply_markup=keyboard)
                return
            if m_type == "voice":
                await self.bot.send_voice(user_id, fid)
                await self.bot.send_message(user_id, text, reply_markup=keyboard)
                return
        await self.bot.send_message(user_id, text, reply_markup=keyboard)

    @staticmethod
    def _build_keyboard(buttons: list[dict[str, Any]]) -> InlineKeyboardMarkup | None:
        if not isinstance(buttons, list) or not buttons:
            return None
        rows: list[list[InlineKeyboardButton]] = []
        by_row: dict[int, list[dict[str, Any]]] = {}
        for b in buttons:
            if not isinstance(b, dict):
                continue
            row = int(b.get("row", 0))
            by_row.setdefault(row, []).append(b)
        for row in sorted(by_row.keys()):
            row_items = []
            for b in sorted(by_row[row], key=lambda i: int(i.get("sort", 0))):
                if b.get("type") == "url":
                    row_items.append(InlineKeyboardButton(text=str(b.get("text", "")), url=str(b.get("target", ""))))
            if row_items:
                rows.append(row_items)
        return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None
