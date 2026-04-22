from __future__ import annotations

import asyncio
import logging

from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from bot.app import AppContext, build_bot, build_dispatcher
from bot.config import load_config
from bot.handlers import admin, admin_inline, user
from bot.middlewares.tracking import UserTrackingMiddleware
from bot.runtime import set_app
from bot.services.broadcast_service import BroadcastService
from bot.services.hybrid_storage import HybridStorage
from bot.services.runtime_storage import RuntimeStorage
from bot.services.storage import JsonStorage


async def main() -> None:
    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    node_storage = JsonStorage(config.data_dir)
    runtime_storage = RuntimeStorage(config.postgres_dsn, config.redis_url)
    storage = HybridStorage(node_storage, runtime_storage)
    await storage.ensure_initialized()
    bot = build_bot(config.bot_token)
    broadcast_service = BroadcastService(storage, bot, config)
    await broadcast_service.start()
    app = AppContext(config=config, storage=storage, broadcast_service=broadcast_service)
    set_app(app)

    dp = build_dispatcher()
    dp.message.middleware(UserTrackingMiddleware())
    dp.callback_query.middleware(UserTrackingMiddleware())
    dp.include_router(admin.router)
    dp.include_router(admin_inline.router)
    dp.include_router(user.router)

    runner: web.AppRunner | None = None
    try:
        if config.bot_mode == "polling":
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot)
        else:
            webhook_url = f"{config.webhook_base_url.rstrip('/')}{config.webhook_path}"
            await bot.set_webhook(webhook_url, secret_token=config.webhook_secret_token, drop_pending_updates=True)

            web_app = web.Application()
            web_app.router.add_get("/healthz", lambda _: web.Response(text="ok"))
            handler = SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=config.webhook_secret_token)
            handler.register(web_app, path=config.webhook_path)
            setup_application(web_app, dp, bot=bot)
            runner = web.AppRunner(web_app)
            await runner.setup()
            site = web.TCPSite(runner, host=config.webhook_listen_host, port=config.webhook_listen_port)
            await site.start()
            await asyncio.Event().wait()
    finally:
        if config.bot_mode == "webhook":
            try:
                await bot.delete_webhook(drop_pending_updates=False)
            except Exception:
                pass
        if runner is not None:
            await runner.cleanup()
        await broadcast_service.stop()
        await storage.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
