from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(slots=True)
class Config:
    bot_token: str
    admin_ids: set[int]
    data_dir: Path
    root_node_id: str
    max_history: int
    bot_mode: str
    postgres_dsn: str
    redis_url: str
    webhook_base_url: str
    webhook_path: str
    webhook_secret_token: str
    webhook_listen_host: str
    webhook_listen_port: int
    broadcast_batch_size: int
    broadcast_rps: float
    broadcast_retry_limit: int
    tz: str
    log_level: str


def load_config() -> Config:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    admin_raw = os.getenv("ADMIN_IDS", "")
    admin_ids = {int(part.strip()) for part in admin_raw.split(",") if part.strip()}
    data_dir = Path(os.getenv("DATA_DIR", "bot/data"))
    root_node_id = os.getenv("ROOT_NODE_ID", "start").strip() or "start"
    max_history = int(os.getenv("MAX_HISTORY", "20"))
    bot_mode = os.getenv("BOT_MODE", "polling").strip().lower()
    postgres_dsn = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@postgres:5432/botdb").strip()
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0").strip()
    webhook_base_url = os.getenv("WEBHOOK_BASE_URL", "").strip()
    webhook_path = os.getenv("WEBHOOK_PATH", "/webhook").strip() or "/webhook"
    webhook_secret_token = os.getenv("WEBHOOK_SECRET_TOKEN", "change-me").strip()
    webhook_listen_host = os.getenv("WEBHOOK_LISTEN_HOST", "0.0.0.0").strip()
    webhook_listen_port = int(os.getenv("WEBHOOK_LISTEN_PORT", "8080"))
    broadcast_batch_size = int(os.getenv("BROADCAST_BATCH_SIZE", "100"))
    broadcast_rps = float(os.getenv("BROADCAST_RPS", "20"))
    broadcast_retry_limit = int(os.getenv("BROADCAST_RETRY_LIMIT", "3"))
    tz = os.getenv("TZ", "Europe/Moscow").strip()
    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required")
    if bot_mode not in {"polling", "webhook"}:
        raise RuntimeError("BOT_MODE must be polling or webhook")
    if bot_mode == "webhook" and not webhook_base_url:
        raise RuntimeError("WEBHOOK_BASE_URL is required for webhook mode")
    return Config(
        bot_token=bot_token,
        admin_ids=admin_ids,
        data_dir=data_dir,
        root_node_id=root_node_id,
        max_history=max_history,
        bot_mode=bot_mode,
        postgres_dsn=postgres_dsn,
        redis_url=redis_url,
        webhook_base_url=webhook_base_url,
        webhook_path=webhook_path,
        webhook_secret_token=webhook_secret_token,
        webhook_listen_host=webhook_listen_host,
        webhook_listen_port=webhook_listen_port,
        broadcast_batch_size=broadcast_batch_size,
        broadcast_rps=broadcast_rps,
        broadcast_retry_limit=broadcast_retry_limit,
        tz=tz,
        log_level=log_level,
    )
