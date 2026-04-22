from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import asyncpg
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RuntimeStorage:
    def __init__(self, postgres_dsn: str, redis_url: str) -> None:
        self.postgres_dsn = postgres_dsn
        self.redis_url = redis_url
        self.pool: asyncpg.Pool | None = None
        self.redis: Redis | None = None
        self.user_state_ttl = 3600
        self.admin_state_ttl = 3600

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(dsn=self.postgres_dsn, min_size=1, max_size=10)
        self.redis = Redis.from_url(self.redis_url, encoding="utf-8", decode_responses=True)
        await self.ensure_schema()

    async def close(self) -> None:
        if self.redis is not None:
            await self.redis.close()
        if self.pool is not None:
            await self.pool.close()

    async def ensure_schema(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_profiles (
                    telegram_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_bot BOOLEAN NOT NULL DEFAULT FALSE,
                    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS user_state (
                    telegram_id BIGINT PRIMARY KEY,
                    current_node TEXT NOT NULL,
                    history JSONB NOT NULL DEFAULT '[]'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS admin_state (
                    telegram_id BIGINT PRIMARY KEY,
                    current_edit_node TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'idle',
                    extra JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS broadcasts (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    scheduled_at TIMESTAMPTZ NULL,
                    created_by BIGINT NOT NULL,
                    report JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    started_at TIMESTAMPTZ NULL,
                    finished_at TIMESTAMPTZ NULL
                );

                CREATE TABLE IF NOT EXISTS broadcast_jobs (
                    id BIGSERIAL PRIMARY KEY,
                    broadcast_id BIGINT NOT NULL REFERENCES broadcasts(id) ON DELETE CASCADE,
                    run_at TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    started_at TIMESTAMPTZ NULL,
                    finished_at TIMESTAMPTZ NULL,
                    error TEXT NULL
                );

                CREATE TABLE IF NOT EXISTS broadcast_events (
                    id BIGSERIAL PRIMARY KEY,
                    broadcast_id BIGINT NOT NULL REFERENCES broadcasts(id) ON DELETE CASCADE,
                    telegram_id BIGINT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_user_profiles_last_seen ON user_profiles(last_seen DESC);
                CREATE INDEX IF NOT EXISTS idx_broadcast_status ON broadcasts(status);
                CREATE INDEX IF NOT EXISTS idx_broadcast_jobs_pending ON broadcast_jobs(status, run_at);
                CREATE INDEX IF NOT EXISTS idx_broadcast_events_broadcast ON broadcast_events(broadcast_id);
                """
            )

    async def track_user(self, user: Any) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_profiles (telegram_id, username, first_name, last_name, is_bot, last_seen, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NOW())
                ON CONFLICT (telegram_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    is_bot = EXCLUDED.is_bot,
                    last_seen = NOW(),
                    updated_at = NOW()
                """,
                int(user.id),
                getattr(user, "username", None),
                getattr(user, "first_name", None),
                getattr(user, "last_name", None),
                bool(getattr(user, "is_bot", False)),
            )

    async def load_user_state(self, user_id: int) -> dict[str, Any]:
        assert self.pool is not None and self.redis is not None
        cache_key = f"user_state:{user_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            return json.loads(cached)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT current_node, history FROM user_state WHERE telegram_id=$1", int(user_id))
        if not row:
            state = {"current_node": "start", "history": []}
        else:
            state = {"current_node": row["current_node"], "history": row["history"] or []}
        await self.redis.setex(cache_key, self.user_state_ttl, json.dumps(state, ensure_ascii=False))
        return state

    async def save_user_state(self, user_id: int, state: dict[str, Any]) -> None:
        assert self.pool is not None and self.redis is not None
        history = state.get("history", [])
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO user_state (telegram_id, current_node, history, updated_at)
                VALUES ($1, $2, $3::jsonb, NOW())
                ON CONFLICT (telegram_id) DO UPDATE SET
                    current_node = EXCLUDED.current_node,
                    history = EXCLUDED.history,
                    updated_at = NOW()
                """,
                int(user_id),
                str(state.get("current_node", "start")),
                json.dumps(history, ensure_ascii=False),
            )
        await self.redis.setex(f"user_state:{user_id}", self.user_state_ttl, json.dumps(state, ensure_ascii=False))

    async def load_admin_state(self, user_id: int) -> dict[str, Any]:
        assert self.pool is not None and self.redis is not None
        cache_key = f"admin_state:{user_id}"
        cached = await self.redis.get(cache_key)
        if cached:
            return json.loads(cached)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT current_edit_node, mode, extra FROM admin_state WHERE telegram_id=$1", int(user_id))
        if not row:
            state = {"current_edit_node": "start", "mode": "idle"}
        else:
            state = {"current_edit_node": row["current_edit_node"], "mode": row["mode"], **(row["extra"] or {})}
        await self.redis.setex(cache_key, self.admin_state_ttl, json.dumps(state, ensure_ascii=False))
        return state

    async def save_admin_state(self, user_id: int, state: dict[str, Any]) -> None:
        assert self.pool is not None and self.redis is not None
        extra = dict(state)
        current_edit_node = str(extra.pop("current_edit_node", "start"))
        mode = str(extra.pop("mode", "idle"))
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO admin_state (telegram_id, current_edit_node, mode, extra, updated_at)
                VALUES ($1, $2, $3, $4::jsonb, NOW())
                ON CONFLICT (telegram_id) DO UPDATE SET
                    current_edit_node = EXCLUDED.current_edit_node,
                    mode = EXCLUDED.mode,
                    extra = EXCLUDED.extra,
                    updated_at = NOW()
                """,
                int(user_id),
                current_edit_node,
                mode,
                json.dumps(extra, ensure_ascii=False),
            )
        cache_payload = {"current_edit_node": current_edit_node, "mode": mode, **extra}
        await self.redis.setex(f"admin_state:{user_id}", self.admin_state_ttl, json.dumps(cache_payload, ensure_ascii=False))

    async def load_user_state_all(self) -> dict[str, Any]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT telegram_id, current_node, history FROM user_state")
        return {str(r["telegram_id"]): {"current_node": r["current_node"], "history": r["history"] or []} for r in rows}

    async def save_user_state_all(self, payload: dict[str, Any]) -> None:
        assert self.pool is not None and self.redis is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM user_state")
                for key, value in payload.items():
                    await conn.execute(
                        """
                        INSERT INTO user_state (telegram_id, current_node, history, updated_at)
                        VALUES ($1, $2, $3::jsonb, NOW())
                        """,
                        int(key),
                        str(value.get("current_node", "start")),
                        json.dumps(value.get("history", []), ensure_ascii=False),
                    )
        keys = [f"user_state:{k}" for k in payload.keys()]
        if keys:
            await self.redis.delete(*keys)

    async def load_admin_state_all(self) -> dict[str, Any]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT telegram_id, current_edit_node, mode, extra FROM admin_state")
        result = {}
        for row in rows:
            result[str(row["telegram_id"])] = {"current_edit_node": row["current_edit_node"], "mode": row["mode"], **(row["extra"] or {})}
        return result

    async def save_admin_state_all(self, payload: dict[str, Any]) -> None:
        assert self.pool is not None and self.redis is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM admin_state")
                for key, value in payload.items():
                    extra = dict(value)
                    current_edit_node = str(extra.pop("current_edit_node", "start"))
                    mode = str(extra.pop("mode", "idle"))
                    await conn.execute(
                        """
                        INSERT INTO admin_state (telegram_id, current_edit_node, mode, extra, updated_at)
                        VALUES ($1, $2, $3, $4::jsonb, NOW())
                        """,
                        int(key),
                        current_edit_node,
                        mode,
                        json.dumps(extra, ensure_ascii=False),
                    )
        keys = [f"admin_state:{k}" for k in payload.keys()]
        if keys:
            await self.redis.delete(*keys)

    async def create_broadcast(self, name: str, payload: dict[str, Any], created_by: int) -> int:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO broadcasts (name, payload, status, created_by, created_at, updated_at)
                VALUES ($1, $2::jsonb, 'draft', $3, NOW(), NOW())
                RETURNING id
                """,
                name,
                json.dumps(payload, ensure_ascii=False),
                int(created_by),
            )
        return int(row["id"])

    async def list_broadcasts(self, limit: int = 20) -> list[dict[str, Any]]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, name, status, scheduled_at, created_at, updated_at FROM broadcasts ORDER BY id DESC LIMIT $1",
                int(limit),
            )
        return [dict(r) for r in rows]

    async def get_broadcast(self, broadcast_id: int) -> dict[str, Any] | None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, name, payload, status, scheduled_at, report, started_at, finished_at FROM broadcasts WHERE id=$1",
                int(broadcast_id),
            )
        return dict(row) if row else None

    async def schedule_broadcast(self, broadcast_id: int, when: datetime) -> None:
        assert self.pool is not None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE broadcasts
                    SET status='scheduled', scheduled_at=$2, updated_at=NOW()
                    WHERE id=$1
                    """,
                    int(broadcast_id),
                    when,
                )
                await conn.execute(
                    """
                    INSERT INTO broadcast_jobs (broadcast_id, run_at, status)
                    VALUES ($1, $2, 'pending')
                    """,
                    int(broadcast_id),
                    when,
                )

    async def cancel_broadcast(self, broadcast_id: int) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("UPDATE broadcasts SET status='canceled', updated_at=NOW() WHERE id=$1", int(broadcast_id))
                await conn.execute("UPDATE broadcast_jobs SET status='canceled', finished_at=NOW() WHERE broadcast_id=$1 AND status='pending'", int(broadcast_id))

    async def claim_due_broadcasts(self, limit: int = 10) -> list[dict[str, Any]]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                WITH due AS (
                    SELECT id, broadcast_id
                    FROM broadcast_jobs
                    WHERE status='pending' AND run_at <= NOW()
                    ORDER BY run_at ASC
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE broadcast_jobs j
                SET status='running', started_at=NOW()
                FROM due
                WHERE j.id = due.id
                RETURNING j.id, j.broadcast_id
                """,
                int(limit),
            )
        return [dict(r) for r in rows]

    async def mark_broadcast_running(self, broadcast_id: int) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE broadcasts SET status='running', started_at=COALESCE(started_at, NOW()), updated_at=NOW() WHERE id=$1",
                int(broadcast_id),
            )

    async def mark_broadcast_done(self, broadcast_id: int, job_id: int, status: str, report: dict[str, Any]) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE broadcasts
                    SET status=$2, report=$3::jsonb, finished_at=NOW(), updated_at=NOW()
                    WHERE id=$1
                    """,
                    int(broadcast_id),
                    status,
                    json.dumps(report, ensure_ascii=False),
                )
                await conn.execute(
                    """
                    UPDATE broadcast_jobs
                    SET status=$2, finished_at=NOW()
                    WHERE id=$1
                    """,
                    int(job_id),
                    "done" if status == "completed" else "failed",
                )

    async def add_broadcast_event(self, broadcast_id: int, telegram_id: int, status: str, error: str | None = None) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO broadcast_events (broadcast_id, telegram_id, status, error, created_at)
                VALUES ($1, $2, $3, $4, NOW())
                """,
                int(broadcast_id),
                int(telegram_id),
                status,
                error,
            )

    async def list_recipients(self) -> list[int]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT telegram_id FROM user_profiles ORDER BY last_seen DESC")
        return [int(r["telegram_id"]) for r in rows]

    async def dedup_broadcast_recipient(self, broadcast_id: int, telegram_id: int, ttl_sec: int = 86400 * 7) -> bool:
        assert self.redis is not None
        key = f"bc:{broadcast_id}:{telegram_id}"
        return bool(await self.redis.set(key, "1", ex=ttl_sec, nx=True))

