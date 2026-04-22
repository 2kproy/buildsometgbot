# buildsometgbot (bstb)

Hybrid Telegram bot constructor:
- **JSON source of truth** for node content (`nodes.json`)
- **PostgreSQL + Redis** for runtime state and operations
- `polling` and `webhook` modes
- in-bot admin constructor + broadcast engine

## Architecture

- JSON content:
  - `bot/data/nodes.json` (windows/text/buttons/settings/media)
- Postgres:
  - user/admin states
  - user profiles (`last_seen`)
  - broadcasts, jobs, events
- Redis:
  - hot cache for user/admin runtime state
  - dedup/rate runtime keys

## Environment

Copy `.env.example` to `.env` and set values:

- `BOT_TOKEN`, `ADMIN_IDS`
- `BOT_MODE=polling|webhook`
- `POSTGRES_DSN`, `REDIS_URL`
- webhook vars (`WEBHOOK_BASE_URL`, `WEBHOOK_PATH`, `WEBHOOK_SECRET_TOKEN`, etc.)
- broadcast tuning (`BROADCAST_BATCH_SIZE`, `BROADCAST_RPS`, `BROADCAST_RETRY_LIMIT`)

## Run (Docker Compose)

```bash
docker compose up --build
```

Services:
- `bot`
- `postgres`
- `redis`
- `nginx`

## Run (local)

```bash
python -m pip install -r requirements.txt
python -m bot.main
```

## Broadcast commands

- `/broadcast_new [name]`
- `/broadcast_list`
- `/broadcast_status <id>`
- `/broadcast_send <id>`
- `/broadcast_schedule <id> <iso_datetime>`
- `/broadcast_cancel <id>`

`/broadcast_new` builds payload from current opened node (`/open ...`).

## Webhook mode

- Set `BOT_MODE=webhook`
- Configure `WEBHOOK_BASE_URL` and related vars
- Nginx proxies to bot webhook endpoint

## Notes

- Single-writer model for JSON edits (`nodes.json`) is assumed.
- Rotate bot token before publishing repo.
