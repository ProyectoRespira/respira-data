# respira-data Environment Variables

All respira-data runtime configuration is done with environment variables loaded from the repository root .env file.

## Where to set these variables

| Context | File to edit |
|---|---|
| Docker Compose local stack | .env in repository root |
| CI/CD job or remote runtime | Environment variables in the runner/platform |

See .env.example for a complete template.

## Core Database (Required)

These variables are required for dbt and runtime DB access unless you use DB_DSN.

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| REMOTE_PG_HOST | Yes | - | dbt/profiles.yml, pipelines/config/settings.py | PostgreSQL host. |
| REMOTE_PG_PORT | Yes | 5432 in .env.example | dbt/profiles.yml, pipelines/config/settings.py | PostgreSQL port. |
| REMOTE_PG_DB | Yes | - | dbt/profiles.yml, pipelines/config/settings.py | Database name. |
| REMOTE_PG_USER | Yes | - | dbt/profiles.yml, pipelines/config/settings.py | Database user. |
| REMOTE_PG_PASSWORD | Yes | - | dbt/profiles.yml, pipelines/config/settings.py | Database password. |
| REMOTE_PG_SSLMODE | No | prefer in code, require in .env.example | dbt/profiles.yml, pipelines/config/settings.py | Typical values: disable, prefer, require, verify-ca, verify-full. |
| REMOTE_PG_SCHEMA | No | airbyte in .env.example | project convention | Source schema convention. Keep if your raw source schema is airbyte. |
| DB_DSN | No | - | pipelines/config/settings.py | Optional SQLAlchemy DSN override for Python tasks. Does not replace REMOTE_PG_* for dbt. |
| DBT_POSTGRES_URI | No (legacy) | - | pipelines/config/settings.py | Backward-compatible legacy DSN input. |
| REMOTE_PG_NAME | No (legacy) | - | pipelines/config/settings.py | Legacy alias for REMOTE_PG_DB. Prefer REMOTE_PG_DB. |

## Prefect API and Worker

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| PREFECT_API_URL | Yes for worker/bootstrap | http://prefect_server:4200/api | scripts/prefect_worker_start.sh, scripts/wait_for_prefect.py | Base URL for Prefect API. |
| PREFECT_WORKER_TYPE | No | process | scripts/prefect_worker_start.sh | Worker type used when creating/starting pools. |
| PREFECT_SCHEDULE_TIMEZONE | No | UTC | scripts/prefect_worker_start.sh | Timezone for cron schedules. |
| PREFECT_CANONICAL_WORK_POOL | No | canonical | scripts/prefect_worker_start.sh | Work pool for canonical flows. |
| PREFECT_PROJECT_RESPIRA_GOLD_WORK_POOL | No | respira_gold | scripts/prefect_worker_start.sh | Work pool for project pipeline. |
| PREFECT_SOCIAL_BROADCAST_WORK_POOL | No | PREFECT_PROJECT_RESPIRA_GOLD_WORK_POOL | scripts/prefect_worker_start.sh | Work pool for social flow. |
| PREFECT_WORK_POOL | No (legacy fallback) | - | scripts/prefect_worker_start.sh | Legacy shared fallback for pool names. |
| PREFECT_API_WAIT_MAX_ATTEMPTS | No | 90 | scripts/prefect_worker_start.sh | Max retries while waiting for API health. |
| PREFECT_API_WAIT_SLEEP_SECONDS | No | 2 | scripts/prefect_worker_start.sh | Delay between health retries. |
| PREFECT_WAIT_TIMEOUT_S | No | 120 | scripts/wait_for_prefect.py | Timeout for helper script wait loop. |
| PREFECT_WAIT_INTERVAL_S | No | 2 | scripts/wait_for_prefect.py | Poll interval for helper script wait loop. |

## Prefect Server UI/API Container (Optional)

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| PREFECT_UI_URL | No | http://localhost:4200 | docker-compose.yml | UI URL for local server container. |
| PREFECT_UI_API_URL | No | http://localhost:4200/api | docker-compose.yml | UI API URL. |
| PREFECT_UI_SERVE_BASE | No | / | docker-compose.yml | UI base path. |
| PREFECT_SERVER_API_HOST | No | 0.0.0.0 | docker-compose.yml | API bind host. |
| PREFECT_SERVER_API_PORT | No | 4200 | docker-compose.yml | API bind port. |

## Deployment Schedules

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| PREFECT_CANONICAL_INCREMENTAL_CRON | No | 5 * * * * | scripts/prefect_worker_start.sh | Canonical incremental schedule. |
| PREFECT_PROJECT_RESPIRA_GOLD_CRON | No | 20 * * * * | scripts/prefect_worker_start.sh | Project pipeline schedule. |
| PREFECT_SOCIAL_BROADCAST_CRON | No | 0 11,20 * * * | scripts/prefect_worker_start.sh | Social broadcast schedule. |

## dbt Runtime Controls

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| DBT_PROJECT_DIR | No | ./dbt | pipelines/config/settings.py | dbt project location. |
| DBT_PROFILES_DIR | No | ./dbt | pipelines/config/settings.py | dbt profiles location. |
| DBT_TARGET | No | prod | pipelines/config/settings.py | dbt target name. |
| DBT_THREADS | No | 1 | pipelines/config/settings.py | dbt execution threads. |
| DBT_USE_PREFECT_DBT | No | false | pipelines/config/settings.py | Use Prefect dbt integration flag. |
| DBT_TIMEOUT_CANONICAL_CORE_S | No | 0 | pipelines/config/settings.py | Timeout seconds for canonical core run; 0 disables timeout. |
| DBT_TIMEOUT_CANONICAL_BATCH_INGEST_S | No | 3600 | pipelines/config/settings.py | Timeout for batched ingest run. |
| DBT_TIMEOUT_CANONICAL_SILVER_S | No | 1800 | pipelines/config/settings.py | Timeout for canonical silver run. |
| DBT_TIMEOUT_PROJECT_S | No | 1200 | pipelines/config/settings.py | Timeout for project run. |
| DBT_TIMEOUT_TESTS_S | No | 1200 | pipelines/config/settings.py | Timeout for dbt tests. |

## Inference and Backfill

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| MODEL_6H_PATH | Required for scheduled inference | - | pipelines/config/settings.py, scripts/prefect_worker_start.sh | Path to 6h model file in container filesystem. |
| MODEL_12H_PATH | Required for scheduled inference | - | pipelines/config/settings.py, scripts/prefect_worker_start.sh | Path to 12h model file in container filesystem. |
| MODEL_6H_VERSION | No | unknown | pipelines/config/settings.py | Metadata/version tracking for model artifact. |
| MODEL_12H_VERSION | No | unknown | pipelines/config/settings.py | Metadata/version tracking for model artifact. |
| DEFAULT_WINDOW_HOURS | No | 24 | pipelines/config/settings.py | Inference feature window size. |
| INFERENCE_MIN_POINTS | No | 18 | pipelines/config/settings.py | Minimum points required for inference. |
| MEASUREMENT_BACKFILL_PROCESS_BATCH_HOURS | No | 720 | pipelines/config/settings.py | Batch size (hours) for measurement backfill process step. |

## Alerts and Social Integrations

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| SLACK_WEBHOOK_URL | No | - | pipelines/config/settings.py | Optional Slack notifications. |
| SOCIAL_DRY_RUN | No | true | pipelines/config/settings.py | Simulate social publishing without sending. |
| SOCIAL_DATA_MAX_AGE_HOURS | No | 6 | pipelines/config/settings.py | Max age allowed for data used in social posts. |
| SOCIAL_MIN_STATIONS_PER_REGION | No | 1 | pipelines/config/settings.py | Minimum stations required per region for social output. |
| TELEGRAM_ENABLED | No | false | pipelines/config/settings.py | Enable Telegram publishing. |
| TELEGRAM_BOT_TOKEN | Required if TELEGRAM_ENABLED=true | - | pipelines/config/settings.py | Telegram bot token. |
| TELEGRAM_CHAT_ID | Required if TELEGRAM_ENABLED=true | - | pipelines/config/settings.py | Telegram target chat id. |
| TWITTER_ENABLED | No | false | pipelines/config/settings.py | Enable X/Twitter publishing. |
| TWITTER_BEARER_TOKEN | Required if TWITTER_ENABLED=true | - | pipelines/config/settings.py | X/Twitter bearer token. |
| TWITTER_API_KEY | Required if TWITTER_ENABLED=true | - | pipelines/config/settings.py | X/Twitter API key. |
| TWITTER_API_SECRET | Required if TWITTER_ENABLED=true | - | pipelines/config/settings.py | X/Twitter API secret. |
| TWITTER_ACCESS_TOKEN | Required if TWITTER_ENABLED=true | - | pipelines/config/settings.py | X/Twitter access token. |
| TWITTER_ACCESS_TOKEN_SECRET | Required if TWITTER_ENABLED=true | - | pipelines/config/settings.py | X/Twitter access token secret. |

## Runtime Metadata Variables (Usually Auto-Set)

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| PREFECT__FLOW_RUN_ID | No | local fallback | pipelines/compat.py | Provided by Prefect runtime. |
| PREFECT_FLOW_RUN_ID | No | local fallback | pipelines/compat.py | Compatibility fallback for flow run id. |
| PREFECT_DEPLOYMENT_NAME | No | - | pipelines/compat.py | Optional deployment name metadata. |

## Notes

- Keep secrets out of git. Use .env locally and secrets managers in CI/production.
- If your password includes $, quote it in .env (example: REMOTE_PG_PASSWORD='pa$$word').
- For local Docker Compose, .env is loaded automatically by the compose CLI and also mounted via env_file.
