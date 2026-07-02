# Backend Environment Variables

All backend configuration is done at runtime. A single image runs across dev, staging, and production by changing only environment variables.

## Where to set these variables

| Context | File to edit |
|---|---|
| **docker-compose** (full stack) | `.env` in the repository root — all backend vars are declared there and passed to the backend container via `environment:` |
| **Backend standalone** (without compose) | `backend/.env` — read directly by Django via `python-dotenv` |

See [`.env.example`](../.env.example) for the docker-compose reference and [`backend/.env.example`](../backend/.env.example) for the standalone backend reference.

---

## Core Application

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| `BACKEND_SECRET_KEY` | Yes in production | `respira-backend-dev-secret-key` | `backend/backend/settings.py` | Django secret key. Generate a strong random value for production. |
| `BACKEND_DEBUG` | No | `false` | `backend/backend/settings.py` | Django debug mode. Must be `false` in production. |
| `BACKEND_PORT` | No | `8000` | `backend/entrypoint.sh`, `docker-compose.yml` | Port gunicorn binds to inside the container. |
| `BACKEND_RUN_MIGRATIONS` | No | `true` | `backend/entrypoint.sh` | Set to `false` to skip automatic migrations on container start. |
| `BACKEND_CORS_ALLOWED_ORIGINS` | No | `""` (empty) | `backend/backend/settings.py` | Comma-separated list of origins allowed to make cross-site requests. In docker-compose, set to `http://frontend:4321` if frontend SSR makes direct backend calls. For local dev: `http://localhost:8000,http://127.0.0.1:8000`. The variable name `BACKEND_CORS_ALLOWED_ORIGINS` is required — `CORS_ALLOWED_ORIGINS` alone will be ignored. |

---

## Database — PostgreSQL

All five core vars must be set together to enable PostgreSQL. If any is missing the backend falls back to SQLite (development only).

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| `BACKEND_POSTGRES_DB` | Yes (PostgreSQL) | — | `backend/backend/settings.py` | Database name. |
| `BACKEND_POSTGRES_USER` | Yes (PostgreSQL) | — | `backend/backend/settings.py` | Database user. |
| `BACKEND_POSTGRES_PASSWORD` | Yes (PostgreSQL) | — | `backend/backend/settings.py` | Database password. |
| `BACKEND_POSTGRES_HOST` | Yes (PostgreSQL) | — | `backend/backend/settings.py` | Database host. |
| `BACKEND_POSTGRES_PORT` | Yes (PostgreSQL) | — | `backend/backend/settings.py` | Database port. |
| `BACKEND_POSTGRES_SCHEMA` | No | `respira_gold` | `backend/backend/settings.py` | Comma-separated schemas for `search_path`. `public` is always appended. |

---

## Database — SSL (optional)

All four are optional. Set only the ones your database provider requires.

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| `BACKEND_POSTGRES_SSLMODE` | No | — | `backend/backend/settings.py` | Added to PostgreSQL `OPTIONS` when set. |
| `BACKEND_POSTGRES_SSLROOTCERT` | No | — | `backend/backend/settings.py` | Path to CA certificate file. |
| `BACKEND_POSTGRES_SSLCERT` | No | — | `backend/backend/settings.py` | Path to client certificate file. |
| `BACKEND_POSTGRES_SSLKEY` | No | — | `backend/backend/settings.py` | Path to client key file. |

---

## Migrations & Process (optional)

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| `BACKEND_APP_USER` | No | `appuser` | `backend/entrypoint.sh` | OS user gunicorn drops privileges to. Only used when container starts as root. |
| `BACKEND_APP_GROUP` | No | `appgroup` | `backend/entrypoint.sh` | OS group gunicorn drops privileges to. Only used when container starts as root. |

---

## Model Column Overrides (optional)

Override these only if your database schema uses different column names than the defaults.

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| `BACKEND_STATION_READINGS_DATE_COLUMN` | No | `date_localtime` | `backend/api/models.py` | Column mapped to `StationReadingsGold.date_utc`. |
| `BACKEND_INFERENCE_RESULTS_6H_COLUMN` | No | `forecast_6h` | `backend/api/models.py` | Column mapped to the 6-hour forecast JSON field. |
| `BACKEND_INFERENCE_RESULTS_12H_COLUMN` | No | `forecast_12h` | `backend/api/models.py` | Column mapped to the 12-hour forecast JSON field. |

---

## Build-Time Variables

| Variable | Required | Default | Where used | Notes |
|---|---|---|---|---|
| `PYTHON_VERSION` | No | `3.10-slim-bookworm` | `backend/Dockerfile` (`ARG`) | Selects the base Python image. Not environment-specific — keep as build-time. |

---

## Notes

- The `BACKEND_CORS_ALLOWED_ORIGINS` variable name is what Django settings reads. Do not use `CORS_ALLOWED_ORIGINS` — it will be silently ignored.
- `docker-compose.yml` passes optional vars with `${VAR:-}` syntax so they resolve to empty strings when not set. The backend settings code ignores empty values for optional vars, falling back to built-in defaults.
