from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus

from pydantic import PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class RuntimeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    DB_DSN: str | None = None
    DBT_PROJECT_DIR: str = str(REPO_ROOT / "dbt")
    DBT_PROFILES_DIR: str = str(REPO_ROOT / "dbt")
    DBT_TARGET: str = "prod"
    DBT_THREADS: int = 1
    DBT_USE_PREFECT_DBT: bool = False

    DBT_TIMEOUT_CANONICAL_CORE_S: int = 0
    DBT_TIMEOUT_CANONICAL_BATCH_INGEST_S: int = 3600
    DBT_TIMEOUT_CANONICAL_SILVER_S: int = 1800
    DBT_TIMEOUT_PROJECT_S: int = 1200
    DBT_TIMEOUT_TESTS_S: int = 1200

    MEASUREMENT_BACKFILL_PROCESS_BATCH_HOURS: int = 720
    MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS: PositiveInt = 168
    MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS: PositiveInt = 2_000_000
    MEASUREMENT_QUEUE_CUTOVER_BATCH_HOURS: PositiveInt = 168
    MEASUREMENT_QUEUE_CUTOVER_MAX_QUEUE_ROWS: PositiveInt = 250_000
    MEASUREMENT_STREAM_STATE_BOOTSTRAP_TIMEOUT_S: int = 1800
    DEFAULT_WINDOW_HOURS: int = 24
    INFERENCE_MIN_POINTS: int = 18
    MODEL_6H_PATH: str | None = None
    MODEL_12H_PATH: str | None = None
    MODEL_6H_VERSION: str = "unknown"
    MODEL_12H_VERSION: str = "unknown"

    SLACK_WEBHOOK_URL: str | None = None

    SOCIAL_DRY_RUN: bool = True
    SOCIAL_DATA_MAX_AGE_HOURS: int = 6
    SOCIAL_MIN_STATIONS_PER_REGION: int = 1

    TELEGRAM_ENABLED: bool = False
    TELEGRAM_BOT_TOKEN: str | None = None
    TELEGRAM_CHAT_ID: str | None = None

    TWITTER_ENABLED: bool = False
    TWITTER_BEARER_TOKEN: str | None = None
    TWITTER_API_KEY: str | None = None
    TWITTER_API_SECRET: str | None = None
    TWITTER_ACCESS_TOKEN: str | None = None
    TWITTER_ACCESS_TOKEN_SECRET: str | None = None

    def database_dsn(self) -> str:
        if self.DB_DSN:
            return self.DB_DSN

        legacy_dsn = os.getenv("DBT_POSTGRES_URI")
        if legacy_dsn:
            if legacy_dsn.startswith("postgresql://"):
                return legacy_dsn.replace("postgresql://", "postgresql+psycopg://", 1)
            return legacy_dsn

        host = os.getenv("REMOTE_PG_HOST")
        port = os.getenv("REMOTE_PG_PORT")
        user = os.getenv("REMOTE_PG_USER")
        password = os.getenv("REMOTE_PG_PASSWORD")
        dbname = os.getenv("REMOTE_PG_DB") or os.getenv("REMOTE_PG_NAME")
        sslmode = os.getenv("REMOTE_PG_SSLMODE", "prefer")

        if not all([host, port, user, password, dbname]):
            raise ValueError(
                "Missing DB connection settings. Set DB_DSN or REMOTE_PG_* environment variables."
            )

        assert host is not None
        assert port is not None
        assert user is not None
        assert password is not None
        assert dbname is not None

        user_enc = quote_plus(user)
        password_enc = quote_plus(password)
        return (
            f"postgresql+psycopg://{user_enc}:{password_enc}@{host}:{port}/{dbname}"
            f"?sslmode={sslmode}"
        )


@lru_cache(maxsize=1)
def get_settings() -> RuntimeSettings:
    return RuntimeSettings()
