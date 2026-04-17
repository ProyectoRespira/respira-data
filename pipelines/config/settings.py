from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict


REPO_ROOT = Path(__file__).resolve().parents[2]


class RuntimeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    DB_DSN: str | None = None
    DBT_PROJECT_DIR: str = str(REPO_ROOT / "dbt")
    DBT_PROFILES_DIR: str = str(REPO_ROOT / "dbt")
    DBT_TARGET: str = "prod"
    DBT_THREADS: int = 1

    DBT_TIMEOUT_CANONICAL_CORE_S: int = 900
    DBT_TIMEOUT_CANONICAL_SILVER_S: int = 1800
    DBT_TIMEOUT_PROJECT_S: int = 1200
    DBT_TIMEOUT_TESTS_S: int = 1200

    DEFAULT_WINDOW_HOURS: int = 24
    INFERENCE_MIN_POINTS: int = 18
    MODEL_6H_PATH: str | None = None
    MODEL_12H_PATH: str | None = None
    MODEL_6H_VERSION: str = "unknown"
    MODEL_12H_VERSION: str = "unknown"

    SLACK_WEBHOOK_URL: str | None = None

    def database_dsn(self) -> str:
        if self.DB_DSN:
            return self.DB_DSN

        legacy_dsn = os.getenv("DBT_POSTGRES_URI")
        if legacy_dsn:
            if legacy_dsn.startswith("postgresql://"):
                return legacy_dsn.replace("postgresql://", "postgresql+psycopg://", 1)
            return legacy_dsn

        host = os.getenv("REMOTE_PG_HOST")
        port_raw = os.getenv("REMOTE_PG_PORT")
        user = os.getenv("REMOTE_PG_USER")
        password = os.getenv("REMOTE_PG_PASSWORD")
        dbname = os.getenv("REMOTE_PG_DB") or os.getenv("REMOTE_PG_NAME")
        sslmode = os.getenv("REMOTE_PG_SSLMODE", "prefer")

        missing = [
            name
            for name, value in (
                ("REMOTE_PG_HOST", host),
                ("REMOTE_PG_PORT", port_raw),
                ("REMOTE_PG_USER", user),
                ("REMOTE_PG_PASSWORD", password),
                ("REMOTE_PG_DB/REMOTE_PG_NAME", dbname),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                "Missing DB connection settings. Set DB_DSN or the following "
                f"environment variables: {', '.join(missing)}."
            )

        assert host and port_raw and user and password and dbname  # for type-narrowing
        try:
            port = int(port_raw)
        except ValueError as exc:
            raise ValueError(f"REMOTE_PG_PORT must be an integer, got {port_raw!r}") from exc
        if not 1 <= port <= 65535:
            raise ValueError(f"REMOTE_PG_PORT out of range: {port}")

        user_enc = quote_plus(user)
        password_enc = quote_plus(password)
        return (
            f"postgresql+psycopg://{user_enc}:{password_enc}@{host}:{port}/{dbname}"
            f"?sslmode={sslmode}"
        )


@lru_cache(maxsize=1)
def get_settings() -> RuntimeSettings:
    return RuntimeSettings()
