from __future__ import annotations

import pytest

from pipelines.config.settings import RuntimeSettings


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for var in [
        "DB_DSN",
        "DBT_POSTGRES_URI",
        "REMOTE_PG_HOST",
        "REMOTE_PG_PORT",
        "REMOTE_PG_USER",
        "REMOTE_PG_PASSWORD",
        "REMOTE_PG_DB",
        "REMOTE_PG_NAME",
        "REMOTE_PG_SSLMODE",
    ]:
        monkeypatch.delenv(var, raising=False)


def _make_settings() -> RuntimeSettings:
    # Ignore any .env found in the repo so test assertions remain deterministic.
    return RuntimeSettings(_env_file=None)  # type: ignore[call-arg]


def test_database_dsn_missing_all(monkeypatch):
    settings = _make_settings()
    with pytest.raises(ValueError, match="Missing DB connection settings"):
        settings.database_dsn()


def test_database_dsn_invalid_port(monkeypatch):
    monkeypatch.setenv("REMOTE_PG_HOST", "db")
    monkeypatch.setenv("REMOTE_PG_PORT", "not-an-int")
    monkeypatch.setenv("REMOTE_PG_USER", "u")
    monkeypatch.setenv("REMOTE_PG_PASSWORD", "p")
    monkeypatch.setenv("REMOTE_PG_DB", "warehouse")

    settings = _make_settings()
    with pytest.raises(ValueError, match="REMOTE_PG_PORT must be an integer"):
        settings.database_dsn()


def test_database_dsn_port_out_of_range(monkeypatch):
    monkeypatch.setenv("REMOTE_PG_HOST", "db")
    monkeypatch.setenv("REMOTE_PG_PORT", "99999")
    monkeypatch.setenv("REMOTE_PG_USER", "u")
    monkeypatch.setenv("REMOTE_PG_PASSWORD", "p")
    monkeypatch.setenv("REMOTE_PG_DB", "warehouse")

    settings = _make_settings()
    with pytest.raises(ValueError, match="out of range"):
        settings.database_dsn()


def test_database_dsn_url_encodes_credentials(monkeypatch):
    monkeypatch.setenv("REMOTE_PG_HOST", "db.example.com")
    monkeypatch.setenv("REMOTE_PG_PORT", "5432")
    monkeypatch.setenv("REMOTE_PG_USER", "user@name")
    monkeypatch.setenv("REMOTE_PG_PASSWORD", "p@ss/word!")
    monkeypatch.setenv("REMOTE_PG_DB", "warehouse")
    monkeypatch.setenv("REMOTE_PG_SSLMODE", "require")

    settings = _make_settings()
    dsn = settings.database_dsn()
    assert dsn.startswith("postgresql+psycopg://user%40name:p%40ss%2Fword%21@db.example.com:5432/warehouse")
    assert dsn.endswith("?sslmode=require")
