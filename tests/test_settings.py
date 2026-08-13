from __future__ import annotations

from pipelines.config.settings import RuntimeSettings


def test_timestamp_queue_retention_defaults_to_168_when_env_is_missing(monkeypatch):
    monkeypatch.delenv("MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS", raising=False)

    settings = RuntimeSettings(_env_file=None)

    assert settings.MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS == 168


def test_timestamp_queue_retention_env_value_overrides_default(monkeypatch):
    monkeypatch.setenv("MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS", "72")

    settings = RuntimeSettings(_env_file=None)

    assert settings.MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS == 72
