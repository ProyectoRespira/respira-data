from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pipelines.tasks.measurement_backfill import (
    _build_measured_at_windows,
    _load_measurement_source_registry,
    _validate_measurement_sources,
)


def _settings() -> SimpleNamespace:
    repo_root = Path(__file__).resolve().parents[1]
    return SimpleNamespace(DBT_PROJECT_DIR=str(repo_root / "dbt"))


def test_load_measurement_source_registry_reads_registered_sources():
    registry = _load_measurement_source_registry(_settings())

    assert "fiuna_airbyte" in registry
    assert "airelibre_airbyte" in registry


def test_validate_measurement_sources_accepts_known_sources():
    settings = _settings()
    registry = _load_measurement_source_registry(settings)

    selected = _validate_measurement_sources(
        settings,
        source_registry=registry,
        requested_sources=["fiuna_airbyte", "meteostat_airbyte"],
    )

    assert selected == ["fiuna_airbyte", "meteostat_airbyte"]


def test_validate_measurement_sources_rejects_unknown_source():
    settings = _settings()
    registry = _load_measurement_source_registry(settings)

    with pytest.raises(ValueError, match="Unknown measurement source"):
        _validate_measurement_sources(
            settings,
            source_registry=registry,
            requested_sources=["does_not_exist"],
        )


def test_build_measured_at_windows_returns_half_open_windows():
    measured_at_from = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    measured_at_to = measured_at_from + timedelta(hours=50)

    windows = _build_measured_at_windows(
        measured_at_from=measured_at_from,
        measured_at_to=measured_at_to,
        batch_hours=24,
    )

    assert windows == [
        {
            "measured_at_from": datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            "measured_at_to": datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        },
        {
            "measured_at_from": datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
            "measured_at_to": datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
        },
        {
            "measured_at_from": datetime(2026, 1, 3, 0, 0, tzinfo=UTC),
            "measured_at_to": datetime(2026, 1, 3, 2, 0, tzinfo=UTC),
        },
    ]


def test_build_measured_at_windows_rejects_non_positive_batch_hours():
    with pytest.raises(ValueError, match="greater than zero"):
        _build_measured_at_windows(
            measured_at_from=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            measured_at_to=datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
            batch_hours=0,
        )
