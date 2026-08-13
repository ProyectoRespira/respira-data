from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from pipelines.tasks.measurement_queue import _cleanup_measurement_timestamp_queue


def _engine_with_rowcount(rowcount: int = 0):
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    marker_result = MagicMock()
    marker_result.scalar_one.return_value = True
    mark_result = MagicMock()
    delete_result = MagicMock()
    delete_result.rowcount = rowcount
    connection.execute.side_effect = [marker_result, mark_result, delete_result]
    return engine, connection


def test_queue_cleanup_applies_age_floor_and_preserves_one_checkpoint_row():
    engine, connection = _engine_with_rowcount(7)

    deleted = _cleanup_measurement_timestamp_queue(engine, retention_hours=168)

    ensure_call, mark_call, delete_call = connection.execute.call_args_list
    assert "information_schema.columns" in str(ensure_call.args[0])
    mark_statement, mark_params = mark_call.args
    delete_statement, delete_params = delete_call.args
    mark_sql = str(mark_statement)
    sql = str(delete_statement)
    assert deleted == 7
    assert "set cleanup_eligible_at = now()" in mark_sql
    assert "int_measurements_values_silver" not in mark_sql
    assert "int_measurements_long" not in mark_sql
    assert "q.measured_at_silver is not null" in mark_sql
    assert mark_params == {"retention_hours": 168}
    assert "q.cleanup_eligible_at is not null" in sql
    assert "now() - make_interval(hours => :retention_hours)" in sql
    assert "exists (" in sql
    assert "checkpoint.data_source_name = q.data_source_name" in sql
    assert "checkpoint.extracted_at > q.extracted_at" in sql
    assert "checkpoint.source_row_id > q.source_row_id" in sql
    assert "q.measured_at_silver is not null" in sql
    assert delete_params == {"retention_hours": 168}


def test_queue_cleanup_adds_marker_column_for_an_existing_pre_retention_queue():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    marker_result = MagicMock()
    marker_result.scalar_one.return_value = False
    delete_result = MagicMock()
    delete_result.rowcount = 0
    connection.execute.side_effect = [
        marker_result,
        MagicMock(),
        MagicMock(),
        delete_result,
    ]

    _cleanup_measurement_timestamp_queue(engine, retention_hours=168)

    _check_call, alter_call, _mark_call, _delete_call = (
        connection.execute.call_args_list
    )
    assert "add column if not exists cleanup_eligible_at" in str(alter_call.args[0])


def test_queue_cleanup_scopes_a_successful_backfill_window():
    engine, connection = _engine_with_rowcount(3)
    measured_from = datetime(2026, 1, 1, tzinfo=UTC)
    measured_to = datetime(2026, 1, 2, tzinfo=UTC)

    _cleanup_measurement_timestamp_queue(
        engine,
        retention_hours=24,
        data_source_name="meteostat_airbyte",
        measured_at_from=measured_from,
        measured_at_to=measured_to,
    )

    _ensure_call, mark_call, delete_call = connection.execute.call_args_list
    mark_sql = str(mark_call.args[0])
    sql = str(delete_call.args[0])
    params = delete_call.args[1]
    assert "q.data_source_name = :data_source_name" in mark_sql
    assert "q.measured_at_silver >= :measured_at_from" in mark_sql
    assert "q.measured_at_silver < :measured_at_to" in mark_sql
    assert "q.data_source_name = :data_source_name" in sql
    assert "q.measured_at_silver >= :measured_at_from" in sql
    assert "q.measured_at_silver < :measured_at_to" in sql
    assert params == {
        "retention_hours": 24,
        "data_source_name": "meteostat_airbyte",
        "measured_at_from": measured_from,
        "measured_at_to": measured_to,
    }


def test_null_time_cleanup_requires_an_explicit_dedicated_scope():
    engine, connection = _engine_with_rowcount(2)

    _cleanup_measurement_timestamp_queue(
        engine,
        retention_hours=168,
        data_source_name="fiuna_airbyte",
        include_null_time_rows=True,
    )

    _ensure_call, mark_call, delete_call = connection.execute.call_args_list
    mark_sql = str(mark_call.args[0])
    delete_sql = str(delete_call.args[0])
    assert "q.measured_at_silver is null" in mark_sql
    assert "q.measured_at_silver is null" in delete_sql
    assert "int_measurements_long" not in mark_sql
    assert "int_measurements_values_silver" not in mark_sql
    assert "q.measured_at_silver is not null" not in mark_sql
    assert "q.measured_at_silver is not null" not in delete_sql


@pytest.mark.parametrize("retention_hours", [0, -1])
def test_queue_cleanup_rejects_non_positive_retention(retention_hours):
    with pytest.raises(ValueError, match="must be positive"):
        _cleanup_measurement_timestamp_queue(
            MagicMock(), retention_hours=retention_hours
        )


def test_null_time_cleanup_rejects_measured_bounds():
    with pytest.raises(ValueError, match="cannot be combined"):
        _cleanup_measurement_timestamp_queue(
            MagicMock(),
            retention_hours=168,
            measured_at_from=datetime(2026, 1, 1, tzinfo=UTC),
            include_null_time_rows=True,
        )
