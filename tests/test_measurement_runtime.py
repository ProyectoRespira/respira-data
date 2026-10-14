from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pipelines.tasks import measurement_runtime as runtime


def test_queue_workload_expands_by_configured_variable_count(monkeypatch):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value.all.return_value = [
        {"data_source_name": "source_a", "queue_rows": 12}
    ]
    monkeypatch.setattr(
        runtime, "_load_source_variable_counts", lambda _settings: {"source_a": 5}
    )

    workloads = runtime.get_unmarked_queue_workload(engine, SimpleNamespace())

    assert workloads == [runtime.QueueWorkload("source_a", 12, 5)]
    assert workloads[0].expanded_rows == 60


def test_incremental_workload_guard_reports_source_breakdown(monkeypatch):
    workloads = [
        runtime.QueueWorkload("source_a", 300_000, 5),
        runtime.QueueWorkload("source_b", 300_000, 2),
    ]
    monkeypatch.setattr(
        runtime, "get_unmarked_queue_workload", lambda *_args: workloads
    )

    with pytest.raises(RuntimeError, match="2,100,000|2100000") as exc_info:
        runtime.validate_incremental_queue_workload(
            MagicMock(),
            SimpleNamespace(MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS=2_000_000),
        )

    assert "source_a=300000 queue/1500000 expanded" in str(exc_info.value)
    assert "canonical_measurement_queue_cutover" in str(exc_info.value)


def test_incremental_workload_guard_accepts_limit(monkeypatch):
    workloads = [runtime.QueueWorkload("source_a", 400_000, 5)]
    monkeypatch.setattr(
        runtime, "get_unmarked_queue_workload", lambda *_args: workloads
    )

    assert (
        runtime.validate_incremental_queue_workload(
            MagicMock(),
            SimpleNamespace(MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS=2_000_000),
        )
        == workloads
    )


def test_window_split_respects_queue_and_expansion_caps(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=8)

    def stats(_engine, _source, lower, upper):
        hours = int((upper - lower).total_seconds() // 3600)
        rows = hours * 100
        return rows, lower, upper - timedelta(microseconds=1)

    monkeypatch.setattr(runtime, "_scope_stats", stats)

    windows = runtime._split_queue_window(
        MagicMock(),
        "source_a",
        start,
        end,
        variable_count=5,
        max_queue_rows=250,
        max_expanded_rows=1_000,
    )

    assert len(windows) == 4
    assert all(window.queue_rows <= 200 for window in windows)
    assert all(window.expanded_rows <= 1_000 for window in windows)


def test_single_timestamp_scope_is_kept_together(monkeypatch):
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    monkeypatch.setattr(
        runtime,
        "_scope_stats",
        lambda *_args: (300_000, timestamp, timestamp),
    )

    windows = runtime._split_queue_window(
        MagicMock(),
        "source_a",
        timestamp,
        timestamp + timedelta(hours=1),
        variable_count=2,
        max_queue_rows=250_000,
        max_expanded_rows=2_000_000,
    )

    assert len(windows) == 1
    assert windows[0].oversized_single_timestamp is True


def test_queue_index_creation_refuses_required_name_with_wrong_columns(monkeypatch):
    engine = MagicMock()
    monkeypatch.setattr(
        runtime,
        "_existing_queue_indexes",
        lambda _connection: [
            {
                "index_name": "idx_measurement_queue_source_measured_at",
                "indisvalid": True,
                "key_columns": ["measured_at_silver"],
            }
        ],
    )

    with pytest.raises(RuntimeError, match="already exists with columns"):
        runtime.ensure_measurement_queue_runtime_indexes(engine)


def test_queue_indexes_are_created_concurrently(monkeypatch):
    engine = MagicMock()
    connection = engine.connect.return_value.execution_options.return_value.__enter__.return_value
    connection.dialect.identifier_preparer.quote.side_effect = lambda value: value
    monkeypatch.setattr(runtime, "_existing_queue_indexes", lambda _connection: [])

    runtime.ensure_measurement_queue_runtime_indexes(engine)

    statements = [call.args[0] for call in connection.exec_driver_sql.call_args_list]
    assert len(statements) == len(runtime.QUEUE_INDEXES)
    assert all(
        statement.startswith("create index concurrently") for statement in statements
    )


def test_legacy_runtime_validation_requires_both_physical_tables():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value.all.return_value = [
        {"relname": "int_measurements_long", "relkind": "r"}
    ]

    with pytest.raises(RuntimeError, match="int_measurements_values_silver"):
        runtime.validate_legacy_runtime_tables(engine)


def test_publish_lock_refuses_overlap():
    engine = MagicMock()
    connection = engine.connect.return_value
    lock_result = MagicMock()
    lock_result.scalar_one.return_value = False
    connection.execute.return_value = lock_result

    with pytest.raises(RuntimeError, match="Another canonical measurement publisher"):
        runtime.acquire_measurement_publish_lock(engine)

    connection.close.assert_called_once_with()


def test_publish_lock_is_released():
    engine = MagicMock()
    connection = engine.connect.return_value
    lock_result = MagicMock()
    lock_result.scalar_one.return_value = True
    activity_result = MagicMock()
    activity_result.mappings.return_value.all.return_value = []
    unlock_result = MagicMock()
    connection.execute.side_effect = [lock_result, activity_result, unlock_result]

    acquired = runtime.acquire_measurement_publish_lock(engine)
    runtime.release_measurement_publish_lock(acquired)

    assert connection.execute.call_count == 3
    connection.close.assert_called_once_with()


def test_publish_lock_refuses_orphaned_silver_query():
    engine = MagicMock()
    connection = engine.connect.return_value
    lock_result = MagicMock()
    lock_result.scalar_one.return_value = True
    activity_result = MagicMock()
    activity_result.mappings.return_value.all.return_value = [
        {"pid": 42, "application_name": "dbt"}
    ]
    connection.execute.side_effect = [lock_result, activity_result]

    with pytest.raises(RuntimeError, match="pid=42"):
        runtime.acquire_measurement_publish_lock(engine)

    connection.close.assert_called_once_with()


def test_release_publish_lock_closes_connection_when_unlock_fails():
    connection = MagicMock()
    connection.execute.side_effect = RuntimeError("database connection lost")

    with pytest.raises(RuntimeError, match="database connection lost"):
        runtime.release_measurement_publish_lock(connection)

    connection.close.assert_called_once_with()
