from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pipelines.flows import measurement_stream_state_bootstrap as flow_module
from pipelines.tasks import measurement_stream_state as state_module


class _Result:
    def __init__(self, *, row: dict[str, int] | None = None, rowcount: int = -1):
        self._row = row
        self.rowcount = rowcount

    def mappings(self):
        return self

    def one(self):
        if self._row is None:
            raise AssertionError("No mapping row configured for this result")
        return self._row


def _call(callable_or_prefect_object, *args, **kwargs):
    fn = getattr(callable_or_prefect_object, "fn", callable_or_prefect_object)
    return fn(*args, **kwargs)


def _engine_with_connection(side_effect):
    engine = MagicMock()
    connection = MagicMock()
    transaction = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    connection.begin.return_value = transaction
    connection.exec_driver_sql.side_effect = side_effect
    return engine, connection, transaction


def test_bootstrap_query_uses_latest_published_identity_and_full_watermark():
    candidate_sql = state_module.CREATE_CANDIDATES_SQL.lower()
    upsert_sql = state_module.UPSERT_STATE_SQL.lower()

    assert "from silver.fct_measurements_silver fact" in candidate_sql
    assert "join lateral" in candidate_sql
    assert "order by" in candidate_sql
    assert "fact.timestamp desc" in candidate_sql
    assert "limit 1" in candidate_sql
    assert "intermediate.int_measurements_values_silver" in candidate_sql
    assert "intermediate.cursor_id as last_cursor_id" in candidate_sql
    assert "intermediate.source_row_id = published.source_row_id" in candidate_sql
    assert "intermediate.value_silver is not distinct from" in candidate_sql
    assert "on conflict (data_source_name, station_code, variable_code)" in upsert_sql
    assert "coalesce(excluded.last_cursor_id, -1)" in upsert_sql
    assert "coalesce(state.last_cursor_id, -1)" in upsert_sql
    assert "updated_at = now()" in upsert_sql
    assert ") > (" in upsert_sql


@pytest.mark.parametrize(
    ("metrics", "message"),
    [
        (
            {
                "published_streams": 0,
                "candidates": 0,
                "missing": 0,
                "incomplete": 0,
                "duplicate_streams": 0,
            },
            "No published silver streams",
        ),
        (
            {
                "published_streams": 2,
                "candidates": 1,
                "missing": 1,
                "incomplete": 0,
                "duplicate_streams": 0,
            },
            "missing_or_mismatched=1",
        ),
        (
            {
                "published_streams": 2,
                "candidates": 2,
                "missing": 0,
                "incomplete": 1,
                "duplicate_streams": 1,
            },
            "duplicate_streams=1",
        ),
    ],
)
def test_candidate_validation_rejects_incomplete_coverage(metrics, message):
    with pytest.raises(
        state_module.MeasurementStreamStateBootstrapError, match=message
    ):
        state_module._validate_candidate_metrics(metrics)


@patch.object(state_module, "execute_sql_file_on_connection")
def test_bootstrap_returns_change_metrics_and_supports_idempotent_reruns(
    mock_execute_ddl,
):
    candidate_row = {
        "published_streams": 3,
        "candidates": 3,
        "missing": 0,
        "incomplete": 0,
        "duplicate_streams": 0,
    }
    change_row = {"inserted": 0, "updated": 0}
    post_row = {"missing": 0, "extra": 0, "mismatched": 0}

    def _execute(sql, *args):
        if sql == state_module.CANDIDATE_METRICS_SQL:
            return _Result(row=candidate_row)
        if sql == state_module.CLASSIFY_CHANGES_SQL:
            return _Result(row=change_row)
        if sql == state_module.UPSERT_STATE_SQL:
            return _Result(rowcount=0)
        if sql == state_module.POST_VALIDATION_SQL:
            return _Result(row=post_row)
        return _Result()

    engine, connection, _transaction = _engine_with_connection(_execute)

    result = state_module._bootstrap_measurement_stream_state(
        engine, statement_timeout_seconds=60
    )

    assert result == {
        "published_streams": 3,
        "candidates": 3,
        "inserted": 0,
        "updated": 0,
        "unchanged": 3,
        "affected": 0,
        "missing": 0,
        "extra": 0,
        "mismatched": 0,
    }
    mock_execute_ddl.assert_called_once_with(connection, state_module.OPS_AUDIT_SQL)
    assert any(
        call.args[0] == state_module.UPSERT_STATE_SQL
        for call in connection.exec_driver_sql.call_args_list
    )


@pytest.mark.parametrize(
    ("change_row", "rowcount", "expected_unchanged"),
    [
        ({"inserted": 3, "updated": 0}, 3, 0),
        ({"inserted": 0, "updated": 2}, 2, 1),
    ],
)
@patch.object(state_module, "execute_sql_file_on_connection")
def test_bootstrap_reports_initial_inserts_and_newer_watermark_updates(
    _mock_execute_ddl,
    change_row,
    rowcount,
    expected_unchanged,
):
    def _execute(sql, *args):
        if sql == state_module.CANDIDATE_METRICS_SQL:
            return _Result(
                row={
                    "published_streams": 3,
                    "candidates": 3,
                    "missing": 0,
                    "incomplete": 0,
                    "duplicate_streams": 0,
                }
            )
        if sql == state_module.CLASSIFY_CHANGES_SQL:
            return _Result(row=change_row)
        if sql == state_module.UPSERT_STATE_SQL:
            return _Result(rowcount=rowcount)
        if sql == state_module.POST_VALIDATION_SQL:
            return _Result(row={"missing": 0, "extra": 0, "mismatched": 0})
        return _Result()

    engine, _connection, _transaction = _engine_with_connection(_execute)

    result = state_module._bootstrap_measurement_stream_state(
        engine, statement_timeout_seconds=60
    )

    assert result["inserted"] == change_row["inserted"]
    assert result["updated"] == change_row["updated"]
    assert result["affected"] == rowcount
    assert result["unchanged"] == expected_unchanged


@patch.object(state_module, "execute_sql_file_on_connection")
def test_failed_candidate_validation_rolls_back_transaction(mock_execute_ddl):
    candidate_row = {
        "published_streams": 2,
        "candidates": 1,
        "missing": 1,
        "incomplete": 0,
        "duplicate_streams": 0,
    }

    def _execute(sql, *args):
        if sql == state_module.CANDIDATE_METRICS_SQL:
            return _Result(row=candidate_row)
        return _Result()

    engine, connection, transaction = _engine_with_connection(_execute)

    with pytest.raises(state_module.MeasurementStreamStateBootstrapError):
        state_module._bootstrap_measurement_stream_state(
            engine, statement_timeout_seconds=60
        )

    transaction.__exit__.assert_called_once()
    assert transaction.__exit__.call_args.args[0] is (
        state_module.MeasurementStreamStateBootstrapError
    )
    assert not any(
        call.args[0] == state_module.UPSERT_STATE_SQL
        for call in connection.exec_driver_sql.call_args_list
    )


def test_bootstrap_rejects_invalid_statement_timeout():
    with pytest.raises(ValueError, match="greater than zero"):
        state_module._bootstrap_measurement_stream_state(
            MagicMock(), statement_timeout_seconds=0
        )


@patch.object(flow_module, "get_run_logger", return_value=MagicMock())
@patch.object(flow_module, "bootstrap_measurement_stream_state")
@patch.object(flow_module, "get_engine")
@patch.object(flow_module, "get_settings")
def test_flow_logs_target_runs_bootstrap_and_disposes_engine(
    mock_get_settings,
    mock_get_engine,
    mock_bootstrap,
    mock_logger,
):
    settings = SimpleNamespace(
        MEASUREMENT_STREAM_STATE_BOOTSTRAP_TIMEOUT_S=1800,
        database_dsn=lambda: (
            "postgresql+psycopg://user:secret@demo-db.example:25060/respira_demo"
        ),
    )
    engine = MagicMock()
    expected = {
        "published_streams": 169,
        "candidates": 169,
        "inserted": 169,
        "updated": 0,
        "unchanged": 0,
        "affected": 169,
        "missing": 0,
        "extra": 0,
        "mismatched": 0,
    }
    mock_get_settings.return_value = settings
    mock_get_engine.return_value = engine
    mock_bootstrap.return_value = expected

    result = _call(flow_module.measurement_stream_state_bootstrap)

    assert result == expected
    mock_bootstrap.assert_called_once_with(engine, statement_timeout_seconds=1800)
    mock_logger.return_value.info.assert_any_call(
        "Bootstrapping measurement stream state on host=%s port=%s database=%s",
        "demo-db.example",
        25060,
        "respira_demo",
    )
    engine.dispose.assert_called_once_with()
