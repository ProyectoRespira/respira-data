from __future__ import annotations

from pathlib import Path

import pytest
from pipelines.tasks import db

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_ops_bootstrap_sql_creates_measurement_stream_state_contract():
    sql = _read("pipelines/sql/02_ops_audit.sql")

    assert "create table if not exists ops.measurement_stream_state" in sql
    assert "primary key (data_source_name, station_code, variable_code)" in sql
    assert "last_cursor_id bigint null" in sql
    assert "last_value_silver double precision not null" in sql
    assert "idx_measurement_stream_state_updated_at" in sql
    assert "idx_measurement_stream_state_data_source_watermark" in sql
    assert "idempotent refreshes should leave it unchanged" in sql


def test_sql_file_splitter_preserves_semicolons_inside_quoted_content(tmp_path):
    sql_path = tmp_path / "quoted_statements.sql"
    sql_path.write_text(
        """
        comment on column ops.measurement_stream_state.data_source_name is
        'Canonical data source name; part of the stream-state primary key.';
        select 'escaped quote: it''s valid; still the same statement';
        do $body$
        begin
            perform 'a semicolon; inside a dollar quote';
        end;
        $body$;
        -- leading comment; remains attached to the following statement
        select 1;
        """,
        encoding="utf-8",
    )

    statements = db.read_sql_statements(sql_path)

    assert len(statements) == 4
    assert "data source name; part" in statements[0]
    assert "it''s valid; still" in statements[1]
    assert "perform 'a semicolon; inside a dollar quote';" in statements[2]
    assert statements[3].endswith("select 1")


def test_dbt_sources_document_measurement_stream_state_contract():
    docs = _read("dbt/models/canonical/sources/docs.md")
    source_yml = _read("dbt/models/canonical/sources/sources_ops.yml")

    assert "{% docs measurement_stream_state_contract %}" in docs
    assert "Deterministic watermark" in docs
    assert "Refreshes must upsert by the primary key" in docs
    assert "- name: ops" in source_yml
    assert "- name: measurement_stream_state" in source_yml
    assert "{{ doc('measurement_stream_state_contract') }}" in source_yml


def test_ops_ddl_errors_are_not_silenced_in_strict_mode(monkeypatch):
    expected_error = db.SQLAlchemyError("ops DDL failed")

    def _raise(_engine, _path):
        raise expected_error

    monkeypatch.setattr(db, "execute_sql_file", _raise)

    with pytest.raises(db.SQLAlchemyError, match="ops DDL failed"):
        db.ensure_ops_audit_tables(object(), strict=True)


def test_ops_ddl_errors_remain_non_blocking_by_default(monkeypatch):
    def _raise(_engine, _path):
        raise db.SQLAlchemyError("ops DDL failed")

    monkeypatch.setattr(db, "execute_sql_file", _raise)

    db.ensure_ops_audit_tables(object())


def test_production_values_use_state_and_bounded_silver_anchors():
    sql = _read(
        "dbt/models/canonical/intermediate/int_measurements_values_silver.sql"
    ).lower()
    anchor_sql = sql.split("anchor_rows as (", maxsplit=1)[1].split(
        "observed_validation as (", maxsplit=1
    )[0]

    assert "source('ops', 'measurement_stream_state')" in anchor_sql
    assert "state.last_value_silver as observed_value_silver" in anchor_sql
    assert "state.last_measured_at_silver" in anchor_sql
    assert "coalesce(state.last_cursor_id, -1)" in anchor_sql
    assert ") < (" in anchor_sql
    assert "source('silver_runtime', 'fct_measurements_silver')" in anchor_sql
    assert "join lateral" in anchor_sql
    assert "fact.timestamp < first_targets.first_measured_at_silver" in anchor_sql
    assert "measurement_batch_measured_at_from() is not none" in anchor_sql
    assert "flags.full_refresh" in anchor_sql
    assert "from {{ this }} existing" not in anchor_sql


def test_production_state_refresh_only_uses_published_advances():
    sql = _read("dbt/models/canonical/ops/measurement_stream_state.sql").lower()

    assert "alias='measurement_stream_state'" in sql
    assert "ref('int_measurements_values_silver')" not in sql
    assert "ref('int_measurement_timestamps_silver')" in sql
    assert "ref('fct_measurements_silver')" in sql
    assert "fact.source_row_id = queue.source_row_id" in sql
    assert "fact.timestamp = queue.measured_at_silver" in sql
    assert "fact.ingested_at = queue.extracted_at" in sql
    assert "fact.value_parsed as last_value_silver" in sql
    assert "state.data_source_name = streams.data_source_name" in sql
    assert "state.station_code = streams.station_code" in sql
    assert "state.variable_code = streams.variable_code" in sql
    assert "using (data_source_name, station_code, variable_code)" not in sql
    assert "coalesce(queue.cursor_id, -1)" in sql
    assert ") > (" in sql
    assert "where rn = 1" in sql
    assert "now() as updated_at" in sql
    assert "does not support --full-refresh" in sql
    assert "measurement_runtime_queue_row_predicate" in sql


def test_incremental_state_selector_targets_only_production_state_model():
    selectors = _read("dbt/selectors.yml")
    state_selector = selectors.split("- name: canonical_incremental_state", maxsplit=1)[
        1
    ].split("- name:", maxsplit=1)[0]

    assert "models/canonical/ops/measurement_stream_state.sql" in state_selector
    assert "models/canonical/shadow" not in state_selector


def test_batch_process_selector_refreshes_state_after_publishing_silver():
    selectors = _read("dbt/selectors.yml")
    state_model = _read("dbt/models/canonical/ops/measurement_stream_state.sql")
    batch_selector = selectors.split("- name: canonical_batch_process", maxsplit=1)[
        1
    ].split("- name:", maxsplit=1)[0]

    assert "models/canonical/silver/fct_measurements_silver.sql" in batch_selector
    assert "models/canonical/ops/measurement_stream_state.sql" in batch_selector
    assert "int_measurements_long.sql" not in batch_selector
    assert "int_measurements_values_silver.sql" not in batch_selector
    assert "depends_on: {{ ref('fct_measurements_silver') }}" in state_model
