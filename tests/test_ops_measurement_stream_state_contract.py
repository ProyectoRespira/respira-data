from __future__ import annotations

from pathlib import Path

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


def test_dbt_sources_document_measurement_stream_state_contract():
    docs = _read("dbt/models/canonical/sources/docs.md")
    source_yml = _read("dbt/models/canonical/sources/sources_ops.yml")

    assert "{% docs measurement_stream_state_contract %}" in docs
    assert "Deterministic watermark" in docs
    assert "Refreshes must upsert by the primary key" in docs
    assert "- name: ops" in source_yml
    assert "- name: measurement_stream_state" in source_yml
    assert "{{ doc('measurement_stream_state_contract') }}" in source_yml


def test_repo_docs_describe_measurement_stream_state_runtime_role():
    doc = _read("docs/ops-measurement-stream-state.md")
    normalized_doc = " ".join(doc.split())

    assert "open-ended canonical measurement incrementals" in doc
    assert "available before scheduled runtime flows start" in normalized_doc
    assert "Change `updated_at` only when the stored state changes." in doc
