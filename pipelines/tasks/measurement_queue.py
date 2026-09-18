from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import text

from pipelines.compat import task

NO_CACHE: Any | None
try:
    from prefect.cache_policies import NO_CACHE as _PREFECT_NO_CACHE
except Exception:  # noqa: BLE001
    NO_CACHE = None
else:
    NO_CACHE = _PREFECT_NO_CACHE

INTERMEDIATE_SCHEMA = "intermediate"
TIMESTAMP_QUEUE_TABLE = "int_measurement_timestamps_silver"


def _cleanup_measurement_timestamp_queue(
    engine,
    retention_hours: int,
    data_source_name: str | None = None,
    measured_at_from: datetime | None = None,
    measured_at_to: datetime | None = None,
    include_null_time_rows: bool = False,
) -> int:
    if retention_hours <= 0:
        raise ValueError(
            "measurement timestamp queue retention hours must be positive."
        )
    if data_source_name is not None and not data_source_name.strip():
        raise ValueError("data_source_name must not be blank.")
    if include_null_time_rows and (
        measured_at_from is not None or measured_at_to is not None
    ):
        raise ValueError(
            "Null-time queue cleanup cannot be combined with measured-time bounds."
        )

    scope_clauses: list[str] = []
    params: dict[str, object] = {"retention_hours": retention_hours}

    if data_source_name is not None:
        scope_clauses.append("q.data_source_name = :data_source_name")
        params["data_source_name"] = data_source_name

    if include_null_time_rows:
        scope_clauses.append("q.measured_at_silver is null")
    else:
        # Null-time rows require their own successful process/test pass and
        # must never be marked by general incremental cleanup.
        scope_clauses.append("q.measured_at_silver is not null")
        if measured_at_from is not None:
            scope_clauses.append("q.measured_at_silver >= :measured_at_from")
            params["measured_at_from"] = measured_at_from
        if measured_at_to is not None:
            scope_clauses.append("q.measured_at_silver < :measured_at_to")
            params["measured_at_to"] = measured_at_to

    # The caller invokes this task only after silver publish, stream-state
    # refresh, and scoped smoke-test gates. Those gates are the durable proof
    # that every queue row in this successful scope is eligible for retention.
    mark_query = text(
        f"""
        update {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE} q
        set cleanup_eligible_at = now()
        where {" and ".join(scope_clauses)}
        """
    )

    delete_clauses = [
        *scope_clauses,
        "q.cleanup_eligible_at is not null",
        "q.extracted_at < now() - make_interval(hours => :retention_hours)",
        "checkpoint.data_source_name = q.data_source_name",
        "(q.extracted_at, q.source_row_id) < "
        "(checkpoint.extracted_at, checkpoint.source_row_id)",
    ]

    if data_source_name is not None:
        checkpoint_query = f"""
            select
              checkpoint.data_source_name,
              checkpoint.extracted_at,
              checkpoint.source_row_id
            from {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE} checkpoint
            where checkpoint.data_source_name = :data_source_name
            order by checkpoint.extracted_at desc, checkpoint.source_row_id desc
            limit 1
        """
    else:
        checkpoint_query = f"""
            select
              source_names.data_source_name,
              checkpoint.extracted_at,
              checkpoint.source_row_id
            from (
              select distinct data_source_name
              from {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE}
            ) source_names
            cross join lateral (
              select
                candidate.extracted_at,
                candidate.source_row_id
              from {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE} candidate
              where candidate.data_source_name = source_names.data_source_name
              order by candidate.extracted_at desc, candidate.source_row_id desc
              limit 1
            ) checkpoint
        """

    delete_query = text(
        f"""
        with source_checkpoints as materialized (
          {checkpoint_query}
        )
        delete from {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE} q
        using source_checkpoints checkpoint
        where {" and ".join(delete_clauses)}
        """
    )

    with engine.begin() as conn:
        # A resume may intentionally skip the dbt ingest model that normally
        # synchronizes this column. Make deployment over an existing queue
        # backward-compatible without requiring an out-of-band migration.
        marker_exists = bool(
            conn.execute(
                text(
                    """
                    select exists (
                      select 1
                      from information_schema.columns
                      where table_schema = :queue_schema
                        and table_name = :queue_table
                        and column_name = 'cleanup_eligible_at'
                    )
                    """
                ),
                {
                    "queue_schema": INTERMEDIATE_SCHEMA,
                    "queue_table": TIMESTAMP_QUEUE_TABLE,
                },
            ).scalar_one()
        )
        if not marker_exists:
            conn.execute(
                text(
                    f"""
                    alter table {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE}
                    add column if not exists cleanup_eligible_at timestamptz
                    """
                )
            )
        conn.execute(mark_query, params)
        delete_result = conn.execute(delete_query, params)

    deleted_rows = int(delete_result.rowcount or 0)
    return max(deleted_rows, 0)


@task(
    name="cleanup_measurement_timestamp_queue",
    **({"cache_policy": NO_CACHE} if NO_CACHE is not None else {}),
)
def cleanup_measurement_timestamp_queue(
    engine,
    retention_hours: int,
    data_source_name: str | None = None,
    measured_at_from: datetime | None = None,
    measured_at_to: datetime | None = None,
    include_null_time_rows: bool = False,
) -> int:
    return _cleanup_measurement_timestamp_queue(
        engine,
        retention_hours=retention_hours,
        data_source_name=data_source_name,
        measured_at_from=measured_at_from,
        measured_at_to=measured_at_to,
        include_null_time_rows=include_null_time_rows,
    )
