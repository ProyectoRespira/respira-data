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
MEASUREMENTS_LONG_TABLE = "int_measurements_long"
MEASUREMENTS_VALUES_TABLE = "int_measurements_values_silver"


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
        processed_table = MEASUREMENTS_LONG_TABLE
    else:
        # Null-time rows require their own successful process/test pass and
        # must never be marked by general incremental cleanup.
        scope_clauses.append("q.measured_at_silver is not null")
        processed_table = MEASUREMENTS_VALUES_TABLE
        if measured_at_from is not None:
            scope_clauses.append("q.measured_at_silver >= :measured_at_from")
            params["measured_at_from"] = measured_at_from
        if measured_at_to is not None:
            scope_clauses.append("q.measured_at_silver < :measured_at_to")
            params["measured_at_to"] = measured_at_to

    # A timed queue row is marked only if it reached value validation; null-time
    # rows are intentionally excluded there, so their dedicated pass is proved
    # by the long model instead. The caller invokes this task after silver
    # publish, stream-state refresh, and smoke-test gates, making the timestamp
    # the durable eligibility record for recent rows that cannot be deleted yet.
    mark_query = text(
        f"""
        update {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE} q
        set cleanup_eligible_at = now()
        where {" and ".join(scope_clauses)}
          and exists (
            select 1
            from {INTERMEDIATE_SCHEMA}.{processed_table} processed
            where processed.data_source_name = q.data_source_name
              and processed.source_row_id = q.source_row_id
              and processed.extracted_at is not distinct from q.extracted_at
              and processed.station_code is not distinct from q.station_code
              and processed.measured_at_silver is not distinct from q.measured_at_silver
          )
        """
    )

    delete_clauses = [
        *scope_clauses,
        "q.cleanup_eligible_at is not null",
        "q.extracted_at < now() - make_interval(hours => :retention_hours)",
        # Keep one deterministic row at the newest extraction edge as the
        # source's durable incremental checkpoint. Requiring a newer
        # (extracted_at, source_row_id) row makes every other old row eligible,
        # including ties from a large Airbyte extraction batch.
        f"""exists (
            select 1
            from {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE} checkpoint
            where checkpoint.data_source_name = q.data_source_name
              and (
                checkpoint.extracted_at > q.extracted_at
                or (
                  checkpoint.extracted_at = q.extracted_at
                  and checkpoint.source_row_id > q.source_row_id
                )
              )
        )""",
    ]

    delete_query = text(
        f"""
        delete from {INTERMEDIATE_SCHEMA}.{TIMESTAMP_QUEUE_TABLE} q
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
