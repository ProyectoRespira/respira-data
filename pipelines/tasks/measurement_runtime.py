from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from sqlalchemy import text

MEASUREMENT_PUBLISH_LOCK_NAME = "respira:canonical-measurement-publish"
QUEUE_SCHEMA = "intermediate"
QUEUE_TABLE = "int_measurement_timestamps_silver"
LEGACY_RUNTIME_TABLES = (
    "int_measurements_long",
    "int_measurements_values_silver",
)


@dataclass(frozen=True)
class QueueWorkload:
    data_source_name: str
    queue_rows: int
    variable_count: int

    @property
    def expanded_rows(self) -> int:
        return self.queue_rows * self.variable_count


@dataclass(frozen=True)
class QueueProcessWindow:
    data_source_name: str
    measured_at_from: datetime
    measured_at_to: datetime
    queue_rows: int
    expanded_rows: int
    oversized_single_timestamp: bool = False


QUEUE_INDEXES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "idx_measurement_queue_source_measured_at",
        ("data_source_name", "measured_at_silver"),
    ),
    (
        "idx_measurement_queue_cleanup_extracted_at",
        ("cleanup_eligible_at", "extracted_at"),
    ),
    (
        "idx_measurement_queue_source_checkpoint",
        ("data_source_name", "extracted_at", "source_row_id"),
    ),
)


def _load_source_variable_counts(settings) -> dict[str, int]:
    project_path = Path(settings.DBT_PROJECT_DIR) / "dbt_project.yml"
    project = yaml.safe_load(project_path.read_text(encoding="utf-8")) or {}
    source_config = (project.get("vars") or {}).get("measurements_sources") or {}
    return {
        source_name: len((config or {}).get("variables") or {})
        for source_name, config in source_config.items()
    }


def get_unmarked_queue_workload(engine, settings) -> list[QueueWorkload]:
    variable_counts = _load_source_variable_counts(settings)
    query = text(
        f"""
        select data_source_name, count(*)::bigint as queue_rows
        from {QUEUE_SCHEMA}.{QUEUE_TABLE}
        where cleanup_eligible_at is null
        group by data_source_name
        order by data_source_name
        """
    )
    with engine.connect() as connection:
        rows = connection.execute(query).mappings().all()

    workloads: list[QueueWorkload] = []
    for row in rows:
        source_name = str(row["data_source_name"])
        variable_count = variable_counts.get(source_name)
        if not variable_count:
            raise RuntimeError(
                f"Queue source {source_name!r} has no configured measurement variables."
            )
        workloads.append(
            QueueWorkload(
                data_source_name=source_name,
                queue_rows=int(row["queue_rows"] or 0),
                variable_count=variable_count,
            )
        )
    return workloads


def validate_incremental_queue_workload(engine, settings) -> list[QueueWorkload]:
    workloads = get_unmarked_queue_workload(engine, settings)
    expanded_rows = sum(workload.expanded_rows for workload in workloads)
    maximum = int(settings.MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS)
    if expanded_rows <= maximum:
        return workloads

    breakdown = ", ".join(
        f"{item.data_source_name}={item.queue_rows} queue/{item.expanded_rows} expanded"
        for item in workloads
    )
    raise RuntimeError(
        "Refusing unbounded canonical silver publication: the unmarked timestamp "
        f"queue would expand to {expanded_rows} rows, above the configured limit "
        f"of {maximum}. Breakdown: {breakdown}. Keep canonical_incremental paused "
        "and run canonical_measurement_queue_cutover in plan mode, then execute its "
        "bounded recovery windows."
    )


def acquire_measurement_publish_lock(engine):
    connection = engine.connect()
    try:
        acquired = bool(
            connection.execute(
                text("select pg_try_advisory_lock(hashtextextended(:name, 0))"),
                {"name": MEASUREMENT_PUBLISH_LOCK_NAME},
            ).scalar_one()
        )
        if not acquired:
            raise RuntimeError(
                "Another canonical measurement publisher holds the warehouse lock. "
                "Do not overlap incremental, backfill, full-refresh, or cutover runs."
            )
        active_backends = list(
            connection.execute(
                text(
                    """
                    select pid, application_name
                    from pg_stat_activity
                    where pid <> pg_backend_pid()
                      and state <> 'idle'
                      and (
                        application_name = 'dbt'
                        or application_name like 'respira_%'
                      )
                      and (
                        query like '%model.respira_data.fct_measurements_silver%'
                        or query like '%silver.fct_measurements_silver%'
                      )
                    order by pid
                    """
                )
            )
            .mappings()
            .all()
        )
        if active_backends:
            backend_summary = ", ".join(
                f"pid={row['pid']} application_name={row['application_name']}"
                for row in active_backends
            )
            raise RuntimeError(
                "An existing silver publication query is still active outside the "
                f"warehouse lock: {backend_summary}. Cancel and verify that backend "
                "before starting another canonical measurement flow."
            )
        return connection
    except Exception:
        connection.close()
        raise


def release_measurement_publish_lock(connection) -> None:
    try:
        connection.execute(
            text("select pg_advisory_unlock(hashtextextended(:name, 0))"),
            {"name": MEASUREMENT_PUBLISH_LOCK_NAME},
        )
    finally:
        connection.close()


@contextmanager
def measurement_publish_lock(engine) -> Iterator[None]:
    connection = acquire_measurement_publish_lock(engine)
    try:
        yield
    finally:
        release_measurement_publish_lock(connection)


def validate_legacy_runtime_tables(engine) -> None:
    query = text(
        """
        select c.relname, c.relkind
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = :schema_name
          and c.relname = any(:table_names)
        """
    )
    with engine.connect() as connection:
        rows = (
            connection.execute(
                query,
                {
                    "schema_name": QUEUE_SCHEMA,
                    "table_names": list(LEGACY_RUNTIME_TABLES),
                },
            )
            .mappings()
            .all()
        )

    kinds = {str(row["relname"]): str(row["relkind"]) for row in rows}
    missing = [name for name in LEGACY_RUNTIME_TABLES if name not in kinds]
    if missing:
        raise RuntimeError(
            "Cutover witness tables are missing: "
            + ", ".join(f"{QUEUE_SCHEMA}.{name}" for name in missing)
            + ". Restore both legacy runtime tables before running cutover."
        )
    invalid = [name for name, kind in kinds.items() if kind not in {"r", "p"}]
    if invalid:
        raise RuntimeError(
            "Cutover witness relations must be physical tables: "
            + ", ".join(f"{QUEUE_SCHEMA}.{name}" for name in invalid)
        )


def _existing_queue_indexes(connection) -> list[dict[str, Any]]:
    query = text(
        """
        select
          index_relation.relname as index_name,
          index_metadata.indisvalid,
          array_agg(column_metadata.attname order by key_position.ordinality)
            filter (where key_position.ordinality <= index_metadata.indnkeyatts)
            as key_columns
        from pg_index index_metadata
        join pg_class table_relation
          on table_relation.oid = index_metadata.indrelid
        join pg_namespace table_namespace
          on table_namespace.oid = table_relation.relnamespace
        join pg_class index_relation
          on index_relation.oid = index_metadata.indexrelid
        cross join lateral unnest(index_metadata.indkey)
          with ordinality as key_position(attribute_number, ordinality)
        left join pg_attribute column_metadata
          on column_metadata.attrelid = table_relation.oid
         and column_metadata.attnum = key_position.attribute_number
        where table_namespace.nspname = :schema_name
          and table_relation.relname = :table_name
          and index_metadata.indpred is null
          and key_position.attribute_number > 0
        group by index_relation.relname, index_metadata.indisvalid,
                 index_metadata.indnkeyatts
        """
    )
    return list(
        connection.execute(
            query,
            {"schema_name": QUEUE_SCHEMA, "table_name": QUEUE_TABLE},
        ).mappings()
    )


def get_measurement_queue_index_status(engine) -> dict[tuple[str, ...], bool]:
    with engine.connect() as connection:
        existing = _existing_queue_indexes(connection)
    return {
        columns: any(
            tuple(row["key_columns"] or ()) == columns and bool(row["indisvalid"])
            for row in existing
        )
        for _name, columns in QUEUE_INDEXES
    }


def ensure_measurement_queue_runtime_indexes(engine) -> None:
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        existing = _existing_queue_indexes(connection)
        preparer = connection.dialect.identifier_preparer

        for desired_name, columns in QUEUE_INDEXES:
            conflicting_names = [
                row
                for row in existing
                if str(row["index_name"]) == desired_name
                and tuple(row["key_columns"] or ()) != columns
            ]
            if conflicting_names:
                actual_columns = tuple(conflicting_names[0]["key_columns"] or ())
                raise RuntimeError(
                    f"Queue index {QUEUE_SCHEMA}.{desired_name} already exists with "
                    f"columns {actual_columns}, expected {columns}. Rename or remove "
                    "the conflicting index before cutover."
                )

            matching = [
                row for row in existing if tuple(row["key_columns"] or ()) == columns
            ]
            if any(bool(row["indisvalid"]) for row in matching):
                continue

            for row in matching:
                invalid_name = preparer.quote(str(row["index_name"]))
                connection.exec_driver_sql(
                    f"drop index concurrently if exists {QUEUE_SCHEMA}.{invalid_name}"
                )

            quoted_columns = ", ".join(preparer.quote(column) for column in columns)
            quoted_name = preparer.quote(desired_name)
            connection.exec_driver_sql(
                f"create index concurrently if not exists {quoted_name} "
                f"on {QUEUE_SCHEMA}.{QUEUE_TABLE} ({quoted_columns})"
            )
            existing.append(
                {
                    "index_name": desired_name,
                    "indisvalid": True,
                    "key_columns": list(columns),
                }
            )


def _scope_stats(
    engine,
    data_source_name: str,
    measured_at_from: datetime,
    measured_at_to: datetime,
) -> tuple[int, datetime | None, datetime | None]:
    query = text(
        f"""
        select
          count(*)::bigint as row_count,
          min(measured_at_silver) as min_measured_at,
          max(measured_at_silver) as max_measured_at
        from {QUEUE_SCHEMA}.{QUEUE_TABLE}
        where cleanup_eligible_at is null
          and data_source_name = :data_source_name
          and measured_at_silver >= :measured_at_from
          and measured_at_silver < :measured_at_to
        """
    )
    with engine.connect() as connection:
        row = (
            connection.execute(
                query,
                {
                    "data_source_name": data_source_name,
                    "measured_at_from": measured_at_from,
                    "measured_at_to": measured_at_to,
                },
            )
            .mappings()
            .one()
        )
    return (
        int(row["row_count"] or 0),
        row["min_measured_at"],
        row["max_measured_at"],
    )


def _split_queue_window(
    engine,
    data_source_name: str,
    measured_at_from: datetime,
    measured_at_to: datetime,
    variable_count: int,
    max_queue_rows: int,
    max_expanded_rows: int,
) -> list[QueueProcessWindow]:
    row_count, minimum, maximum = _scope_stats(
        engine, data_source_name, measured_at_from, measured_at_to
    )
    if row_count == 0 or minimum is None or maximum is None:
        return []

    expanded_rows = row_count * variable_count
    if row_count <= max_queue_rows and expanded_rows <= max_expanded_rows:
        return [
            QueueProcessWindow(
                data_source_name=data_source_name,
                measured_at_from=measured_at_from,
                measured_at_to=measured_at_to,
                queue_rows=row_count,
                expanded_rows=expanded_rows,
            )
        ]

    if minimum == maximum:
        return [
            QueueProcessWindow(
                data_source_name=data_source_name,
                measured_at_from=measured_at_from,
                measured_at_to=measured_at_to,
                queue_rows=row_count,
                expanded_rows=expanded_rows,
                oversized_single_timestamp=True,
            )
        ]

    midpoint = minimum + (maximum - minimum) / 2
    if midpoint <= minimum:
        midpoint = minimum + timedelta(microseconds=1)
    return [
        *_split_queue_window(
            engine,
            data_source_name,
            measured_at_from,
            midpoint,
            variable_count,
            max_queue_rows,
            max_expanded_rows,
        ),
        *_split_queue_window(
            engine,
            data_source_name,
            midpoint,
            measured_at_to,
            variable_count,
            max_queue_rows,
            max_expanded_rows,
        ),
    ]


def plan_unmarked_queue_windows(
    engine,
    data_source_name: str,
    variable_count: int,
    batch_hours: int,
    max_queue_rows: int,
    max_expanded_rows: int,
    measured_at_from: datetime | None = None,
    measured_at_to: datetime | None = None,
) -> list[QueueProcessWindow]:
    if batch_hours <= 0 or max_queue_rows <= 0 or max_expanded_rows <= 0:
        raise ValueError("Cutover window limits must be greater than zero.")
    if measured_at_from and measured_at_to and measured_at_from >= measured_at_to:
        raise ValueError("measured_at_from must be earlier than measured_at_to.")

    bucket_query = text(
        f"""
        select
          date_bin(
            make_interval(hours => :batch_hours),
            measured_at_silver,
            timestamptz '2000-01-01 00:00:00+00'
          ) as bucket_start,
          count(*)::bigint as row_count,
          min(measured_at_silver) as min_measured_at,
          max(measured_at_silver) as max_measured_at
        from {QUEUE_SCHEMA}.{QUEUE_TABLE}
        where cleanup_eligible_at is null
          and data_source_name = :data_source_name
          and measured_at_silver is not null
          and (
            cast(:measured_at_from as timestamptz) is null
            or measured_at_silver >= cast(:measured_at_from as timestamptz)
          )
          and (
            cast(:measured_at_to as timestamptz) is null
            or measured_at_silver < cast(:measured_at_to as timestamptz)
          )
        group by 1
        order by 1
        """
    )
    with engine.connect() as connection:
        buckets = (
            connection.execute(
                bucket_query,
                {
                    "data_source_name": data_source_name,
                    "batch_hours": batch_hours,
                    "measured_at_from": measured_at_from,
                    "measured_at_to": measured_at_to,
                },
            )
            .mappings()
            .all()
        )
    if not buckets:
        return []

    step = timedelta(hours=batch_hours)
    windows: list[QueueProcessWindow] = []
    for bucket in buckets:
        bucket_start = bucket["bucket_start"]
        bucket_end = bucket_start + step
        window_start = (
            max(bucket_start, measured_at_from)
            if measured_at_from is not None
            else bucket_start
        )
        window_end = (
            min(bucket_end, measured_at_to)
            if measured_at_to is not None
            else bucket_end
        )
        row_count = int(bucket["row_count"] or 0)
        expanded_rows = row_count * variable_count
        if row_count <= max_queue_rows and expanded_rows <= max_expanded_rows:
            windows.append(
                QueueProcessWindow(
                    data_source_name=data_source_name,
                    measured_at_from=window_start,
                    measured_at_to=window_end,
                    queue_rows=row_count,
                    expanded_rows=expanded_rows,
                )
            )
            continue
        windows.extend(
            _split_queue_window(
                engine,
                data_source_name,
                window_start,
                window_end,
                variable_count,
                max_queue_rows,
                max_expanded_rows,
            )
        )
    return windows


def get_unmarked_null_time_row_count(engine, data_source_name: str) -> int:
    query = text(
        f"""
        select count(*)::bigint
        from {QUEUE_SCHEMA}.{QUEUE_TABLE}
        where cleanup_eligible_at is null
          and data_source_name = :data_source_name
          and measured_at_silver is null
        """
    )
    with engine.connect() as connection:
        return int(
            connection.execute(
                query, {"data_source_name": data_source_name}
            ).scalar_one()
            or 0
        )
