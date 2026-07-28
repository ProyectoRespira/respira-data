from __future__ import annotations

from typing import Any, TypedDict

from sqlalchemy.engine import Engine

from pipelines.compat import task
from pipelines.tasks.db import OPS_AUDIT_SQL, execute_sql_file_on_connection

NO_CACHE: Any | None
try:
    from prefect.cache_policies import NO_CACHE as _PREFECT_NO_CACHE
except Exception:  # noqa: BLE001
    NO_CACHE = None
else:
    NO_CACHE = _PREFECT_NO_CACHE

BOOTSTRAP_LOCK_NAME = "respira.measurement_stream_state_bootstrap"


class MeasurementStreamStateBootstrapResult(TypedDict):
    published_streams: int
    candidates: int
    inserted: int
    updated: int
    unchanged: int
    affected: int
    missing: int
    extra: int
    mismatched: int


class MeasurementStreamStateBootstrapError(RuntimeError):
    """Raised when stream-state bootstrap coverage is incomplete or inconsistent."""


CREATE_CANDIDATES_SQL = """
create temporary table measurement_stream_state_bootstrap_candidates
on commit drop
as
with latest_published as (
    select
        streams.id as stream_id,
        data_sources.name::text as data_source_name,
        stations.code::text as station_code,
        variables.code::text as variable_code,
        published.source_row_id::text as source_row_id,
        published.timestamp as measured_at_silver,
        published.ingested_at as extracted_at,
        published.value_parsed::double precision as value_silver
    from core.dim_streams streams
    join lateral (
        select
            fact.source_row_id,
            fact.timestamp,
            fact.ingested_at,
            fact.value_parsed
        from silver.fct_measurements_silver fact
        where fact.stream_id = streams.id
        order by
            fact.timestamp desc,
            fact.ingested_at desc,
            fact.source_row_id desc
        limit 1
    ) published on true
    join core.dim_data_sources data_sources
      on data_sources.id = streams.data_source_id
    join core.dim_stations stations
      on stations.id = streams.station_id
    join core.dim_variables variables
      on variables.id = streams.variable_id
)
select
    published.stream_id,
    published.data_source_name,
    published.station_code,
    published.variable_code,
    published.measured_at_silver as last_measured_at_silver,
    intermediate.cursor_id as last_cursor_id,
    published.extracted_at as last_extracted_at,
    published.source_row_id as last_source_row_id,
    published.value_silver as last_value_silver,
    intermediate.source_row_id is not null
      and intermediate.data_source_name is not distinct from published.data_source_name
      and intermediate.station_code is not distinct from published.station_code
      and intermediate.variable_code is not distinct from published.variable_code
      and intermediate.measured_at_silver is not distinct from published.measured_at_silver
      and intermediate.extracted_at is not distinct from published.extracted_at
      and intermediate.value_silver is not distinct from published.value_silver
      as is_exact_match
from latest_published published
left join intermediate.int_measurements_values_silver intermediate
  on intermediate.source_row_id = published.source_row_id
 and intermediate.variable_code = published.variable_code
"""

CANDIDATE_METRICS_SQL = """
select
    count(*)::bigint as published_streams,
    count(*) filter (where is_exact_match)::bigint as candidates,
    count(*) filter (where not is_exact_match)::bigint as missing,
    count(*) filter (
        where is_exact_match
          and (
            data_source_name is null
            or station_code is null
            or variable_code is null
            or last_measured_at_silver is null
            or last_extracted_at is null
            or last_source_row_id is null
            or last_value_silver is null
          )
    )::bigint as incomplete,
    (
        select count(*)::bigint
        from (
            select data_source_name, station_code, variable_code
            from measurement_stream_state_bootstrap_candidates
            where is_exact_match
            group by data_source_name, station_code, variable_code
            having count(*) > 1
        ) duplicate_streams
    ) as duplicate_streams
from measurement_stream_state_bootstrap_candidates
"""

CLASSIFY_CHANGES_SQL = """
select
    count(*) filter (
        where state.data_source_name is null
    )::bigint as inserted,
    count(*) filter (
        where state.data_source_name is not null
          and (
            candidate.last_measured_at_silver,
            coalesce(candidate.last_cursor_id, -1),
            candidate.last_extracted_at,
            candidate.last_source_row_id
          ) > (
            state.last_measured_at_silver,
            coalesce(state.last_cursor_id, -1),
            state.last_extracted_at,
            state.last_source_row_id
          )
    )::bigint as updated
from measurement_stream_state_bootstrap_candidates candidate
left join ops.measurement_stream_state state
  using (data_source_name, station_code, variable_code)
where candidate.is_exact_match
"""

UPSERT_STATE_SQL = """
insert into ops.measurement_stream_state as state (
    data_source_name,
    station_code,
    variable_code,
    last_measured_at_silver,
    last_cursor_id,
    last_extracted_at,
    last_source_row_id,
    last_value_silver,
    updated_at
)
select
    data_source_name,
    station_code,
    variable_code,
    last_measured_at_silver,
    last_cursor_id,
    last_extracted_at,
    last_source_row_id,
    last_value_silver,
    now()
from measurement_stream_state_bootstrap_candidates
where is_exact_match
on conflict (data_source_name, station_code, variable_code) do update
set
    last_measured_at_silver = excluded.last_measured_at_silver,
    last_cursor_id = excluded.last_cursor_id,
    last_extracted_at = excluded.last_extracted_at,
    last_source_row_id = excluded.last_source_row_id,
    last_value_silver = excluded.last_value_silver,
    updated_at = now()
where (
    excluded.last_measured_at_silver,
    coalesce(excluded.last_cursor_id, -1),
    excluded.last_extracted_at,
    excluded.last_source_row_id
) > (
    state.last_measured_at_silver,
    coalesce(state.last_cursor_id, -1),
    state.last_extracted_at,
    state.last_source_row_id
)
"""

POST_VALIDATION_SQL = """
select
    (
        select count(*)::bigint
        from measurement_stream_state_bootstrap_candidates candidate
        left join ops.measurement_stream_state state
          using (data_source_name, station_code, variable_code)
        where candidate.is_exact_match
          and state.data_source_name is null
    ) as missing,
    (
        select count(*)::bigint
        from ops.measurement_stream_state state
        left join measurement_stream_state_bootstrap_candidates candidate
          on candidate.is_exact_match
         and candidate.data_source_name = state.data_source_name
         and candidate.station_code = state.station_code
         and candidate.variable_code = state.variable_code
        where candidate.data_source_name is null
    ) as extra,
    (
        select count(*)::bigint
        from measurement_stream_state_bootstrap_candidates candidate
        join ops.measurement_stream_state state
          using (data_source_name, station_code, variable_code)
        where candidate.is_exact_match
          and (
            state.last_measured_at_silver is distinct from candidate.last_measured_at_silver
            or state.last_cursor_id is distinct from candidate.last_cursor_id
            or state.last_extracted_at is distinct from candidate.last_extracted_at
            or state.last_source_row_id is distinct from candidate.last_source_row_id
            or state.last_value_silver is distinct from candidate.last_value_silver
          )
    ) as mismatched
"""


def _integer_metrics(row: Any) -> dict[str, int]:
    return {key: int(value or 0) for key, value in row.items()}


def _validate_candidate_metrics(metrics: dict[str, int]) -> None:
    if metrics["published_streams"] == 0:
        raise MeasurementStreamStateBootstrapError(
            "No published silver streams were found; refusing to bootstrap empty state."
        )

    errors: list[str] = []
    if metrics["missing"]:
        errors.append(f"missing_or_mismatched={metrics['missing']}")
    if metrics["incomplete"]:
        errors.append(f"incomplete={metrics['incomplete']}")
    if metrics["duplicate_streams"]:
        errors.append(f"duplicate_streams={metrics['duplicate_streams']}")
    if metrics["candidates"] != metrics["published_streams"]:
        errors.append(
            "candidate_count=" f"{metrics['candidates']}/{metrics['published_streams']}"
        )

    if errors:
        raise MeasurementStreamStateBootstrapError(
            "Stream-state bootstrap candidate validation failed: " + ", ".join(errors)
        )


def _validate_post_metrics(metrics: dict[str, int]) -> None:
    errors = [f"{key}={value}" for key, value in metrics.items() if value]
    if errors:
        raise MeasurementStreamStateBootstrapError(
            "Stream-state bootstrap post-validation failed: " + ", ".join(errors)
        )


def _bootstrap_measurement_stream_state(
    engine: Engine, statement_timeout_seconds: int
) -> MeasurementStreamStateBootstrapResult:
    if statement_timeout_seconds <= 0:
        raise ValueError("statement_timeout_seconds must be greater than zero.")

    timeout_ms = statement_timeout_seconds * 1000

    with engine.connect() as connection, connection.begin():
        connection.exec_driver_sql("set transaction isolation level repeatable read")
        connection.exec_driver_sql(f"set local statement_timeout = {timeout_ms}")
        connection.exec_driver_sql("set local lock_timeout = 30000")
        connection.exec_driver_sql(
            "select pg_advisory_xact_lock(hashtext(%s))", (BOOTSTRAP_LOCK_NAME,)
        )

        execute_sql_file_on_connection(connection, OPS_AUDIT_SQL)
        connection.exec_driver_sql(CREATE_CANDIDATES_SQL)
        connection.exec_driver_sql(
            "analyze measurement_stream_state_bootstrap_candidates"
        )

        candidate_metrics = _integer_metrics(
            connection.exec_driver_sql(CANDIDATE_METRICS_SQL).mappings().one()
        )
        _validate_candidate_metrics(candidate_metrics)

        change_metrics = _integer_metrics(
            connection.exec_driver_sql(CLASSIFY_CHANGES_SQL).mappings().one()
        )
        affected = connection.exec_driver_sql(UPSERT_STATE_SQL).rowcount
        expected_affected = change_metrics["inserted"] + change_metrics["updated"]
        if affected != expected_affected:
            raise MeasurementStreamStateBootstrapError(
                "Stream-state bootstrap affected-row count was inconsistent: "
                f"expected={expected_affected}, actual={affected}."
            )

        post_metrics = _integer_metrics(
            connection.exec_driver_sql(POST_VALIDATION_SQL).mappings().one()
        )
        _validate_post_metrics(post_metrics)

        unchanged = candidate_metrics["candidates"] - expected_affected
        return {
            "published_streams": candidate_metrics["published_streams"],
            "candidates": candidate_metrics["candidates"],
            "inserted": change_metrics["inserted"],
            "updated": change_metrics["updated"],
            "unchanged": unchanged,
            "affected": affected,
            "missing": post_metrics["missing"],
            "extra": post_metrics["extra"],
            "mismatched": post_metrics["mismatched"],
        }


@task(
    name="bootstrap_measurement_stream_state",
    **({"cache_policy": NO_CACHE} if NO_CACHE is not None else {}),
)
def bootstrap_measurement_stream_state(
    engine: Engine, statement_timeout_seconds: int
) -> MeasurementStreamStateBootstrapResult:
    return _bootstrap_measurement_stream_state(engine, statement_timeout_seconds)
