# respira-data

`respira-data` is the data platform for Proyecto Respira. It ingests raw sensor
data replicated by Airbyte into Postgres, builds a reusable canonical layer with
dbt, and orchestrates canonical plus project-specific pipelines with Prefect.

Today the only active project is `respira_gold`, but the repository is already
structured as a modular monorepo:

- `dbt/models/canonical`: reusable ingestion, normalization, dimensions, and silver models
- `dbt/models/projects/respira_gold`: project-specific marts and inference features
- `pipelines/flows`: Prefect orchestration for canonical and project pipelines
- `pipelines/config/projects.py`: registry of project-level runtime configuration
- `scripts/prefect_worker_start.sh`: worker bootstrap, deployment registration, and scheduling

## Deploy Quickstart

For a first working deploy against a clean database that already has raw Airbyte
tables in `airbyte`, use this sequence.

1. Prepare `.env`.

```bash
cp .env.example .env
```

Set at least:

- `REMOTE_PG_HOST`
- `REMOTE_PG_PORT`
- `REMOTE_PG_DB`
- `REMOTE_PG_USER`
- `REMOTE_PG_PASSWORD`
- `REMOTE_PG_SSLMODE`
- `MODEL_6H_PATH`
- `MODEL_12H_PATH`

Important:

- If the password contains `$`, wrap it in single quotes inside `.env`.
- `MODEL_6H_PATH` and `MODEL_12H_PATH` must point to files available inside the
  containers, usually under `/app/models/...`.

2. Start the local stack.

```bash
docker compose -f docker-compose.yml up -d --build
docker compose ps
```

For development mode with host bind mounts (official multi-file override
pattern), use:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.dev.yml ps
```

3. Validate dbt connectivity.

```bash
docker compose exec app bash -lc "cd /app/dbt && dbt debug --target prod"
```

4. Install dbt packages and load seeds.

```bash
docker compose exec app bash -lc "cd /app/dbt && dbt clean"
docker compose exec app bash -lc "cd /app/dbt && dbt deps"
docker compose exec app bash -lc "cd /app/dbt && dbt seed --target prod --full-refresh"
```

5. Build canonical layers.

```bash
docker compose exec app bash -lc "cd /app/dbt && dbt run --target prod --selector canonical_core"
docker compose exec app bash -lc "cd /app/dbt && dbt run --target prod --selector canonical_silver"
```

6. Create operational and inference tables.

```bash
docker compose exec app bash -lc "cd /app && python3 pipelines/flows/warehouse_bootstrap.py"
```

7. Build the project layer.

```bash
docker compose exec app bash -lc "cd /app/dbt && dbt run --target prod --selector project_respira_gold"
docker compose exec app bash -lc "cd /app/dbt && dbt test --target prod --selector project_respira_gold_tests"
```

8. Run inference or the full project pipeline

```bash
docker compose exec prefect_worker bash -lc "cd /app && python3 pipelines/flows/project_inference.py"
docker compose exec prefect_worker bash -lc "cd /app && python3 pipelines/flows/project_pipeline.py"
```

9. Optional: trigger Prefect deployments from the local server instead of
running the flow files directly.

```bash
docker compose exec prefect_server prefect deployment ls
docker compose exec prefect_server prefect deployment run 'warehouse_bootstrap/warehouse-bootstrap'
docker compose exec prefect_server prefect deployment run 'canonical_incremental/canonical-incremental'
docker compose exec prefect_server prefect deployment run 'project_pipeline/project-pipeline-respira_gold'
```

## Deploy Notes

- `warehouse_bootstrap.py` uses `create schema if not exists ...`, so the DB
  runtime user must have `CREATE` on the database, or those bootstrap steps
  will fail.
- `station_inference_features` is now a persisted incremental table. If you are
  upgrading from an older local state where it exists as a view, drop it before
  rebuilding:

```bash
docker compose exec app bash -lc 'PGPASSWORD="$REMOTE_PG_PASSWORD" psql "host=$REMOTE_PG_HOST port=$REMOTE_PG_PORT dbname=$REMOTE_PG_DB user=$REMOTE_PG_USER sslmode=$REMOTE_PG_SSLMODE" -c "drop view if exists respira_gold.station_inference_features cascade;"'
```

- `respira_gold.inference_results` now stores one row per
  `inference_run_id + station_id` with `forecast_6h`, `forecast_12h`, and
  `aqi_input`. If you still have the old table shape from a previous run,
  recreate it before bootstrapping again:

```bash
docker compose exec app bash -lc 'PGPASSWORD="$REMOTE_PG_PASSWORD" psql "host=$REMOTE_PG_HOST port=$REMOTE_PG_PORT dbname=$REMOTE_PG_DB user=$REMOTE_PG_USER sslmode=$REMOTE_PG_SSLMODE" -c "drop table if exists respira_gold.inference_results cascade;"'
docker compose exec app bash -lc "cd /app && python3 pipelines/flows/warehouse_bootstrap.py"
```

## What a DevOps Teammate Should Know First

- Docker Compose does not start a local Postgres instance. This stack connects
  to an external Postgres warehouse configured through `.env`.
- Airbyte is assumed to replicate raw tables into the `airbyte` schema of that
  external Postgres database.
- The worker auto-registers Prefect deployments on startup. In local
  development, the worker bootstrap script is the operational source of truth
  for schedules.
- The platform has two pipeline layers:
  - `canonical_*` builds reusable shared data
  - `project_*` builds project-specific marts and optional inference outputs
- All timestamps in the silver layer are expected to be UTC. FIUNA source
  timestamps arrive as local UTC-3 wall-clock time and are converted to UTC in
  staging before they reach silver.

## Runtime Topology

`docker-compose.yml` starts three services:

- `prefect_server`: local Prefect API and UI on `http://localhost:4200`
- `app`: generic runner container used for dbt commands, ad hoc flow execution,
  and local shell access
- `prefect_worker`: long-running worker process that creates deployments and
  polls the Prefect work pool

Important runtime details:

- Base deploy mode (`docker-compose.yml`) runs image-baked code for both `app`
  and `prefect_worker` (no host bind mounts to `/app`).
- Development mode (`docker-compose.dev.yml`) restores host bind mounts to
  `/app` for both `app` and `prefect_worker`.
- In development mode, ensure `HOST_WORKSPACE_FOLDER` points to the repository
  root containing `pipelines/flows` to avoid path mismatches.
- `prefect_worker` uses `Dockerfile.worker`, which includes the extra inference
  dependencies.
- `app` uses `Dockerfile` and is the default place for dbt commands.
- `make smoke-test` runs with host `poetry`, not inside Docker Compose.

## Repository Layout

- `dbt/`: dbt project, macros, seeds, and models
- `dbt/models/canonical/`: shared canonical models
- `dbt/models/projects/respira_gold/`: project-specific models for `respira_gold`
- `dbt/seeds/`: metadata for organizations, projects, variables, stations, and project scoping
- `pipelines/flows/`: Prefect flows such as `canonical_incremental` and `project_pipeline`
- `pipelines/tasks/`: dbt execution, warehouse bootstrap, inference, notifications, and audit helpers
- `pipelines/config/`: runtime settings, dbt selectors, and registered projects
- `pipelines/sql/`: SQL used by operational bootstrap tasks
- `scripts/`: operational helper scripts, especially worker startup and deployment registration
- `tests/`: orchestration and inference-adjacent tests
- `src/inference/`: inference runtime code used by project inference flows

## Data and Schema Model

The warehouse is organized into logical schemas:

- `airbyte`: raw replicated source tables, managed outside this repo
- `staging`: source-specific dbt staging views
- `intermediate`: dbt normalization and shaping views
- `core`: canonical dimensions and metadata models
- `silver`: canonical reusable fact layer
- `respira_gold`: project-specific marts, features, and inference tables
- `ops`: operational audit, measurement stream state, and inference status tables

Current architectural rules:

- Canonical models should not depend on project-specific marts.
- Project scope is metadata-driven through seeds such as
  `project_data_sources.csv` and `project_organizations.csv`.
- Project-specific inference tables live in the project schema, while audit
  tables live in `ops`.

## Environment Variables

The values in `.env` are loaded into both the `app` and `prefect_worker`
containers. For dbt-based operations, the `REMOTE_PG_*` values are still
required because `dbt/profiles.yml` reads them directly.

Required database settings:

- `REMOTE_PG_HOST`
- `REMOTE_PG_PORT`
- `REMOTE_PG_DB`
- `REMOTE_PG_USER`
- `REMOTE_PG_PASSWORD`
- `REMOTE_PG_SSLMODE`

Optional database setting for Python tasks:

- `DB_DSN`: optional SQLAlchemy DSN for non-dbt Python tasks. Useful, but it
  does not replace the `REMOTE_PG_*` values required by dbt.

Prefect and worker settings:

- `PREFECT_API_URL`: defaults to `http://prefect_server:4200/api`
- `PREFECT_WORKER_TYPE`: defaults to `process`
- `PREFECT_CANONICAL_WORK_POOL`: defaults to `canonical`
- `PREFECT_PROJECT_RESPIRA_GOLD_WORK_POOL`: defaults to `respira_gold`
- `PREFECT_SCHEDULE_TIMEZONE`: defaults to `UTC`

Schedule settings:

- `PREFECT_CANONICAL_INCREMENTAL_CRON`: defaults to `5 * * * *`
- `PREFECT_PROJECT_RESPIRA_GOLD_CRON`: defaults to `20 * * * *`

dbt runtime settings:

- `DBT_TARGET`: defaults to `prod`
- `DBT_THREADS`: defaults to `1`
- `DBT_TIMEOUT_CANONICAL_CORE_S`
- `DBT_TIMEOUT_CANONICAL_SILVER_S`
- `DBT_TIMEOUT_PROJECT_S`
- `DBT_TIMEOUT_TESTS_S`

Inference settings:

- `MODEL_6H_PATH`
- `MODEL_12H_PATH`
- `MODEL_6H_VERSION`
- `MODEL_12H_VERSION`
- `DEFAULT_WINDOW_HOURS`
- `INFERENCE_MIN_POINTS`

Alerting:

- `SLACK_WEBHOOK_URL`: optional; used for flow failure alerts and dbt test alerts

## Local Bootstrap

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Fill in the remote warehouse credentials and, if scheduled inference is
   needed, set `MODEL_6H_PATH` and `MODEL_12H_PATH`.

3. Build and start the stack:

```bash
make up-build
```

4. Open Prefect UI at `http://localhost:4200`.

5. For a fresh or reset database, run the initial bootstrap and first load:

```bash
make prefect-bootstrap
make run-canonical-incremental
make run-project-pipeline
```

What happens automatically when `prefect_worker` starts:

- waits for the Prefect API health check
- creates or updates the `canonical` and `respira_gold` work pools
- deploys `warehouse_bootstrap`
- deploys `canonical_measurement_backfill` without a schedule
- deploys `canonical_incremental`
- deploys `canonical_full_refresh`
- deploys `project_pipeline(project_code=respira_gold)`
- starts one worker process per configured work pool

If both `MODEL_6H_PATH` and `MODEL_12H_PATH` are present, the project pipeline
is deployed with its schedule. If either model path is missing, the deployment
is still created but without a schedule.

## Daily Operations

Common commands:

```bash
make up
make up-build
make down
make ps
make logs
make logs-worker
make shell
make dbt-debug
make prefect-bootstrap
make run-canonical-incremental
make run-canonical-full-refresh
make run-project-pipeline
make run-project-inference
make run-social-broadcast
make smoke-test
```

What each operational command does:

- `make prefect-bootstrap`: creates `ops` audit tables and project inference
  tables, but does not run dbt
- `make run-canonical-incremental`: runs `dbt deps`, loads and tests shared
  core seeds, then runs canonical core and canonical silver
- `make run-canonical-full-refresh`: manual maintenance flow for a full
  shared-seed reload, canonical rebuild, and tests
- `make run-project-pipeline`: runs dbt for `respira_gold`, project tests, and
  inference if enabled
- `make run-project-inference`: runs inference only
- `make run-social-broadcast`: runs social broadcast generation/publishing flow
- `make smoke-test`: lightweight orchestration test suite on the host machine

dbt-only layered commands:

```bash
make run-canonical-core
make run-canonical-silver
make run-project-respira_gold
make build
make build-fr
```

Use `build-fr` after major schema or logic changes that require a full dbt
rebuild.

## Prefect Deployment and Scheduling Model

The local scheduling model is controlled by
`scripts/prefect_worker_start.sh`.

Current behavior:

- `warehouse_bootstrap` is deployed without a schedule and is intended to be
  triggered after deploys or when runtime tables must be re-ensured
- `canonical_incremental` is deployed on a cron schedule
- `canonical_full_refresh` is deployed without a schedule and is intended to be
  manual
- `canonical_measurement_backfill` is deployed without a schedule for bounded,
  parameterized backfills and queue-resume validation from the Prefect UI
- `project_pipeline(project_code=respira_gold)` is deployed on a cron schedule
  only when both model paths are configured
- the worker re-registers these deployments every time it restarts

Operational implications:

- If you change cron settings, restart `prefect_worker` so deployments are
  re-created with the new schedule.
- `social_broadcast` defaults to `0 11,20 * * *` in `UTC` and can be
  overridden with `PREFECT_SOCIAL_BROADCAST_CRON`.
- If you add model paths after startup, restart `prefect_worker` to attach the
  project schedule.
- Editing deployment YAML files in `pipelines/deployments/` is not enough for
  local behavior unless the worker bootstrap logic is updated or deployments are
  re-applied explicitly.

## Observability and Audit Tables

This repository uses warehouse tables for runtime auditability.

Created by `make prefect-bootstrap`:

- `ops.dbt_run_audit`
- `ops.measurement_stream_state`
- `ops.inference_station_status`
- `respira_gold.inference_runs`
- `respira_gold.inference_results`

Useful operational checks:

- use Prefect UI for run history and task logs
- use `make logs-worker` for deployment and worker startup issues
- inspect `ops.dbt_run_audit` for dbt command status and summaries
- inspect `ops.inference_station_status` for per-station inference failures
- inspect `respira_gold.inference_runs` and `respira_gold.inference_results`
  for project inference lifecycle and outputs

## Common Runbooks

Fresh database or rebuilt warehouse:

```bash
make prefect-bootstrap
make dbt-deps
make seed
make run-canonical-incremental
make run-project-pipeline
```

Troubleshooting dbt connectivity:

```bash
make dbt-debug
make logs
make logs-worker
```

If a project deployment is missing from Prefect UI:

1. Check `make logs-worker`
2. Confirm `PREFECT_API_URL` and work pool settings
3. Confirm `MODEL_6H_PATH` and `MODEL_12H_PATH` if the schedule should exist
4. Restart the worker with `make down` and `make up-build`

If inference should run but does not:

1. Confirm the project has `inference_enabled=True` in
   `pipelines/config/projects.py`
2. Confirm model paths exist inside the container filesystem
3. Run `make run-project-inference`
4. Inspect `ops.inference_station_status` and `respira_gold.inference_runs`

If you change project registration:

1. Update `pipelines/config/projects.py`
2. Add or update the dbt models under `dbt/models/projects/<project_code>`
3. Update seeds for project metadata and project-data-source membership
4. Update worker bootstrap deployment logic if the new project needs scheduling
5. Restart the worker to register the new deployment

## Extending the Platform

To add a new project:

- create `dbt/models/projects/<project_code>/`
- register the project in `dbt/seeds/projects.csv`
- add `project_data_sources.csv` and `project_organizations.csv` entries
- add a `ProjectConfig` entry in `pipelines/config/projects.py`
- decide whether the project has inference and, if yes, define its source and
  result tables
- update deployment/bootstrap logic if the project should run on a schedule

To add a new Airbyte data source, use the checklist below.

## Adding a New Airbyte Data Source

Use this checklist whenever we connect a new Airbyte stream and want it to flow
through the canonical layer and into one or more projects.

1. Define the canonical source name.

   Use a stable snake_case name such as `my_provider_airbyte`. This is the
   identifier that will appear in dbt models, seeds, project scoping, and
   audits.

2. Register the raw Airbyte table in dbt sources.

   Add the replicated raw table name under
   `dbt/models/canonical/sources/sources_airbyte.yml`.

   If Airbyte creates multiple raw tables for the same provider, list all of
   them there.

3. Create a staging model in `dbt/models/canonical/staging`.

   Add a model such as `stg_my_provider_measurements.sql` that reads from the
   raw Airbyte source and normalizes it to the canonical staging contract.

   Every staging model should emit at least:
   - `_airbyte_raw_id`
   - `extracted_at`
   - `data_source_name`
   - `station_code`
   - `measured_at_raw`
   - `measured_at_parsed`
   - `is_measured_at_valid`
   - `raw_payload`

   Add `cursor_id` when the source has a reliable sequential identifier, and
   keep any extra columns needed later for station enrichment.

   Sources that need stateful timestamp cleanup should use the FIUNA-style
   three-layer pattern: a raw normalization view, a persisted repaired table,
   and a thin public `stg_*` view. Keep source-specific continuity out of the
   shared intermediate models.

4. Add tests and documentation for the new staging model.

   Register the model in `dbt/models/canonical/staging/schema.yml` with:
   - `not_null` tests on the canonical required fields
   - an `accepted_values` test for `data_source_name`
   - uniqueness tests if the source has a natural cursor or key

5. Register the source in `dbt/dbt_project.yml`.

   Add a new entry under `vars.measurements_sources` with:
   - `relation`
   - `station_code_col`
   - `measured_at_col`
   - `raw_payload_col`
   - `is_measured_at_valid_col`
   - `cursor_id_col` when available
   - the complete optional prepared-time trio: `measured_at_silver_col`,
     `is_time_imputed_col`, and `time_impute_method_col`
   - the `variables` mapping from canonical variable code to staging column

   `int_measurements_long` uses this registry to union all measurement sources,
   so forgetting this step means the new source will never reach silver.

6. Add metadata seeds for the new source.

   Update:
   - `dbt/seeds/data_sources.csv` to register the source and its
     `organization_code`
   - `dbt/seeds/project_data_sources.csv` for every project that should consume
     it
   - `dbt/seeds/project_organizations.csv` if the organization is now part of a
     project
   - `dbt/seeds/organizations.csv` if this is a brand-new organization

7. Add or update variable metadata if the source introduces new measurements.

   If the source contains variables we do not model yet, update:
   - `dbt/seeds/variables.csv`
   - `dbt/seeds/variable_rules.csv` when parsing or validation rules are needed

8. Update station enrichment if the source contributes station metadata.

   If the new Airbyte payload provides coordinates, names, or station
   descriptors that should feed the canonical station dimension, update
   `dbt/models/canonical/intermediate/int_stations_candidates.sql`.

   If the source depends on hand-maintained station metadata, update
   `dbt/seeds/stations_static.csv` instead.

9. Add timestamp repair logic if the source needs custom handling.

   Implement source-specific repair in a persisted prepared staging model and
   expose the three canonical time fields through the public staging view.
   Register all three prepared-time columns together; sources without custom
   repair retain the generic parsed-time pass-through behavior.

10. Validate the full path from canonical to project.

```bash
make dbt-deps
make seed
make run-canonical-incremental
make run-project-pipeline
make smoke-test
```

After that, confirm that:

- the new source appears in canonical silver outputs
- `respira_gold` only receives it if it was added to
  `dbt/seeds/project_data_sources.csv`
- station and variable dimensions look correct
- there are no leftover hardcoded references to the old source set
