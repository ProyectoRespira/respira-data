# respira-data

Data platform for Proyecto Respira.

This repository is organized as a modular monorepo:

- `dbt/models/canonical`: reusable canonical ingestion, normalization, dimensions, and silver layer
- `dbt/models/projects/respira_gold`: project-specific marts and inference features
- `prefect/flows`: orchestration for canonical and project pipelines
- Postgres as the local development warehouse

All commands are executed via Docker Compose.

## Quick start

1. Copy env file and set DB credentials:

```bash
cp .env.example .env
```

2. Start everything:

```bash
make up-build
```

This starts:

- `prefect_server`
- `app`
- `prefect_worker`

`prefect_worker` automatically:

- waits for Prefect API
- creates or updates work pool `default`
- registers canonical and project deployments
- starts a Prefect worker polling that pool

Open Prefect UI at `http://localhost:4200`.

## Useful commands

```bash
make run-canonical-incremental
make run-project-pipeline
make smoke-test
make logs
make down
```

## Adding a new Airbyte data source

Use this checklist whenever we connect a new Airbyte stream and want it to
flow through the canonical layer and into one or more projects.

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

   `dbt/models/canonical/intermediate/int_measurements_time_silver.sql`
   currently contains source-specific logic for `fiuna_airbyte`. If the new
   source has broken timestamps, delayed cursors, or custom imputation rules,
   add that logic there explicitly.

10. Validate the full path from canonical to project.

    Recommended commands:

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
