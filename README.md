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
