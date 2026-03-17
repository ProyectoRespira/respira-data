# respira-data

Data platform for Proyecto Respira.

This repository contains:
- dbt models (bronze/silver/gold)
- Prefect flows for orchestration
- Postgres as the local development warehouse

All commands are executed via Docker Compose.

## Quick start (clean containers)

1. Copy env file and set DB credentials:

```bash
cp .env.example .env
```

2. Start everything:

```bash
make up-build
```

This will start:
- `prefect_server`
- `app`
- `prefect_worker`

`prefect_worker` automatically:
- waits for Prefect API
- creates/updates work pool `default`
- registers deployments
- starts a Prefect worker polling that pool

Open Prefect UI at `http://localhost:4200`.

## Useful commands

```bash
make ps
make logs
make logs-worker
make smoke-test
make down
```
