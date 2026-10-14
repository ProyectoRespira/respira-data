SHELL := /bin/bash
.DEFAULT_GOAL := help

# Docker Compose wrapper (v2)
DC := docker compose
DC_DEV := docker compose -f docker-compose.yml -f docker-compose.dev.yml

# Run dbt inside the app container (ephemeral)
DBT := $(DC) run --rm app bash -lc

# Run Prefect flows inside the app container (ephemeral)
PREFECT_RUN := $(DC) run --rm app bash -lc

# Run inference-related flows inside the worker container (ephemeral)
WORKER_RUN := $(DC) run --rm prefect_worker bash -lc

# Optional: run arbitrary shell inside the app container (ephemeral)
APP_SHELL := $(DC) run --rm app bash
APP_SHELL_DEV := $(DC_DEV) run --rm app bash

.PHONY: help
help:
	@echo "Targets:"
	@echo "  up                Start services (prefect_server, app, prefect_worker)"
	@echo "  up-build          Rebuild images and start all services"
	@echo "  up-dev            Start services in dev mode (with bind mounts)"
	@echo "  up-build-dev      Rebuild and start services in dev mode"
	@echo "  down              Stop services"
	@echo "  down-dev          Stop services in dev mode"
	@echo "  ps                Show running containers"
	@echo "  ps-dev            Show running containers in dev mode"
	@echo "  logs              Tail logs (all services)"
	@echo "  logs-dev          Tail logs (all services) in dev mode"
	@echo "  logs-worker       Tail logs (prefect_worker)"
	@echo "  logs-worker-dev   Tail logs (prefect_worker) in dev mode"
	@echo "  shell             Open a shell in the app container"
	@echo "  shell-dev         Open a shell in the app container (dev mode)"
	@echo ""
	@echo "dbt:"
	@echo "  dbt-debug         dbt debug"
	@echo "  dbt-deps          dbt deps"
	@echo "  seed              dbt seed"
	@echo "  seed-fr           dbt seed --full-refresh"
	@echo "  run               dbt run (all models)"
	@echo "  test              dbt test (all tests)"
	@echo ""
	@echo "Layered runs:"
	@echo "  run-canonical-core    dbt run --selector canonical_core"
	@echo "  run-canonical-silver  dbt run --selector canonical_silver"
	@echo "  run-project-respira_gold dbt run --selector project_respira_gold"
	@echo ""
	@echo "Build flows:"
	@echo "  build             deps + seed + run-all + test"
	@echo "  build-fr          deps + seed(full refresh) + run(full refresh) + test"
	@echo ""
	@echo "Prefect:"
	@echo "  prefect-bootstrap Ensure ops tables and project inference tables"
	@echo "  run-canonical-incremental Run canonical_incremental flow"
	@echo "  run-canonical-full-refresh Run canonical_full_refresh flow (manual)"
	@echo "  run-canonical-measurement-backfill Run canonical_measurement_backfill flow (manual)"
	@echo "  run-canonical-measurement-queue-cutover-plan Plan guarded queue cutover recovery (manual)"
	@echo "  run-project-pipeline Run project_pipeline for respira_gold"
	@echo "  run-project-inference Run project_inference for respira_gold"
	@echo "  run-social-broadcast Run social_broadcast flow for respira_gold"
	@echo "  smoke-test        Run minimal unit tests for orchestration"
	@echo ""
	@echo "Selection helpers:"
	@echo "  ls                dbt ls"
	@echo "  docs              dbt docs generate"
	@echo ""
	@echo "Variables:"
	@echo "  TARGET=prod       (default: prod)"

# Default dbt target (use --target prod for now)
TARGET ?= prod

.PHONY: up
up:
	$(DC) up -d

.PHONY: up-build
up-build:
	$(DC) up -d --build

.PHONY: up-dev
up-dev:
	$(DC_DEV) up -d

.PHONY: up-build-dev
up-build-dev:
	$(DC_DEV) up -d --build

.PHONY: down
down:
	$(DC) down

.PHONY: down-dev
down-dev:
	$(DC_DEV) down

.PHONY: ps
ps:
	$(DC) ps

.PHONY: ps-dev
ps-dev:
	$(DC_DEV) ps

.PHONY: logs
logs:
	$(DC) logs -f --tail=200

.PHONY: logs-dev
logs-dev:
	$(DC_DEV) logs -f --tail=200

.PHONY: logs-worker
logs-worker:
	$(DC) logs -f --tail=200 prefect_worker

.PHONY: logs-worker-dev
logs-worker-dev:
	$(DC_DEV) logs -f --tail=200 prefect_worker

.PHONY: shell
shell:
	$(APP_SHELL)

.PHONY: shell-dev
shell-dev:
	$(APP_SHELL_DEV)

# -----------------------
# dbt basics
# -----------------------
.PHONY: dbt-debug
dbt-debug:
	$(DBT) "cd dbt && dbt debug --target $(TARGET)"

.PHONY: dbt-deps
dbt-deps:
	$(DBT) "cd dbt && dbt deps"

.PHONY: seed
seed:
	$(DBT) "cd dbt && dbt seed --target $(TARGET)"

.PHONY: seed-fr
seed-fr:
	$(DBT) "cd dbt && dbt seed --target $(TARGET) --full-refresh"

.PHONY: run
run:
	$(DBT) "cd dbt && dbt run --target $(TARGET)"

.PHONY: test
test:
	$(DBT) "cd dbt && dbt test --target $(TARGET)"

.PHONY: ls
ls:
	$(DBT) "cd dbt && dbt ls --target $(TARGET)"

.PHONY: docs
docs:
	$(DBT) "cd dbt && dbt docs generate --target $(TARGET)"

# -----------------------
# Layered runs
# -----------------------
.PHONY: run-canonical-core
run-canonical-core:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_core"

.PHONY: run-canonical-silver
run-canonical-silver:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_silver"

.PHONY: run-project-respira_gold
run-project-respira_gold:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector project_respira_gold"

# -----------------------
# Build flows
# -----------------------
.PHONY: build
build: dbt-deps seed run test

# Full refresh for when schemas/logic change significantly
.PHONY: build-fr
build-fr: dbt-deps seed-fr
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_full_refresh --full-refresh"
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector project_respira_gold"
	$(DBT) "cd dbt && dbt test --target $(TARGET) --selector project_respira_gold_tests"

# A fast inner-loop option (no deps, no seed) for iteration
.PHONY: quick
quick:
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_core"
	$(DBT) "cd dbt && dbt run --target $(TARGET) --selector canonical_silver"

# -----------------------
# Prefect flows
# -----------------------
.PHONY: prefect-bootstrap
prefect-bootstrap:
	$(PREFECT_RUN) "python3 scripts/wait_for_prefect.py && python3 pipelines/flows/warehouse_bootstrap.py"

.PHONY: run-canonical-incremental
run-canonical-incremental:
	$(PREFECT_RUN) "python3 scripts/wait_for_prefect.py && python3 pipelines/flows/canonical_incremental.py"

.PHONY: run-canonical-full-refresh
run-canonical-full-refresh:
	$(PREFECT_RUN) "python3 scripts/wait_for_prefect.py && python3 pipelines/flows/canonical_full_refresh.py"

.PHONY: run-canonical-measurement-backfill
run-canonical-measurement-backfill:
	$(PREFECT_RUN) "python3 scripts/wait_for_prefect.py && python3 pipelines/flows/canonical_measurement_backfill.py"

.PHONY: run-canonical-measurement-queue-cutover-plan
run-canonical-measurement-queue-cutover-plan:
	$(PREFECT_RUN) "python3 scripts/wait_for_prefect.py && python3 pipelines/flows/canonical_measurement_queue_cutover.py"

.PHONY: run-project-inference
run-project-inference:
	$(WORKER_RUN) "python3 pipelines/flows/project_inference.py"

.PHONY: run-project-pipeline
run-project-pipeline:
	$(WORKER_RUN) "python3 pipelines/flows/project_pipeline.py"

.PHONY: smoke-test
smoke-test:
	poetry run pytest -q tests/test_artifacts.py tests/test_dbt_tasks_command.py tests/test_gates.py tests/test_inference_json.py tests/test_projects_config.py tests/test_inference_flow.py tests/test_measurement_backfill.py

.PHONY: run-social-broadcast
run-social-broadcast:
	$(WORKER_RUN) "python3 pipelines/flows/social_broadcast.py"
