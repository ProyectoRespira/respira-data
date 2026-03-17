# Prefect Orchestration

Esta carpeta contiene la orquestación Prefect 3 para un repositorio modular con dos tipos de pipeline:

- `canonical_*` para la capa canónica reusable
- `project_*` para pipelines específicos de proyecto

## Variables de entorno

Conexión a BD:

- `DB_DSN` recomendado
- o `REMOTE_PG_HOST`, `REMOTE_PG_PORT`, `REMOTE_PG_USER`, `REMOTE_PG_PASSWORD`, `REMOTE_PG_DB`

dbt:

- `DBT_PROJECT_DIR` default `./dbt`
- `DBT_PROFILES_DIR` default `./dbt`
- `DBT_TARGET` default `prod`
- `DBT_THREADS` default `1`
- `DBT_TIMEOUT_CANONICAL_CORE_S` default `900`
- `DBT_TIMEOUT_CANONICAL_SILVER_S` default `1800`
- `DBT_TIMEOUT_PROJECT_S` default `1200`
- `DBT_TIMEOUT_TESTS_S` default `1200`

Inferencia:

- `DEFAULT_WINDOW_HOURS` default `24`
- `INFERENCE_MIN_POINTS` default `18`
- `MODEL_6H_PATH` requerido para correr inferencia
- `MODEL_12H_PATH` requerido para correr inferencia
- `MODEL_6H_VERSION` default `unknown`
- `MODEL_12H_VERSION` default `unknown`

Alertas:

- `SLACK_WEBHOOK_URL` opcional

## Flujos disponibles

- `prefect/flows/warehouse_bootstrap.py:warehouse_bootstrap`
- `prefect/flows/canonical_incremental.py:canonical_incremental`
- `prefect/flows/canonical_full_refresh.py:canonical_full_refresh`
- `prefect/flows/project_inference.py:project_inference`
- `prefect/flows/project_pipeline.py:project_pipeline`

## Ejecución local

Desde raíz del repositorio:

- `make prefect-bootstrap`
- `make run-canonical-incremental`
- `make run-canonical-full-refresh`
- `make run-project-pipeline`
- `make run-project-inference`

## Deployments automáticos

Al iniciar `prefect_worker`, el script de bootstrap:

1. espera a que Prefect API esté lista
2. crea o actualiza el work pool `default`
3. despliega `canonical_incremental`
4. despliega `canonical_full_refresh`
5. despliega `project_pipeline(project_code=respira_gold)`
6. inicia el worker

Si `MODEL_6H_PATH` y `MODEL_12H_PATH` no están definidos, el pipeline del proyecto se registra sin schedule.

## Auditoría operativa

`prefect/sql/02_ops_audit.sql` crea:

- `ops.dbt_run_audit`
- `ops.inference_station_status`

Además, `warehouse_bootstrap` crea tablas de inferencia por proyecto según `prefect/config/projects.py`. Para `respira_gold`:

- `respira_gold.inference_runs`
- `respira_gold.inference_results`

## Política actual

- `canonical_incremental` falla si falla `dbt deps`, `canonical_core` o `canonical_silver`
- `project_pipeline` falla si falla el run dbt del proyecto
- `project_pipeline` alerta por Slack si fallan tests del proyecto, pero no corta el pipeline por fallas de data tests
- `project_pipeline` corre inferencia solo si el proyecto la tiene habilitada
