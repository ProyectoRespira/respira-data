# Prefect Orchestration (Respira Data)

Esta carpeta contiene la orquestación Prefect 3 para ejecución por etapas de dbt, auditoría operativa y la inferencia por estación.

## Variables de entorno

Requeridas para correr flujos con conexión a BD:

- `DB_DSN` (recomendado) con formato `postgresql+psycopg://user:pass@host:port/dbname`
- O alternativamente `REMOTE_PG_HOST`, `REMOTE_PG_PORT`, `REMOTE_PG_USER`, `REMOTE_PG_PASSWORD`, `REMOTE_PG_DB` (`REMOTE_PG_NAME` también aceptado) y opcional `REMOTE_PG_SSLMODE`

Variables dbt:

- `DBT_PROJECT_DIR` (default: `./dbt`)
- `DBT_PROFILES_DIR` (default: `./dbt`)
- `DBT_TARGET` (default: `prod`)
- `DBT_THREADS` (default: `1`)
- `DBT_TIMEOUT_CORE_S` (default: `900`)
- `DBT_TIMEOUT_FACTS_S` (default: `1800`)
- `DBT_TIMEOUT_GOLD_S` (default: `1200`)
- `DBT_TIMEOUT_TESTS_S` (default: `1200`)

Variables inferencia:

- `DEFAULT_WINDOW_HOURS` (default: `24`)
- `INFERENCE_MIN_POINTS` (default: `18`)
- `MODEL_6H_PATH` (requerido para inferencia)
- `MODEL_12H_PATH` (requerido para inferencia)
- `MODEL_6H_VERSION` (default: `unknown`)
- `MODEL_12H_VERSION` (default: `unknown`)

Alertas:

- `SLACK_WEBHOOK_URL` (opcional)

## Flujos disponibles

- `prefect/flows/warehouse_bootstrap.py:warehouse_bootstrap`
- `prefect/flows/dbt_incremental.py:dbt_incremental`
- `prefect/flows/dbt_gold.py:dbt_gold`
- `prefect/flows/dbt_full_refresh.py:dbt_full_refresh` (manual)
- `prefect/flows/inference_per_station.py:inference_per_station`
- `prefect/flows/gold_then_inference.py:gold_then_inference`

## Ejecución local

Desde raíz del repositorio:

- `make prefect-bootstrap`
- `make run-dbt-incremental`
- `make run-dbt-gold`
- `make run-inference`
- `make run-gold-then-inference`

O directo por Python:

- `python3 prefect/flows/dbt_incremental.py`
- `python3 prefect/flows/dbt_gold.py`
- `python3 prefect/flows/dbt_full_refresh.py`
- `python3 prefect/flows/inference_per_station.py`
- `python3 prefect/flows/gold_then_inference.py`

## Deployments Prefect 3

Archivos de deployment en `prefect/deployments/`:

- `dbt_incremental.yaml`: cron `5 * * * *` UTC
- `dbt_gold.yaml`: cron `15 * * * *` UTC
- `inference_per_station.yaml`: cron `25 * * * *` UTC
- `gold_then_inference.yaml`: cron `20 * * * *` UTC
- `dbt_full_refresh.yaml`: sin schedule (manual)

Aplicación (puede variar según versión exacta de CLI Prefect 3):

1. Ajustar `work_pool.name` y parámetros según tu entorno.
2. Aplicar cada YAML con el comando de deployment de tu instalación Prefect 3.
3. Verificar en UI que los schedules estén en UTC y activos.

## Concurrencia

Cada deployment crítico define límite de concurrencia:

- `limit: 1`
- `collision_strategy: ENQUEUE`

Esto evita solapes y pone corridas concurrentes en cola.

## Auditoría operativa (`ops` schema)

`prefect/sql/02_ops_audit.sql` crea:

- `ops.dbt_run_audit`: una fila por etapa dbt (deps/run/test) con resumen de `run_results.json`
- `ops.inference_runs`: metadata global de una corrida de inferencia
- `ops.inference_station_status`: estado por estación (`success`, `skipped`, `failed`)
- `ops.inference_results`: resultados JSONB por estación y horizonte (6h/12h)

Bootstrap:

- `warehouse_bootstrap` ejecuta `prefect/sql/01_schema.sql` si existe
- luego ejecuta siempre `prefect/sql/02_ops_audit.sql`

## Política de alertas

- `dbt_gold`: si falla el comando de run gold, el flujo falla
- `gold_tests`: falla de tests genera alerta Slack, pero no rompe el flujo por defecto
- si no existe `SLACK_WEBHOOK_URL`, se loguea y continúa

## Runbook básico

1. Fallo en `dbt_incremental`:
- revisar `ops.dbt_run_audit` (selector `core`/`facts`)
- reintentar `make run-dbt-incremental`

2. Fallo en `dbt_gold`:
- revisar tabla de auditoría y `run_results_json`
- resolver fallo de modelo/test y relanzar `make run-dbt-gold`

3. Fallo parcial en inferencia:
- revisar `ops.inference_station_status` para `reason_code`
- resultados exitosos quedan persistidos y no se duplican por upsert

4. Full refresh:
- ejecutar `make run-dbt-full-refresh` (o por deployment manual)
- validar con auditoría y tests gold
