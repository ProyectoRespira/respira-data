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
- `DBT_USE_PREFECT_DBT` default `false`
- `DBT_TIMEOUT_CANONICAL_CORE_S` default `0` (`0` disables the timeout)
- `DBT_TIMEOUT_CANONICAL_BATCH_INGEST_S` default `3600`
- `DBT_TIMEOUT_CANONICAL_SILVER_S` default `1800`
- `DBT_TIMEOUT_PROJECT_S` default `1200`
- `DBT_TIMEOUT_TESTS_S` default `1200`
- `MEASUREMENT_BACKFILL_PROCESS_BATCH_HOURS` default `720`
- `MEASUREMENT_STREAM_STATE_BOOTSTRAP_TIMEOUT_S` default `1800`

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

- `pipelines/flows/warehouse_bootstrap.py:warehouse_bootstrap`
- `pipelines/flows/measurement_stream_state_bootstrap.py:measurement_stream_state_bootstrap`
- `pipelines/flows/canonical_incremental.py:canonical_incremental`
- `pipelines/flows/canonical_full_refresh.py:canonical_full_refresh`
- `pipelines/flows/canonical_measurement_backfill.py:canonical_measurement_backfill`
- `pipelines/flows/project_inference.py:project_inference`
- `pipelines/flows/project_pipeline.py:project_pipeline`

## Ejecución local

Desde raíz del repositorio:

- `make prefect-bootstrap`
- `make run-canonical-incremental`
- `make run-canonical-full-refresh`
- `make run-canonical-measurement-backfill`
- `make run-project-pipeline`
- `make run-project-inference`

`canonical_measurement_backfill` no reconstruye `int_measurement_payloads` por defecto. Ese modelo queda como auditoria opcional de payloads crudos y se puede activar con `include_payload_audit=True` cuando haga falta.
Tambien admite `run_prep=False` y `run_ingest=False` para retomar una corrida pesada ya materializada sin rehacer staging global ni el aterrizaje row-grain.

## Deployments automáticos

Al iniciar `prefect_worker`, el script de bootstrap:

1. espera a que Prefect API esté lista
2. crea o actualiza los work pools `canonical` y `respira_gold`
3. despliega `warehouse_bootstrap`, `measurement_stream_state_bootstrap`,
   `canonical_incremental` y `canonical_full_refresh` en `canonical`
4. verifica que los dos deployments manuales de bootstrap existan en Prefect;
   si falta uno, el worker falla antes de iniciar
5. despliega `project_pipeline(project_code=respira_gold)` en `respira_gold`
6. inicia un worker por cada work pool configurado

Si `MODEL_6H_PATH` y `MODEL_12H_PATH` no están definidos, el pipeline del proyecto se registra sin schedule.

## Auditoría operativa

`pipelines/sql/02_ops_audit.sql` crea:

- `ops.dbt_run_audit`
- `ops.measurement_stream_state`
- `ops.inference_station_status`

Además, `warehouse_bootstrap` crea tablas de inferencia por proyecto según `prefect/config/projects.py`. Para `respira_gold`:

- `respira_gold.inference_runs`
- `respira_gold.inference_results`

Los deployments `warehouse-bootstrap` y
`measurement-stream-state-bootstrap` no tienen schedule. Registrarlos no
ejecuta el DDL. El segundo crea/verifica estrictamente
`ops.measurement_stream_state`, lo completa desde silver + intermediate, y
valida cobertura total antes de confirmar la transacción. Ver
`docs/ops-measurement-stream-state.md` para el procedimiento de cutover,
re-ejecución idempotente y rollback.

## Política actual

- `canonical_incremental` corre `dbt deps`, carga y valida las seeds compartidas de `core`, y luego ejecuta `canonical_core` y `canonical_silver`; cualquier falla bloquea el flow
- `canonical_full_refresh` recarga las seeds compartidas con `--full-refresh` y las valida antes de reconstruir y probar la capa canonica
- `canonical_incremental` usa `canonical_incremental_core`, que omite la recreacion de staging views puras para evitar esperas largas por locks, pero si actualiza `stg_fiuna_measurements_repaired` y los station caches incrementales. Si cambias SQL de staging o bootstrappeas un entorno nuevo, corre `canonical_full_refresh`; el backfill construye las capas FIUNA preparadas durante `canonical_batch_ingest`.
- `project_pipeline` corre `dbt deps`, luego `dbt seed` y `dbt test` bloqueante para project seeds, y despues `dbt run` del proyecto
- `project_pipeline` falla si falla el seed del proyecto, sus seed tests bloqueantes, o el run dbt del proyecto
- `project_pipeline` alerta por Slack si fallan tests del proyecto, pero no corta el pipeline por fallas de data tests
- `project_pipeline` corre inferencia solo si el proyecto la tiene habilitada
