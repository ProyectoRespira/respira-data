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
- `MEASUREMENT_TIMESTAMP_QUEUE_RETENTION_HOURS` default `168`; debe ser mayor
  que cero
- `MEASUREMENT_INCREMENTAL_MAX_EXPANDED_ROWS` default `2000000`; un incremental
  que excede este limite falla antes de publicar silver
- `MEASUREMENT_QUEUE_CUTOVER_BATCH_HOURS` default `168`
- `MEASUREMENT_QUEUE_CUTOVER_MAX_QUEUE_ROWS` default `250000`
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
- `pipelines/flows/canonical_shadow_publish.py:canonical_shadow_publish`
- `pipelines/flows/canonical_incremental.py:canonical_incremental`
- `pipelines/flows/canonical_full_refresh.py:canonical_full_refresh`
- `pipelines/flows/canonical_measurement_backfill.py:canonical_measurement_backfill`
- `pipelines/flows/canonical_measurement_queue_cutover.py:canonical_measurement_queue_cutover`
- `pipelines/flows/project_inference.py:project_inference`
- `pipelines/flows/project_pipeline.py:project_pipeline`

## Ejecución local

Desde raíz del repositorio:

- `make prefect-bootstrap`
- `make run-canonical-incremental`
- `make run-canonical-full-refresh`
- `make run-canonical-measurement-backfill`
- `make run-canonical-measurement-queue-cutover-plan`
- `make run-project-pipeline`
- `make run-project-inference`

`canonical_measurement_backfill` no reconstruye `int_measurement_payloads` por defecto, y los selectores normales tampoco lo ejecutan. La auditoria persistente se activa solamente con `include_payload_audit=True` junto con `run_ingest=True`. Para inspeccion manual acotada, `canonical_debug_payload_audit` reemplaza una tabla `debug_int_measurement_payloads` usando una fuente y ambos limites de `extracted_at`.
Tambien admite `run_prep=False` y `run_ingest=False` para retomar una corrida pesada ya materializada sin rehacer staging global ni el aterrizaje row-grain. `run_ingest=False` solo esta soportado mientras todas las filas necesarias sigan retenidas en la cola `intermediate.int_measurement_timestamps_silver`; si ya fueron limpiadas, el flujo falla y se debe repetir con `run_ingest=True`. El contrato completo de granularidad, bounds y elegibilidad de limpieza esta en `docs/measurement-timestamp-queue.md`.

## Deployments automáticos

Al iniciar `prefect_worker`, el script de bootstrap:

1. espera a que Prefect API esté lista
2. crea o actualiza los work pools `canonical` y `respira_gold`
3. despliega `warehouse_bootstrap`, `measurement_stream_state_bootstrap`,
   `canonical_shadow_publish`, `canonical_measurement_backfill`,
   `canonical_measurement_queue_cutover`,
   `canonical_incremental` y `canonical_full_refresh` en `canonical`
4. verifica que los deployments manuales de bootstrap, shadow publish y
   measurement backfill y queue cutover existan en Prefect;
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

`canonical-shadow-publish` también es manual y tiene concurrencia `1`. Escribe
solo `silver.fct_measurements_silver_shadow` y
`ops.measurement_stream_state_shadow`; exige
un reset inicial desde el estado productivo y luego permite validar continuidad
entre lotes sin reset. Ver `docs/canonical-shadow-publish.md` para parámetros,
replay histórico, validación y rollback.

`canonical-measurement-backfill` es manual, no tiene schedule y tiene
concurrencia `1` con estrategia `ENQUEUE`. Se puede ejecutar desde Prefect UI
con bounds acotados y luego repetir con `run_ingest=False` mientras las filas
requeridas sigan en la cola de timestamps.

La limpieza de la cola ocurre automaticamente despues de publish silver,
refresh de `ops.measurement_stream_state` y smoke tests exitosos. En backfills
se ejecuta por ventana; con `run_tests=False` no se elimina nada. La limpieza
incremental normal excluye filas null-time, y el borde `max(extracted_at)` de
cada fuente siempre conserva una fila como checkpoint aunque sea mas antiguo
que la retencion configurada.

`int_measurements_long` e `int_measurements_values_silver` son transformaciones
ephemeral en runtime: dbt las inserta como CTEs y no persiste su historico. Para
inspeccion manual, `canonical_debug_intermediate` materializa tablas `debug_*`
solamente con source y ventana acotados. El retiro reversible de las tablas
legacy esta documentado en `docs/runtime-measurement-intermediates.md`.

## Política actual

- `canonical_incremental` falla si falla `dbt deps`, los seeds/tests compartidos, `canonical_incremental_core`, `canonical_silver` o `canonical_incremental_state`
- `canonical_incremental` usa `ops.measurement_stream_state` para carry-forward y solo lo refresca despues de publicar silver correctamente; repetir entradas estables no avanza state ni cambia `updated_at`
- `canonical_incremental` usa `canonical_incremental_core`, que omite la recreacion de staging views puras para evitar esperas largas por locks, pero si actualiza `stg_fiuna_measurements_repaired` y los station caches incrementales. Si cambias SQL de staging o bootstrappeas un entorno nuevo, corre `canonical_full_refresh`; el backfill construye las capas FIUNA preparadas durante `canonical_batch_ingest`.
- `project_pipeline` corre `dbt deps`, luego `dbt seed` y `dbt test` bloqueante para project seeds, y despues `dbt run` del proyecto
- `project_pipeline` falla si falla el seed del proyecto, sus seed tests bloqueantes, o el run dbt del proyecto
- `project_pipeline` alerta por Slack si fallan tests del proyecto, pero no corta el pipeline por fallas de data tests
- `project_pipeline` corre inferencia solo si el proyecto la tiene habilitada
