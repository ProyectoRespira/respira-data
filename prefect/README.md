# Prefect Orchestration Plan v2

## 1. Objetivo
Diseñar una orquestación en Prefect que controle de forma confiable:
1. Ejecución de dbt por etapas, con validaciones y detención temprana.
2. Observabilidad de ejecución (estado, duración, volumen procesado, artefactos dbt).
3. Inferencia por estación usando las últimas 24 horas de `"respira-gold".station_inference_features`.

Este documento define arquitectura y plan de implementación. No implementa código final todavía.

## 2. Viabilidad con el proyecto actual
Sí, es viable con la base actual del repo (`prefect_shell` + dbt CLI), con estas mejoras mínimas:
1. Flujos separados para `full-refresh` y operación incremental.
2. Selectores dbt estables por capa (`core`, `facts`, `gold`, `tests`).
3. Lectura y persistencia de artefactos dbt (`run_results.json`, `manifest.json`, `sources.json`).
4. Políticas de retry/timeout por tipo de tarea.

## 2.1 Plan de migración a prefect-dbt (3 pasos)
Objetivo:
Migrar de ejecución por shell (`prefect_shell`) a ejecución nativa dbt en Prefect para mejorar observabilidad por modelo y control de reintentos.

Cambios esperados en dbt:
1. No se requieren cambios grandes en modelos SQL.
2. Cambios recomendados de bajo impacto:
   - `selectors.yml` para capas estables.
   - tags opcionales por criticidad/capa.
   - estandarizar comandos por `target prod`.

Cambios esperados en Prefect:
1. Reemplazar tasks `shell_run_command(\"dbt ...\")` por tasks de `prefect-dbt`.
2. Parametrizar `project_dir`, `profiles_dir`, `target`, `selectors`.
3. Capturar resultados por nodo dbt (modelo/test/source freshness).

### Paso 1: Base operativa (sin cambiar lógica dbt)
1. Instalar y fijar versiones compatibles de `prefect-dbt`, `dbt-core`, `dbt-postgres`.
2. Crear flow `dbt_incremental` con tareas separadas:
   - `deps`
   - `run @core`
   - `run @facts`
   - `run @gold`
   - `test @gold_tests`
3. Mantener `dbt_full_refresh` como flow manual.

Salida esperada:
1. Ejecutar dbt desde Prefect sin shell genérico.
2. Logs más claros por comando y estado.

### Paso 2: Observabilidad por nodo
1. Activar captura de eventos/resultados por nodo dbt.
2. Parsear `run_results.json` para persistencia en `ops.dbt_run_audit`.
3. Exponer en Prefect UI:
   - nodos exitosos/fallidos
   - duración por nodo
   - errores compilación/ejecución

Salida esperada:
1. Diagnóstico fino por tabla/test.
2. Reintentos orientados por etapa.

### Paso 3: Operación avanzada
1. Crear flow coordinador `gold_then_inference`.
2. Agregar límites de concurrencia y políticas de retry por task.
3. Definir alertas por SLA y umbrales de fallo.

Salida esperada:
1. Ejecución horaria robusta con dependencia dbt -> inferencia.
2. Runbook de recuperación con trazabilidad completa.

Riesgos y mitigaciones:
1. Riesgo: incompatibilidad de versiones.
   - Mitigación: matriz de versiones bloqueada en `pyproject.toml`.
2. Riesgo: diferencias de entorno local/servidor en `profiles.yml`.
   - Mitigación: pruebas con `target prod` en deployment de staging antes de producción.
3. Riesgo: sobrecarga de tests pesados.
   - Mitigación: separar tests online (`gold_tests`) y batch completo en ventana nocturna.

## 3. Estado actual y gaps
Estado actual:
1. `prefect/flows/dbt_build.py` ejecuta comandos dbt en bloque.
2. `prefect/flows/warebouse_bootstrap.py` ejecuta bootstrap SQL.

Gaps:
1. No hay gates de calidad entre etapas.
2. No hay observabilidad por modelo (solo logs de consola).
3. No hay control de concurrencia entre corridas.
4. No existe flujo de inferencia.
5. No hay runbook formal para backfill y recuperación.

## 4. Diseño objetivo de flujos

### 4.1 Flujos principales
1. `warehouse_bootstrap` (manual, excepcional)
2. `dbt_full_refresh` (manual, mantenimiento)
3. `dbt_incremental` (programado, base de datos)
4. `dbt_gold` (programado, capa de producto)
5. `inference_per_station` (programado, dependiente de `dbt_gold`)
6. `gold_then_inference` (coordinador)

### 4.2 Encadenamiento recomendado
`dbt_incremental` -> `dbt_gold` -> `inference_per_station`

Regla de control:
1. Si falla `dbt_incremental`, se corta la cadena.
2. Si falla `dbt_gold`, no se ejecuta inferencia.
3. Si falla inferencia en una estación, el flujo continúa y marca `station_status=failed`.

## 5. Control operacional de dbt

### 5.1 Etapas dbt (tasks separadas)
1. `dbt deps`
2. `dbt seed` (solo cuando aplique)
3. `dbt source freshness` (opcional, recomendado en producción)
4. `dbt run` por selector
5. `dbt test` por selector
6. `collect_dbt_artifacts`

### 5.2 Selectores recomendados
Definir `selectors.yml` para evitar comandos frágiles.
Selectores objetivo:
1. `core`
2. `facts`
3. `gold`
4. `gold_tests`
5. `full_refresh_safe`

Comandos objetivo con selector:
1. `dbt run --target prod --select @core`
2. `dbt run --target prod --select @facts`
3. `dbt run --target prod --select @gold`
4. `dbt test --target prod --select @gold_tests`

### 5.3 Políticas de ejecución
1. `threads`: configurable por deployment (`1` para estabilidad, `4` para throughput).
2. `timeout`: por task (ej. `run facts` más alto que `run gold`).
3. `retries`: por task, no solo a nivel flow.
4. `retry_delay`: exponencial para fallas transitorias.

## 6. Observabilidad (dbt + Prefect)

### 6.1 Métricas mínimas por corrida
1. `flow_run_id`, `deployment`, `target`, `git_sha`.
2. `started_at`, `ended_at`, `duration_s`.
3. `dbt_status`: success/failed/cancelled.
4. `models_passed`, `models_failed`, `tests_passed`, `tests_failed`.
5. `rows_affected` por modelo incremental crítico.

### 6.2 Artefactos dbt a recolectar
1. `target/run_results.json`
2. `target/manifest.json`
3. `target/sources.json` (si se usa freshness)

Acción recomendada:
Persistir resumen parseado en una tabla operativa, por ejemplo `ops.dbt_run_audit`.

### 6.3 Alertas
Alertar en:
1. Falla de `dbt_gold`.
2. `tests_failed > 0` en `gold_tests`.
3. Duración fuera de umbral (SLA breach).
4. Inferencia con `failed_stations > threshold`.

## 7. Diseño de inferencia por estación

### 7.1 Contrato del runner
Script CLI de inferencia:
1. `--as-of` (UTC)
2. `--window-hours` (default: 24)
3. `--target` (`prod`)
4. `--model-path`
5. `--min-points` (default sugerido: 18 o 24)

### 7.2 Lógica base
1. Obtener estaciones candidatas desde `station_inference_features`.
2. Para cada estación, leer ventana `as_of - 24h` a `as_of`.
3. Validar completitud mínima de features.
4. Inferir con modelo preentrenado.
5. Persistir outputs y estado por estación.

### 7.3 Idempotencia y deduplicación
1. Crear `inference_run_id` por ejecución.
2. Clave única recomendada: `(inference_run_id, station_id)`.
3. Reintento no duplica filas.

### 7.4 Persistencia esperada
1. `inference_runs`: metadata del run.
2. `inference_results`: predicciones por estación.
3. `inference_station_status` (opcional): `success/skipped/failed` + motivo.

## 8. Scheduling y despliegues

### 8.1 Schedules sugeridos
1. `dbt_incremental`: cada hora en `:05`.
2. `dbt_gold`: cada hora en `:15`.
3. `inference_per_station`: cada hora en `:25`.
4. `dbt_full_refresh`: manual.

### 8.2 Concurrencia
1. Limitar a 1 corrida activa por deployment crítico.
2. Cancelar o poner en cola corridas solapadas.

## 9. Runbook de operación

### 9.1 Fallo en dbt incremental
1. Reintento automático.
2. Si persiste: ejecutar `dbt run --select` por capa para aislar.
3. Escalar si afecta SLA.

### 9.2 Fallo en dbt gold
1. Bloquear inferencia.
2. Reintentar gold con `threads=1`.
3. Si falla por datos, abrir incidente de calidad de datos.

### 9.3 Fallo parcial de inferencia
1. Guardar estación fallida con motivo.
2. Reintento solo de estaciones fallidas.
3. No repetir estaciones exitosas del mismo `inference_run_id`.

### 9.4 Full refresh
1. Solo manual.
2. Ventana de mantenimiento.
3. Revalidación con `dbt test` post-refresh.

## 10. SLAs y SLOs iniciales
1. `dbt_gold` completado en < 20 min (p95).
2. `gold_then_inference` completado en < 35 min (p95).
3. `failed_stations_ratio < 5%` por corrida de inferencia.
4. Disponibilidad de corrida horaria > 99%.

## 11. Seguridad y configuración
1. Secretos en Prefect Blocks o variables de entorno seguras.
2. No hardcodear credenciales en flows.
3. Parametrizar `target`, `threads`, `selectors`, `model_path`.
4. Registrar versión de modelo (`model_version`) en resultados.

## 12. Plan de implementación por fases

### Fase 1: Control dbt
1. Separar `dbt_incremental`, `dbt_gold`, `dbt_full_refresh`.
2. Introducir selectors estables.
3. Añadir tasks por etapa y gates de falla.

### Fase 2: Observabilidad
1. Parsear `run_results.json`.
2. Persistir auditoría en `ops.dbt_run_audit`.
3. Configurar alertas básicas.

### Fase 3: Inference runner
1. Implementar CLI de inferencia.
2. Persistir `inference_runs` y `inference_results`.
3. Añadir deduplicación e idempotencia.

### Fase 4: Encadenamiento y operación
1. Crear `gold_then_inference`.
2. Configurar schedules y límites de concurrencia.
3. Ejecutar pruebas end-to-end y runbook.

## 13. Criterios de aceptación
1. Flujos dbt separados y observables por etapa.
2. Artefactos dbt recolectados y auditados por corrida.
3. Inferencia ejecutada por estación con ventana de 24h.
4. Reintentos sin duplicar resultados.
5. Schedules y controles de concurrencia activos.

## 14. Decisiones pendientes
1. Confirmar destino final de auditoría (`ops` schema/table).
2. Confirmar política `min_points` para inferencia.
3. Definir versión y lifecycle del modelo IA.
4. Definir política de backfill histórico.

## 15. Estructura de implementación propuesta

```text
respira-data/
├─ prefect/
│  ├─ README.md                                  # (MOD) Plan maestro de orquestación y operación
│  ├─ flows/
│  │  ├─ warehouse_bootstrap.py                  # (MOD) Bootstrap SQL manual/one-shot
│  │  ├─ dbt_full_refresh.py                     # (NEW) Flujo manual de reconstrucción completa dbt
│  │  ├─ dbt_incremental.py                      # (NEW) Flujo programado por capas core/facts/gold
│  │  ├─ dbt_gold.py                             # (NEW) Flujo de solo Gold para operación rápida
│  │  ├─ inference_per_station.py                # (NEW) Flujo de inferencia por estación (ventana 24h)
│  │  └─ gold_then_inference.py                  # (NEW) Coordinador dbt_gold -> inferencia
│  ├─ tasks/
│  │  ├─ dbt_tasks.py                            # (NEW) Wrappers prefect-dbt: deps/run/test/freshness
│  │  ├─ artifacts.py                            # (NEW) Parseo y persistencia de run_results/manifest
│  │  ├─ inference_tasks.py                      # (NEW) Lectura features, inferencia, persistencia resultados
│  │  └─ notifications.py                        # (NEW) Alertas y manejo de fallos por umbral/SLA
│  ├─ deployments/
│  │  ├─ dbt_incremental.yaml                    # (NEW) Deployment schedule y parámetros de incremental
│  │  ├─ dbt_gold.yaml                           # (NEW) Deployment schedule de Gold
│  │  ├─ inference_per_station.yaml              # (NEW) Deployment schedule de inferencia
│  │  ├─ gold_then_inference.yaml                # (NEW) Deployment coordinador
│  │  └─ dbt_full_refresh.yaml                   # (NEW) Deployment manual sin schedule
│  ├─ sql/
│  │  ├─ 01_schema.sql                           # (EXISTING) Bootstrap inicial (ya existe)
│  │  └─ 02_ops_audit.sql                        # (NEW) DDL de tablas ops para auditoría dbt/inferencia
│  ├─ scripts/
│  │  ├─ run_inference.py                        # (NEW) CLI del modelo IA preentrenado
│  │  └─ validate_inference_input.py             # (NEW) Validación de calidad/completitud de ventana 24h
│  └─ config/
│     ├─ settings.py                             # (NEW) Parámetros runtime: target, threads, timeouts, paths
│     └─ selectors.py                            # (NEW) Mapeo de selectores dbt usados por Prefect
├─ dbt/
│  ├─ selectors.yml                              # (NEW) Selectores estables: core, facts, gold, gold_tests
│  └─ dbt_project.yml                            # (MOD) Tags/vars de soporte operativo si se requieren
├─ src/
│  └─ inference/
│     ├─ model_loader.py                         # (NEW) Carga de modelo preentrenado/versionado
│     ├─ feature_adapter.py                      # (NEW) Conversión de station_inference_features -> tensor/frame
│     └─ predictor.py                            # (NEW) API interna de predicción por estación
├─ pyproject.toml                                # (MOD) Dependencias prefect-dbt y librerías de inferencia
└─ Makefile                                      # (MOD) Targets operativos: deploy-prefect, run-flows, smoke tests
```

Notas de alcance:
1. Cambios obligatorios mínimos para arrancar: `prefect/flows/*`, `prefect/tasks/*`, `dbt/selectors.yml`, `pyproject.toml`.
2. Cambios opcionales recomendados para operación robusta: `prefect/deployments/*`, `prefect/sql/02_ops_audit.sql`, `prefect/notifications.py`.
3. El código de inferencia puede vivir en `prefect/scripts/` o `src/inference/`; se propone separar lógica reusable en `src/`.
