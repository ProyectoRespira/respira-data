# Conector de Vigilancia del Aire (VA)

Trae el histórico del sensor AirGradient de Vigilancia del Aire (O-1PST,
`locationId` 209581, "Barrio Cañada del Ybyray") a `airbyte.va_respira_measures_past`.

VA administra el sensor en su propia organización de AirGradient y no compartió
un token. En su lugar expone `measures/past` a través de un proxy con usuario y
contraseña (`https://pr.pachamamita.eu`). Los registros tienen la misma forma que
los de AirGradient, así que `stg_respira_measurements` los une con
`Respira_measures_past`. El sensor queda con `data_source_name = respira_airbyte`
y el código `respira_209581`.

El proxy no trae coordenadas: la estación está en `dbt/seeds/stations_static.csv`
(lat/lon tomadas del mapa público de AirGradient, licencia CC BY-SA 4.0).

## Comportamiento del endpoint

Probado el 2026-11-11:

- `from` y `to` en ISO-8601 UTC, inclusivos en ambos extremos.
- Ventanas de más de 10 días devuelven 422. El manifest pide de a 7.
- El histórico empieza el 2026-10-02.
- Credenciales malas devuelven 401. El conector lo informa como error de
  configuración ("la contraseña cambió").

## Crear el conector en Airbyte Cloud

1. Builder → *Import a YAML* → `manifest.yaml`. Publicarlo en el workspace.
2. Crear la fuente con `username`, `password` y `start_date = 2026-10-01T00:00:00Z`.
   La contraseña va solo en Airbyte; no se guarda en el repo. VA la va a rotar.
3. Crear la conexión hacia el destino Postgres de producción:
   - Namespace: el mismo esquema `airbyte` que usa la conexión de Respira.
   - Stream prefix: `va_respira_` (la tabla queda `va_respira_measures_past`).
   - Sync mode: **Incremental | Append + Deduped**, cursor `timestamp`,
     clave `locationId` + `timestamp`.
   - Horario: que **no se solape** con la sincronización de Respira y termine
     antes del `canonical_incremental` (`:05` de cada hora). Ver más abajo.

## Por qué Append + Deduped y por qué el horario

- Al reanudar desde el estado, el CDK vuelve a pedir el último registro ya
  sincronizado (el extremo es inclusivo). Con *Deduped* la tabla final no
  duplica; silver hace merge por `(stream_id, timestamp)`, así que reprocesarlo
  no cambia nada.
- VA y Respira comparten el checkpoint de dbt (`max(extracted_at)` por
  `data_source_name`). Si las dos sincronizaciones se pisan, filas de VA
  extraídas antes pero confirmadas después de un lote de Respira quedan detrás
  del checkpoint y no se procesan nunca.

## Probarlo localmente

Con Docker, usando el mismo runtime que Airbyte Cloud:

```sh
IMG=airbyte/source-declarative-manifest:latest
# config.json = {"__injected_declarative_manifest": <manifest como JSON>,
#                "username": "...", "password": "...", "start_date": "2026-10-01T00:00:00Z"}
docker run --rm -v "$PWD:/t" $IMG check    --config /t/config.json
docker run --rm -v "$PWD:/t" $IMG discover --config /t/config.json
docker run --rm -v "$PWD:/t" $IMG read     --config /t/config.json --catalog /t/catalog.json
```

No dejar `config.json` en el repo: tiene la contraseña.
