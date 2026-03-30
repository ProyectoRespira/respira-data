# Inference Test Harness

Prueba local de inferencia sin base de datos, usando:

- un CSV sintético con features equivalentes a `station_inference_features`
- los modelos `.pkl` reales desde `models/`
- los metodos de `src/inference/`

## Estructura

- `fake_station_inference_features.csv`: dataset de prueba
- `generate_fake_csv.py`: regenera el CSV sintético
- `run_fake_inference.py`: ejecuta la inferencia y deja JSONs en `artifacts/`
- `app.py`: app de Streamlit para inspeccionar resultados

## Crear el entorno local

Desde la raiz del repo:

```bash
python3 -m venv inference/test/.venv
source inference/test/.venv/bin/activate
pip install -r inference/test/requirements.txt
```

## Regenerar el CSV

```bash
source inference/test/.venv/bin/activate
python inference/test/generate_fake_csv.py
```

## Ejecutar la inferencia local

```bash
source inference/test/.venv/bin/activate
python inference/test/run_fake_inference.py
```

## Abrir la app

```bash
source inference/test/.venv/bin/activate
streamlit run inference/test/app.py
```

## Escenarios incluidos

- Estacion 1: 24h completas hasta `as_of`
- Estacion 2: datos viejos; la ultima lectura fue hace un mes
- Estacion 3: faltantes intermedios en la ventana reciente
- Estacion 4: 24h completas, pero la ultima lectura fue hace 8h
