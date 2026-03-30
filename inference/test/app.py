from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from harness import CSV_PATH, TEST_DIR, run_fake_inference


st.set_page_config(page_title="Inference Test Harness", layout="wide")


@st.cache_data(show_spinner=False)
def load_payload() -> dict:
    return run_fake_inference()


def main() -> None:
    st.title("Inference Test Harness")
    st.caption("Prueba local de inferencia usando CSV sintético y los metodos de src/inference.")

    payload = load_payload()
    run_meta = payload["meta"]
    inference_run = payload["inference_run"]
    station_status = pd.DataFrame(payload["station_status"])
    station_summaries = pd.DataFrame(payload["station_summaries"])
    inference_results = payload["inference_results"]

    with st.sidebar:
        st.subheader("Contexto")
        st.write(f"CSV: `{run_meta['csv_path']}`")
        st.write(f"as_of: `{run_meta['as_of']}`")
        st.write(f"Modelo 6h: `{Path(run_meta['model_6h_path']).name}`")
        st.write(f"Modelo 12h: `{Path(run_meta['model_12h_path']).name}`")
        st.write(f"Carpeta test: `{TEST_DIR}`")

    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric("Stations", len(station_summaries))
    col_b.metric("Success", int((station_status["status"] == "success").sum()) if not station_status.empty else 0)
    col_c.metric("Skipped", int((station_status["status"] == "skipped").sum()) if not station_status.empty else 0)
    col_d.metric("Results", len(inference_results))

    st.subheader("Resumen por estacion")
    st.dataframe(station_summaries, use_container_width=True)

    st.subheader("Estado por estacion")
    st.dataframe(station_status, use_container_width=True)

    st.subheader("JSONs")
    json_col_1, json_col_2 = st.columns(2)
    with json_col_1:
        st.markdown("**inference_run**")
        st.json(inference_run)
    with json_col_2:
        st.markdown("**inference_results**")
        st.json(inference_results)

    if not inference_results:
        st.warning("No hubo resultados de inferencia para graficar.")
        return

    st.subheader("Grafico de predicciones")
    station_options = sorted({int(result["station_id"]) for result in inference_results})
    selected_station = st.selectbox("Estacion", station_options, format_func=lambda value: f"Station {value}")
    selected_horizon = st.selectbox("Horizonte", [6, 12], index=0, format_func=lambda value: f"{value}h")

    selected_result = next(
        result
        for result in inference_results
        if int(result["station_id"]) == selected_station and int(result["horizon_hours"]) == selected_horizon
    )
    input_points = payload["station_inputs"].get(selected_station, [])
    fig = build_plot(input_points=input_points, prediction_points=selected_result["predictions_json"]["points"])
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**inference_result seleccionado**")
    st.json(selected_result)


def build_plot(input_points: list[dict], prediction_points: list[dict]) -> go.Figure:
    input_frame = pd.DataFrame(input_points)
    prediction_frame = pd.DataFrame(prediction_points)

    fig = go.Figure()
    if not input_frame.empty:
        fig.add_trace(
            go.Scatter(
                x=pd.to_datetime(input_frame["ts"], utc=True),
                y=input_frame["aqi_pm2_5"],
                mode="lines+markers",
                name="AQI input",
            )
        )
    if not prediction_frame.empty:
        fig.add_trace(
            go.Scatter(
                x=pd.to_datetime(prediction_frame["ts"], utc=True),
                y=prediction_frame["yhat"],
                mode="lines+markers",
                name="Prediccion",
            )
        )

    fig.update_layout(
        height=420,
        margin={"l": 10, "r": 10, "t": 20, "b": 10},
        xaxis_title="Fecha UTC",
        yaxis_title="AQI PM2.5",
        legend_title="Serie",
    )
    return fig


if __name__ == "__main__":
    main()
