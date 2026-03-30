from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


TEST_DIR = Path(__file__).resolve().parent
CSV_PATH = TEST_DIR / "fake_station_inference_features.csv"


def build_dataset(as_of: datetime | None = None) -> pd.DataFrame:
    as_of_value = _reference_as_of(as_of)
    rows: list[dict] = []

    rows.extend(_station_rows(1, as_of_value, start_offset_hours=23, end_offset_hours=0))
    rows.extend(
        _station_rows(
            2,
            as_of_value - timedelta(days=30),
            start_offset_hours=23,
            end_offset_hours=0,
        )
    )
    rows.extend(
        _station_rows(
            3,
            as_of_value,
            start_offset_hours=23,
            end_offset_hours=0,
            missing_offsets={3, 5, 10, 11, 12, 13, 14, 15},
        )
    )
    rows.extend(_station_rows(4, as_of_value - timedelta(hours=8), start_offset_hours=23, end_offset_hours=0))

    frame = pd.DataFrame(rows).sort_values(["station_id", "date_utc"]).reset_index(drop=True)
    return frame


def write_csv(path: Path = CSV_PATH, as_of: datetime | None = None) -> Path:
    frame = build_dataset(as_of=as_of)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def _station_rows(
    station_id: int,
    anchor: datetime,
    start_offset_hours: int,
    end_offset_hours: int,
    missing_offsets: set[int] | None = None,
) -> list[dict]:
    missing = missing_offsets or set()
    rows: list[dict] = []
    for hours_back in range(start_offset_hours, end_offset_hours - 1, -1):
        if hours_back in missing:
            continue
        ts = anchor - timedelta(hours=hours_back)
        rows.append(_build_row(station_id=station_id, ts=ts))
    return rows


def _build_row(station_id: int, ts: datetime) -> dict:
    hour = ts.hour
    weekday = ts.weekday()
    station_factor = station_id * 4.5
    wave = math.sin((hour / 24.0) * 2 * math.pi)
    wave_shift = math.cos((hour / 24.0) * 2 * math.pi)
    pm2_5 = 18 + station_factor + (wave * 9) + (weekday * 0.7)
    aqi_pm2_5 = pm2_5 * 2.2

    return {
        "station_id": station_id,
        "date_utc": ts.astimezone(timezone.utc).isoformat(),
        "scenario": {
            1: "24h completas hasta as_of",
            2: "datos viejos; ultima lectura hace un mes",
            3: "faltantes intermedios en la ventana",
            4: "24h completas pero la ultima lectura fue hace 8h",
        }[station_id],
        "pm1": round(pm2_5 * 0.58, 3),
        "pm2_5": round(pm2_5, 3),
        "pm10": round(pm2_5 * 1.42, 3),
        "pm2_5_avg_6h": round(pm2_5 - 1.7, 3),
        "pm2_5_max_6h": round(pm2_5 + 3.3, 3),
        "pm2_5_skew_6h": round(wave * 0.35, 3),
        "pm2_5_std_6h": round(2.6 + station_id * 0.2, 3),
        "aqi_pm2_5": round(aqi_pm2_5, 3),
        "aqi_pm10": round(pm2_5 * 1.75, 3),
        "aqi_level": float(min(5, max(1, round(aqi_pm2_5 / 55)))),
        "aqi_pm2_5_max_24h": round(aqi_pm2_5 + 9.5, 3),
        "aqi_pm2_5_skew_24h": round(wave_shift * 0.28, 3),
        "aqi_pm2_5_std_24h": round(6.2 + station_id * 0.4, 3),
        "pm2_5_region_avg": round(pm2_5 - 1.2, 3),
        "pm2_5_region_max": round(pm2_5 + 5.0, 3),
        "pm2_5_region_skew": round(wave * 0.22, 3),
        "pm2_5_region_std": round(3.9 + station_id * 0.15, 3),
        "aqi_region_avg": round(aqi_pm2_5 - 4.0, 3),
        "aqi_region_max": round(aqi_pm2_5 + 12.0, 3),
        "aqi_region_skew": round(wave_shift * 0.19, 3),
        "aqi_region_std": round(8.4 + station_id * 0.2, 3),
        "level_region_max": float(min(5, max(1, round(aqi_pm2_5 / 55)))),
        "temperature": round(22 + wave_shift * 4 + station_id * 0.3, 3),
        "humidity": round(58 - wave * 8 + station_id, 3),
        "pressure": round(1008 + wave_shift * 2.2, 3),
        "wind_speed": round(2.8 + abs(wave) * 2.1 + station_id * 0.1, 3),
        "wind_dir_sin": round(math.sin(((hour + station_id) / 24.0) * 2 * math.pi), 6),
        "wind_dir_cos": round(math.cos(((hour + station_id) / 24.0) * 2 * math.pi), 6),
    }


def _reference_as_of(as_of: datetime | None) -> datetime:
    value = as_of or datetime.now(timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.replace(minute=0, second=0, microsecond=0)


if __name__ == "__main__":
    output_path = write_csv()
    print(f"CSV generado en {output_path}")
