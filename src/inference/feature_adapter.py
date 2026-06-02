from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_FEATURE_COLUMNS = [
    "pm1",
    "pm2_5",
    "pm10",
    "pm2_5_avg_6h",
    "pm2_5_max_6h",
    "pm2_5_skew_6h",
    "pm2_5_std_6h",
    "aqi_pm2_5",
    "aqi_pm10",
    "aqi_level",
    "aqi_pm2_5_max_24h",
    "aqi_pm2_5_skew_24h",
    "aqi_pm2_5_std_24h",
    "pm2_5_region_avg",
    "pm2_5_region_max",
    "pm2_5_region_skew",
    "pm2_5_region_std",
    "aqi_region_avg",
    "aqi_region_max",
    "aqi_region_skew",
    "aqi_region_std",
    "level_region_max",
    "temperature",
    "humidity",
    "pressure",
    "wind_speed",
    "wind_dir_sin",
    "wind_dir_cos",
]

REQUIRED_COLUMNS = ["station_id", "date_utc", *REQUIRED_FEATURE_COLUMNS]


def rows_to_feature_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)

    missing = [column for column in REQUIRED_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required feature columns: {missing}")

    frame = frame[REQUIRED_COLUMNS].copy()
    frame["date_utc"] = pd.to_datetime(frame["date_utc"], utc=True, errors="coerce")

    frame["station_id"] = pd.to_numeric(frame["station_id"], errors="coerce")
    if frame["station_id"].isna().any():
        raise ValueError("station_id contains null or non-numeric values")
    frame["station_id"] = frame["station_id"].astype("int64")

    for column in REQUIRED_FEATURE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")

    frame = frame.sort_values("date_utc", ascending=True)

    duplicate_mask = frame.duplicated(subset=["date_utc"], keep="last")
    if duplicate_mask.any():
        duplicate_count = int(duplicate_mask.sum())
        logger.warning(
            "Dropping %s duplicated date_utc rows and keeping the last occurrence",
            duplicate_count,
        )
        frame = frame[~duplicate_mask]

    frame = frame.set_index("date_utc", drop=False)
    return frame
