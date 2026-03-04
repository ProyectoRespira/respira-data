from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PREFECT_ROOT = Path(__file__).resolve().parents[1]
FLOWS_ROOT = Path(__file__).resolve().parent
if str(PREFECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PREFECT_ROOT))
if str(FLOWS_ROOT) not in sys.path:
    sys.path.insert(0, str(FLOWS_ROOT))

from compat import flow, get_run_logger
from dbt_gold import dbt_gold
from inference_per_station import inference_per_station


@flow(name="gold_then_inference")
def gold_then_inference(as_of: datetime | None = None) -> None:
    logger = get_run_logger()
    as_of_value = as_of.astimezone(timezone.utc) if as_of else datetime.now(timezone.utc)

    logger.info("Starting gold_then_inference with as_of=%s", as_of_value.isoformat())

    # dbt_gold raises on dbt command failures. In that case inference is not executed.
    dbt_gold()
    inference_per_station(as_of=as_of_value)


if __name__ == "__main__":
    gold_then_inference()
