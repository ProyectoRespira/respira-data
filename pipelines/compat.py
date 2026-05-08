from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Callable
from typing import Any


def _import_real_prefect() -> Any | None:
    try:
        prefect_module = importlib.import_module("prefect")
        if not hasattr(prefect_module, "flow"):
            return None
        return prefect_module
    except Exception:  # noqa: BLE001
        return None


_PREFECT = _import_real_prefect()


def _identity_decorator(*dargs, **dkwargs):  # noqa: ANN002, ANN003
    if dargs and callable(dargs[0]) and len(dargs) == 1 and not dkwargs:
        return dargs[0]

    def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        return func

    return _decorator


if _PREFECT is not None:
    flow = _PREFECT.flow
    task = _PREFECT.task

    def get_run_logger() -> Any:
        return _PREFECT.get_run_logger()

else:
    flow = _identity_decorator
    task = _identity_decorator

    def get_run_logger() -> logging.Logger:
        return logging.getLogger("prefect-fallback")


def get_flow_context() -> dict[str, Any]:
    flow_run_id = (
        os.getenv("PREFECT__FLOW_RUN_ID") or os.getenv("PREFECT_FLOW_RUN_ID") or "local"
    )
    deployment = os.getenv("PREFECT_DEPLOYMENT_NAME")

    if _PREFECT is not None:
        try:
            runtime = importlib.import_module("prefect.runtime")
            runtime_flow_run = getattr(runtime, "flow_run", None)
            if runtime_flow_run is not None:
                flow_run_id = str(
                    getattr(runtime_flow_run, "id", flow_run_id) or flow_run_id
                )
                deployment = deployment or getattr(
                    runtime_flow_run, "deployment_id", None
                )
        except Exception:  # noqa: BLE001
            pass

    return {
        "flow_run_id": flow_run_id,
        "deployment": str(deployment) if deployment is not None else None,
    }
