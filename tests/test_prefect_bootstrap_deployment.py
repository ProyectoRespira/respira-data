from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_worker_start_registers_manual_warehouse_bootstrap_deployment():
    script = _read("scripts/prefect_worker_start.sh")

    assert "pipelines/flows/warehouse_bootstrap.py:warehouse_bootstrap" in script
    assert '"warehouse-bootstrap"' in script
    assert '"${PREFECT_CANONICAL_WORK_POOL}"' in script


def test_checked_in_warehouse_bootstrap_deployment_is_manual_only():
    deployment = _read("pipelines/deployments/warehouse_bootstrap.yaml")

    assert "name: warehouse-bootstrap" in deployment
    assert (
        "entrypoint: pipelines/flows/warehouse_bootstrap.py:warehouse_bootstrap"
        in deployment
    )
    assert "name: canonical" in deployment
    assert "concurrency_limit:" in deployment
    assert "schedules:" not in deployment
