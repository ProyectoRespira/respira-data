from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def _deployment_block(script: str, deployment_name: str) -> str:
    return script.split(f'"{deployment_name}"', maxsplit=1)[1].split(
        "deploy_flow", maxsplit=1
    )[0]


def _assert_manual_deploy_call(script: str, deployment_name: str) -> None:
    block = _deployment_block(script, deployment_name)
    assert '"${PREFECT_CANONICAL_WORK_POOL}"' in block
    assert '""' in block
    assert "--concurrency-limit 1" in block
    assert "--collision-strategy ENQUEUE" in block


def test_worker_start_registers_manual_warehouse_bootstrap_deployment():
    script = _read("scripts/prefect_worker_start.sh")

    assert "pipelines/flows/warehouse_bootstrap.py:warehouse_bootstrap" in script
    assert '"warehouse-bootstrap"' in script
    assert '"${PREFECT_CANONICAL_WORK_POOL}"' in script


def test_worker_start_registers_and_verifies_stream_state_bootstrap_deployment():
    script = _read("scripts/prefect_worker_start.sh")

    assert (
        "pipelines/flows/measurement_stream_state_bootstrap.py:"
        "measurement_stream_state_bootstrap"
    ) in script
    assert '"measurement-stream-state-bootstrap"' in script
    assert 'verify_deployment "warehouse_bootstrap/warehouse-bootstrap"' in script
    assert (
        '"measurement_stream_state_bootstrap/measurement-stream-state-bootstrap"'
        in script
    )
    _assert_manual_deploy_call(script, "warehouse-bootstrap")
    _assert_manual_deploy_call(script, "measurement-stream-state-bootstrap")


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


def test_checked_in_stream_state_bootstrap_deployment_is_manual_only():
    deployment = _read("pipelines/deployments/measurement_stream_state_bootstrap.yaml")

    assert "name: measurement-stream-state-bootstrap" in deployment
    assert (
        "entrypoint: pipelines/flows/measurement_stream_state_bootstrap.py:"
        "measurement_stream_state_bootstrap" in deployment
    )
    assert "name: canonical" in deployment
    assert "limit: 1" in deployment
    assert "schedules:" not in deployment


def test_worker_start_registers_and_verifies_shadow_publish_deployment():
    script = _read("scripts/prefect_worker_start.sh")

    assert (
        "pipelines/flows/canonical_shadow_publish.py:canonical_shadow_publish" in script
    )
    assert '"canonical-shadow-publish"' in script
    assert (
        'verify_deployment "canonical_shadow_publish/canonical-shadow-publish"'
        in script
    )
    _assert_manual_deploy_call(script, "canonical-shadow-publish")


def test_checked_in_shadow_publish_deployment_is_manual_only():
    deployment = _read("pipelines/deployments/canonical_shadow_publish.yaml")

    assert "name: canonical-shadow-publish" in deployment
    assert (
        "entrypoint: pipelines/flows/canonical_shadow_publish.py:"
        "canonical_shadow_publish" in deployment
    )
    assert "name: canonical" in deployment
    assert "limit: 1" in deployment
    assert "schedules:" not in deployment


def test_worker_start_registers_and_verifies_measurement_backfill_deployment():
    script = _read("scripts/prefect_worker_start.sh")

    assert (
        "pipelines/flows/canonical_measurement_backfill.py:"
        "canonical_measurement_backfill" in script
    )
    assert '"canonical-measurement-backfill"' in script
    assert '"canonical_measurement_backfill/canonical-measurement-backfill"' in script
    _assert_manual_deploy_call(script, "canonical-measurement-backfill")


def test_checked_in_measurement_backfill_deployment_is_manual_only():
    deployment = _read("pipelines/deployments/canonical_measurement_backfill.yaml")

    assert "name: canonical-measurement-backfill" in deployment
    assert (
        "entrypoint: pipelines/flows/canonical_measurement_backfill.py:"
        "canonical_measurement_backfill" in deployment
    )
    assert "name: canonical" in deployment
    assert "limit: 1" in deployment
    assert "collision_strategy: ENQUEUE" in deployment
    assert "schedules:" not in deployment
