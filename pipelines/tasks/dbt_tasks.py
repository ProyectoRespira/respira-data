from __future__ import annotations

import importlib
import shlex
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pipelines.compat import get_run_logger, task


@dataclass
class DbtTaskResult:
    status: str
    started_at: datetime
    ended_at: datetime
    duration_s: int
    command: str
    selector: str | None
    artifact_dir: str
    run_results_path: str | None
    stdout: str | None = None
    stderr: str | None = None


def _timeout_for_command(settings, command: str, selector: str | None) -> int:
    if command == "test":
        return settings.DBT_TIMEOUT_TESTS_S
    if selector == "canonical_core":
        return settings.DBT_TIMEOUT_CANONICAL_CORE_S
    if selector == "canonical_silver":
        return settings.DBT_TIMEOUT_CANONICAL_SILVER_S
    if selector == "canonical_full_refresh":
        return max(
            settings.DBT_TIMEOUT_CANONICAL_CORE_S,
            settings.DBT_TIMEOUT_CANONICAL_SILVER_S,
            settings.DBT_TIMEOUT_PROJECT_S,
        )
    if selector and selector.startswith("project_"):
        return settings.DBT_TIMEOUT_PROJECT_S
    return settings.DBT_TIMEOUT_CANONICAL_SILVER_S


def _build_dbt_command(
    settings, command: str, selector: str | None, full_refresh: bool
) -> list[str]:
    command_tokens = shlex.split(command)
    root_command = command_tokens[0] if command_tokens else ""
    supports_threads = root_command in {
        "run",
        "test",
        "build",
        "seed",
        "snapshot",
        "clone",
    }
    cmd: list[str] = [
        "dbt",
        *command_tokens,
        "--project-dir",
        settings.DBT_PROJECT_DIR,
        "--profiles-dir",
        settings.DBT_PROFILES_DIR,
        "--target",
        settings.DBT_TARGET,
    ]
    if supports_threads:
        cmd.extend(["--threads", str(settings.DBT_THREADS)])
    if selector:
        cmd.extend(["--selector", selector])
    if full_refresh:
        cmd.append("--full-refresh")
    return cmd


def _command_has_run_results(command: str) -> bool:
    return command in {"run", "test", "build"}


def _run_subprocess(
    settings, command: str, selector: str | None, full_refresh: bool
) -> DbtTaskResult:
    logger = get_run_logger()
    artifact_dir = str(Path(settings.DBT_PROJECT_DIR) / "target")
    run_results_path = str(Path(artifact_dir) / "run_results.json")
    cmd = _build_dbt_command(settings, command, selector, full_refresh)
    timeout_s = _timeout_for_command(settings, command, selector)
    started_at = datetime.now(UTC)

    logger.info("Running dbt command: %s", shlex.join(cmd))
    logger.info("dbt timeout set to %ss", timeout_s)

    try:
        completed = subprocess.run(
            cmd,
            cwd=settings.DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
        status = "success" if completed.returncode == 0 else "failed"
        stdout = completed.stdout
        stderr = completed.stderr
    except subprocess.TimeoutExpired as exc:
        status = "failed"
        stdout = (
            exc.stdout.decode("utf-8", errors="replace")
            if isinstance(exc.stdout, bytes)
            else exc.stdout
        ) or ""
        stderr = (
            exc.stderr.decode("utf-8", errors="replace")
            if isinstance(exc.stderr, bytes)
            else exc.stderr
        ) or ""
        stderr = f"{stderr}\nCommand timed out after {timeout_s}s".strip()

    ended_at = datetime.now(UTC)
    duration_s = int((ended_at - started_at).total_seconds())

    if stdout:
        logger.info("dbt stdout (tail): %s", stdout[-3000:])
    if stderr:
        logger.warning("dbt stderr (tail): %s", stderr[-3000:])

    final_run_results_path = (
        run_results_path
        if _command_has_run_results(command) and Path(run_results_path).exists()
        else None
    )

    return DbtTaskResult(
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        duration_s=duration_s,
        command=shlex.join(cmd),
        selector=selector,
        artifact_dir=artifact_dir,
        run_results_path=final_run_results_path,
        stdout=stdout,
        stderr=stderr,
    )


def _resolve_prefect_dbt_operation():
    candidates = [
        ("prefect_dbt.cli.commands", "DbtCoreOperation"),
        ("prefect_dbt.cli", "DbtCoreOperation"),
    ]
    for module_name, class_name in candidates:
        try:
            module = importlib.import_module(module_name)
            op_class = getattr(module, class_name, None)
            if op_class is not None:
                return op_class
        except Exception:  # noqa: BLE001
            continue
    return None


def _run_with_prefect_dbt_if_available(
    settings, command: str, selector: str | None, full_refresh: bool
) -> DbtTaskResult | None:
    logger = get_run_logger()
    operation_class = _resolve_prefect_dbt_operation()
    if operation_class is None:
        return None

    timeout_s = _timeout_for_command(settings, command, selector)
    cmd = _build_dbt_command(settings, command, selector, full_refresh)
    cmd_with_binary = shlex.join(cmd)
    started_at = datetime.now(UTC)

    try:
        logger.info("Running dbt via prefect-dbt: %s", cmd_with_binary)
        operation = operation_class(
            commands=[cmd_with_binary],
            project_dir=settings.DBT_PROJECT_DIR,
            profiles_dir=settings.DBT_PROFILES_DIR,
            overwrite_profiles=False,
            stream_output=True,
        )
        operation.run()
        status = "success"
        stdout = None
        stderr = None
    except Exception as exc:  # noqa: BLE001
        logger.warning("prefect-dbt path failed, falling back to subprocess: %s", exc)
        return None

    ended_at = datetime.now(UTC)
    artifact_dir = str(Path(settings.DBT_PROJECT_DIR) / "target")
    run_results_path = str(Path(artifact_dir) / "run_results.json")

    duration_s = int((ended_at - started_at).total_seconds())
    if duration_s > timeout_s:
        status = "failed"
        stderr = f"Command exceeded configured timeout of {timeout_s}s"

    final_run_results_path = (
        run_results_path
        if _command_has_run_results(command) and Path(run_results_path).exists()
        else None
    )

    return DbtTaskResult(
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        duration_s=duration_s,
        command=shlex.join(cmd),
        selector=selector,
        artifact_dir=artifact_dir,
        run_results_path=final_run_results_path,
        stdout=stdout,
        stderr=stderr,
    )


def _run_dbt(
    settings, command: str, selector: str | None, full_refresh: bool
) -> DbtTaskResult:
    pref_result = _run_with_prefect_dbt_if_available(
        settings, command, selector, full_refresh
    )
    if pref_result is not None:
        return pref_result
    return _run_subprocess(settings, command, selector, full_refresh)


@task(name="dbt_deps")
def dbt_deps(settings) -> DbtTaskResult:
    return _run_dbt(settings, command="deps", selector=None, full_refresh=False)


@task(name="dbt_run_selector")
def dbt_run_selector(
    settings, selector: str, full_refresh: bool = False
) -> DbtTaskResult:
    return _run_dbt(
        settings, command="run", selector=selector, full_refresh=full_refresh
    )


@task(name="dbt_test_selector")
def dbt_test_selector(settings, selector: str) -> DbtTaskResult:
    return _run_dbt(settings, command="test", selector=selector, full_refresh=False)


@task(name="dbt_source_freshness")
def dbt_source_freshness(settings) -> DbtTaskResult:
    return _run_dbt(
        settings, command="source freshness", selector=None, full_refresh=False
    )
