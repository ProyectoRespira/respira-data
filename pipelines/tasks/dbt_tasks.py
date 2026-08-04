from __future__ import annotations

import importlib
import json
import os
import shlex
import signal
import subprocess
import threading
import time
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


def _normalize_timeout(timeout_s: int | None) -> int | None:
    if timeout_s is None or timeout_s <= 0:
        return None
    return timeout_s


def _timeout_for_command(settings, command: str, selector: str | None) -> int | None:
    if command == "test":
        return _normalize_timeout(settings.DBT_TIMEOUT_TESTS_S)
    if selector in {"canonical_core", "canonical_incremental_core"}:
        return _normalize_timeout(settings.DBT_TIMEOUT_CANONICAL_CORE_S)
    if selector == "canonical_silver":
        return _normalize_timeout(settings.DBT_TIMEOUT_CANONICAL_SILVER_S)
    if selector == "canonical_batch_prep":
        return _normalize_timeout(settings.DBT_TIMEOUT_CANONICAL_CORE_S)
    if selector in {"canonical_batch_ingest", "canonical_batch_payload_audit"}:
        return _normalize_timeout(settings.DBT_TIMEOUT_CANONICAL_BATCH_INGEST_S)
    if selector == "canonical_batch_process":
        return _normalize_timeout(settings.DBT_TIMEOUT_CANONICAL_SILVER_S)
    if selector in {"canonical_shadow_state", "canonical_shadow_publish"}:
        return _normalize_timeout(settings.DBT_TIMEOUT_CANONICAL_SILVER_S)
    if selector == "canonical_batch_smoke_tests":
        return _normalize_timeout(settings.DBT_TIMEOUT_TESTS_S)
    if selector == "canonical_full_refresh":
        return _normalize_timeout(
            max(
                settings.DBT_TIMEOUT_CANONICAL_CORE_S,
                settings.DBT_TIMEOUT_CANONICAL_SILVER_S,
                settings.DBT_TIMEOUT_PROJECT_S,
            )
        )
    if selector and selector.startswith("project_"):
        return _normalize_timeout(settings.DBT_TIMEOUT_PROJECT_S)
    return _normalize_timeout(settings.DBT_TIMEOUT_CANONICAL_SILVER_S)


def _build_dbt_command(
    settings,
    command: str,
    selector: str | None,
    full_refresh: bool,
    vars_payload: dict[str, object] | None = None,
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
    if vars_payload:
        cmd.extend(["--vars", _serialize_dbt_vars(vars_payload)])
    return cmd


def _serialize_dbt_vars(vars_payload: dict[str, object]) -> str:
    def _default(value: object) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    return json.dumps(vars_payload, default=_default, separators=(",", ":"))


def _command_has_run_results(command: str) -> bool:
    return command in {"run", "test", "build"}


def _terminate_process_group(process: subprocess.Popen, grace_s: float = 10.0) -> None:
    if process.poll() is not None:
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    deadline = time.monotonic() + grace_s
    while process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)

    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return


def _stream_subprocess_output(
    logger_method,
    prefix: str,
    stream,
    buffer: list[str],
) -> None:
    if stream is None:
        return

    try:
        for raw_line in iter(stream.readline, ""):
            buffer.append(raw_line)
            line = raw_line.rstrip()
            if line:
                logger_method("%s%s", prefix, line)
    finally:
        stream.close()


def _run_subprocess(
    settings,
    command: str,
    selector: str | None,
    full_refresh: bool,
    vars_payload: dict[str, object] | None = None,
) -> DbtTaskResult:
    logger = get_run_logger()
    artifact_dir = str(Path(settings.DBT_PROJECT_DIR) / "target")
    run_results_path = str(Path(artifact_dir) / "run_results.json")
    cmd = _build_dbt_command(
        settings,
        command,
        selector,
        full_refresh,
        vars_payload=vars_payload,
    )
    timeout_s = _timeout_for_command(settings, command, selector)
    started_at = datetime.now(UTC)

    logger.info("Running dbt command: %s", shlex.join(cmd))
    if timeout_s is None:
        logger.info("dbt timeout disabled")
    else:
        logger.info("dbt timeout set to %ss", timeout_s)

    process: subprocess.Popen[str] | None = None
    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    stdout_thread: threading.Thread | None = None
    stderr_thread: threading.Thread | None = None
    try:
        process = subprocess.Popen(
            cmd,
            cwd=settings.DBT_PROJECT_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        stdout_thread = threading.Thread(
            target=_stream_subprocess_output,
            args=(logger.info, "dbt stdout: ", process.stdout, stdout_chunks),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_stream_subprocess_output,
            args=(logger.warning, "dbt stderr: ", process.stderr, stderr_chunks),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        if timeout_s is None:
            process.wait()
        else:
            process.wait(timeout=timeout_s)
        status = "success" if process.returncode == 0 else "failed"
    except subprocess.TimeoutExpired:
        if process is not None:
            _terminate_process_group(process)
        status = "failed"
        stderr_chunks.append(f"Command timed out after {timeout_s}s\n")
    except KeyboardInterrupt:
        if process is not None:
            _terminate_process_group(process)
        raise
    finally:
        if stdout_thread is not None:
            stdout_thread.join(timeout=5)
        if stderr_thread is not None:
            stderr_thread.join(timeout=5)

    ended_at = datetime.now(UTC)
    duration_s = int((ended_at - started_at).total_seconds())
    stdout = "".join(stdout_chunks)
    stderr = "".join(stderr_chunks)

    logger.info("dbt command finished with status=%s duration=%ss", status, duration_s)

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
    settings,
    command: str,
    selector: str | None,
    full_refresh: bool,
    vars_payload: dict[str, object] | None = None,
) -> DbtTaskResult | None:
    logger = get_run_logger()
    if not getattr(settings, "DBT_USE_PREFECT_DBT", False):
        return None

    operation_class = _resolve_prefect_dbt_operation()
    if operation_class is None:
        return None

    timeout_s = _timeout_for_command(settings, command, selector)
    cmd = _build_dbt_command(
        settings,
        command,
        selector,
        full_refresh,
        vars_payload=vars_payload,
    )
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
    if timeout_s is not None and duration_s > timeout_s:
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
    settings,
    command: str,
    selector: str | None,
    full_refresh: bool,
    vars_payload: dict[str, object] | None = None,
) -> DbtTaskResult:
    pref_result = _run_with_prefect_dbt_if_available(
        settings,
        command,
        selector,
        full_refresh,
        vars_payload=vars_payload,
    )
    if pref_result is not None:
        return pref_result
    return _run_subprocess(
        settings,
        command,
        selector,
        full_refresh,
        vars_payload=vars_payload,
    )


@task(name="dbt_deps")
def dbt_deps(settings) -> DbtTaskResult:
    return _run_dbt(settings, command="deps", selector=None, full_refresh=False)


@task(name="dbt_seed_selector")
def dbt_seed_selector(
    settings,
    selector: str,
    full_refresh: bool = False,
    vars_payload: dict[str, object] | None = None,
) -> DbtTaskResult:
    return _run_dbt(
        settings,
        command="seed",
        selector=selector,
        full_refresh=full_refresh,
        vars_payload=vars_payload,
    )


@task(name="dbt_run_selector")
def dbt_run_selector(
    settings,
    selector: str,
    full_refresh: bool = False,
    vars_payload: dict[str, object] | None = None,
) -> DbtTaskResult:
    return _run_dbt(
        settings,
        command="run",
        selector=selector,
        full_refresh=full_refresh,
        vars_payload=vars_payload,
    )


@task(name="dbt_test_selector")
def dbt_test_selector(
    settings, selector: str, vars_payload: dict[str, object] | None = None
) -> DbtTaskResult:
    return _run_dbt(
        settings,
        command="test",
        selector=selector,
        full_refresh=False,
        vars_payload=vars_payload,
    )


@task(name="dbt_source_freshness")
def dbt_source_freshness(settings) -> DbtTaskResult:
    return _run_dbt(
        settings, command="source freshness", selector=None, full_refresh=False
    )
