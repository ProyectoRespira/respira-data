from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path

PREFECT_ROOT = Path(__file__).resolve().parents[1]
if str(PREFECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PREFECT_ROOT))

from compat import flow, get_run_logger
from config.settings import get_settings


@flow(name="dbt_build")
def dbt_build() -> None:
    logger = get_run_logger()
    settings = get_settings()

    commands = [
        ["dbt", "deps"],
        ["dbt", "seed", "--full-refresh"],
        ["dbt", "run", "--select", "staging+ intermediate+ marts.core+"],
        ["dbt", "run", "--select", "marts.facts+"],
    ]

    for cmd in commands:
        logger.info("Running command: %s", shlex.join(cmd))
        completed = subprocess.run(
            cmd,
            cwd=settings.DBT_PROJECT_DIR,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.stdout:
            logger.info("stdout (tail): %s", completed.stdout[-3000:])
        if completed.stderr:
            logger.warning("stderr (tail): %s", completed.stderr[-3000:])
        if completed.returncode != 0:
            raise RuntimeError(f"dbt_build failed running: {shlex.join(cmd)}")


if __name__ == "__main__":
    dbt_build()
