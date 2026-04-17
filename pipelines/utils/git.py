from __future__ import annotations

import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def git_sha(cwd: Path | str | None = None) -> str | None:
    """Return the short HEAD sha of the repository, or None on any failure.

    Shared by all Prefect flows that need a deployment traceability marker.
    Fails soft: git missing, not a repo, or non-zero exit all yield ``None``.
    """

    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(cwd) if cwd is not None else REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except (FileNotFoundError, OSError):
        return None

    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None
