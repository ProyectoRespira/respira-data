from __future__ import annotations

import hashlib
import os
import pickle
from pathlib import Path
from typing import Any, Iterable


class LoadedModel:
    def __init__(self, model: Any, source_path: str):
        self.model = model
        self.source_path = source_path

    def predict(self, features_frame):
        if not hasattr(self.model, "predict"):
            raise TypeError("Loaded object does not implement predict")
        return self.model.predict(features_frame)


def _default_allowed_dirs() -> list[Path]:
    env_dirs = os.getenv("MODEL_ALLOWED_DIRS")
    if env_dirs:
        return [Path(p).resolve() for p in env_dirs.split(os.pathsep) if p]
    # Default: repo-local `models/` directory (two levels up from this file).
    repo_models = Path(__file__).resolve().parents[2] / "models"
    return [repo_models.resolve()]


def _is_within(child: Path, parents: Iterable[Path]) -> bool:
    for parent in parents:
        try:
            child.relative_to(parent)
        except ValueError:
            continue
        return True
    return False


def _verify_checksum(model_path: Path, expected_hex: str) -> None:
    hasher = hashlib.sha256()
    with model_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            hasher.update(chunk)
    actual = hasher.hexdigest()
    if actual.lower() != expected_hex.strip().lower():
        raise ValueError(
            f"SHA-256 mismatch for model {model_path}: expected {expected_hex}, got {actual}"
        )


def load_pickle_model(
    path: str,
    *,
    allowed_dirs: Iterable[str | Path] | None = None,
    require_checksum: bool | None = None,
) -> LoadedModel:
    """Load a pickled model with defence-in-depth against untrusted paths.

    Security controls:
    - The resolved (realpath) model path must live under one of the allow-listed
      directories. Defaults to the repo `models/` directory, overridable via the
      `MODEL_ALLOWED_DIRS` env var (os.pathsep separated) or the `allowed_dirs`
      argument.
    - If a sibling `<model>.sha256` file exists, its contents are verified
      against the file before unpickling. When `require_checksum=True` (or env
      `MODEL_REQUIRE_CHECKSUM=1`), the sidecar MUST exist.

    Path allow-listing prevents env-var tampering from redirecting the loader
    to an attacker-controlled pickle; checksum verification prevents in-place
    tampering of a trusted path.
    """

    model_path = Path(path).resolve(strict=False)
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")

    allow_list = (
        [Path(p).resolve() for p in allowed_dirs] if allowed_dirs is not None else _default_allowed_dirs()
    )
    if not _is_within(model_path, allow_list):
        raise PermissionError(
            f"Refusing to load model outside allow-listed directories: {model_path} "
            f"(allowed: {[str(p) for p in allow_list]})"
        )

    if require_checksum is None:
        require_checksum = os.getenv("MODEL_REQUIRE_CHECKSUM", "0") == "1"

    checksum_path = model_path.with_suffix(model_path.suffix + ".sha256")
    if checksum_path.exists():
        expected = checksum_path.read_text().strip().split()[0]
        _verify_checksum(model_path, expected)
    elif require_checksum:
        raise FileNotFoundError(
            f"Missing required checksum sidecar: {checksum_path}"
        )

    with model_path.open("rb") as fh:
        model = pickle.load(fh)  # noqa: S301  path + checksum verified above

    if not hasattr(model, "predict"):
        raise TypeError(f"Model at {model_path} does not expose a predict method")

    return LoadedModel(model=model, source_path=str(model_path))
