from __future__ import annotations

import hashlib
import pickle
from pathlib import Path

import pytest

from inference.model_loader import load_pickle_model


class _Dummy:
    def predict(self, X):  # noqa: N803
        return X


def _write_pickle(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as fh:
        pickle.dump(_Dummy(), fh)


def test_rejects_path_outside_allowed_dirs(tmp_path: Path) -> None:
    outside = tmp_path / "outside" / "model.pkl"
    allowed = tmp_path / "allowed"
    _write_pickle(outside)
    allowed.mkdir()

    with pytest.raises(PermissionError):
        load_pickle_model(str(outside), allowed_dirs=[allowed])


def test_accepts_path_inside_allowed_dirs(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    model_path = allowed / "model.pkl"
    _write_pickle(model_path)

    loaded = load_pickle_model(str(model_path), allowed_dirs=[allowed])
    assert Path(loaded.source_path) == model_path.resolve()


def test_verifies_sha256_sidecar(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    model_path = allowed / "model.pkl"
    _write_pickle(model_path)
    digest = hashlib.sha256(model_path.read_bytes()).hexdigest()

    sidecar = model_path.with_suffix(model_path.suffix + ".sha256")
    sidecar.write_text(digest + "\n")

    loaded = load_pickle_model(str(model_path), allowed_dirs=[allowed])
    assert loaded.model.predict([1]) == [1]


def test_rejects_mismatched_sha256(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    model_path = allowed / "model.pkl"
    _write_pickle(model_path)

    sidecar = model_path.with_suffix(model_path.suffix + ".sha256")
    sidecar.write_text("0" * 64 + "\n")

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        load_pickle_model(str(model_path), allowed_dirs=[allowed])


def test_require_checksum_missing_sidecar(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    model_path = allowed / "model.pkl"
    _write_pickle(model_path)

    with pytest.raises(FileNotFoundError, match="checksum sidecar"):
        load_pickle_model(str(model_path), allowed_dirs=[allowed], require_checksum=True)


def test_traversal_via_symlink_blocked(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    outside = tmp_path / "outside" / "evil.pkl"
    _write_pickle(outside)

    link = allowed / "model.pkl"
    link.symlink_to(outside)

    with pytest.raises(PermissionError):
        load_pickle_model(str(link), allowed_dirs=[allowed])
