from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any


class LoadedModel:
    def __init__(self, model: Any, source_path: str):
        self.model = model
        self.source_path = source_path

    def predict(self, features_frame):
        if not hasattr(self.model, "predict"):
            raise TypeError("Loaded object does not implement predict")
        return self.model.predict(features_frame)


def load_pickle_model(path: str) -> LoadedModel:
    model_path = Path(path)
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")

    with model_path.open("rb") as fh:
        model = pickle.load(fh)

    if not hasattr(model, "predict"):
        raise TypeError(f"Model at {model_path} does not expose a predict method")

    return LoadedModel(model=model, source_path=str(model_path))
