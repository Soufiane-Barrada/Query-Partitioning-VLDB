from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

import numpy as np
import joblib
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

from .models import q_error, q_loss


class TargetNormalizer:
    """Invertible target normalizer: none | standard | robust | minmax."""
    def __init__(self, kind: str):
        self.kind = kind
        self.center = 0.0
        self.scale  = 1.0
        self.min_   = 0.0
        self.max_   = 1.0

    def fit(self, y: np.ndarray):
        y = np.asarray(y, float)
        if self.kind == "standard":
            self.center = float(np.mean(y))
            self.scale  = float(np.std(y)) or 1.0
        elif self.kind == "robust":
            q1, q3 = np.percentile(y, [25, 75])
            self.center = float(np.median(y))
            self.scale  = float(q3 - q1) or 1.0
        elif self.kind == "minmax":
            self.min_ = float(np.min(y))
            self.max_ = float(np.max(y))
            self.scale = (self.max_ - self.min_) or 1.0
        elif self.kind == "none":
            self.center, self.scale = 0.0, 1.0
        else:
            raise ValueError(f"Unknown target_norm: {self.kind}")

    def transform(self, y: np.ndarray) -> np.ndarray:
        y = np.asarray(y, float)
        if self.kind == "none":
            return y
        if self.kind == "minmax":
            return (y - self.min_) / self.scale
        return (y - self.center) / self.scale

    def inverse(self, y: np.ndarray) -> np.ndarray:
        y = np.asarray(y, float)
        if self.kind == "none":
            return y
        if self.kind == "minmax":
            return y * self.scale + self.min_
        return y * self.scale + self.center


def regression_metrics(y_true, y_pred) -> Dict[str, float]:
    qe = q_error(y_true, y_pred)
    ql = q_loss(y_true, y_pred)
    return {
        "Q-median": float(np.median(qe)),
        "Q-mean": float(np.mean(qe)),
        "Q-p95": float(np.percentile(qe, 95)),
        "Qloss-mean": float(np.mean(ql)),
        "Qloss-median": float(np.median(ql)),
        "Qloss-p95": float(np.percentile(ql, 95)),
        "MAE": float(mean_absolute_error(y_true, y_pred)),
        "RMSE": float(root_mean_squared_error(y_true, y_pred)),
        "R2": float(r2_score(y_true, y_pred)),
    }


def make_run_dir(base: Path, name: str) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    p = base / f"{ts}-{name}"
    p.mkdir(parents=True, exist_ok=False)
    return p


def save_json(d: Dict[str, Any], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(d, f, indent=2, sort_keys=True)


def save_model(model, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
