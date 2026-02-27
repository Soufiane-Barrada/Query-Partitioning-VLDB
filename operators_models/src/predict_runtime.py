from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


#  helpers

_ROW_KEYS = ("input_rows", "output_rows", "input_rows_left", "input_rows_right")


def _softplus(x):
    x = np.asarray(x, float)
    return np.log1p(np.exp(-np.abs(x))) + np.maximum(x, 0.0)


def lgbm_qloss_postprocess_pred(raw_pred):
    return _softplus(np.asarray(raw_pred, float))


def from_model_target(y_target, predict_log: bool):
    y_target = np.asarray(y_target, float)
    if predict_log:
        y_time = np.exp(y_target) - 1e-6
        return np.maximum(y_time, 0.0)
    return y_target


class TargetNormalizer:
    """Invertible target normalizer: none | standard | robust | minmax."""
    def __init__(self, kind: str):
        self.kind = kind
        self.center = 0.0
        self.scale  = 1.0
        self.min_   = 0.0
        self.max_   = 1.0

    def inverse(self, y: np.ndarray) -> np.ndarray:
        y = np.asarray(y, float)
        if self.kind == "none":
            return y
        if self.kind == "minmax":
            return y * self.scale + self.min_
        return y * self.scale + self.center


# loading + transforms

def load_run(run_path: Path) -> dict:
    """
    run_path can be either:
      - directory containing model_final.joblib, transforms.json, metrics.json
      - path to model_final.joblib inside that directory
    """
    if run_path.is_dir():
        run_dir = run_path
        model_path = run_dir / "model_final.joblib"
    else:
        model_path = run_path
        run_dir = model_path.parent

    if not model_path.exists():
        raise FileNotFoundError(f"Missing model file: {model_path}")
    if not (run_dir / "transforms.json").exists():
        raise FileNotFoundError(f"Missing transforms.json in: {run_dir}")
    if not (run_dir / "metrics.json").exists():
        raise FileNotFoundError(f"Missing metrics.json in: {run_dir}")

    model = joblib.load(model_path)

    with open(run_dir / "transforms.json") as f:
        tr = json.load(f)

    feature_columns = tr.get("feature_columns")
    if not feature_columns:
        raise KeyError("transforms.json missing 'feature_columns' (or it is empty).")

    log_counts = bool(tr.get("input_log_counts", False))
    scaled_cols = tr.get("input_scaled_cols", [])
    scaler_name = tr.get("input_scaler", "none")

    scaler = None
    if scaler_name != "none":
        scaler_path = run_dir / "input_scaler.joblib"
        if not scaler_path.exists():
            raise FileNotFoundError(
                f"transforms.json says input_scaler={scaler_name!r} but missing file: {scaler_path}"
            )
        scaler = joblib.load(scaler_path)

    tcfg = tr.get("target", {})
    predict_log = bool(tcfg.get("predict_log", False))
    target_norm = tcfg.get("norm", "none")

    tnorm = TargetNormalizer(target_norm)
    tnorm.center = float(tcfg.get("center", 0.0))
    tnorm.scale  = float(tcfg.get("scale", 1.0))
    tnorm.min_   = float(tcfg.get("min", 0.0))
    tnorm.max_   = float(tcfg.get("max", 1.0))

    with open(run_dir / "metrics.json") as f:
        m = json.load(f)
    cfg = m.get("config", {})

    # these come from the training meta
    model_name = cfg.get("model", "xgb")      # "xgb" | "lgbm" | "cat"
    loss       = cfg.get("loss", "qloss")     # "qloss" | "mse" | "huber"
    time_unit  = cfg.get("time_unit", "s")    # "s" | "ms"

    return dict(
        model=model,
        feature_columns=list(feature_columns),
        log_counts=log_counts,
        scaled_cols=list(scaled_cols) if scaled_cols else [],
        scaler=scaler,
        predict_log=predict_log,
        target_norm=target_norm,
        tnorm=tnorm,
        model_name=model_name,
        loss=loss,
        time_unit=time_unit,
    )


def apply_input_transforms(X: pd.DataFrame, *, log_counts: bool, scaled_cols, scaler):
    X = X.copy()

    if log_counts:
        for c in X.columns:
            lc = c.lower()
            if any(k in lc for k in _ROW_KEYS):
                X[c] = np.log1p(np.clip(X[c].astype(float), 0.0, None))

    if scaler is not None and scaled_cols:
        # only transform cols that exist
        cols = [c for c in scaled_cols if c in X.columns]
        if cols:
            X[cols] = scaler.transform(X[cols])

    return X


def predict_single(run_info: dict, features: dict) -> dict:
    cols = run_info["feature_columns"]

    row_vals = []
    for c in cols:
        if c not in features:
            raise KeyError(f"Missing feature '{c}' in input JSON.")
        row_vals.append(float(features[c]))

    X = pd.DataFrame([row_vals], columns=cols)

    Xp = apply_input_transforms(
        X,
        log_counts=run_info["log_counts"],
        scaled_cols=run_info["scaled_cols"],
        scaler=run_info["scaler"],
    )

    y_mod = run_info["model"].predict(Xp)

    # LGBM + QLoss uses softplus link at inference
    if run_info["model_name"] == "lgbm" and run_info["loss"] == "qloss":
        y_mod = lgbm_qloss_postprocess_pred(y_mod)

    # Undo target normalization only for mse/huber runs where used
    if run_info["loss"] in ("mse", "huber") and run_info["target_norm"] != "none":
        y_mod = run_info["tnorm"].inverse(y_mod)

    # Undo log-target if used
    y_time = from_model_target(y_mod, predict_log=run_info["predict_log"])
    y_time = np.clip(y_time, 1e-6, None)

    return {"time": float(y_time[0]), "unit": run_info["time_unit"]}



def main():
    ap = argparse.ArgumentParser(description="Predict operator runtime from a trained run directory.")
    ap.add_argument(
        "--run-path",
        required=True,
        help="Path to run directory OR to model_final.joblib inside it."
    )
    args = ap.parse_args()

    run_path = Path(args.run_path).expanduser().resolve()
    run_info = load_run(run_path)

    # Read a single JSON object from stdin
    features = json.load(sys.stdin)

    out = predict_single(run_info, features)
    json.dump(out, sys.stdout)
    sys.stdout.write("\n")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
