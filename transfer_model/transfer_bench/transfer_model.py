from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Protocol

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import LogLocator, NullFormatter



class BaseModel(Protocol):
    name: str

    def fit(self, df: pd.DataFrame) -> Dict[str, float]:
        ...

    def predict(self, df: pd.DataFrame, params: Dict[str, float]) -> np.ndarray:
        ...


# Linear model


@dataclass(frozen=True)
class AbcdLinearModel:
    """
    y = a + b*rows + c*row_size_bytes + d*(rows*row_size_bytes)
    """
    name: str = "abcd_linear"

    def fit(self, df: pd.DataFrame) -> Dict[str, float]:
        rows = df["rows"].to_numpy(dtype=np.float64)
        rs = df["row_size_bytes"].to_numpy(dtype=np.float64)
        y = df["transfer_ms_median"].to_numpy(dtype=np.float64)

        X = np.stack([np.ones_like(rows), rows, rs, rows * rs], axis=1)
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)

        a, b, c, d = (float(coef[0]), float(coef[1]), float(coef[2]), float(coef[3]))

        # clamp negatives for Java usage
        a = max(0.0, a)
        b = max(0.0, b)
        c = max(0.0, c)
        d = max(0.0, d)

        return {
            "aMs": a,
            "bMsPerRow": b,
            "cMsPerRowSizeByte": c,
            "dMsPerOutputByte": d,
        }

    def predict(self, df: pd.DataFrame, params: Dict[str, float]) -> np.ndarray:
        rows = df["rows"].to_numpy(dtype=np.float64)
        rs = df["row_size_bytes"].to_numpy(dtype=np.float64)
        return (
            params["aMs"]
            + params["bMsPerRow"] * rows
            + params["cMsPerRowSizeByte"] * rs
            + params["dMsPerOutputByte"] * (rows * rs)
        )


# Metrics

def rmse(y: np.ndarray, yhat: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y - yhat) ** 2)))


def r2(y: np.ndarray, yhat: np.ndarray) -> float:
    ss_res = float(np.sum((y - yhat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    return 1.0 - (ss_res / ss_tot)


# Plots

def save_pred_vs_actual_plot(
    y: np.ndarray,
    yhat: np.ndarray,
    plots_dir: Path,
    direction: str,
    model_name: str,
) -> None:
    plots_dir.mkdir(parents=True, exist_ok=True)

    plt.figure()
    plt.scatter(y, yhat, s=12)
    mn = float(min(y.min(), yhat.min()))
    mx = float(max(y.max(), yhat.max()))
    plt.plot([mn, mx], [mn, mx], linewidth=1)

    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("actual transfer_ms (median)")
    plt.ylabel("predicted transfer_ms")
    plt.title(f"{model_name}: predicted vs actual ({direction})")
    plt.tight_layout()
    plt.savefig(plots_dir / f"pred_vs_actual_{model_name}_{direction}.png", dpi=200)
    plt.close()


def save_bytes_scatter_with_prediction_line(
    df: pd.DataFrame,
    params: Dict[str, float],
    model: BaseModel,
    plots_dir: Path,
    direction: str,
    model_name: str,
) -> None:
    """
    Scatter: x=bytes_feature, y=transfer_ms_median.
    Line: model predictions evaluated over log-spaced bytes_feature values.
    To produce a single 2D line, we fix row_size_bytes to a representative value
    (median row_size_bytes for this direction) and vary rows so that
    rows * row_size_bytes == bytes_feature.
    """
    plots_dir.mkdir(parents=True, exist_ok=True)

    x = df["bytes_feature"].to_numpy(dtype=np.float64)
    y = df["transfer_ms_median"].to_numpy(dtype=np.float64)

    # representative row size for a stable 1D curve
    rs_fixed = float(np.median(df["row_size_bytes"].to_numpy(dtype=np.float64)))
    if rs_fixed <= 0.0:
        raise ValueError("median row_size_bytes must be > 0")

    # build log-spaced bytes grid across observed range
    xmin = float(x.min())
    xmax = float(x.max())
    if xmin <= 0.0:
        raise ValueError("bytes_feature must be > 0 for log plot")
    grid_bytes = np.exp(np.linspace(np.log(xmin), np.log(xmax), 200))

    # choose rows so that rows * rs_fixed = grid_bytes
    grid_rows = grid_bytes / rs_fixed

    df_line = pd.DataFrame(
        {
            "rows": grid_rows,
            "row_size_bytes": np.full_like(grid_rows, rs_fixed),
            "bytes_feature": grid_bytes,
            "transfer_ms_median": np.nan,
        }
    )
    y_line = model.predict(df_line, params)

    plt.figure(figsize=(8, 5))
    plt.scatter(x, y, s=10)
    plt.plot(grid_bytes, y_line, linewidth=2, color='orange')

    plt.xscale("log")
    plt.yscale("log")

    ax = plt.gca()
    ax.xaxis.set_major_locator(LogLocator(base=10))
    ax.xaxis.set_minor_locator(LogLocator(base=10, subs=[]))
    ax.xaxis.set_minor_formatter(NullFormatter())

    ax.yaxis.set_major_locator(LogLocator(base=10))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=[]))
    ax.yaxis.set_minor_formatter(NullFormatter())

    ax.set_ylim(1, 10000)

    plt.xlabel("Table size (Bytes)")
    plt.ylabel("Transfer time (ms)")
    plt.tight_layout()
    plt.savefig(plots_dir / f"bytes_scatter_with_fit_{model_name}_{direction}.png", dpi=200)
    plt.close()


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True, help="Path to transfer_median.csv")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--runtime-config", required=True)
    ap.add_argument("--model", choices=["abcd_linear"], required=True)
    args = ap.parse_args()

    df = pd.read_csv(Path(args.data))

    required = {"direction", "rows", "row_size_bytes", "bytes_feature", "transfer_ms_median"}
    if not required.issubset(df.columns):
        raise ValueError(f"CSV missing required columns: {required}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    plots_dir = out_dir / "plots_fit"
    plots_dir.mkdir(parents=True, exist_ok=True)

    # Model registry
    model_registry: Dict[str, BaseModel] = {
        "abcd_linear": AbcdLinearModel(),
    }
    model = model_registry[args.model]

    models_by_direction: dict[str, dict[str, float]] = {}

    for direction in ["duckdb_to_datafusion", "datafusion_to_duckdb"]:
        sub = df[df["direction"] == direction].copy()
        if sub.empty:
            raise ValueError(f"No rows found for direction={direction}")

        params = model.fit(sub)
        yhat = model.predict(sub, params)

        y = sub["transfer_ms_median"].to_numpy(dtype=np.float64)

        print(f"\n=== direction={direction} ===")
        print(f"model: {model.name}")
        print(json.dumps(params, indent=2))
        print(f"RMSE(ms): {rmse(y, yhat):.6f}")
        print(f"R2     : {r2(y, yhat):.6f}")

        # plots
        save_pred_vs_actual_plot(y, yhat, plots_dir, direction, model.name)
        save_bytes_scatter_with_prediction_line(sub, params, model, plots_dir, direction, model.name)

        models_by_direction[direction] = params

    # - duckdb_to_datafusion -> receiver is DATAFUSION
    # - datafusion_to_duckdb -> receiver is DUCKDB
    transfer_models = {
        "DATAFUSION": models_by_direction["duckdb_to_datafusion"],
        "DUCKDB": models_by_direction["datafusion_to_duckdb"],
    }

    model_path = out_dir / "transfer_model.json"
    model_path.write_text(
        json.dumps({"transferModels": transfer_models, "modelType": model.name}, indent=2),
        encoding="utf-8",
    )

    # update runtime-config.json
    cfg_path = Path(args.runtime_config)
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg["transferModels"] = transfer_models
    cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

    print(f"\nWrote: {model_path}")
    print(f"Updated runtime config: {cfg_path}")
    print(f"Wrote fit plots in: {plots_dir}")


if __name__ == "__main__":
    main()
