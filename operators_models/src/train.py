from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import joblib
import wandb

from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler
from sklearn.metrics import make_scorer, mean_squared_error

from .config import ExperimentConfig, default_paths, CLEANERS, FEATURES, MODEL_FACTORIES, PARAM_SPACES
from . import preprocess as _  # register cleaners/features
from .preprocess import transform_time_unit, to_model_target, from_model_target
from .utils import regression_metrics, make_run_dir, save_json, save_model, TargetNormalizer
from .visualize import (
    plot_actual_vs_pred, plot_feature_importance, plot_qerror_distribution,
    plot_qerror_vs_time_binned, plot_over_under_by_time_bins
)
from . import models as mdl
from .models import q_error, qloss_scorer, lgbm_qloss_postprocess_pred
from .partitioners import make_three_way_indices
from .splitting import parse_test_queries_txt, normalize_query_id_series, assign_bins

_ROWS_COL_KEYS = ("input_rows", "output_rows", "input_rows_left", "input_rows_right")


def load_df(data_root: str, engine: str, op: str) -> pd.DataFrame:
    p = Path(data_root)
    if p.suffix.lower() == ".csv" and p.exists():
        return pd.read_csv(p)
    path = default_paths(data_root, engine, op)
    if not path.exists():
        raise FileNotFoundError(f"Missing dataset: {path}")
    return pd.read_csv(path)


def _rows_cols(df: pd.DataFrame) -> List[str]:
    cols = []
    for c in df.columns:
        lc = c.lower()
        if any(k in lc for k in _ROWS_COL_KEYS):
            cols.append(c)
    return cols


def _build_scaler(kind: str):
    if kind == "none": return None
    if kind == "standard": return StandardScaler()
    if kind == "robust": return RobustScaler()
    if kind == "minmax": return MinMaxScaler()
    raise ValueError(f"Unknown input_norm: {kind}")


def fit_input_transformers(X_train: pd.DataFrame, *, log_counts: bool, input_norm: str):
    """
    Fit-only on X_train, returning (scaler, log_cols, scaled_cols).
    """
    log_cols = _rows_cols(X_train) if log_counts else []

    scaler = _build_scaler(input_norm)
    scaled_cols: List[str] = []
    if scaler is not None:
        for c in X_train.columns:
            if c.startswith(("jt_", "op_")):
                continue
            # strict numeric-only scaling
            try:
                _ = X_train[c].astype(float)
                scaled_cols.append(c)
            except Exception:
                continue
        if scaled_cols:
            Xt = X_train.copy()
            for c in log_cols:
                Xt[c] = np.log1p(np.clip(Xt[c].astype(float), 0.0, None))
            scaler.fit(Xt[scaled_cols])
    return scaler, log_cols, scaled_cols


def apply_input_transformers(X: pd.DataFrame, *, log_cols: List[str], scaler, scaled_cols: List[str]) -> pd.DataFrame:
    X = X.copy()
    for c in log_cols:
        X[c] = np.log1p(np.clip(X[c].astype(float), 0.0, None))
    if scaler is not None and scaled_cols:
        X[scaled_cols] = scaler.transform(X[scaled_cols])
    return X


def _fmt_hps_for_name(hps: Dict) -> str:
    def g(k, default=None): return hps.get(k, default)
    parts = []
    if g("max_depth") is not None:        parts.append(f"d{int(g('max_depth'))}")
    if g("n_estimators") is not None:     parts.append(f"ne{int(g('n_estimators'))}")
    if g("learning_rate") is not None:    parts.append(f"lr{float(g('learning_rate')):.2e}")
    if g("min_child_weight") is not None: parts.append(f"mcw{int(g('min_child_weight'))}")
    if g("subsample") is not None:        parts.append(f"sub{float(g('subsample')):.2f}")
    if g("colsample_bytree") is not None: parts.append(f"col{float(g('colsample_bytree')):.2f}")
    if g("reg_alpha") is not None:        parts.append(f"ra{float(g('reg_alpha')):.2g}")
    if g("reg_lambda") is not None:       parts.append(f"rl{float(g('reg_lambda')):.2g}")
    if g("gamma") is not None:            parts.append(f"gm{float(g('gamma')):.2g}")
    return "-".join(parts)


def _finalize_wandb_name(cfg: ExperimentConfig, overrides: Dict) -> str:
    base = cfg.run_name or f"{cfg.engine}-{cfg.op}-{cfg.model}"
    hp_suffix = _fmt_hps_for_name(overrides or {})
    name = base if not hp_suffix else f"{base}-{hp_suffix}"
    if wandb.run is not None:
        wandb.run.name = name[:128]
    return name


def _validate_cfg(cfg: ExperimentConfig):
    if int(cfg.bin_count) != 20:
        raise SystemExit("This pipeline enforces 20 buckets. Use --bin-count 20.")
    if cfg.loss == "qloss":
        if cfg.predict_log:
            raise SystemExit("QLoss requires --predict-log 0.")
        if cfg.target_norm != "none":
            raise SystemExit("QLoss requires --target-norm none.")
        if cfg.model not in ("xgb", "lgbm"):
            raise SystemExit("QLoss is only supported for --model xgb or lgbm.")
    if cfg.val_size <= 0 or cfg.test_size <= 0 or (cfg.val_size + cfg.test_size) >= 0.9:
        raise SystemExit("Choose reasonable --val-size and --test-size (both >0, sum < 0.9).")
    if cfg.splitter == "bucketed" and not cfg.failed_queries_txt:
        raise SystemExit("--failed-queries-txt is required when --splitter bucketed.")


def _oversample_by_bins(X: pd.DataFrame, y_mod: np.ndarray, y_time: np.ndarray, bins: np.ndarray,
                        *, n_bins: int, seed: int):
    # Equalize bins 1..n_bins (ignore invalid class 0)
    bin_ids = np.arange(1, n_bins + 1)
    counts = {b: int(np.sum(bins == b)) for b in bin_ids}
    non_empty = [c for c in counts.values() if c > 0]
    target_n = max(non_empty) if non_empty else 0

    rng = np.random.RandomState(seed)
    add_idx_chunks = []
    for b in bin_ids:
        idx_b = np.where(bins == b)[0]
        n_b = len(idx_b)
        if n_b == 0 or n_b >= target_n:
            continue
        need = target_n - n_b
        sampled = rng.choice(idx_b, size=need, replace=True)
        add_idx_chunks.append(sampled)

    if not add_idx_chunks:
        return X, y_mod, y_time

    add_idx = np.concatenate(add_idx_chunks)
    X2 = pd.concat([X, X.iloc[add_idx]], axis=0)
    y2 = np.concatenate([y_mod, y_mod[add_idx]])
    t2 = np.concatenate([y_time, y_time[add_idx]])
    print(f"[oversample] size: {len(X)} -> {len(X2)} (+{len(X2) - len(X)})")
    return X2, y2, t2


def run(cfg: ExperimentConfig):
    _validate_cfg(cfg)

    resources_dir = Path(cfg.resources_dir).expanduser().resolve()
    test_queries_path = Path(cfg.test_queries_txt).expanduser().resolve()

    # 1) Load & clean
    raw = load_df(cfg.data_root, cfg.engine, cfg.op)
    clean = CLEANERS[cfg.op](raw, cfg.engine)

    # strict required columns
    if "elapsed_ms" not in clean.columns:
        raise KeyError("cleaned dataframe missing 'elapsed_ms'")
    if cfg.query_col not in clean.columns:
        raise KeyError(f"cleaned dataframe missing query_col='{cfg.query_col}'")

    # 2) Exclude test queries
    test_qids = parse_test_queries_txt(test_queries_path)
    qnorm = normalize_query_id_series(clean[cfg.query_col])
    mask_excl = qnorm.isin(test_qids)
    excluded_rows = int(mask_excl.sum())
    excluded_present = sorted(set(qnorm[mask_excl].tolist()))

    clean = clean.loc[~mask_excl].reset_index(drop=True)
    if len(clean) == 0:
        raise RuntimeError("After excluding test queries, dataset is empty.")

    # 3) Build features/targets from the filtered dataset
    X_full = FEATURES[cfg.op](clean)
    y_ms = clean["elapsed_ms"].astype(float).to_numpy()

    y_seconds = transform_time_unit(y_ms, "s")

    # training/eval use cfg.time_unit
    y_time = transform_time_unit(y_ms, cfg.time_unit)
    y_model_base = to_model_target(y_time, bool(cfg.predict_log))

    # 4) Split indices (writes split artifacts to resources/split_artifacts/)
    tr_idx, va_idx, te_idx, split_report = make_three_way_indices(
        splitter=cfg.splitter,
        df=clean,
        y_seconds=y_seconds,
        resources_dir=resources_dir,
        engine=cfg.engine,
        op=cfg.op,
        query_col=cfg.query_col,
        test_queries_txt=str(test_queries_path),
        excluded_test_queries=excluded_present,
        excluded_rows_count=excluded_rows,
        failed_queries_txt=cfg.failed_queries_txt,
        val_size=cfg.val_size,
        test_size=cfg.test_size,
        seed=cfg.seed,
        n_bins=int(cfg.bin_count),
    )

    # 5) Slice splits
    X_tr_raw = X_full.iloc[tr_idx].copy()
    X_va_raw = X_full.iloc[va_idx].copy()
    X_te_raw = X_full.iloc[te_idx].copy()

    y_time_tr, y_time_va, y_time_te = y_time[tr_idx], y_time[va_idx], y_time[te_idx]
    y_sec_tr, y_sec_va, y_sec_te = y_seconds[tr_idx], y_seconds[va_idx], y_seconds[te_idx]
    y_mod_tr, y_mod_va, y_mod_te = y_model_base[tr_idx], y_model_base[va_idx], y_model_base[te_idx]

    # load edges (strict)
    edges_obj = json.loads(Path(split_report.edges_file).read_text())
    edges = np.asarray(edges_obj["edges"], float)

    # 6) Optional balancing
    if cfg.balance_train:
        bins_tr = assign_bins(y_sec_tr, edges, invalid_class=0)
        X_tr_raw, y_mod_tr, y_time_tr = _oversample_by_bins(
            X_tr_raw, y_mod_tr, y_time_tr, bins_tr,
            n_bins=int(cfg.bin_count), seed=cfg.seed
        )
        

    # 7) Fit input transforms on train, then apply to splits
    scaler_eval, log_cols_eval, scaled_cols_eval = fit_input_transformers(
        X_tr_raw, log_counts=bool(cfg.log_counts), input_norm=cfg.input_norm
    )
    X_tr = apply_input_transformers(X_tr_raw, log_cols=log_cols_eval, scaler=scaler_eval, scaled_cols=scaled_cols_eval)
    X_va = apply_input_transformers(X_va_raw, log_cols=log_cols_eval, scaler=scaler_eval, scaled_cols=scaled_cols_eval)
    X_te = apply_input_transformers(X_te_raw, log_cols=log_cols_eval, scaler=scaler_eval, scaled_cols=scaled_cols_eval)

    # 8) Target normalization on train, mse/huber only
    tnorm_eval = TargetNormalizer(cfg.target_norm)
    if cfg.loss in ("mse", "huber") and cfg.target_norm != "none":
        tnorm_eval.fit(y_mod_tr)
        y_mod_tr_fit = tnorm_eval.transform(y_mod_tr)
        y_mod_va_fit = tnorm_eval.transform(y_mod_va)
        y_mod_te_fit = tnorm_eval.transform(y_mod_te)
    else:
        y_mod_tr_fit, y_mod_va_fit, y_mod_te_fit = y_mod_tr, y_mod_va, y_mod_te

    # 9) W&B init
    meta = dict(
        engine=cfg.engine, op=cfg.op, model=cfg.model,
        time_unit=cfg.time_unit, predict_log=bool(cfg.predict_log),
        target_norm=cfg.target_norm, loss=cfg.loss,
        log_counts=bool(cfg.log_counts), input_norm=cfg.input_norm,
        splitter=cfg.splitter, query_col=cfg.query_col,
        failed_queries_txt=cfg.failed_queries_txt,
        test_queries_txt=str(test_queries_path),
        excluded_rows_count=excluded_rows,
        val_size=cfg.val_size, test_size=cfg.test_size, seed=cfg.seed,
        bin_count=int(cfg.bin_count), balance_train=bool(cfg.balance_train),
        split_artifacts=dict(edges_file=split_report.edges_file),
    )
    if cfg.log_wandb and wandb is not None:
        tags = [
            cfg.engine, cfg.op, cfg.model,
            f"time:{cfg.time_unit}", f"log:{'y' if cfg.predict_log else 'n'}",
            f"inorm:{cfg.input_norm}", f"tnorm:{cfg.target_norm}",
            f"loss:{cfg.loss}", f"split:{cfg.splitter}",
        ]
        wandb.init(project=cfg.project, name=None, group=f"{cfg.engine}-{cfg.op}-{cfg.model}", tags=tags, config={**meta})

    # 10) Hyperparams overrides
    overrides: Dict = {}
    if cfg.hpo == "wandb" and wandb.run is not None:
        for k, v in dict(wandb.config).items():
            if isinstance(k, str) and k.startswith(f"{cfg.model}."):
                overrides[k.split(".", 1)[1]] = v
    if cfg.overrides:
        for k, v in cfg.overrides.items():
            if k.startswith(f"{cfg.model}."):
                overrides[k.split(".", 1)[1]] = v

    objective_name = cfg.loss

    def new_model():
        return MODEL_FACTORIES[cfg.model](objective=objective_name, seed=cfg.seed, **overrides)

    # Optional random HPO (eval-stage: uses Train transform/target-space)
    if cfg.hpo == "random":
        param_space = PARAM_SPACES.get(cfg.model, lambda: {})()
        if param_space:
            base = new_model()
            if cfg.loss == "qloss":
                scoring = qloss_scorer()
            else:
                scoring = make_scorer(lambda yt, yp: -float(np.sqrt(mean_squared_error(yt, yp))), greater_is_better=True)
            base, _search = mdl.random_search(
                base, param_space, X_tr, y_mod_tr_fit, scoring=scoring, n_iter=cfg.hpo_iters, seed=cfg.seed
            )
            overrides.update(base.get_params())

    run_name = _finalize_wandb_name(cfg, overrides)

    run_base = Path("runs") / cfg.engine / cfg.op
    run_dir = make_run_dir(run_base, run_name)

    # 11) Evaluation stage A: train on Train, eval on Val
    model_a = new_model()
    model_a.fit(X_tr, y_mod_tr_fit)

    pred_tr_mod = model_a.predict(X_tr)
    pred_va_mod = model_a.predict(X_va)
    if cfg.model == "lgbm" and cfg.loss == "qloss":
        pred_tr_mod = lgbm_qloss_postprocess_pred(pred_tr_mod)
        pred_va_mod = lgbm_qloss_postprocess_pred(pred_va_mod)
    if cfg.loss in ("mse", "huber") and cfg.target_norm != "none":
        pred_tr_mod = tnorm_eval.inverse(pred_tr_mod)
        pred_va_mod = tnorm_eval.inverse(pred_va_mod)

    pred_tr = np.clip(from_model_target(pred_tr_mod, bool(cfg.predict_log)), 0.0, None)
    pred_va = np.clip(from_model_target(pred_va_mod, bool(cfg.predict_log)), 0.0, None)

    train_metrics = regression_metrics(y_time_tr, pred_tr)
    val_metrics = regression_metrics(y_time_va, pred_va)

    # Train / Val csvs with qerror and sorted ascending by qerror
    df_tr = pd.DataFrame({"actual": y_time_tr, "pred": pred_tr})
    df_tr["qerror"] = q_error(df_tr["actual"].to_numpy(), df_tr["pred"].to_numpy())
    df_tr = df_tr.sort_values("qerror", ascending=True, kind="mergesort").reset_index(drop=True)
    df_tr.to_csv(run_dir / "pred_train.csv", index=False)

    df_va = pd.DataFrame({"actual": y_time_va, "pred": pred_va})
    df_va["qerror"] = q_error(df_va["actual"].to_numpy(), df_va["pred"].to_numpy())
    df_va = df_va.sort_values("qerror", ascending=True, kind="mergesort").reset_index(drop=True)
    df_va.to_csv(run_dir / "pred_val.csv", index=False)

    # 12) Evaluation stage B: train on Train+Val, eval Test
    X_trva_raw = pd.concat([X_tr_raw, X_va_raw], axis=0)

    # apply same eval transforms to tr+val and test
    X_trva = apply_input_transformers(X_trva_raw, log_cols=log_cols_eval, scaler=scaler_eval, scaled_cols=scaled_cols_eval)

    y_trva_fit = np.concatenate([y_mod_tr_fit, y_mod_va_fit])

    model_b = new_model()
    model_b.fit(X_trva, y_trva_fit)

    pred_te_mod = model_b.predict(X_te)
    if cfg.model == "lgbm" and cfg.loss == "qloss":
        pred_te_mod = lgbm_qloss_postprocess_pred(pred_te_mod)
    if cfg.loss in ("mse", "huber") and cfg.target_norm != "none":
        pred_te_mod = tnorm_eval.inverse(pred_te_mod)

    pred_te = np.clip(from_model_target(pred_te_mod, bool(cfg.predict_log)), 1e-6, None)
    test_metrics = regression_metrics(y_time_te, pred_te)
    # Test csv with qerror and sorted ascending by qerror
    df_te = pd.DataFrame({"actual": y_time_te, "pred": pred_te})
    df_te["qerror"] = q_error(df_te["actual"].to_numpy(), df_te["pred"].to_numpy())
    df_te = df_te.sort_values("qerror", ascending=True, kind="mergesort").reset_index(drop=True)
    df_te.to_csv(run_dir / "pred_test.csv", index=False)

    # 13) Plots
    plot_actual_vs_pred(y_time_tr, pred_tr, run_dir / "scatter_train.png", f"{run_name}: train [{cfg.time_unit}]", unit=cfg.time_unit)
    plot_actual_vs_pred(y_time_va, pred_va, run_dir / "scatter_val.png",   f"{run_name}: val [{cfg.time_unit}]",   unit=cfg.time_unit)
    plot_actual_vs_pred(y_time_te, pred_te, run_dir / "scatter_test.png",  f"{run_name}: test [{cfg.time_unit}]",  unit=cfg.time_unit)

    for split_name, yt, yp in [("train", y_time_tr, pred_tr), ("val", y_time_va, pred_va), ("test", y_time_te, pred_te)]:
        qe = q_error(yt, yp)
        plot_qerror_distribution(qe, run_dir / f"qerror_{split_name}.png", f"{run_name}: Q-error ({split_name})")
        plot_qerror_vs_time_binned(
            yt, yp, run_dir / f"qerror_vs_time_{split_name}.png",
            f"{run_name}: Q-error vs time ({split_name})", n_bins=20, log_x=True
        )
        plot_over_under_by_time_bins(
            yt, yp, run_dir / f"over_under_by_time_{split_name}.png",
            f"{run_name}: Over vs Under by time ({split_name})",
            n_bins=20, log_x=True, count_axis="log", annotate_counts=True,
            under_color="#1b7837", over_color="#1f4e79", empty_fill="#555555", empty_alpha=0.45
        )

    # Feature importance (from model_b which is the best-eval model, like before)
    importances = getattr(model_b, "feature_importances_", None)
    if importances is not None:
        fnames = list(X_trva.columns)
        pd.DataFrame({"feature": fnames, "importance": importances}).to_csv(run_dir / "feature_importance.csv", index=False)
        plot_feature_importance(fnames, importances, run_dir / "feature_importance.png", f"{run_name}: feature importance (eval model)")

    # 14) Final retrain on all data (train+val+test) and save only this model + its transforms
    X_all_raw = X_full.copy()
    y_all_time = y_time.copy()
    y_all_mod = y_model_base.copy()

    # If balance_train=1, we also balance the final training set (since it's "the" training now)
    if cfg.balance_train:
        bins_all = assign_bins(y_seconds, edges, invalid_class=0)
        X_all_raw, y_all_mod, y_all_time = _oversample_by_bins(
            X_all_raw, y_all_mod, y_all_time, bins_all, n_bins=int(cfg.bin_count), seed=cfg.seed
        )

    # Fit transforms on ALL for the final saved model
    scaler_final, log_cols_final, scaled_cols_final = fit_input_transformers(
        X_all_raw, log_counts=bool(cfg.log_counts), input_norm=cfg.input_norm
    )
    X_all = apply_input_transformers(X_all_raw, log_cols=log_cols_final, scaler=scaler_final, scaled_cols=scaled_cols_final)

    # Fit target normalizer (mse/huber only) for the final saved model
    tnorm_final = TargetNormalizer(cfg.target_norm)
    if cfg.loss in ("mse", "huber") and cfg.target_norm != "none":
        tnorm_final.fit(y_all_mod)
        y_all_fit = tnorm_final.transform(y_all_mod)
    else:
        y_all_fit = y_all_mod

    model_final = new_model()
    model_final.fit(X_all, y_all_fit)

    # Save the final model
    save_model(model_final, run_dir / "model_final.joblib")

    # Save transforms for the final model
    transforms = {
        "feature_columns": list(X_full.columns),
        "input_log_counts": bool(cfg.log_counts),
        "input_scaled_cols": scaled_cols_final,
        "input_scaler": (type(scaler_final).__name__ if scaler_final else "none"),
        "target": {
            "predict_log": bool(cfg.predict_log),
            "norm": cfg.target_norm,
            "center": getattr(tnorm_final, "center", 0.0),
            "scale": getattr(tnorm_final, "scale", 1.0),
            "min": getattr(tnorm_final, "min_", 0.0),
            "max": getattr(tnorm_final, "max_", 1.0),
        },
    }
    save_json(transforms, run_dir / "transforms.json")
    if scaler_final is not None:
        joblib.dump(scaler_final, run_dir / "input_scaler.joblib")

    # Save metrics
    save_json({
        "config": meta,
        "hyperparams": overrides,
        "train": train_metrics,
        "val": val_metrics,
        "test": test_metrics,
        "notes": {
            "excluded_test_queries_rows": excluded_rows,
            "excluded_test_queries_present": excluded_present,
            "final_model_retrained_on": "train+val+test (after exclusions)",
            "split_artifacts_in_resources_dir": str(resources_dir / "split_artifacts"),
            "split_report_file": str(resources_dir / "split_artifacts" / f"split_{cfg.engine}_{cfg.op}_report.json"),
        },
    }, run_dir / "metrics.json")

    # W&B logging
    if cfg.log_wandb and wandb is not None:
        wandb.log({
            **{f"train/{k}": v for k, v in train_metrics.items()},
            **{f"val/{k}": v for k, v in val_metrics.items()},
            **{f"test/{k}": v for k, v in test_metrics.items()},
            "excluded_test_queries_rows": excluded_rows,
        })
        for p in [
            "scatter_train.png","scatter_val.png","scatter_test.png",
            "feature_importance.png",
            "qerror_train.png","qerror_val.png","qerror_test.png",
            "qerror_vs_time_train.png","qerror_vs_time_val.png","qerror_vs_time_test.png",
            "over_under_by_time_train.png","over_under_by_time_val.png","over_under_by_time_test.png",
        ]:
            fp = run_dir / p
            if fp.exists():
                wandb.log({p: wandb.Image(str(fp))})
        wandb.save(str(run_dir / "model_final.joblib"))
        wandb.finish()

    print(json.dumps({"train": train_metrics, "val": val_metrics, "test": test_metrics}, indent=2))
    print(f"Excluded rows from test_queries.txt: {excluded_rows}")
    print(f"Saved run to: {run_dir}")
    print(f"Split artifacts at: {resources_dir / 'split_artifacts'}")


# ----------------------- CLI ------------------------------------------------

def parse_args():
    ap = argparse.ArgumentParser(description="Train operator runtime model with bucketed or random splitting (excludes test_queries.txt).")

    ap.add_argument("--data-root", required=True)
    ap.add_argument("--resources-dir", default="/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/python/resources")

    ap.add_argument("--engine", required=True, choices=["duckdb", "datafusion"])
    ap.add_argument("--op", required=True, choices=["joins", "filters", "aggregates", "sorts"])

    ap.add_argument("--model", default="xgb", choices=["xgb","lgbm","cat"])
    ap.add_argument("--loss", default="qloss", choices=["qloss","mse","huber"])
    ap.add_argument("--predict-log", type=int, default=0)
    ap.add_argument("--target-norm", default="none", choices=["none","standard","robust","minmax"])

    ap.add_argument("--log-counts", type=int, default=0)
    ap.add_argument("--input-norm", default="none", choices=["none","standard","robust","minmax"])

    ap.add_argument("--splitter", default="bucketed", choices=["bucketed","random"])
    ap.add_argument("--query-col", default="query")

    ap.add_argument("--test-queries-txt",
                    default="/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/python/resources/test_queries.txt")

    ap.add_argument("--failed-queries-txt", default=None, help="Required for splitter=bucketed. Absolute or relative to resources-dir.")
    ap.add_argument("--val-size", type=float, default=0.2)
    ap.add_argument("--test-size", type=float, default=0.2)
    ap.add_argument("--bin-count", type=int, default=20)
    ap.add_argument("--balance-train", type=int, default=0)

    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--log-wandb", type=int, default=1)
    ap.add_argument("--project", default="FlexOps")
    ap.add_argument("--run-name", default=None)

    ap.add_argument("--hpo", default="none", choices=["none","random","wandb"])
    ap.add_argument("--hpo-iters", type=int, default=30)

    ap.add_argument("--time-unit", default="s", choices=["s","ms"])

    ap.add_argument("--override", nargs=2, action="append", default=[], metavar=("KEY", "VAL"))

    args = ap.parse_args()

    def _parse_val(v: str):
        vl = v.lower()
        if vl in {"true","false"}:
            return vl == "true"
        try:
            if v.isdigit() or (v.startswith("-") and v[1:].isdigit()):
                return int(v)
            return float(v)
        except ValueError:
            return v

    ov = {k: _parse_val(v) for k, v in args.override}

    cfg = ExperimentConfig(
        data_root=args.data_root,
        resources_dir=args.resources_dir,
        engine=args.engine,
        op=args.op,
        model=args.model,
        time_unit=args.time_unit,
        predict_log=int(args.predict_log),
        target_norm=args.target_norm,
        loss=args.loss,
        log_counts=int(args.log_counts),
        input_norm=args.input_norm,
        splitter=args.splitter,
        query_col=args.query_col,
        test_queries_txt=args.test_queries_txt,
        failed_queries_txt=args.failed_queries_txt,
        val_size=float(args.val_size),
        test_size=float(args.test_size),
        bin_count=int(args.bin_count),
        balance_train=int(args.balance_train),
        seed=int(args.seed),
        log_wandb=int(args.log_wandb),
        project=args.project,
        run_name=args.run_name,
        hpo=args.hpo,
        hpo_iters=int(args.hpo_iters),
        overrides=ov,
    )
    return cfg


if __name__ == "__main__":
    cfg = parse_args()
    run(cfg)
