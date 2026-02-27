from __future__ import annotations
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Callable, Any


@dataclass
class ExperimentConfig:
    data_root: str
    resources_dir: str  # for split artifacts

    engine: str   # "duckdb" | "datafusion"
    op: str       # "joins" | "filters" | "aggregates" | "sorts"
    model: str    # "xgb" | "lgbm" | "cat"

    time_unit: str = "s"  # "ms" | "s"

    # Target transforms & objective
    predict_log: int = 0
    target_norm: str = "none"  # only for mse/huber
    loss: str = "qloss"        # "qloss" | "mse" | "huber"

    # Input transforms
    log_counts: int = 0
    input_norm: str = "none"   # "none" | "standard" | "robust" | "minmax"

    # Split
    splitter: str = "bucketed"  # "bucketed" | "random"
    query_col: str = "query_id"

    # Exclusions
    test_queries_txt: str = "/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/python/resources/test_queries.txt"

    # Bucketed splitter only
    failed_queries_txt: str | None = None

    val_size: float = 0.2
    test_size: float = 0.2
    bin_count: int = 20  # fixed to 20 in this pipeline

    # balancing (oversampling) based on the SAME stable buckets
    balance_train: int = 0

    seed: int = 42

    # Logging / saving
    log_wandb: int = 1
    project: str = "FlexOps"
    run_name: str | None = None

    # HPO
    hpo: str = "none"  # "none" | "random" | "wandb"
    hpo_iters: int = 30

    overrides: Dict[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def default_paths(data_root: str, engine: str, op: str) -> Path:
    eroot = {"duckdb": "duckdb_ops", "datafusion": "datafusion_ops"}[engine.lower()]
    return Path(data_root) / eroot / f"{op}.csv"


# registries
CLEANERS: Dict[str, Callable] = {}
FEATURES: Dict[str, Callable] = {}
MODEL_FACTORIES: Dict[str, Callable] = {}
PARAM_SPACES: Dict[str, Callable] = {}
