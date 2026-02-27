from __future__ import annotations

import sys
from pathlib import Path

# Ensure we import the shared distributed_executor from CHOP/distributed_runner
THIS_DIR = Path(__file__).resolve().parent
ROOT = THIS_DIR.parent.parent
DIST_RUNNER_DIR = ROOT / "distributed_runner"
if str(DIST_RUNNER_DIR) not in sys.path:
    sys.path.insert(0, str(DIST_RUNNER_DIR))

from distributed_executor.engines import (
    _datafusion_mem_str_from_mb,
    _duckdb_mem_str_from_mb,
    _sql_str,
    _unify_batch_nullability,
    register_parquet_views_simple,
    run_whole_query_datafusion,
    run_whole_query_duckdb,
    setup_datafusion,
    setup_duckdb,
    setup_engines,
)

__all__ = [
    "_datafusion_mem_str_from_mb",
    "_duckdb_mem_str_from_mb",
    "_sql_str",
    "_unify_batch_nullability",
    "register_parquet_views_simple",
    "run_whole_query_datafusion",
    "run_whole_query_duckdb",
    "setup_datafusion",
    "setup_duckdb",
    "setup_engines",
]
