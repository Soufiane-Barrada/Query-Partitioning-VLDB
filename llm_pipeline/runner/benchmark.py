from __future__ import annotations

import pandas as pd
import numpy as np
import pyarrow as pa

from .distributed_runner import DistributedRunner
from .models import SubQuery


# Normalization helpers

def _arrow_to_pandas_normalized(tbl: pa.Table) -> pd.DataFrame:
    df = tbl.to_pandas(types_mapper=None)
    df.columns = [str(c).lower() for c in df.columns]

    for col in df.columns:
        s = df[col]
        if pd.api.types.is_datetime64tz_dtype(s):
            df[col] = s.dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[ns]")
        elif pd.api.types.is_datetime64_dtype(s):
            df[col] = s.astype("datetime64[ns]")
        elif pd.api.types.is_bool_dtype(s):
            df[col] = s.astype("boolean")
        elif pd.api.types.is_integer_dtype(s):
            df[col] = s.astype("Int64")
        elif pd.api.types.is_float_dtype(s):
            df[col] = s.astype("float64")
        else:
            try:
                df[col] = s.astype("string")
            except Exception:
                pass

    return df


def _sort_df_by_all_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.reset_index(drop=True)
    return df.sort_values(by=list(df.columns), ascending=True, na_position="last").reset_index(drop=True)


def _series_equal(a: pd.Series, b: pd.Series, float_tol: float) -> bool:
    a_is_num = pd.api.types.is_numeric_dtype(a)
    b_is_num = pd.api.types.is_numeric_dtype(b)

    if a_is_num or b_is_num:
        a_vals = a.astype("float64")
        b_vals = b.astype("float64")
        a_nan = a_vals.isna().values
        b_nan = b_vals.isna().values
        if not np.array_equal(a_nan, b_nan):
            return False
        mask = ~a_nan
        if float_tol == 0.0:
            return np.array_equal(a_vals.values[mask], b_vals.values[mask])
        return np.allclose(a_vals.values[mask], b_vals.values[mask], rtol=0.0, atol=float_tol)

    return a.equals(b)


def _tables_exact_equal(
    t1: pa.Table,
    t2: pa.Table,
    *,
    float_tol: float = 0.0,
    ignore_column_order: bool = True,
) -> bool:
    if t1.num_rows != t2.num_rows:
        print(f"Table row count mismatch: {t1.num_rows} ≠ {t2.num_rows}")
        return False

    df1 = _arrow_to_pandas_normalized(t1)
    df2 = _arrow_to_pandas_normalized(t2)

    cols1 = list(df1.columns)
    cols2 = list(df2.columns)
    if set(cols1) != set(cols2):
        print("Table columns mismatch.")
        return False

    if ignore_column_order:
        cols_sorted = sorted(cols1)
        df1 = df1[cols_sorted]
        df2 = df2[cols_sorted]
    else:
        if cols1 != cols2:
            return False

    df1 = _sort_df_by_all_columns(df1)
    df2 = _sort_df_by_all_columns(df2)

    for col in df1.columns:
        if not _series_equal(df1[col], df2[col], float_tol=float_tol):
            print(f"'{col}' column mismatch.")
            return False

    return True


def make_subqueries(sql1: str, sql2: str, engine1: str, engine2: str) -> list[SubQuery]:
    return [
        SubQuery(id="s1", engine=engine1, sql=sql1),
        SubQuery(id="s2", engine=engine2, inputs={"s1": "s1"}, sql=sql2),
    ]


def benchmark_distributed_only(
    *,
    query_id: str,
    subqueries: list[SubQuery],
    runner: DistributedRunner,
    baseline_tbl: pa.Table,
    whole_duckdb_median_s: float,
    whole_datafusion_median_s: float,
    whole_duckdb_rows: int,
    whole_datafusion_rows: int,
    do_check: bool,
) -> pd.DataFrame:
    runner.results.clear()
    runner.timings.clear()

    distributed_results = runner.run(subqueries)
    distributed_times = runner.timings
    dist_tbl = distributed_results[subqueries[-1].id]
    s1_tbl = distributed_results[subqueries[0].id]

    transfer_rows = int(s1_tbl.num_rows)
    try:
        transfer_bytes = int(getattr(s1_tbl, "nbytes", 0) or 0)
    except Exception:
        transfer_bytes = 0
    transfer_avg_row_bytes = (transfer_bytes / transfer_rows) if transfer_rows > 0 else None

    root_rows = int(dist_tbl.num_rows)
    try:
        root_bytes = int(getattr(dist_tbl, "nbytes", 0) or 0)
    except Exception:
        root_bytes = 0
    root_avg_row_bytes = (root_bytes / root_rows) if root_rows > 0 else None

    results_match = True
    if do_check:
        results_match = _tables_exact_equal(dist_tbl, baseline_tbl, float_tol=0.0, ignore_column_order=True)

    row = {
        "query_id": query_id,
        "distributed_total": float(distributed_times["total_distributed"]),
        "duckdb_whole": float(whole_duckdb_median_s),
        "datafusion_whole": float(whole_datafusion_median_s),
        "results_match": bool(results_match),
        "distributed_rows": int(dist_tbl.num_rows),
        "duckdb_rows": int(whole_duckdb_rows),
        "datafusion_rows": int(whole_datafusion_rows),
        "transfer_actual_rows": transfer_rows,
        "transfer_actual_bytes": transfer_bytes,
        "transfer_actual_avg_row_bytes": transfer_avg_row_bytes,
        "root_actual_avg_row_bytes": root_avg_row_bytes,
    }

    for sid, t in distributed_times.items():
        if sid.startswith("s"):
            row[f"{sid}_time"] = float(t)

    return pd.DataFrame([row])
