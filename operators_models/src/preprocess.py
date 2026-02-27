from __future__ import annotations
import numpy as np
import pandas as pd
from typing import List
from .config import CLEANERS, FEATURES

RANDOM_SEED = 42


def _to_num(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _drop_half_zero_zero(df: pd.DataFrame, in_col="input_rows", out_col="output_rows", frac=0.5) -> pd.DataFrame:
    df = df.copy()
    if in_col not in df.columns or out_col not in df.columns:
        return df
    mask = (df[in_col] == 0) & (df[out_col] == 0)
    idx = df.index[mask]
    if len(idx) == 0:
        return df
    k = int(len(idx) * frac)
    if k == 0:
        return df
    rng = np.random.RandomState(RANDOM_SEED)
    to_drop = rng.choice(idx, size=k, replace=False)
    return df.drop(index=to_drop)


# sorts

def clean_sorts(df: pd.DataFrame, engine: str) -> pd.DataFrame:
    df = df.copy()
    df = _to_num(df, ["input_rows", "output_rows", "row_size_in", "sort_keys_count", "elapsed_ms"])

    if engine.lower() == "duckdb":
        m_fill_from_out = df["input_rows"].isna() & (df["output_rows"] > 4096)
        df.loc[m_fill_from_out, "input_rows"] = df.loc[m_fill_from_out, "output_rows"]
        m_out_zero = df["output_rows"] == 0
        df.loc[m_out_zero, "input_rows"] = 0

    df = _drop_half_zero_zero(df, "input_rows", "output_rows", frac=0.5)
    return df.reset_index(drop=True)


def finalize_sort_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["is_followed_by_limit"] = (out["output_rows"] < out["input_rows"]).astype("int8")
    keep = ["input_rows", "row_size_in", "sort_keys_count", "is_followed_by_limit"]
    return out[keep].copy()




# joins 

JOIN_TYPES = ["CROSS", "FULL", "INNER", "LEFT", "RIGHT", "SEMI"]


def clean_joins(df: pd.DataFrame, engine: str) -> pd.DataFrame:
    df = df.copy()
    df = _to_num(df, [
        "input_rows_left", "input_rows_right", "output_rows",
        "row_size_left", "row_size_right", "elapsed_ms",
    ])
    req_num = ["input_rows_left","input_rows_right","output_rows","row_size_left","row_size_right","elapsed_ms"]
    df = df.dropna(subset=req_num)
    df = df[(df["row_size_left"] != 0) & (df["row_size_right"] != 0)].copy()

    if "join_type_normalized" not in df.columns:
        raise KeyError("clean_joins requires 'join_type_normalized' column.")

    for jt in JOIN_TYPES:
        df[f"jt_{jt.lower()}"] = 0

    for idx, val in df["join_type_normalized"].items():
        if pd.isna(val):
            continue
        jt = str(val).strip().upper()
        if jt in JOIN_TYPES:
            df.loc[idx, f"jt_{jt.lower()}"] = 1

    for jt in JOIN_TYPES:
        df[f"jt_{jt.lower()}"] = df[f"jt_{jt.lower()}"].astype("int8")

    return df.reset_index(drop=True)


def finalize_join_features(df: pd.DataFrame) -> pd.DataFrame:
    keep = [
        "input_rows_left", "input_rows_right", "output_rows",
        "row_size_right", "row_size_left",
    ] + [f"jt_{jt.lower()}" for jt in JOIN_TYPES]
    return df[keep].copy()




# filters

ALL_OPS = ["between","eq","ge","gt","in","is_not_null","is_null","le","lt","ne"]


def clean_filters(df: pd.DataFrame, engine: str) -> pd.DataFrame:
    df = df.copy()
    df = _to_num(df, ["input_rows", "output_rows", "row_size_in", "elapsed_ms"])

    if "normalized_ops" not in df.columns:
        raise KeyError("clean_filters requires 'normalized_ops' column.")

    df = df[~df["normalized_ops"].isna()].copy()

    if engine.lower() == "duckdb":
        m_fix = df["input_rows"].isna() & (df["output_rows"] == 0) & (df["elapsed_ms"] == 0)
        df.loc[m_fix, "input_rows"] = 0
        df = df[~df["input_rows"].isna()].copy()

    df = df[df["row_size_in"] != 0].copy()

    for op in ALL_OPS:
        df[f"op_{op}"] = 0

    for idx, val in df["normalized_ops"].items():
        if pd.isna(val) or str(val).strip() == "":
            continue
        tokens = {t.strip() for t in str(val).split(";") if t.strip()}
        for op in tokens.intersection(ALL_OPS):
            df.loc[idx, f"op_{op}"] = 1

    for op in ALL_OPS:
        df[f"op_{op}"] = df[f"op_{op}"].astype("int8")

    return df.reset_index(drop=True)


def finalize_filter_features(df: pd.DataFrame) -> pd.DataFrame:
    op_cols = [f"op_{op}" for op in ALL_OPS if f"op_{op}" in df.columns]
    keep = ["input_rows", "output_rows", "row_size_in", "logical_ops_count"] + op_cols
    return df[keep].copy()


# aggregates 
def clean_aggregates(df: pd.DataFrame, engine: str) -> pd.DataFrame:
    df = df.copy()
    df = _to_num(df, [
        "input_rows", "output_rows", "row_size_in", "row_size_out",
        "group_keys_count", "aggregation_count", "distinct_count", "elapsed_ms",
    ])

    eng = engine.lower()
    if eng == "datafusion":
        if "group_keys_count" in df.columns:
            df = df[df["group_keys_count"] != 0]
        df = df[df["row_size_in"] != 0]
    elif eng == "duckdb":
        m_drop = df["input_rows"].isna() & (df["output_rows"] > 0)
        df = df[~m_drop].copy()
        m_fill = df["input_rows"].isna() & (df["output_rows"] == 0)
        df.loc[m_fill, "input_rows"] = 0

    return df.reset_index(drop=True)


def finalize_aggregate_features(df: pd.DataFrame) -> pd.DataFrame:
    keep = [
        "input_rows", "output_rows", "row_size_in",
        "group_keys_count", "aggregation_count", "distinct_count",
    ]
    return df[keep].copy()



#  time & target transforms
def transform_time_unit(y_ms: np.ndarray, unit: str) -> np.ndarray:
    y_ms = np.asarray(y_ms, float)
    if unit == "ms":
        return y_ms
    if unit == "s":
        return y_ms / 1000.0
    raise ValueError(f"Unknown time unit: {unit}")


def to_model_target(y_time: np.ndarray, predict_log: bool) -> np.ndarray:
    y_time = np.asarray(y_time, float)
    return np.log(y_time + 1e-6) if predict_log else y_time


def from_model_target(y_target: np.ndarray, predict_log: bool) -> np.ndarray:
    y_target = np.asarray(y_target, float)
    if predict_log:
        y_time = np.exp(y_target) - 1e-6
        return np.maximum(y_time, 0.0)
    return y_target



#  registry updates 
CLEANERS.update({
    "joins": clean_joins,
    "filters": clean_filters,
    "aggregates": clean_aggregates,
    "sorts": clean_sorts,
})

FEATURES.update({
    "joins": finalize_join_features,
    "filters": finalize_filter_features,
    "aggregates": finalize_aggregate_features,
    "sorts": finalize_sort_features,
})
