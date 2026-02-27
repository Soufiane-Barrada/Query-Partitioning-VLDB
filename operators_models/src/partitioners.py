from __future__ import annotations
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd

from .splitting import (
    get_or_create_edges,
    parse_failed_queries_from_report,
    split_random_three_way,
    split_bucketed_three_way,
    SplitReport,
    write_split_artifacts,
)


def make_three_way_indices(
    *,
    splitter: str,
    df: pd.DataFrame,
    y_seconds: np.ndarray,
    resources_dir: Path,
    engine: str,
    op: str,
    query_col: str,
    test_queries_txt: str,
    excluded_test_queries: list[str],
    excluded_rows_count: int,
    failed_queries_txt: str | None,
    val_size: float,
    test_size: float,
    seed: int,
    n_bins: int = 20,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, SplitReport]:
    edges, edges_file = get_or_create_edges(resources_dir, engine, op, y_seconds, n_bins=n_bins)

    if splitter == "random":
        tr, va, te = split_random_three_way(len(df), val_size=val_size, test_size=test_size, seed=seed)
        report = SplitReport(
            engine=engine, op=op, splitter=splitter,
            n_bins=n_bins, val_size=val_size, test_size=test_size, seed=seed,
            edges_file=str(edges_file),
            test_queries_txt=str(test_queries_txt),
            excluded_test_queries=list(excluded_test_queries),
            excluded_rows_count=int(excluded_rows_count),
            failed_queries_txt=None,
            required_train_queries_missing_after_exclusion=[],
            violations=[],
            bucket_summary=[],
        )
        write_split_artifacts(resources_dir, engine, op, tr=tr, va=va, te=te, report=report)
        return tr, va, te, report

    if splitter == "bucketed":
        if not failed_queries_txt:
            raise ValueError("splitter=bucketed requires failed_queries_txt.")

        txt_path = Path(failed_queries_txt).expanduser()
        if not txt_path.is_absolute():
            txt_path = resources_dir / failed_queries_txt

        failed = parse_failed_queries_from_report(txt_path)

        tr, va, te, summary, violations, missing_required = split_bucketed_three_way(
            df, y_seconds, edges,
            query_col=query_col,
            required_train_queries=failed,
            val_size=val_size,
            test_size=test_size,
            seed=seed,
        )

        report = SplitReport(
            engine=engine, op=op, splitter=splitter,
            n_bins=n_bins, val_size=val_size, test_size=test_size, seed=seed,
            edges_file=str(edges_file),
            test_queries_txt=str(test_queries_txt),
            excluded_test_queries=list(excluded_test_queries),
            excluded_rows_count=int(excluded_rows_count),
            failed_queries_txt=str(txt_path),
            required_train_queries_missing_after_exclusion=list(missing_required),
            violations=violations,
            bucket_summary=summary,
        )
        write_split_artifacts(resources_dir, engine, op, tr=tr, va=va, te=te, report=report)
        return tr, va, te, report

    raise ValueError(f"Unknown splitter '{splitter}'. Expected 'random' or 'bucketed'.")
