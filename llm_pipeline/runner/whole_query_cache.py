from __future__ import annotations

import csv
import hashlib
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa

from .engines import run_whole_query_duckdb, run_whole_query_datafusion


_WQ_CACHE_FIELDS = [
    "created_at",
    "run_id",
    "query_id",
    "engine",
    "run_idx",
    "elapsed_s",
    "result_rows",
    "sql_sha1",
    "parquet_dir",
    "engine_mem_mb",
    "python_version",
    "duckdb_version",
    "datafusion_version",
    "pyarrow_version",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _get_pkg_version(pkg: str) -> str:
    try:
        import importlib.metadata as md
        return md.version(pkg)
    except Exception:
        return "unknown"


def _cache_env_versions() -> Tuple[str, str, str, str]:
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    duckdb_version = _get_pkg_version("duckdb")
    datafusion_version = _get_pkg_version("datafusion")
    pyarrow_version = _get_pkg_version("pyarrow")
    return python_version, duckdb_version, datafusion_version, pyarrow_version


def _read_rows(cache_path: Path) -> List[Dict[str, Any]]:
    if not cache_path.exists():
        return []
    out: List[Dict[str, Any]] = []
    with cache_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if row:
                out.append(row)
    return out


def _append_row(cache_path: Path, row: Dict[str, Any]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = cache_path.exists() and cache_path.stat().st_size > 0
    with cache_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_WQ_CACHE_FIELDS)
        if not file_exists:
            w.writeheader()
        w.writerow(row)


def _median(vals: List[float]) -> float:
    v = sorted(vals)
    return v[len(v) // 2]


def _pick_cached_run_group_loose(
    rows: List[Dict[str, Any]],
    *,
    query_id: str,
    engine: str,
    engine_mem_mb: Optional[int],
    n_runs: int,
) -> Tuple[Optional[str], Dict[int, Dict[str, Any]], bool]:
    """
    LOOSE matching: only (query_id, engine, engine_mem_mb).
    Prefer most recent COMPLETE run_id group.
    """
    key_engine_mem = "" if engine_mem_mb is None else str(int(engine_mem_mb))

    filtered: List[Dict[str, Any]] = []
    for r in rows:
        if r.get("query_id") != query_id:
            continue
        if r.get("engine") != engine:
            continue
        if (r.get("engine_mem_mb") or "") != key_engine_mem:
            continue
        # must have run_id + run_idx
        if not r.get("run_id"):
            continue
        try:
            int(r.get("run_idx", "-1"))
        except Exception:
            continue
        filtered.append(r)

    if not filtered:
        return None, {}, False

    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in filtered:
        groups.setdefault(r["run_id"], []).append(r)

    def group_score(run_id: str) -> Tuple[bool, str]:
        g = groups[run_id]
        idxs = set()
        latest = ""
        for rr in g:
            latest = max(latest, rr.get("created_at", ""))
            try:
                idxs.add(int(rr.get("run_idx", "-1")))
            except Exception:
                pass
        return (len(idxs) >= n_runs, latest)

    # complete first, then newest
    candidates = sorted(groups.keys(), key=lambda rid: group_score(rid), reverse=True)
    chosen = candidates[0]
    chosen_rows = groups[chosen]

    by_idx: Dict[int, Dict[str, Any]] = {}
    for rr in sorted(chosen_rows, key=lambda x: x.get("created_at", "")):
        try:
            idx = int(rr.get("run_idx", "-1"))
        except Exception:
            continue
        if idx < 0:
            continue
        if idx not in by_idx:
            by_idx[idx] = rr

    return chosen, by_idx, (len(by_idx) >= n_runs)


@dataclass
class WholeQueryMedian:
    median_s: float
    result_rows: int
    run_id_used: Optional[str]
    sample_table: Optional[pa.Table]  # Only returned if we actually executed run_idx=0 in this call


def get_or_run_whole_query_median(
    *,
    engine: str,  # "duckdb" | "datafusion"
    query_id: str,
    sql: str,
    con: Any = None,
    ctx: Any = None,
    cache_path: Path,
    parquet_dir: Path,
    engine_mem_mb: Optional[int],
    n_runs: int,
    rerun: bool,
) -> WholeQueryMedian:
    """
    Ensures we have N runs in cache (unless rerun=False and cache already complete),
    returns median time. Cache lookup is LOOSE: (query_id, engine, engine_mem_mb).
    """
    parquet_dir_key = str(parquet_dir.resolve())
    sql_sha1 = _sha1_text(sql)
    python_version, duckdb_version, datafusion_version, pyarrow_version = _cache_env_versions()

    cache_rows: List[Dict[str, Any]] = []
    if cache_path.exists() and not rerun:
        cache_rows = _read_rows(cache_path)

    run_id: Optional[str] = None
    by_idx: Dict[int, Dict[str, Any]] = {}
    complete = False

    if cache_rows and not rerun:
        run_id, by_idx, complete = _pick_cached_run_group_loose(
            cache_rows, query_id=query_id, engine=engine, engine_mem_mb=engine_mem_mb, n_runs=n_runs
        )
        if complete:
            times = [float(by_idx[i]["elapsed_s"]) for i in range(n_runs)]
            rows_val = int(by_idx.get(0, next(iter(by_idx.values())))["result_rows"])
            return WholeQueryMedian(_median(times), rows_val, run_id, sample_table=None)

    # Need to run missing/all
    if rerun or run_id is None:
        run_id = uuid.uuid4().hex
        by_idx = {}

    missing = [i for i in range(n_runs) if i not in by_idx]
    times_by_idx: Dict[int, float] = {}
    rows_by_idx: Dict[int, int] = {}
    sample_tbl: Optional[pa.Table] = None

    # Fill any partial cached runs we selected
    for idx, rr in by_idx.items():
        try:
            times_by_idx[int(idx)] = float(rr["elapsed_s"])
            rows_by_idx[int(idx)] = int(rr["result_rows"])
        except Exception:
            pass

    for run_idx in missing:
        if engine == "duckdb":
            tbl, elapsed = run_whole_query_duckdb(con, sql)
        elif engine == "datafusion":
            tbl, elapsed = run_whole_query_datafusion(ctx, sql)
        else:
            raise ValueError(f"Unknown engine: {engine}")

        rcount = int(tbl.num_rows)

        if run_idx == 0:
            sample_tbl = tbl
        else:
            tbl = None  # release

        times_by_idx[run_idx] = float(elapsed)
        rows_by_idx[run_idx] = rcount

        row = {
            "created_at": _now_iso(),
            "run_id": run_id,
            "query_id": query_id,
            "engine": engine,
            "run_idx": str(run_idx),
            "elapsed_s": f"{elapsed:.12f}",
            "result_rows": str(rcount),
            "sql_sha1": sql_sha1,
            "parquet_dir": parquet_dir_key,
            "engine_mem_mb": "" if engine_mem_mb is None else str(int(engine_mem_mb)),
            "python_version": python_version,
            "duckdb_version": duckdb_version,
            "datafusion_version": datafusion_version,
            "pyarrow_version": pyarrow_version,
        }
        _append_row(cache_path, row)

    times = [times_by_idx[i] for i in range(n_runs)]
    rows_val = rows_by_idx.get(0, next(iter(rows_by_idx.values())))
    return WholeQueryMedian(_median(times), rows_val, run_id, sample_table=sample_tbl)


def has_complete_whole_query_cache(
    *,
    cache_path: Path,
    query_id: str,
    engine: str,
    engine_mem_mb: Optional[int],
    n_runs: int,
) -> bool:
    """
    Check if the cache has a complete run group for (query_id, engine, engine_mem_mb).
    Uses the same loose matching as get_or_run_whole_query_median.
    """
    if not cache_path.exists():
        return False

    rows = _read_rows(cache_path)
    _, _, complete = _pick_cached_run_group_loose(
        rows,
        query_id=query_id,
        engine=engine,
        engine_mem_mb=engine_mem_mb,
        n_runs=n_runs,
    )
    return complete
