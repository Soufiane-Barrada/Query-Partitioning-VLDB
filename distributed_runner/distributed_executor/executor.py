from __future__ import annotations

from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
import csv
import hashlib
import sys
import uuid
from datetime import datetime, timezone
import statistics


import pyarrow as pa

from .models import QueryPlan
from .engines import (
    run_whole_query_duckdb,
    run_whole_query_datafusion,
)
from .distributed_runner import DistributedRunner


# Whole-query cache (CSV)
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
    # Avoid importing the package just for version
    try:
        import importlib.metadata as md
        return md.version(pkg)
    except Exception:
        return "unknown"


def _ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _read_whole_query_cache_rows(cache_path: Path) -> List[Dict[str, Any]]:
    if not cache_path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with cache_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if not r:
                continue
            rows.append(r)
    return rows


def _append_whole_query_cache_row(cache_path: Path, row: Dict[str, Any]) -> None:
    _ensure_parent_dir(cache_path)
    file_exists = cache_path.exists() and cache_path.stat().st_size > 0

    with cache_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_WQ_CACHE_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _pick_cached_run_group(
    all_rows: List[Dict[str, Any]],
    *,
    query_id: str,
    engine: str,
    engine_mem_mb: Optional[int],
    n_runs: int,
) -> Tuple[Optional[str], Dict[int, Dict[str, Any]], bool]:
    """
    cache lookup:
      - Only match on (query_id, engine, engine_mem_mb)
    Prefer:
      1) the most recent complete run_id group (>= n_runs distinct run_idx)
      2) else the most recent group (may be partial)
    """
    key_engine_mem = "" if engine_mem_mb is None else str(int(engine_mem_mb))

    filtered: List[Dict[str, Any]] = []
    for r in all_rows:
        try:
            if r.get("query_id") != query_id:
                continue
            if r.get("engine") != engine:
                continue
            if (r.get("engine_mem_mb") or "") != key_engine_mem:
                continue
            rid = r.get("run_id")
            if not rid:
                continue
            int(r.get("run_idx", "-1"))
        except Exception:
            continue
        filtered.append(r)

    if not filtered:
        return None, {}, False

    # group rows by run_id
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in filtered:
        rid = r.get("run_id")
        if not rid:
            continue
        groups.setdefault(rid, []).append(r)

    if not groups:
        return None, {}, False

    def group_meta(rid: str) -> Tuple[bool, str]:
        rows = groups[rid]
        idxs = set()
        latest = ""
        for rr in rows:
            latest = max(latest, rr.get("created_at", ""))
            try:
                idxs.add(int(rr.get("run_idx", "-1")))
            except Exception:
                pass
        return (len(idxs) >= n_runs, latest)

    candidates = sorted(
        groups.keys(),
        key=lambda rid: (group_meta(rid)[0], group_meta(rid)[1]),
        reverse=True,
    )

    chosen = candidates[0]
    rows = groups[chosen]

    by_idx: Dict[int, Dict[str, Any]] = {}
    for rr in sorted(rows, key=lambda x: x.get("created_at", "")):
        try:
            idx = int(rr.get("run_idx", "-1"))
        except Exception:
            continue
        if idx < 0:
            continue
        if idx not in by_idx:
            by_idx[idx] = rr

    is_complete = (len(by_idx) >= n_runs)
    return chosen, by_idx, is_complete


def _median(values: List[float]) -> float:
    v = sorted(values)
    return v[len(v) // 2]


def _cache_key_env_versions() -> Tuple[str, str, str, str]:
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    duckdb_version = _get_pkg_version("duckdb")
    datafusion_version = _get_pkg_version("datafusion")
    pyarrow_version = _get_pkg_version("pyarrow")
    return python_version, duckdb_version, datafusion_version, pyarrow_version


# Cut-boundary-only transfer metrics (post-timing)
def _compute_cut_boundary_transfer_metrics(
    plan: QueryPlan,
    results: Dict[str, pa.Table],
) -> Dict[str, Any]:
    """
    Cut-boundary-only transfer metrics.
    We compute from the producer's output Arrow table:
      - rows  = Q1_output.num_rows
      - bytes = Q1_output.nbytes (in-memory Arrow)
      - avg_row_bytes = bytes / rows
    """
    dp = plan.dp_summary
    if dp is None or not dp.has_cut:
        return {"rows": 0, "bytes": 0, "avg_row_bytes": None}

    # Consumer is the node with inputs (Q2).
    consumers = [n for n in plan.dag.nodes if n.inputs]
    if len(consumers) == 1:
        consumer = consumers[0]
    else:
        # Robust fallback: prefer final node if it has inputs
        id_to_node = {n.id: n for n in plan.dag.nodes}
        final = id_to_node.get(plan.dag.final_node_id)
        consumer = final if (final is not None and final.inputs) else (consumers[0] if consumers else None)

    if consumer is None:
        return {"rows": 0, "bytes": 0, "avg_row_bytes": None}

    upstream_ids = list(dict.fromkeys(consumer.inputs.values()))  # unique
    src_id = upstream_ids[0] if upstream_ids else None
    if src_id is None:
        return {"rows": 0, "bytes": 0, "avg_row_bytes": None}

    tbl = results.get(src_id)
    if tbl is None:
        return {"rows": None, "bytes": None, "avg_row_bytes": None}

    rows = int(tbl.num_rows)
    nbytes = int(getattr(tbl, "nbytes", 0) or 0)
    avg_row = (nbytes / rows) if rows > 0 else None
    return {"rows": rows, "bytes": nbytes, "avg_row_bytes": avg_row}


# -----------------------------
# Main execution
# -----------------------------

_N_RUNS = 5


def _run_or_load_whole_queries(
    plan: QueryPlan,
    runner: DistributedRunner,
    *,
    whole_query_cache_path: str | Path | None,
    rerun_whole_query: bool,
    parquet_dir: str | Path | None,
    engine_mem_mb: Optional[int],
    allow_execute: bool,
) -> Dict[str, Any]:
    """
    Return whole-query stats for DuckDB + DataFusion.
    If allow_execute=False, require a complete cache group for each engine.
    """
    cache_path = Path(whole_query_cache_path) if whole_query_cache_path is not None else None
    parquet_dir_key = str(Path(parquet_dir).resolve()) if parquet_dir is not None else ""
    sql_sha1 = _sha1_text(plan.original_sql)
    python_version, duckdb_version, datafusion_version, pyarrow_version = _cache_key_env_versions()

    cache_rows: List[Dict[str, Any]] = []
    if cache_path is not None and cache_path.exists() and not rerun_whole_query:
        cache_rows = _read_whole_query_cache_rows(cache_path)

    def run_or_load_whole_query(engine: str) -> Tuple[float, int, str]:
        """
        Returns (median_time, result_rows, run_id_used).
        """
        nonlocal cache_rows

        existing_run_id: Optional[str] = None
        existing_by_idx: Dict[int, Dict[str, Any]] = {}
        existing_complete = False

        if cache_path is not None and not rerun_whole_query and cache_rows:
            existing_run_id, existing_by_idx, existing_complete = _pick_cached_run_group(
                cache_rows,
                query_id=plan.query_id,
                engine=engine,
                engine_mem_mb=engine_mem_mb,
                n_runs=_N_RUNS,
            )

            if existing_complete:
                times = []
                rows_val = None
                for i in range(_N_RUNS):
                    rr = existing_by_idx[i]
                    times.append(float(rr["elapsed_s"]))
                    if rows_val is None:
                        rows_val = int(rr["result_rows"])
                assert rows_val is not None
                return _median(times), rows_val, str(existing_run_id)

        if not allow_execute:
            missing_hint = "cache file missing"
            if cache_path is not None and cache_path.exists():
                missing_hint = "cache missing required runs"
            raise RuntimeError(
                f"Whole-query cache incomplete for {engine} ({missing_hint}). "
                f"Run the whole-query script first for query_id={plan.query_id}."
            )

        if rerun_whole_query or existing_run_id is None:
            run_id = uuid.uuid4().hex
            existing_by_idx = {}
        else:
            run_id = existing_run_id

        missing = [i for i in range(_N_RUNS) if i not in existing_by_idx]

        times_by_idx: Dict[int, float] = {}
        rows_by_idx: Dict[int, int] = {}

        for idx, rr in existing_by_idx.items():
            try:
                times_by_idx[int(idx)] = float(rr["elapsed_s"])
                rows_by_idx[int(idx)] = int(rr["result_rows"])
            except Exception:
                pass

        for run_idx in missing:
            if engine == "duckdb":
                tbl, elapsed = run_whole_query_duckdb(runner.con, plan.original_sql)
                rcount = tbl.num_rows
                tbl = None
            elif engine == "datafusion":
                tbl, elapsed = run_whole_query_datafusion(runner.ctx, plan.original_sql)
                rcount = tbl.num_rows
                tbl = None
            else:
                raise ValueError(f"Unknown engine: {engine}")

            times_by_idx[run_idx] = float(elapsed)
            rows_by_idx[run_idx] = int(rcount)

            if cache_path is not None:
                row = {
                    "created_at": _now_iso(),
                    "run_id": run_id,
                    "query_id": plan.query_id,
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
                _append_whole_query_cache_row(cache_path, row)

        times = [times_by_idx[i] for i in range(_N_RUNS)]
        rows_val = rows_by_idx.get(0, next(iter(rows_by_idx.values())))
        return _median(times), rows_val, run_id

    duck_time, duck_rows, duck_run_id = run_or_load_whole_query("duckdb")
    df_time, df_rows, df_run_id = run_or_load_whole_query("datafusion")

    return {
        "duckdb_time": duck_time,
        "datafusion_time": df_time,
        "full_duckdb_rows": duck_rows,
        "full_datafusion_rows": df_rows,
        "whole_query_cache": {
            "duckdb_run_id": duck_run_id,
            "datafusion_run_id": df_run_id,
            "cache_path": str(cache_path) if cache_path is not None else None,
            "rerun_whole_query": rerun_whole_query,
        },
    }


def _run_distributed_benchmark(
    plan: QueryPlan,
    runner: DistributedRunner,
) -> Dict[str, Any]:
    """
    Run the distributed DAG N times and return summary statistics.
    """
    run_data: List[Tuple[float, Dict[str, float], int, Optional[float], Dict[str, Any]]] = []
    for _ in range(_N_RUNS):
        _ = runner.run(plan.dag.nodes)

        total_time = float(runner.timings["total_distributed"])
        timings_copy = dict(runner.timings)

        final_tbl = runner.results[plan.dag.final_node_id]
        final_rows = int(final_tbl.num_rows)
        final_bytes = final_tbl.nbytes
        root_avg_row_bytes = (final_bytes / final_rows) if final_rows > 0 else None

        transfer_metrics = _compute_cut_boundary_transfer_metrics(plan, runner.results)
        runner.results.clear()

        run_data.append((total_time, timings_copy, final_rows, root_avg_row_bytes, transfer_metrics))

    run_data.sort(key=lambda x: x[0])
    dist_times = [t for (t, _, _, _, _) in run_data]
    dist_fastest_time = dist_times[0]
    dist_slowest_time = dist_times[-1]
    dist_std_time = statistics.stdev(dist_times) if len(dist_times) > 1 else 0.0

    dist_total_time, distributed_timings, dist_final_rows, dist_root_avg_row_bytes, dist_transfer = run_data[
        _N_RUNS // 2
    ]

    return {
        "distributed_time": dist_total_time,
        "distributed_time_fastest": dist_fastest_time,
        "distributed_time_slowest": dist_slowest_time,
        "distributed_time_std": dist_std_time,
        "distributed_timings": distributed_timings,
        "distributed_transfer": dist_transfer,
        "distributed_root_avg_row_bytes": dist_root_avg_row_bytes,
        "distributed_final_rows": dist_final_rows,
    }


def execute_plan(
    plan: QueryPlan,
    runner: DistributedRunner,
    *,
    whole_query_cache_path: str | Path | None = None,
    rerun_whole_query: bool = False,
    parquet_dir: str | Path | None = None,
    engine_mem_mb: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Execute one QueryPlan with a shared DistributedRunner:
      - Whole-query DuckDB and DataFusion: N_RUNS times each (median)
        * Can be skipped by reading cached runs from whole_query_cache.csv
        * Each actual whole-query run is appended to the cache OUTSIDE timed sections.
      - Distributed DAG: N_RUNS times (median by total time)
    """
    whole = _run_or_load_whole_queries(
        plan,
        runner,
        whole_query_cache_path=whole_query_cache_path,
        rerun_whole_query=rerun_whole_query,
        parquet_dir=parquet_dir,
        engine_mem_mb=engine_mem_mb,
        allow_execute=True,
    )
    dist = _run_distributed_benchmark(plan, runner)

    return {
        "plan": plan,
        **whole,
        **dist,
    }


def execute_whole_queries(
    plan: QueryPlan,
    runner: DistributedRunner,
    *,
    whole_query_cache_path: str | Path,
    rerun_whole_query: bool = False,
    parquet_dir: str | Path | None = None,
    engine_mem_mb: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Execute whole queries on both engines and append to the cache.
    """
    return _run_or_load_whole_queries(
        plan,
        runner,
        whole_query_cache_path=whole_query_cache_path,
        rerun_whole_query=rerun_whole_query,
        parquet_dir=parquet_dir,
        engine_mem_mb=engine_mem_mb,
        allow_execute=True,
    )


def execute_distributed_only(
    plan: QueryPlan,
    runner: DistributedRunner,
    *,
    whole_query_cache_path: str | Path,
    parquet_dir: str | Path | None = None,
    engine_mem_mb: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Execute distributed DAG only, loading whole-query stats from cache.
    """
    whole = _run_or_load_whole_queries(
        plan,
        runner,
        whole_query_cache_path=whole_query_cache_path,
        rerun_whole_query=False,
        parquet_dir=parquet_dir,
        engine_mem_mb=engine_mem_mb,
        allow_execute=False,
    )
    dist = _run_distributed_benchmark(plan, runner)

    return {
        "plan": plan,
        **whole,
        **dist,
    }


# -----------------------------
# CSV row builder (extended)
# -----------------------------

_DEFAULT_OP_KINDS = ["OTHER", "FILTER", "JOIN", "SORT", "AGGREGATE"]


def _add_op_kind_counts(row: Dict[str, Any], prefix: str, counts: Optional[Dict[str, int]]) -> None:
    for k in _DEFAULT_OP_KINDS:
        row[f"{prefix}_opcnt_{k}"] = int((counts or {}).get(k, 0)) if counts is not None else None
    row[f"{prefix}_opcnt_total"] = int(sum((counts or {}).values())) if counts is not None else None


def build_stats_row(exec_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Turn an execution result into a flat dict of statistics suitable for CSV/DF.
    Includes:
      - existing runtime/speedup fields
      - selected analysis.* fields from JSON plan 
      - cut-boundary transfer actual rows/bytes/avg_row_bytes
      - actual avg row size at the root (distributed final result)
    """
    plan: QueryPlan = exec_result["plan"]
    dp = plan.dp_summary

    duck_time = exec_result["duckdb_time"]
    df_time = exec_result["datafusion_time"]
    dist_time = exec_result["distributed_time"]

    def safe_speedup(base: float, dist: float) -> float | None:
        if dist and dist > 0.0:
            return base / dist
        return None

    row: Dict[str, Any] = {
        "query_id": plan.query_id,
        "has_cut": dp.has_cut if dp is not None else False,
        "cut_node_id": dp.cut_node_id if dp is not None else None,
        "q1_engine": dp.q1_engine if dp is not None else None,
        "q2_engine": dp.q2_engine if dp is not None else None,
        "cost_all_duckdb": dp.cost_all_duckdb if dp is not None else None,
        "cost_all_datafusion": dp.cost_all_datafusion if dp is not None else None,
        "chosen_cost": dp.chosen_cost if dp is not None else None,
        "duckdb_time": duck_time,
        "datafusion_time": df_time,
        "distributed_time": dist_time,
        "distributed_time_fastest": exec_result["distributed_time_fastest"],
        "distributed_time_slowest": exec_result["distributed_time_slowest"],
        "distributed_time_std": exec_result["distributed_time_std"],
        "speedup_vs_duckdb": safe_speedup(duck_time, dist_time),
        "speedup_vs_datafusion": safe_speedup(df_time, dist_time),
        "final_rows_duckdb": exec_result["full_duckdb_rows"],
        "final_rows_datafusion": exec_result["full_datafusion_rows"],
        "final_rows_distributed": exec_result["distributed_final_rows"],
        "root_actual_avg_row_bytes": exec_result.get("distributed_root_avg_row_bytes"),


    }

    # Per-subquery timings (exclude the total)
    for sid, t in exec_result["distributed_timings"].items():
        if sid == "total_distributed":
            continue
        row[f"time_{sid}"] = t

    # ---- Cut-boundary transfer (actual) ----
    tr = exec_result.get("distributed_transfer") or {}
    row["transfer_actual_rows"] = tr.get("rows")
    row["transfer_actual_bytes"] = tr.get("bytes")
    row["transfer_actual_avg_row_bytes"] = tr.get("avg_row_bytes")

    # ---- analysis.* from JSON plan ----
    a = plan.analysis

    # pipeline
    if a is not None and a.pipeline is not None:
        p = a.pipeline
        row["pipeline_run_id"] = p.run_id
        row["pipeline_do_optimize"] = p.do_optimize
        row["pipeline_dp_mode"] = p.dp_mode
        row["pipeline_transfer_extra_constant_ms"] = p.transfer_extra_constant_ms
    else:
        row["pipeline_run_id"] = None
        row["pipeline_do_optimize"] = None
        row["pipeline_dp_mode"] = None
        row["pipeline_transfer_extra_constant_ms"] = None

    # planStats
    if a is not None and a.plan_stats is not None:
        ps = a.plan_stats
        row["plan_total_nodes"] = ps.total_nodes
        row["plan_max_depth"] = ps.max_depth
        row["plan_root_output_rows"] = ps.root_output_rows
        row["plan_root_row_size_out_bytes"] = ps.root_row_size_out_bytes
        _add_op_kind_counts(row, "plan", ps.operator_kind_counts)
    else:
        row["plan_total_nodes"] = None
        row["plan_max_depth"] = None
        row["plan_root_output_rows"] = None
        row["plan_root_row_size_out_bytes"] = None
        _add_op_kind_counts(row, "plan", None)

    # cut (nullable)
    if a is not None and a.cut is not None:
        c = a.cut
        row["cut_depth"] = c.cut_depth
        row["cut_node_operator_kind"] = c.cut_node_operator_kind
        row["cut_output_rows"] = c.cut_output_rows
        row["cut_row_size_out_bytes"] = c.cut_row_size_out_bytes
        row["cut_output_bytes"] = c.cut_output_bytes
        row["cut_transfer_estimated_ms"] = c.transfer_estimated_ms

        if c.q1_stats is not None:
            q1 = c.q1_stats
            row["q1_node_count"] = q1.node_count
            row["q1_max_depth"] = q1.max_depth
            _add_op_kind_counts(row, "q1", q1.operator_kind_counts)
        else:
            row["q1_node_count"] = None
            row["q1_max_depth"] = None
            _add_op_kind_counts(row, "q1", None)

        if c.q2_stats is not None:
            q2 = c.q2_stats
            row["q2_node_count"] = q2.node_count
            row["q2_max_depth"] = q2.max_depth
            _add_op_kind_counts(row, "q2", q2.operator_kind_counts)
        else:
            row["q2_node_count"] = None
            row["q2_max_depth"] = None
            _add_op_kind_counts(row, "q2", None)

    else:
        row["cut_depth"] = None
        row["cut_node_operator_kind"] = None
        row["cut_output_rows"] = None
        row["cut_row_size_out_bytes"] = None
        row["cut_output_bytes"] = None
        row["cut_transfer_estimated_ms"] = None

        row["q1_node_count"] = None
        row["q1_max_depth"] = None
        _add_op_kind_counts(row, "q1", None)

        row["q2_node_count"] = None
        row["q2_max_depth"] = None
        _add_op_kind_counts(row, "q2", None)

    return row
