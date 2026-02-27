"""
Module entrypoint
"""

from __future__ import annotations

import argparse
import csv
import errno
import json
import multiprocessing as mp
import os
import queue as queue_mod
import signal
import sys
import traceback
from contextlib import contextmanager
from pathlib import Path
from statistics import median, stdev
from typing import Any, Dict, List, Optional
import resource


class TimeoutException(Exception):
    pass


@contextmanager
def time_limit(seconds: int):
    def signal_handler(signum, frame):
        raise TimeoutException(f"Operation timed out after {seconds} seconds")

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(int(seconds))
    try:
        yield
    finally:
        signal.alarm(0)


def load_queries_from_dir(directory: Path, *, query_files: Optional[List[str]] = None) -> List[Path]:
    if query_files:
        out: List[Path] = []
        for fname in query_files:
            p = directory / fname
            if not p.exists():
                raise FileNotFoundError(f"Query file not found: {p}")
            if p.suffix.lower() != ".sql":
                raise ValueError(f"Query file must end with .sql: {p}")
            out.append(p)
        return out

    return sorted(directory.glob("*.sql"))


def append_jsonl(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")


def append_csv_row(path: Path, *, fieldnames: List[str], row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerow({k: row.get(k) for k in fieldnames})


def write_error(
    *,
    error_log_path: Path,
    query_id: str,
    query_path: Path,
    status: str,
    tb: Optional[str] = None,
    exitcode: Optional[int] = None,
    sig: Optional[int] = None,
) -> None:
    error_log_path.parent.mkdir(parents=True, exist_ok=True)
    with error_log_path.open("a", encoding="utf-8") as f:
        f.write("\n--- Failure ---\n")
        f.write(f"Query ID: {query_id}\n")
        f.write(f"Query Path: {query_path}\n")
        f.write(f"Status: {status}\n")
        if exitcode is not None:
            f.write(f"Exit code: {exitcode}\n")
        if sig is not None:
            try:
                f.write(f"Signal: {sig} ({signal.Signals(sig).name})\n")
            except Exception:
                f.write(f"Signal: {sig}\n")
        if tb:
            f.write("Traceback:\n")
            f.write(tb)
            if not tb.endswith("\n"):
                f.write("\n")


_DIST_STATS_FIELDS = [
    "query_id",
    "has_cut",
    "q1_engine",
    "q2_engine",
    "duckdb_time",
    "datafusion_time",
    "distributed_time",
    "distributed_time_std",
    "final_rows_duckdb",
    "final_rows_datafusion",
    "final_rows_distributed",
    "transfer_actual_rows",
    "transfer_actual_bytes",
    "transfer_actual_avg_row_bytes",
    "root_actual_avg_row_bytes",
    "time_q1",
    "time_q2",
]


def _set_child_mem_limit_mb(mem_mb: Optional[int]) -> None:
    if mem_mb is None:
        return

    requested = int(mem_mb) * 1024 * 1024
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    if hard != resource.RLIM_INFINITY:
        requested = min(requested, hard)
    resource.setrlimit(resource.RLIMIT_AS, (requested, requested))


def _terminate_then_kill(p: mp.Process, *, terminate_grace_sec: float, kill_grace_sec: float) -> None:
    if not p.is_alive():
        return
    try:
        p.terminate()
    except Exception:
        pass
    p.join(timeout=terminate_grace_sec)

    if p.is_alive():
        try:
            p.kill()
        except Exception:
            pass
        p.join(timeout=kill_grace_sec)


def _child_run_one_query(
    query_path_str: str,
    parquet_dir_str: str,
    endpoint: str,
    model: str,
    schema_name: str,
    split_timeout: int,
    n_runs: int,
    engine_mem_mb: Optional[int],
    whole_query_cache_path_str: str,
    rerun_whole_query: bool,
    run_same_engine: bool,
    run_missing_whole_query: bool,
    out_q: Any,
    child_mem_mb: Optional[int],
) -> None:
    _set_child_mem_limit_mb(child_mem_mb)

    import pandas as pd
    import pyarrow as pa  # noqa: F401

    sys.path.append(os.path.abspath("."))

    from runner.engines import setup_engines, run_whole_query_duckdb
    from runner.distributed_runner import DistributedRunner
    from runner.benchmark import make_subqueries, benchmark_distributed_only
    from runner.whole_query_cache import (
        get_or_run_whole_query_median,
        has_complete_whole_query_cache,
    )
    from query_splitter.splitter import QuerySplitter
    from query_splitter.prompts import get_schema_prompts

    def _is_oom_exception(e: BaseException) -> bool:
        if isinstance(e, MemoryError):
            return True
        if isinstance(e, OSError) and getattr(e, "errno", None) == errno.ENOMEM:
            return True
        try:
            import duckdb  # type: ignore

            if isinstance(e, duckdb.OutOfMemoryException):
                return True
        except Exception:
            pass
        return False

    con = None
    ctx = None
    runner = None
    try:
        query_path = Path(query_path_str)
        parquet_dir = Path(parquet_dir_str)
        whole_query_cache_path = Path(whole_query_cache_path_str)

        query_id = query_path.name[: query_path.name.find(".")]
        sql = query_path.read_text(encoding="utf-8")

        schema_prompt, tables_info = get_schema_prompts(schema_name)
        splitter = QuerySplitter(
            endpoint_url=endpoint,
            model_name=model,
            system_schema_override=schema_prompt,
            system_tables_info_override=tables_info,
        )

        with time_limit(split_timeout):
            split = splitter.split_query(sql)

        has_cut = bool(split.get("has_cut", True))
        if not has_cut:
            out_q.put(
                {
                    "status": "no_cut",
                    "query_id": query_id,
                    "split": split,
                    "engine_mem_mb": engine_mem_mb,
                }
            )
            return

        q1_engine = str(split.get("q1_engine", "")).strip().lower()
        q2_engine = str(split.get("q2_engine", "")).strip().lower()
        if q1_engine not in ("duckdb", "datafusion") or q2_engine not in ("duckdb", "datafusion"):
            raise ValueError(
                "LLM output for cut query must include q1_engine and q2_engine "
                "as 'duckdb' or 'datafusion'. Got: "
                f"q1_engine={q1_engine!r}, q2_engine={q2_engine!r}"
            )

        con, ctx = setup_engines(parquet_dir, engine_mem_mb=engine_mem_mb)
        runner = DistributedRunner(con, ctx)

        if not rerun_whole_query and not run_missing_whole_query:
            duck_complete = has_complete_whole_query_cache(
                cache_path=whole_query_cache_path,
                query_id=query_id,
                engine="duckdb",
                engine_mem_mb=engine_mem_mb,
                n_runs=n_runs,
            )
            df_complete = has_complete_whole_query_cache(
                cache_path=whole_query_cache_path,
                query_id=query_id,
                engine="datafusion",
                engine_mem_mb=engine_mem_mb,
                n_runs=n_runs,
            )
            if not (duck_complete and df_complete):
                out_q.put(
                    {
                        "status": "missing_whole_cache",
                        "query_id": query_id,
                        "engine_mem_mb": engine_mem_mb,
                    }
                )
                return

        wq_duck = get_or_run_whole_query_median(
            engine="duckdb",
            query_id=query_id,
            sql=sql,
            con=con,
            cache_path=whole_query_cache_path,
            parquet_dir=parquet_dir,
            engine_mem_mb=engine_mem_mb,
            n_runs=n_runs,
            rerun=rerun_whole_query,
        )
        wq_df = get_or_run_whole_query_median(
            engine="datafusion",
            query_id=query_id,
            sql=sql,
            ctx=ctx,
            cache_path=whole_query_cache_path,
            parquet_dir=parquet_dir,
            engine_mem_mb=engine_mem_mb,
            n_runs=n_runs,
            rerun=rerun_whole_query,
        )

        baseline_tbl = wq_duck.sample_table
        if baseline_tbl is None:
            baseline_tbl, _ = run_whole_query_duckdb(con, sql)
        baseline_tbl = baseline_tbl.rename_columns([c.lower() for c in baseline_tbl.column_names])

        def _run_distributed_for_engines(engine1: str, engine2: str) -> Dict[str, Any]:
            sql1, sql2 = split["sql1"], split["sql2"]

            order_name_map = {
                ("duckdb", "datafusion"): "duckdb_first",
                ("datafusion", "duckdb"): "datafusion_first",
                ("duckdb", "duckdb"): "duckdb_only",
                ("datafusion", "datafusion"): "datafusion_only",
            }
            order_name = order_name_map[(engine1, engine2)]

            run_rows: List[Dict[str, Any]] = []
            for run_idx in range(1, n_runs + 1):
                subs = make_subqueries(sql1, sql2, engine1, engine2)
                do_check = run_idx == 1

                df_row = benchmark_distributed_only(
                    query_id=query_id,
                    subqueries=subs,
                    runner=runner,
                    baseline_tbl=baseline_tbl,
                    whole_duckdb_median_s=wq_duck.median_s,
                    whole_datafusion_median_s=wq_df.median_s,
                    whole_duckdb_rows=wq_duck.result_rows,
                    whole_datafusion_rows=wq_df.result_rows,
                    do_check=do_check,
                )

                row = df_row.iloc[0].to_dict()
                row["order"] = order_name
                row["run"] = run_idx
                run_rows.append(row)

            df_runs = pd.DataFrame(run_rows)

            all_match = bool(df_runs["results_match"].all())
            if not all_match:
                return {"status": "mismatch", "order": order_name}

            dist_totals = [float(x) for x in df_runs["distributed_total"].tolist()]
            dist_median = float(median(dist_totals))
            dist_std = float(stdev(dist_totals)) if len(dist_totals) > 1 else 0.0

            rr = df_runs.sort_values("distributed_total").reset_index(drop=True)
            rep = rr.iloc[len(rr) // 2]

            return {
                "status": "ok",
                "order": order_name,
                "q1_engine": engine1,
                "q2_engine": engine2,
                "distributed_median_s": dist_median,
                "distributed_std_s": dist_std,
                "distributed_rows": int(rep["distributed_rows"]),
                "time_s1_rep_s": float(rep.get("s1_time", float("nan"))),
                "time_s2_rep_s": float(rep.get("s2_time", float("nan"))),
                "transfer_actual_rows": rep.get("transfer_actual_rows"),
                "transfer_actual_bytes": rep.get("transfer_actual_bytes"),
                "transfer_actual_avg_row_bytes": rep.get("transfer_actual_avg_row_bytes"),
                "root_actual_avg_row_bytes": rep.get("root_actual_avg_row_bytes"),
            }

        primary_result = _run_distributed_for_engines(q1_engine, q2_engine)
        if primary_result.get("status") != "ok":
            out_q.put(
                {
                    "status": "mismatch",
                    "query_id": query_id,
                    "split": split,
                    "engine_mem_mb": engine_mem_mb,
                }
            )
            return

        same_engine_results: Dict[str, Any] = {}
        if run_same_engine:
            same_engine = q1_engine if q1_engine in ("duckdb", "datafusion") else None
            if same_engine is not None:
                same_engine_results[same_engine] = _run_distributed_for_engines(same_engine, same_engine)

        out_q.put(
            {
                "status": "ok",
                "query_id": query_id,
                "engine_mem_mb": engine_mem_mb,
                "n_runs": int(n_runs),
                "split": split,
                "order": primary_result.get("order"),
                "q1_engine": q1_engine,
                "q2_engine": q2_engine,
                "duckdb_whole_median_s": float(wq_duck.median_s),
                "datafusion_whole_median_s": float(wq_df.median_s),
                "duckdb_whole_rows": int(wq_duck.result_rows),
                "datafusion_whole_rows": int(wq_df.result_rows),
                "duckdb_whole_run_id": wq_duck.run_id_used,
                "datafusion_whole_run_id": wq_df.run_id_used,
                "distributed_median_s": primary_result.get("distributed_median_s"),
                "distributed_std_s": primary_result.get("distributed_std_s"),
                "distributed_rows": primary_result.get("distributed_rows"),
                "time_s1_rep_s": primary_result.get("time_s1_rep_s"),
                "time_s2_rep_s": primary_result.get("time_s2_rep_s"),
                "transfer_actual_rows": primary_result.get("transfer_actual_rows"),
                "transfer_actual_bytes": primary_result.get("transfer_actual_bytes"),
                "transfer_actual_avg_row_bytes": primary_result.get("transfer_actual_avg_row_bytes"),
                "root_actual_avg_row_bytes": primary_result.get("root_actual_avg_row_bytes"),
                "same_engine_results": same_engine_results,
            }
        )

    except TimeoutException:
        out_q.put(
            {
                "status": "split_timeout",
                "query_id": Path(query_path_str).name,
            }
        )
    except BaseException as e:
        status = "oom" if _is_oom_exception(e) else "error"
        out_q.put(
            {
                "status": status,
                "query_id": Path(query_path_str).name,
                "traceback": traceback.format_exc(),
            }
        )
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        runner = None
        ctx = None
        con = None


def run_query_in_subprocess(
    *,
    query_path: Path,
    parquet_dir: Path,
    endpoint: str,
    model: str,
    schema_name: str,
    split_timeout: int,
    n_runs: int,
    engine_mem_mb: Optional[int],
    whole_query_cache_path: Path,
    rerun_whole_query: bool,
    run_same_engine: bool,
    run_missing_whole_query: bool,
    child_mem_mb: Optional[int],
    timeout_sec: Optional[float],
    terminate_grace_sec: float,
    kill_grace_sec: float,
) -> Dict[str, Any]:
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue(maxsize=1)

    p = ctx.Process(
        target=_child_run_one_query,
        args=(
            str(query_path),
            str(parquet_dir),
            endpoint,
            model,
            schema_name,
            int(split_timeout),
            int(n_runs),
            engine_mem_mb,
            str(whole_query_cache_path),
            bool(rerun_whole_query),
            bool(run_same_engine),
            bool(run_missing_whole_query),
            q,
            child_mem_mb,
        ),
    )
    p.start()

    if timeout_sec is None:
        p.join()
    else:
        p.join(timeout=timeout_sec)
        if p.is_alive():
            _terminate_then_kill(p, terminate_grace_sec=terminate_grace_sec, kill_grace_sec=kill_grace_sec)
            exitcode = p.exitcode
            sig = (-exitcode) if (exitcode is not None and exitcode < 0) else None
            return {"status": "timeout", "exitcode": exitcode, "signal": sig}

    try:
        return q.get_nowait()
    except queue_mod.Empty:
        exitcode = p.exitcode
        sig = (-exitcode) if (exitcode is not None and exitcode < 0) else None
        return {"status": "killed", "exitcode": exitcode, "signal": sig}
    finally:
        try:
            q.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Proof-of-concept split runner: only execute LLM-chosen cut queries (single assigned order)."
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--queries-dir", required=True)
    parser.add_argument("--parquet-dir", required=True)

    parser.add_argument("--query-files", nargs="*", default=None)
    parser.add_argument("--max-queries", type=int, default=None, help="Max number of CUT+MATCH queries to execute.")

    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument(
        "--schema",
        required=True,
        choices=["TPCH10", "TPCH1", "JOB", "SO"],
        help="Schema to use for LLM prompt context.",
    )
    parser.add_argument("--split-timeout", type=int, default=300)

    parser.add_argument("--n-runs", type=int, default=5)
    parser.add_argument("--engine-mem-mb", type=int, default=None)

    parser.add_argument("--child-mem-mb", type=int, default=None, help="Hard process cap via RLIMIT_AS (POSIX).")
    parser.add_argument("--timeout-sec", type=float, default=None, help="Kill a query subprocess after N seconds.")
    parser.add_argument("--terminate-grace-sec", type=float, default=5.0)
    parser.add_argument("--kill-grace-sec", type=float, default=2.0)

    parser.add_argument("--distributed-stats-file", default="distributed_stats.csv")
    parser.add_argument("--same-engine-duckdb-stats-file", default="distributed_stats_duckdb_duckdb.csv")
    parser.add_argument("--same-engine-datafusion-stats-file", default="distributed_stats_datafusion_datafusion.csv")
    parser.add_argument("--splits-log-file", default="query_splits.jsonl")
    parser.add_argument("--whole-query-cache-file", default="whole_query_cache.csv")
    parser.add_argument("--rerun-whole-query", action="store_true")
    parser.add_argument(
        "--run-missing-whole-query",
        action="store_true",
        help="If set, run and append missing whole-query cache entries; otherwise require full cache.",
    )
    parser.add_argument("--error-log-file", default="errors.txt")
    parser.add_argument(
        "--run-same-engine-distributed",
        action="store_true",
        help="Also run cut queries with (duckdb, duckdb) and (datafusion, datafusion).",
    )

    args = parser.parse_args()

    mp.set_start_method("spawn", force=True)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    queries_dir = Path(args.queries_dir)
    parquet_dir = Path(args.parquet_dir)
    if not queries_dir.is_dir():
        raise SystemExit(f"queries-dir is not a directory: {queries_dir}")
    if not parquet_dir.is_dir():
        raise SystemExit(f"parquet-dir is not a directory: {parquet_dir}")

    dist_stats_csv = out_dir / args.distributed_stats_file
    same_engine_duckdb_csv = out_dir / args.same_engine_duckdb_stats_file
    same_engine_df_csv = out_dir / args.same_engine_datafusion_stats_file
    splits_log = out_dir / args.splits_log_file
    whole_query_cache = out_dir / args.whole_query_cache_file
    error_log = out_dir / args.error_log_file

    query_paths = load_queries_from_dir(queries_dir, query_files=args.query_files)

    executed_ok = 0
    no_cut_count = 0
    mismatch_count = 0
    failed_count = 0

    for qp in query_paths:
        if args.max_queries is not None and executed_ok >= args.max_queries:
            break

        query_id = qp.name
        print(f"\n=== Processing {query_id} ===")

        result = run_query_in_subprocess(
            query_path=qp,
            parquet_dir=parquet_dir,
            endpoint=args.endpoint,
            model=args.model,
            schema_name=args.schema,
            split_timeout=args.split_timeout,
            n_runs=args.n_runs,
            engine_mem_mb=args.engine_mem_mb,
            whole_query_cache_path=whole_query_cache,
            rerun_whole_query=args.rerun_whole_query,
            run_missing_whole_query=args.run_missing_whole_query,
            run_same_engine=args.run_same_engine_distributed,
            child_mem_mb=args.child_mem_mb,
            timeout_sec=args.timeout_sec,
            terminate_grace_sec=args.terminate_grace_sec,
            kill_grace_sec=args.kill_grace_sec,
        )

        status = result.get("status", "error")

        if status == "no_cut":
            no_cut_count += 1
            split = result.get("split", {})
            print(f"  LLM decided not to cut {query_id}; skipping execution.")
            append_jsonl(
                splits_log,
                {
                    "query_id": query_id,
                    "original_sql": qp.read_text(encoding="utf-8"),
                    "has_cut": False,
                    "engine_mem_mb": args.engine_mem_mb,
                    **({"llm_raw": split} if split else {}),
                },
            )
            continue

        if status == "mismatch":
            mismatch_count += 1
            split = result.get("split", {})
            print(f"  Mismatch detected for {query_id}, excluding from distributed_stats.")
            append_jsonl(
                splits_log,
                {
                    "query_id": query_id,
                    "original_sql": qp.read_text(encoding="utf-8"),
                    "has_cut": True,
                    "q1_engine": split.get("q1_engine"),
                    "q2_engine": split.get("q2_engine"),
                    "sql1": split.get("sql1"),
                    "sql2": split.get("sql2"),
                    "results_match": False,
                    "engine_mem_mb": args.engine_mem_mb,
                },
            )
            continue

        if status == "ok":
            split = result.get("split", {})
            append_jsonl(
                splits_log,
                {
                    "query_id": query_id,
                    "original_sql": qp.read_text(encoding="utf-8"),
                    "has_cut": True,
                    "q1_engine": result.get("q1_engine"),
                    "q2_engine": result.get("q2_engine"),
                    "sql1": split.get("sql1"),
                    "sql2": split.get("sql2"),
                    "results_match": True,
                    "engine_mem_mb": args.engine_mem_mb,
                },
            )

            row = {
                "query_id": result["query_id"],
                "has_cut": True,
                "q1_engine": result.get("q1_engine"),
                "q2_engine": result.get("q2_engine"),
                "duckdb_time": result.get("duckdb_whole_median_s"),
                "datafusion_time": result.get("datafusion_whole_median_s"),
                "distributed_time": result.get("distributed_median_s"),
                "distributed_time_std": result.get("distributed_std_s"),
                "final_rows_duckdb": result.get("duckdb_whole_rows"),
                "final_rows_datafusion": result.get("datafusion_whole_rows"),
                "final_rows_distributed": result.get("distributed_rows"),
                "transfer_actual_rows": result.get("transfer_actual_rows"),
                "transfer_actual_bytes": result.get("transfer_actual_bytes"),
                "transfer_actual_avg_row_bytes": result.get("transfer_actual_avg_row_bytes"),
                "root_actual_avg_row_bytes": result.get("root_actual_avg_row_bytes"),
                "time_q1": result.get("time_s1_rep_s"),
                "time_q2": result.get("time_s2_rep_s"),
            }
            append_csv_row(dist_stats_csv, fieldnames=_DIST_STATS_FIELDS, row=row)

            if args.run_same_engine_distributed:
                same_engine_results = result.get("same_engine_results", {})
                duckdb_same = same_engine_results.get("duckdb")
                datafusion_same = same_engine_results.get("datafusion")
                same_entry = duckdb_same if duckdb_same is not None else datafusion_same

                if same_entry:
                    if same_entry.get("status") == "ok":
                        same_row = {
                            **row,
                            "q1_engine": same_entry.get("q1_engine"),
                            "q2_engine": same_entry.get("q2_engine"),
                            "distributed_time": same_entry.get("distributed_median_s"),
                            "distributed_time_std": same_entry.get("distributed_std_s"),
                            "final_rows_distributed": same_entry.get("distributed_rows"),
                            "time_q1": same_entry.get("time_s1_rep_s"),
                            "time_q2": same_entry.get("time_s2_rep_s"),
                            "transfer_actual_rows": same_entry.get("transfer_actual_rows"),
                            "transfer_actual_bytes": same_entry.get("transfer_actual_bytes"),
                            "transfer_actual_avg_row_bytes": same_entry.get("transfer_actual_avg_row_bytes"),
                            "root_actual_avg_row_bytes": same_entry.get("root_actual_avg_row_bytes"),
                        }
                        if same_entry.get("q1_engine") == "duckdb":
                            append_csv_row(
                                same_engine_duckdb_csv,
                                fieldnames=_DIST_STATS_FIELDS,
                                row=same_row,
                            )
                        else:
                            append_csv_row(
                                same_engine_df_csv,
                                fieldnames=_DIST_STATS_FIELDS,
                                row=same_row,
                            )
                    else:
                        se = same_entry.get("q1_engine") or "unknown"
                        print(f"  Same-engine ({se}) mismatch for {query_id}")

            executed_ok += 1
            continue

        if status == "split_timeout":
            failed_count += 1
            print(f"  Split timeout for {query_id}")
            append_jsonl(
                splits_log,
                {
                    "query_id": query_id,
                    "original_sql": qp.read_text(encoding="utf-8"),
                    "status": "split_timeout",
                    "engine_mem_mb": args.engine_mem_mb,
                },
            )
            write_error(error_log_path=error_log, query_id=query_id, query_path=qp, status=status)
            continue

        if status in ("timeout", "killed"):
            failed_count += 1
            print(f"  {status} for {query_id}")
            append_jsonl(
                splits_log,
                {
                    "query_id": query_id,
                    "original_sql": qp.read_text(encoding="utf-8"),
                    "status": status,
                    "exitcode": result.get("exitcode"),
                    "signal": result.get("signal"),
                    "engine_mem_mb": args.engine_mem_mb,
                },
            )
            write_error(
                error_log_path=error_log,
                query_id=query_id,
                query_path=qp,
                status=status,
                exitcode=result.get("exitcode"),
                sig=result.get("signal"),
            )
            continue

        if status == "missing_whole_cache":
            failed_count += 1
            print(f"  Missing whole-query cache for {query_id}")
            append_jsonl(
                splits_log,
                {
                    "query_id": query_id,
                    "original_sql": qp.read_text(encoding="utf-8"),
                    "status": status,
                    "engine_mem_mb": args.engine_mem_mb,
                },
            )
            write_error(
                error_log_path=error_log,
                query_id=query_id,
                query_path=qp,
                status=status,
            )
            continue

        failed_count += 1
        print(f"  Failed to process {query_id}: {status}")
        append_jsonl(
            splits_log,
            {
                "query_id": query_id,
                "original_sql": qp.read_text(encoding="utf-8"),
                "status": status,
                "traceback": result.get("traceback"),
                "engine_mem_mb": args.engine_mem_mb,
            },
        )
        write_error(
            error_log_path=error_log,
            query_id=query_id,
            query_path=qp,
            status=status,
            tb=result.get("traceback"),
        )

    print("\nProcessing complete!")
    print(f"  Output dir: {out_dir}")
    print(f"  Executed cut+match queries: {executed_ok}")
    print(f"  No-cut queries (LLM): {no_cut_count}")
    print(f"  Mismatched cut queries: {mismatch_count}")
    print(f"  Failed queries (timeouts/errors/oom): {failed_count}")
    print(f"  distributed stats: {dist_stats_csv}")
    if args.run_same_engine_distributed:
        print(f"  same-engine (duckdb->duckdb): {same_engine_duckdb_csv}")
        print(f"  same-engine (datafusion->datafusion): {same_engine_df_csv}")
    print(f"  query splits log: {splits_log}")
    print(f"  whole-query cache: {whole_query_cache}")
    print(f"  errors: {error_log}")


if __name__ == "__main__":
    main()

