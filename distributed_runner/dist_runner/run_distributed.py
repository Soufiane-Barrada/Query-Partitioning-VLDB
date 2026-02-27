from __future__ import annotations

"""
Module entrypoint for the distributed runner.
This is a direct copy of CHOP/python/run_distributed.py so behaviour stays identical.
"""

import argparse
import errno
import multiprocessing as mp
import queue as queue_mod
import signal
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import duckdb
import resource


def find_json_plans(plans_dir: Path) -> List[Path]:
    return sorted(plans_dir.glob("*.json"))


def load_processed_paths(log_path: Path) -> Set[str]:
    processed: Set[str] = set()
    if log_path.exists():
        with log_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    processed.add(line)
    return processed


def append_processed_path(log_path: Path, abs_path: str) -> None:
    with log_path.open("a", encoding="utf-8") as f:
        f.write(abs_path + "\n")


def _plan_variant_key(abs_path: str, plan_id: str) -> str:
    return f"{abs_path}#{plan_id}"


def _is_processed(processed_paths: Set[str], abs_path: str, plan_id: str) -> bool:
    return _plan_variant_key(abs_path, plan_id) in processed_paths


def _select_plan_variant(plan: Any, plan_id: Optional[str]) -> Any:
    if not plan_id:
        return plan
    if plan_id == "main" and not getattr(plan, "cut_plans", None):
        return plan
    variants = list(getattr(plan, "cut_plans", []) or [])
    for v in variants:
        if getattr(v, "plan_id", None) == plan_id:
            use_analysis = (variants and v is variants[0]) or getattr(v, "analysis", None) is not None
            return type(plan)(
                query_id=plan.query_id,
                original_sql=plan.original_sql,
                dag=v.dag,
                dp_summary=v.dp_summary,
                analysis=v.analysis if getattr(v, "analysis", None) is not None else (plan.analysis if use_analysis else None),
                cut_plans=variants,
            )
    if plan_id == "main":
        return plan
    raise ValueError(f"Plan variant not found: {plan_id}")


def _pick_variants_to_run(plan: Any, run_all_cuts: bool, run_cuts_for_nocut_only: bool) -> List[Any]:
    variants = list(getattr(plan, "cut_plans", []) or [])
    if not variants:
        return []

    main = variants[0]
    main_dp = getattr(main, "dp_summary", None)
    main_has_cut = bool(main_dp and getattr(main_dp, "has_cut", False))

    def is_cut(v: Any) -> bool:
        plan_id = getattr(v, "plan_id", None)
        if plan_id and plan_id != "main":
            return True
        dp = getattr(v, "dp_summary", None)
        return dp is not None and bool(getattr(dp, "has_cut", False))

    if run_all_cuts:
        return [v for v in variants if is_cut(v)]

    if run_cuts_for_nocut_only:
        if not main_has_cut:
            return [v for v in variants if is_cut(v)]
        return [main]

    if main_has_cut:
        return [main]
    return []


def _set_child_mem_limit_mb(mem_mb: Optional[int]) -> None:
    if mem_mb is None:
        return

    requested = int(mem_mb) * 1024 * 1024
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)

    if hard != resource.RLIM_INFINITY:
        requested = min(requested, hard)

    resource.setrlimit(resource.RLIMIT_AS, (requested, requested))


def _is_oom_exception(e: BaseException) -> bool:
    if isinstance(e, MemoryError):
        return True
    if isinstance(e, OSError) and getattr(e, "errno", None) == errno.ENOMEM:
        return True

    if isinstance(e, duckdb.OutOfMemoryException):
        return True

    return False


def _child_run_whole_queries(
    json_path_str: str,
    parquet_dir_str: str,
    child_mem_mb: Optional[int],
    engine_mem_mb: Optional[int],
    whole_query_cache_csv_str: str,
    rerun_whole_query: bool,
    out_q: Any,
) -> None:
    _set_child_mem_limit_mb(child_mem_mb)

    from distributed_executor import (
        load_query_plan,
        setup_engines,
        DistributedRunner,
        execute_whole_queries,
    )

    con = None
    ctx = None
    runner = None

    try:
        json_path = Path(json_path_str)
        parquet_dir = Path(parquet_dir_str)
        whole_query_cache_csv = Path(whole_query_cache_csv_str)

        plan = load_query_plan(json_path)

        con, ctx = setup_engines(parquet_dir, engine_mem_mb=engine_mem_mb)
        runner = DistributedRunner(con, ctx)

        result = execute_whole_queries(
            plan,
            runner,
            whole_query_cache_path=whole_query_cache_csv,
            rerun_whole_query=rerun_whole_query,
            parquet_dir=parquet_dir,
            engine_mem_mb=engine_mem_mb,
        )
        out_q.put({"status": "ok", "result": result})

    except BaseException as e:
        status = "oom" if _is_oom_exception(e) else "error"
        out_q.put({"status": status, "traceback": traceback.format_exc()})

    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        runner = None
        ctx = None
        con = None


def _child_run_one_plan(
    json_path_str: str,
    parquet_dir_str: str,
    child_mem_mb: Optional[int],
    engine_mem_mb: Optional[int],
    whole_query_cache_csv_str: str,
    override_engine: Optional[str],
    plan_id: Optional[str],
    out_q: Any,
) -> None:
    _set_child_mem_limit_mb(child_mem_mb)

    from distributed_executor import (
        QueryPlan,
        DpSummary,
        Dag,
        SubQuery,
        load_query_plan,
        setup_engines,
        DistributedRunner,
        execute_distributed_only,
        build_stats_row,
    )

    con = None
    ctx = None
    runner = None

    try:
        json_path = Path(json_path_str)
        parquet_dir = Path(parquet_dir_str)
        whole_query_cache_csv = Path(whole_query_cache_csv_str)

        plan = load_query_plan(json_path)
        plan = _select_plan_variant(plan, plan_id)
        if override_engine is not None:
            engine = str(override_engine).strip().lower()
            if engine not in ("duckdb", "datafusion"):
                raise ValueError(f"Invalid override engine: {override_engine}")

            new_nodes = [
                SubQuery(
                    id=n.id,
                    engine=engine,
                    sql=n.sql,
                    inputs=dict(n.inputs),
                    schema=n.schema,
                )
                for n in plan.dag.nodes
            ]
            new_dag = Dag(final_node_id=plan.dag.final_node_id, nodes=new_nodes)

            dp = plan.dp_summary
            new_dp = None
            if dp is not None:
                new_dp = DpSummary(
                    has_cut=dp.has_cut,
                    cut_node_id=dp.cut_node_id,
                    q1_engine=engine,
                    q2_engine=engine,
                    cost_all_duckdb=dp.cost_all_duckdb,
                    cost_all_datafusion=dp.cost_all_datafusion,
                    chosen_cost=dp.chosen_cost,
                )

            plan = QueryPlan(
                query_id=plan.query_id,
                original_sql=plan.original_sql,
                dag=new_dag,
                dp_summary=new_dp,
                analysis=plan.analysis,
            )

        con, ctx = setup_engines(parquet_dir, engine_mem_mb=engine_mem_mb)
        runner = DistributedRunner(con, ctx)

        exec_result = execute_distributed_only(
            plan,
            runner,
            whole_query_cache_path=whole_query_cache_csv,
            parquet_dir=parquet_dir,
            engine_mem_mb=engine_mem_mb,
        )
        row = build_stats_row(exec_result)
        out_q.put({"status": "ok", "row": row})

    except BaseException as e:
        status = "oom" if _is_oom_exception(e) else "error"
        out_q.put({"status": status, "traceback": traceback.format_exc()})

    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        runner = None
        ctx = None
        con = None


def _terminate_then_kill(
    p: mp.Process,
    *,
    terminate_grace_sec: float,
    kill_grace_sec: float,
) -> None:
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


def run_whole_queries_in_subprocess(
    *,
    json_path: Path,
    parquet_dir: Path,
    child_mem_mb: Optional[int],
    engine_mem_mb: Optional[int],
    whole_query_cache_csv: Path,
    rerun_whole_query: bool,
    timeout_sec: Optional[float],
    terminate_grace_sec: float,
    kill_grace_sec: float,
) -> Dict[str, Any]:
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue(maxsize=1)

    p = ctx.Process(
        target=_child_run_whole_queries,
        args=(
            str(json_path),
            str(parquet_dir),
            child_mem_mb,
            engine_mem_mb,
            str(whole_query_cache_csv),
            rerun_whole_query,
            q,
        ),
    )
    p.start()

    if timeout_sec is None:
        p.join()
    else:
        p.join(timeout=timeout_sec)
        if p.is_alive():
            _terminate_then_kill(
                p,
                terminate_grace_sec=terminate_grace_sec,
                kill_grace_sec=kill_grace_sec,
            )
            exitcode = p.exitcode
            sig = None
            if exitcode is not None and exitcode < 0:
                sig = -exitcode
            return {"status": "timeout", "exitcode": exitcode, "signal": sig}

    try:
        return q.get_nowait()
    except queue_mod.Empty:
        exitcode = p.exitcode
        sig = None
        if exitcode is not None and exitcode < 0:
            sig = -exitcode
        return {"status": "killed", "exitcode": exitcode, "signal": sig}
    finally:
        try:
            q.close()
        except Exception:
            pass


def run_plan_in_subprocess(
    *,
    json_path: Path,
    parquet_dir: Path,
    child_mem_mb: Optional[int],
    engine_mem_mb: Optional[int],
    whole_query_cache_csv: Path,
    override_engine: Optional[str],
    plan_id: Optional[str],
    timeout_sec: Optional[float],
    terminate_grace_sec: float,
    kill_grace_sec: float,
) -> Dict[str, Any]:
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue(maxsize=1)

    p = ctx.Process(
        target=_child_run_one_plan,
        args=(
            str(json_path),
            str(parquet_dir),
            child_mem_mb,
            engine_mem_mb,
            str(whole_query_cache_csv),
            override_engine,
            plan_id,
            q,
        ),
    )
    p.start()

    if timeout_sec is None:
        p.join()
    else:
        p.join(timeout=timeout_sec)
        if p.is_alive():
            _terminate_then_kill(
                p,
                terminate_grace_sec=terminate_grace_sec,
                kill_grace_sec=kill_grace_sec,
            )
            exitcode = p.exitcode
            sig = None
            if exitcode is not None and exitcode < 0:
                sig = -exitcode
            return {"status": "timeout", "exitcode": exitcode, "signal": sig}

    try:
        return q.get_nowait()
    except queue_mod.Empty:
        exitcode = p.exitcode
        sig = None
        if exitcode is not None and exitcode < 0:
            sig = -exitcode

        return {"status": "killed", "exitcode": exitcode, "signal": sig}
    finally:
        try:
            q.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run distributed query plans from JSON and collect statistics."
    )
    parser.add_argument("--plans-dir", required=True)
    parser.add_argument("--parquet-dir", required=True)

    parser.add_argument("--output-dir", default="out")
    parser.add_argument("--output-csv-name", default="distributed_stats.csv")
    parser.add_argument("--same-engine-duckdb-csv-name", default="distributed_stats_duckdb_duckdb.csv")
    parser.add_argument("--same-engine-datafusion-csv-name", default="distributed_stats_datafusion_datafusion.csv")
    parser.add_argument("--exhaustive-output-csv-name", default="distributed_stats_exhaustive.csv")
    parser.add_argument("--processed-log-name", default="processed_plans.txt")
    parser.add_argument("--error-log-name", default="errors.txt")

    parser.add_argument("--whole-query-cache-name", default="whole_query_cache.csv")
    parser.add_argument(
        "--rerun-whole-query",
        action="store_true",
        help="Force re-running whole-query DuckDB/DataFusion runs even if cache exists.",
    )
    parser.add_argument(
        "--skip-whole-queries",
        action="store_true",
        help="Skip whole-query runs and rely on existing cache entries.",
    )
    parser.add_argument(
        "--skip-distributed",
        action="store_true",
        help="Skip distributed runs (whole-query only).",
    )
    parser.add_argument(
        "--run-same-engine-distributed",
        action="store_true",
        help="Also run cut plans with (duckdb, duckdb) and (datafusion, datafusion).",
    )
    parser.add_argument(
        "--run-all-cuts",
        action="store_true",
        help="Run distributed for all cut variants in each JSON (skip no-cut variants).",
    )
    parser.add_argument(
        "--run-cuts-for-nocut-only",
        action="store_true",
        help="If the main plan has no cut, run all cut variants instead.",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--verbose", action="store_true")

    parser.add_argument("--child-mem-mb", type=int, default=None)

    parser.add_argument("--engine-mem-mb", type:int, default=None)

    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=None,
        help="If set, kill child plan execution after this many seconds (prevents parent hang).",
    )
    parser.add_argument(
        "--terminate-grace-sec",
        type=float,
        default=5.0,
        help="Grace period after terminate() before kill().",
    )
    parser.add_argument(
        "--kill-grace-sec",
        type=float,
        default=2.0,
        help="Grace period after kill() before giving up.",
    )

    args = parser.parse_args()

    mp.set_start_method("spawn", force=True)

    plans_dir = Path(args.plans_dir)
    parquet_dir = Path(args.parquet_dir)
    if not plans_dir.is_dir():
        raise SystemExit(f"plans-dir is not a directory: {plans_dir}")
    if not parquet_dir.is_dir():
        raise SystemExit(f"parquet-dir is not a directory: {parquet_dir}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_csv = output_dir / args.output_csv_name
    same_engine_duckdb_csv = output_dir / args.same_engine_duckdb_csv_name
    same_engine_df_csv = output_dir / args.same_engine_datafusion_csv_name
    exhaustive_output_csv = output_dir / args.exhaustive_output_csv_name
    processed_log_path = output_dir / args.processed_log_name
    error_log_path = output_dir / args.error_log_name
    whole_query_cache_csv = output_dir / args.whole_query_cache_name

    json_files = find_json_plans(plans_dir)
    if args.limit is not None:
        json_files = json_files[: args.limit]
    if not json_files:
        raise SystemExit(f"No JSON plan files found in {plans_dir}")

    processed_paths = load_processed_paths(processed_log_path)
    if args.skip_distributed:
        processed_paths = set()
    output_exists = output_csv.exists()
    same_engine_duckdb_exists = same_engine_duckdb_csv.exists()
    same_engine_df_exists = same_engine_df_csv.exists()
    exhaustive_output_exists = exhaustive_output_csv.exists()

    error_count = 0
    success_count = 0

    exhaustive_mode = bool(args.run_all_cuts or args.run_cuts_for_nocut_only)
    main_stats_by_query_id: Dict[str, Dict[str, Any]] = {}
    if exhaustive_mode and output_exists:
        try:
            import pandas as pd

            df_main = pd.read_csv(output_csv)
            if "query_id" in df_main.columns and not df_main.empty:
                df_main["query_id"] = df_main["query_id"].astype(str)
                main_stats_by_query_id = {
                    str(row["query_id"]): dict(row) for row in df_main.to_dict("records")
                }
        except Exception:
            if args.verbose:
                print("  (warning) failed to load existing main results")

    for i, json_path in enumerate(json_files, start=1):
        abs_path = str(json_path.resolve())

        if not exhaustive_mode and abs_path in processed_paths:
            if args.verbose:
                print(f"[{i}/{len(json_files)}] Skipping already processed ({json_path.name})")
            continue

        from distributed_executor import load_query_plan

        plan = load_query_plan(json_path)
        variants_to_run = _pick_variants_to_run(
            plan,
            run_all_cuts=bool(args.run_all_cuts),
            run_cuts_for_nocut_only=bool(args.run_cuts_for_nocut_only),
        )

        if args.verbose:
            print(f"[{i}/{len(json_files)}] Running plan {plan.query_id} ({json_path.name})")

        if not args.skip_whole_queries:
            whole_result = run_whole_queries_in_subprocess(
                json_path=json_path,
                parquet_dir=parquet_dir,
                child_mem_mb=args.child_mem_mb,
                engine_mem_mb=args.engine_mem_mb,
                whole_query_cache_csv=whole_query_cache_csv,
                rerun_whole_query=args.rerun_whole_query,
                timeout_sec=args.timeout_sec,
                terminate_grace_sec=args.terminate_grace_sec,
                kill_grace_sec=args.kill_grace_sec,
            )
            if whole_result.get("status") != "ok":
                status = whole_result.get("status", "error")
                with open(error_log_path, "a", encoding="utf-8") as f:
                    f.write(f"\n--- Failure (whole queries) ---\n")
                    f.write(f"Query ID: {plan.query_id}\n")
                    f.write(f"JSON Path: {json_path}\n")
                    f.write(f"Status: {status}\n")
                    if status in ("killed", "timeout"):
                        f.write(f"Exit code: {whole_result.get('exitcode')}\n")
                        sig = whole_result.get("signal")
                        if sig is not None:
                            try:
                                f.write(f"Signal: {sig} ({signal.Signals(sig).name})\n")
                            except Exception:
                                f.write(f"Signal: {sig}\n")
                    else:
                        f.write("Traceback:\n")
                        f.write(whole_result.get("traceback", ""))
                        f.write("\n")

                if args.verbose:
                    print(f"[{i}/{len(json_files)}] {status} in whole-query run {plan.query_id}")
                error_count += 1
                continue

            if args.verbose:
                r = whole_result.get("result") or {}
                print(
                    f"  whole duckdb: {r.get('duckdb_time', 0.0):.4f}s, "
                    f"whole datafusion: {r.get('datafusion_time', 0.0):.4f}s"
                )

        if args.skip_distributed:
            success_count += 1
            if args.verbose:
                print("  (skipped distributed)")
            continue

        if not variants_to_run:
            if args.verbose:
                print("  (no cut variants to run; skipping distributed)")
            success_count += 1
            continue
        for v in variants_to_run:
            plan_id = getattr(v, "plan_id", None) or "main"

            if exhaustive_mode and _is_processed(processed_paths, abs_path, plan_id):
                if args.verbose:
                    print(f"  (skip processed) plan_id={plan_id}")
                continue

            use_cached_main = False
            assigned_row: Dict[str, Any]
            if plan_id == "main" and exhaustive_mode:
                cached = main_stats_by_query_id.get(str(plan.query_id))
                if cached is not None:
                    use_cached_main = True
                    assigned_row = dict(cached)
                    dp = getattr(v, "dp_summary", None)
                    assigned_row["cut_node_id"] = (
                        dp.cut_node_id if dp is not None else None
                    )
            if not use_cached_main:
                result = run_plan_in_subprocess(
                    json_path=json_path,
                    parquet_dir=parquet_dir,
                    child_mem_mb=args.child_mem_mb,
                    engine_mem_mb=args.engine_mem_mb,
                    whole_query_cache_csv=whole_query_cache_csv,
                    override_engine=None,
                    plan_id=plan_id,
                    timeout_sec=args.timeout_sec,
                    terminate_grace_sec=args.terminate_grace_sec,
                    kill_grace_sec=args.kill_grace_sec,
                )
                if result.get("status") == "ok":
                    assigned_row = result["row"]
                else:
                    assigned_row = {}

            if use_cached_main or (not use_cached_main and result.get("status") == "ok"):
                if exhaustive_mode:
                    key = _plan_variant_key(abs_path, plan_id)
                    append_processed_path(processed_log_path, key)
                    processed_paths.add(key)
                else:
                    append_processed_path(processed_log_path, abs_path)
                    processed_paths.add(abs_path)

                if plan_id == "main" and not use_cached_main:
                    import pandas as pd

                    pd.DataFrame([assigned_row]).to_csv(
                        output_csv,
                        mode="a",
                        header=not output_exists,
                        index=False,
                    )
                    output_exists = True

                if exhaustive_mode:
                    import pandas as pd

                    row_extra = dict(assigned_row)
                    row_extra["plan_id"] = plan_id
                    row_extra["is_main"] = plan_id == "main"
                    pd.DataFrame([row_extra]).to_csv(
                        exhaustive_output_csv,
                        mode="a",
                        header=not exhaustive_output_exists,
                        index=False,
                    )
                    exhaustive_output_exists = True
                success_count += 1

                if args.run_same_engine_distributed and not use_cached_main:
                    dp = getattr(v, "dp_summary", None)
                    same_engine = None if dp is None else dp.q1_engine
                    same_engine_time = None

                    if same_engine not in ("duckdb", "datafusion"):
                        same_engine = None

                    if same_engine is not None:
                        same_result = run_plan_in_subprocess(
                            json_path=json_path,
                            parquet_dir=parquet_dir,
                            child_mem_mb=args.child_mem_mb,
                            engine_mem_mb=args.engine_mem_mb,
                            whole_query_cache_csv=whole_query_cache_csv,
                            override_engine=same_engine,
                            plan_id=plan_id,
                            timeout_sec=args.timeout_sec,
                            terminate_grace_sec=args.terminate_grace_sec,
                            kill_grace_sec=args.kill_grace_sec,
                        )
                        if same_result.get("status") == "ok":
                            import pandas as pd

                            same_row = same_result["row"]
                            same_engine_time = same_row.get("distributed_time")
                            if same_engine == "duckdb":
                                pd.DataFrame([same_row]).to_csv(
                                    same_engine_duckdb_csv,
                                    mode="a",
                                    header=not same_engine_duckdb_exists,
                                    index=False,
                                )
                                same_engine_duckdb_exists = True
                            else:
                                pd.DataFrame([same_row]).to_csv(
                                    same_engine_df_csv,
                                    mode="a",
                                    header=not same_engine_df_exists,
                                    index=False,
                                )
                                same_engine_df_exists = True
                        else:
                            status = same_result.get("status", "error")
                            with open(error_log_path, "a", encoding="utf-8") as f:
                                f.write(f"\n--- Failure (same-engine {same_engine}) ---\n")
                                f.write(f"Query ID: {plan.query_id}\n")
                                f.write(f"Plan ID: {plan_id}\n")
                                f.write(f"JSON Path: {json_path}\n")
                                f.write(f"Status: {status}\n")
                                if status in ("killed", "timeout"):
                                    f.write(f"Exit code: {same_result.get('exitcode')}\n")
                                    sig = same_result.get("signal")
                                    if sig is not None:
                                        try:
                                            f.write(f"Signal: {sig} ({signal.Signals(sig).name})\n")
                                        except Exception:
                                            f.write(f"Signal: {sig}\n")
                                else:
                                    f.write("Traceback:\n")
                                    f.write(same_result.get("traceback", ""))
                                    f.write("\n")

                if args.verbose:
                    msg = (
                        f"  plan_id={plan_id} "
                        f"duckdb: {assigned_row['duckdb_time']:.4f}s, "
                        f"datafusion: {assigned_row['datafusion_time']:.4f}s, "
                        f"distributed: {assigned_row['distributed_time']:.4f}s"
                    )
                    if args.run_same_engine_distributed and not use_cached_main:
                        if same_engine_time is not None and same_engine is not None:
                            msg += (
                                f", distributed ({same_engine}->{same_engine}): "
                                f"{float(same_engine_time):.4f}s"
                            )
                    if use_cached_main:
                        msg += " (cached main)"
                    print(msg)
                continue

            error_count += 1
            status = result.get("status", "error")

            with open(error_log_path, "a", encoding="utf-8") as f:
                f.write(f"\n--- Failure {error_count} ---\n")
                f.write(f"Query ID: {plan.query_id}\n")
                f.write(f"Plan ID: {plan_id}\n")
                f.write(f"JSON Path: {json_path}\n")
                f.write(f"Status: {status}\n")

                if status in ("killed", "timeout"):
                    f.write(f"Exit code: {result.get('exitcode')}\n")
                    sig = result.get("signal")
                    if sig is not None:
                        try:
                            f.write(f"Signal: {sig} ({signal.Signals(sig).name})\n")
                        except Exception:
                            f.write(f"Signal: {sig}\n")
                else:
                    f.write("Traceback:\n")
                    f.write(result.get("traceback", ""))
                    f.write("\n")

            if args.verbose:
                print(f"[{i}/{len(json_files)}] {status} in plan {plan.query_id} ({plan_id})")
                if status in ("oom", "killed", "timeout"):
                    print("  (next query runs in a fresh process with fresh engines)")

    if args.verbose:
        print(f"\nDone. Success: {success_count}, Failures: {error_count}")
        print(f"CSV: {output_csv}")
        if args.run_same_engine_distributed:
            print(f"Same-engine (duckdb->duckdb) CSV: {same_engine_duckdb_csv}")
            print(f"Same-engine (datafusion->datafusion) CSV: {same_engine_df_csv}")
        if exhaustive_mode:
            print(f"Exhaustive CSV: {exhaustive_output_csv}")
        print(f"Processed log: {processed_log_path}")
        print(f"Errors: {error_log_path}")
        print(f"Whole-query cache: {whole_query_cache_csv}")


if __name__ == "__main__":
    main()

