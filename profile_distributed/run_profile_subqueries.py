
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

# Dependencies: distributed_runner, duckdb_profiler (datafusion_profiler is invoked via subprocess)
_CHOP_DIR = Path(__file__).resolve().parents[1]
for _p in ("distributed_runner", "duckdb_profiler"):
    _d = _CHOP_DIR / _p
    if _d.is_dir() and str(_d) not in sys.path:
        sys.path.insert(0, str(_d))

from distributed_executor import (
    load_query_plan,
    setup_duckdb,
    setup_engines,
    DistributedRunner,
    SubQuery,
)
from duckdb_pipeline.run_duckdb_pipeline import run_one as duckdb_run_one


def _select_variants(plan: Any, *, exhaustive_all: bool, exhaustive_nocut: bool) -> List[Any]:
    variants = list(getattr(plan, "cut_plans", []) or [])
    if not variants:
        return []

    main = variants[0]
    main_dp = getattr(main, "dp_summary", None)
    main_has_cut = bool(main_dp and getattr(main_dp, "has_cut", False))

    def is_cut(v: Any) -> bool:
        dp = getattr(v, "dp_summary", None)
        return bool(dp and getattr(dp, "has_cut", False))

    if exhaustive_all:
        return [v for v in variants if is_cut(v)]

    if exhaustive_nocut:
        if main_has_cut:
            return [main]
        return [v for v in variants if is_cut(v)]

    if main_has_cut:
        return [main]
    return []


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_sql(out_dir: Path, name: str, sql: str) -> Path:
    p = out_dir / f"{name}.sql"
    p.write_text(sql, encoding="utf-8")
    return p


def _find_q1_q2_nodes(dag_nodes: List[Any], final_node_id: str) -> tuple[Any, Any]:
    id_to = {n.id: n for n in dag_nodes}
    q2 = id_to.get(final_node_id)
    if q2 is None:
        raise ValueError(f"Final node {final_node_id} not found in DAG")
    roots = [n for n in dag_nodes if not (n.inputs or {})]
    q1 = None
    for r in roots:
        if r.id != q2.id:
            q1 = r
            break
    if q1 is None:
        # Fallback: pick any non-final node
        for n in dag_nodes:
            if n.id != q2.id:
                q1 = n
                break
    if q1 is None:
        raise ValueError("Could not identify q1 node in DAG")
    return q1, q2


def _run_q1_for_s1(
    runner: DistributedRunner,
    q1_node: Any,
) -> pa.Table:
    results = runner.run([q1_node])
    if q1_node.id not in results:
        raise RuntimeError(f"Q1 output missing for {q1_node.id}")
    return results[q1_node.id]


def _run_duckdb_profile(
    *,
    sql_path: Path,
    out_dir: Path,
    parquet_dir: Path,
    duckdb_threads: Optional[int],
    keep_projects: bool,
    write_tree: bool,
    tree_cols: bool,
    tree_metrics: bool,
    tree_show_ms: bool,
    s1_table: Optional[pa.Table],
) -> None:
    con = setup_duckdb(parquet_dir)
    if duckdb_threads is not None:
        con.execute(f"PRAGMA threads={int(duckdb_threads)}")
    if s1_table is not None:
        con.register("s1", s1_table)
    ops_set: set = set()
    _ = duckdb_run_one(
        con,
        sql_path,
        out_dir,
        str(parquet_dir),
        "engine",
        disable_topn_rule=True,
        keep_projects=keep_projects,
        debug=False,
        write_tree=write_tree,
        tree_cols=tree_cols,
        ops_set=ops_set,
        tree_metrics=tree_metrics,
        tree_show_ms=tree_show_ms,
        allow_fallbacks=True,
    )
    con.close()


def _parse_extra_tables(items: Iterable[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid --extra-parquet entry: {item}")
        name, path = item.split("=", 1)
        name = name.strip()
        path = path.strip()
        if not name or not path:
            raise ValueError(f"Invalid --extra-parquet entry: {item}")
        out[name] = path
    return out


def _load_llm_splits(
    jsonl_path: Path,
    *,
    query_ids: Optional[set] = None,
) -> List[Dict[str, Any]]:
    """Load successful cut splits from a ``query_splits.jsonl`` produced by
    ``run_proofOfConcept_paper.py``.

    A split is considered *successful* when:
      - ``has_cut`` is ``True``
      - ``results_match`` is ``True``  (or absent, for backwards compat)
      - no error ``status`` field is present

    If *query_ids* is given, only entries whose ``query_id`` is in that list
    are returned.
    """
    splits: List[Dict[str, Any]] = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry: Dict[str, Any] = json.loads(line)
            # Skip non-cut entries
            if not entry.get("has_cut", False):
                continue
            # Skip mismatches
            if entry.get("results_match") is False:
                continue
            # Skip error / timeout / killed / etc.
            if "status" in entry:
                continue
            # Must have sql1 and sql2
            if not entry.get("sql1") or not entry.get("sql2"):
                continue
            # Optional query-id filter
            if query_ids is not None:
                qid = entry.get("query_id", "")
                if qid not in query_ids:
                    continue
            splits.append(entry)
    return splits


def _run_datafusion_profile(
    *,
    df_profiler_cmd: List[str],
    sql_path: Path,
    out_dir: Path,
    parquet_dir: Path,
    target_partitions: Optional[int],
    write_tree: bool,
    tree_cols: bool,
    tree_plain: bool,
    extra_parquet: Optional[Dict[str, str]],
) -> None:
    cmd = list(df_profiler_cmd)
    cmd += [
        "--in-sql-file",
        str(sql_path),
        "--in-parquet-dir",
        str(parquet_dir),
        "--out-json-dir",
        str(out_dir),
        "--size-mode",
        "engine",
    ]
    if write_tree:
        cmd.append("--write-tree")
    if tree_plain:
        cmd.append("--tree-plain")
    if tree_cols:
        cmd.append("--tree-cols")
    if target_partitions is not None:
        cmd += ["--target-partitions", str(int(target_partitions))]
    if extra_parquet:
        for name, path in extra_parquet.items():
            cmd += ["--extra-parquet", f"{name}={path}"]
    subprocess.run(cmd, check=True)


def _run_llm_splits(
    *,
    splits: List[Dict[str, Any]],
    out_dir: Path,
    parquet_dir: Path,
    runner: DistributedRunner,
    df_cmd: List[str],
    args: argparse.Namespace,
) -> None:
    """Profile subqueries from LLM-generated splits (``query_splits.jsonl``)."""
    tree_metrics = True
    tree_show_ms = not args.tree_plain
    tree_cols = bool(args.tree_cols) and not args.tree_plain

    for idx, entry in enumerate(splits, 1):
        query_id = entry["query_id"]
        # Normalise: strip .sql suffix if present so directory names are clean
        qid_clean = query_id.replace(".sql", "") if query_id.endswith(".sql") else query_id
        sql1 = entry["sql1"]
        sql2 = entry["sql2"]
        q1_engine = str(entry.get("q1_engine", "duckdb")).strip().lower()
        q2_engine = str(entry.get("q2_engine", "duckdb")).strip().lower()
        original_sql = entry.get("original_sql")

        print(f"\n[{idx}/{len(splits)}] Profiling LLM split for {query_id}  "
              f"(q1={q1_engine}, q2={q2_engine})")

        variant_out = out_dir / qid_clean / "llm"
        _ensure_dir(variant_out)

        # -- optional whole-query profiling --------------------------------
        if args.profile_whole and original_sql:
            whole_out = out_dir / qid_clean / "whole"
            _ensure_dir(whole_out)
            if not args.skip_duckdb:
                whole_sql_path = _write_sql(whole_out, "whole_duckdb", original_sql)
                _run_duckdb_profile(
                    sql_path=whole_sql_path,
                    out_dir=whole_out,
                    parquet_dir=parquet_dir,
                    duckdb_threads=args.duckdb_threads,
                    keep_projects=args.keep_projects,
                    write_tree=args.write_tree,
                    tree_cols=tree_cols,
                    tree_metrics=tree_metrics,
                    tree_show_ms=tree_show_ms,
                    s1_table=None,
                )
            if not args.skip_datafusion:
                whole_sql_path = _write_sql(whole_out, "whole_datafusion", original_sql)
                _run_datafusion_profile(
                    df_profiler_cmd=df_cmd,
                    sql_path=whole_sql_path,
                    out_dir=whole_out,
                    parquet_dir=parquet_dir,
                    target_partitions=args.df_target_partitions,
                    write_tree=args.write_tree,
                    tree_cols=tree_cols,
                    tree_plain=bool(args.tree_plain),
                    extra_parquet=None,
                )

        # -- execute q1 to materialise s1 ---------------------------------
        q1_node = SubQuery(id="q1", engine=q1_engine, sql=sql1, inputs={})
        try:
            q1_tbl = _run_q1_for_s1(runner, q1_node)
        except Exception as exc:
            print(f"  [skip] could not execute q1 for {query_id}: {exc}")
            continue

        # -- profile q1 ----------------------------------------------------
        if q1_engine == "duckdb" and not args.skip_duckdb:
            q1_sql_path = _write_sql(variant_out, "q1_duckdb", sql1)
            _run_duckdb_profile(
                sql_path=q1_sql_path,
                out_dir=variant_out,
                parquet_dir=parquet_dir,
                duckdb_threads=args.duckdb_threads,
                keep_projects=args.keep_projects,
                write_tree=args.write_tree,
                tree_cols=tree_cols,
                tree_metrics=tree_metrics,
                tree_show_ms=tree_show_ms,
                s1_table=None,
            )
        if q1_engine == "datafusion" and not args.skip_datafusion:
            q1_sql_path = _write_sql(variant_out, "q1_datafusion", sql1)
            _run_datafusion_profile(
                df_profiler_cmd=df_cmd,
                sql_path=q1_sql_path,
                out_dir=variant_out,
                parquet_dir=parquet_dir,
                target_partitions=args.df_target_partitions,
                write_tree=args.write_tree,
                tree_cols=tree_cols,
                tree_plain=bool(args.tree_plain),
                extra_parquet=None,
            )

        # -- profile q2 (needs s1 registered) -----------------------------
        if q2_engine == "duckdb" and not args.skip_duckdb:
            q2_sql_path = _write_sql(variant_out, "q2_duckdb", sql2)
            _run_duckdb_profile(
                sql_path=q2_sql_path,
                out_dir=variant_out,
                parquet_dir=parquet_dir,
                duckdb_threads=args.duckdb_threads,
                keep_projects=args.keep_projects,
                write_tree=args.write_tree,
                tree_cols=tree_cols,
                tree_metrics=tree_metrics,
                tree_show_ms=tree_show_ms,
                s1_table=q1_tbl,
            )
        if q2_engine == "datafusion" and not args.skip_datafusion:
            q2_sql_path = _write_sql(variant_out, "q2_datafusion", sql2)
            s1_path = variant_out / "s1.parquet"
            pq.write_table(q1_tbl, s1_path)
            _run_datafusion_profile(
                df_profiler_cmd=df_cmd,
                sql_path=q2_sql_path,
                out_dir=variant_out,
                parquet_dir=parquet_dir,
                target_partitions=args.df_target_partitions,
                write_tree=args.write_tree,
                tree_cols=tree_cols,
                tree_plain=bool(args.tree_plain),
                extra_parquet={"s1": str(s1_path)},
            )

    print(f"\nLLM split profiling complete — {len(splits)} queries processed.")


def _run_calcite_plans(
    *,
    json_files: List[Path],
    out_dir: Path,
    parquet_dir: Path,
    runner: DistributedRunner,
    df_cmd: List[str],
    args: argparse.Namespace,
) -> None:
    """Profile subqueries from Calcite-generated JSON plan files (original mode)."""
    if args.exhaustive_all and args.exhaustive_nocut:
        raise SystemExit("Choose only one of --exhaustive-all or --exhaustive-nocut.")

    for json_path in json_files:
        plan = load_query_plan(json_path)
        tree_metrics = True
        tree_show_ms = not args.tree_plain
        tree_cols = bool(args.tree_cols) and not args.tree_plain
        if args.profile_whole:
            whole_out = out_dir / plan.query_id / "whole"
            _ensure_dir(whole_out)
            if not args.skip_duckdb:
                whole_sql_path = _write_sql(whole_out, "whole_duckdb", plan.original_sql)
                _run_duckdb_profile(
                    sql_path=whole_sql_path,
                    out_dir=whole_out,
                    parquet_dir=parquet_dir,
                    duckdb_threads=args.duckdb_threads,
                    keep_projects=args.keep_projects,
                    write_tree=args.write_tree,
                    tree_cols=tree_cols,
                    tree_metrics=tree_metrics,
                    tree_show_ms=tree_show_ms,
                    s1_table=None,
                )

            if not args.skip_datafusion:
                whole_sql_path = _write_sql(whole_out, "whole_datafusion", plan.original_sql)
                _run_datafusion_profile(
                    df_profiler_cmd=df_cmd,
                    sql_path=whole_sql_path,
                    out_dir=whole_out,
                    parquet_dir=parquet_dir,
                    target_partitions=args.df_target_partitions,
                    write_tree=args.write_tree,
                    tree_cols=tree_cols,
                    tree_plain=bool(args.tree_plain),
                    extra_parquet=None,
                )
        variants = _select_variants(
            plan,
            exhaustive_all=bool(args.exhaustive_all),
            exhaustive_nocut=bool(args.exhaustive_nocut),
        )

        for variant in variants:
            dp = getattr(variant, "dp_summary", None)
            if dp is None or not getattr(dp, "has_cut", False):
                continue

            plan_id = getattr(variant, "plan_id", None) or "main"
            dag = variant.dag
            q1, q2 = _find_q1_q2_nodes(dag.nodes, dag.final_node_id)

            variant_out = out_dir / plan.query_id / plan_id
            _ensure_dir(variant_out)

            q1_tbl = _run_q1_for_s1(runner, q1)

            if q1.engine == "duckdb" and not args.skip_duckdb:
                q1_sql_path = _write_sql(variant_out, f"{q1.id}_duckdb", q1.sql)
                _run_duckdb_profile(
                    sql_path=q1_sql_path,
                    out_dir=variant_out,
                    parquet_dir=parquet_dir,
                    duckdb_threads=args.duckdb_threads,
                    keep_projects=args.keep_projects,
                    write_tree=args.write_tree,
                    tree_cols=tree_cols,
                    tree_metrics=tree_metrics,
                    tree_show_ms=tree_show_ms,
                    s1_table=None,
                )

            if q1.engine == "datafusion" and not args.skip_datafusion:
                q1_sql_path = _write_sql(variant_out, f"{q1.id}_datafusion", q1.sql)
                _run_datafusion_profile(
                    df_profiler_cmd=df_cmd,
                    sql_path=q1_sql_path,
                    out_dir=variant_out,
                    parquet_dir=parquet_dir,
                    target_partitions=args.df_target_partitions,
                    write_tree=args.write_tree,
                    tree_cols=tree_cols,
                    tree_plain=bool(args.tree_plain),
                    extra_parquet=None,
                )

            if q2.engine == "duckdb" and not args.skip_duckdb:
                q2_sql_path = _write_sql(variant_out, f"{q2.id}_duckdb", q2.sql)
                _run_duckdb_profile(
                    sql_path=q2_sql_path,
                    out_dir=variant_out,
                    parquet_dir=parquet_dir,
                    duckdb_threads=args.duckdb_threads,
                    keep_projects=args.keep_projects,
                    write_tree=args.write_tree,
                    tree_cols=tree_cols,
                    tree_metrics=tree_metrics,
                    tree_show_ms=tree_show_ms,
                    s1_table=q1_tbl,
                )

            if q2.engine == "datafusion" and not args.skip_datafusion:
                q2_sql_path = _write_sql(variant_out, f"{q2.id}_datafusion", q2.sql)
                s1_path = variant_out / "s1.parquet"
                pq.write_table(q1_tbl, s1_path)
                _run_datafusion_profile(
                    df_profiler_cmd=df_cmd,
                    sql_path=q2_sql_path,
                    out_dir=variant_out,
                    parquet_dir=parquet_dir,
                    target_partitions=args.df_target_partitions,
                    write_tree=args.write_tree,
                    tree_cols=tree_cols,
                    tree_plain=bool(args.tree_plain),
                    extra_parquet={"s1": str(s1_path)},
                )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Profile per-subquery physical plans for cut queries.\n\n"
                    "Supports two input modes:\n"
                    "  (1) --plans-dir   : Calcite-generated JSON plan files\n"
                    "  (2) --splits-jsonl: LLM-generated splits from run_proofOfConcept_paper.py\n\n"
                    "At least one of the two must be provided.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # ---- input sources (at least one required) ---------------------------
    ap.add_argument(
        "--plans-dir",
        default=None,
        help="Directory with Calcite JSON plan files (original mode).",
    )
    ap.add_argument(
        "--splits-jsonl",
        default=None,
        help="Path to query_splits.jsonl produced by run_proofOfConcept_paper.py.",
    )
    ap.add_argument(
        "--query-ids",
        nargs="*",
        default=None,
        help="Only profile these query IDs (space-separated list, e.g. "
             "'10053.sql 10129.sql').  Works with both --plans-dir (matches "
             "against JSON file stems) and --splits-jsonl (matches against "
             "query_id field).  By default all queries are profiled.",
    )

    # ---- common arguments ------------------------------------------------
    ap.add_argument("--parquet-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument(
        "--exhaustive-all",
        action="store_true",
        help="Run exhaustive cut variants for all queries (including non-cut).",
    )
    ap.add_argument(
        "--exhaustive-nocut",
        action="store_true",
        help="Run main cut for cut queries, exhaustive cuts for non-cut queries.",
    )
    ap.add_argument("--keep-projects", action="store_true")
    ap.add_argument("--write-tree", action="store_true")
    ap.add_argument("--tree-cols", action="store_true")
    ap.add_argument(
        "--tree-plain",
        action="store_true",
        help="Tree output without ms or cols (keeps in/out).",
    )
    ap.add_argument("--duckdb-threads", type=int, default=None)
    ap.add_argument("--df-target-partitions", type=int, default=None)
    ap.add_argument(
        "--profile-whole",
        action="store_true",
        help="Also profile the full (uncut) query on both engines.",
    )
    ap.add_argument(
        "--df-profiler-cmd",
        default=None,
        help="Command to run the DataFusion profiler binary (string).",
    )
    ap.add_argument(
        "--skip-duckdb",
        action="store_true",
        help="Skip DuckDB subquery profiling.",
    )
    ap.add_argument(
        "--skip-datafusion",
        action="store_true",
        help="Skip DataFusion subquery profiling.",
    )
    args = ap.parse_args()

    # ---- validate inputs -------------------------------------------------
    if args.plans_dir is None and args.splits_jsonl is None:
        raise SystemExit("At least one of --plans-dir or --splits-jsonl is required.")

    parquet_dir = Path(args.parquet_dir)
    out_dir = Path(args.out_dir)
    _ensure_dir(out_dir)

    # ---- DataFusion profiler command -------------------------------------
    if args.df_profiler_cmd:
        df_cmd = shlex.split(args.df_profiler_cmd)
    else:
        df_manifest = _CHOP_DIR / "datafusion_profiler" / "Cargo.toml"
        df_cmd = [
            "cargo",
            "run",
            "--release",
            "--quiet",
            "--bin",
            "df_metrics_dump_subquery",
            "--manifest-path",
            str(df_manifest),
            "--",
        ]

    # ---- engines ---------------------------------------------------------
    con_exec, ctx_exec = setup_engines(parquet_dir)
    runner = DistributedRunner(con_exec, ctx_exec)

    # ---- build normalised query-id filter set ----------------------------
    query_id_filter: Optional[set] = None
    if args.query_ids:
        # Accept "10053", "10053.sql", or "10053.json" — build a set that
        # matches all plausible forms so both JSONL query_ids ("10053.sql")
        # and JSON file stems ("10053") are covered.
        raw = set(args.query_ids)
        bare = {qid.removesuffix(".sql").removesuffix(".json") for qid in raw}
        with_sql = {b + ".sql" for b in bare}
        query_id_filter = raw | bare | with_sql

    # ---- mode 1: Calcite JSON plans -------------------------------------
    if args.plans_dir is not None:
        plans_dir = Path(args.plans_dir)
        json_files = sorted(plans_dir.glob("*.json"))
        if query_id_filter is not None:
            json_files = [
                f for f in json_files
                if f.stem in query_id_filter
            ]
        if args.limit is not None:
            json_files = json_files[: args.limit]
        if not json_files:
            print(f"Warning: no JSON plan files found in {plans_dir}")
        else:
            _run_calcite_plans(
                json_files=json_files,
                out_dir=out_dir,
                parquet_dir=parquet_dir,
                runner=runner,
                df_cmd=df_cmd,
                args=args,
            )

    # ---- mode 2: LLM splits from query_splits.jsonl ---------------------
    if args.splits_jsonl is not None:
        jsonl_path = Path(args.splits_jsonl)
        if not jsonl_path.is_file():
            raise SystemExit(f"--splits-jsonl file not found: {jsonl_path}")
        splits = _load_llm_splits(jsonl_path, query_ids=query_id_filter)
        if args.limit is not None:
            splits = splits[: args.limit]
        if not splits:
            print(f"Warning: no successful cut splits found in {jsonl_path}")
        else:
            print(f"Loaded {len(splits)} successful LLM split(s) to profile.")
            _run_llm_splits(
                splits=splits,
                out_dir=out_dir,
                parquet_dir=parquet_dir,
                runner=runner,
                df_cmd=df_cmd,
                args=args,
            )

    con_exec.close()
    print("\nAll profiling complete.")


if __name__ == "__main__":
    main()
