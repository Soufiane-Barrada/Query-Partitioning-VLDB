import argparse, pathlib, sys

THIS_DIR = pathlib.Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

import duckdb
from duckdb_planbench import (
    collect_unified,
    annotate,
    prune_cte,
    prune_projects_one_child,
    strip_internal,
)
from duckdb_planbench.io import save_json, draw_tree
from pathlib import Path


def _sql_str(s: str) -> str:
    # Escape single quotes for SQL literals
    return "'" + s.replace("'", "''") + "'"


def register_parquet_views_simple(con: duckdb.DuckDBPyConnection, dirpath: str):
    p = Path(dirpath)
    if not p.exists():
        raise FileNotFoundError(f"parquet dir not found: {dirpath}")
    for f in sorted(p.glob("*.parquet")):
        view = f.stem  # e.g., customers.parquet -> customers
        path_lit = _sql_str(str(f))
        con.execute(f'CREATE OR REPLACE TEMP VIEW "{view}" AS SELECT * FROM read_parquet({path_lit});')


def run_one(
    con,
    sql_path: pathlib.Path,
    out_dir: pathlib.Path,
    parquet_dir: str,
    size_mode: str,
    disable_topn_rule: bool,
    keep_projects: bool,
    debug: bool,
    write_tree: bool,
    tree_cols: bool,
    ops_set: set,
    tree_metrics: bool = True,
    tree_show_ms: bool = True,
    allow_fallbacks: bool = False,
):
    base = sql_path.stem
    out_main = out_dir / f"{base}.json"
    out_profile = out_dir / f"{base}.raw_duck_profile.json"
    out_plan = out_dir / f"{base}.duck_plan.json"
    out_tree = out_dir / f"{base}.tree.txt"

    try:
        # 1) Collect, including lineage.
        doc = collect_unified(
            sql_path,
            connection=con,
            raw_profile_out=out_profile,
            raw_plan_only_out=None,
            disable_topn_rule=disable_topn_rule,
            parquet_dir=parquet_dir,
            ops_set=ops_set,
        )

        # 2) Annotate (op kinds + row sizes)
        doc = annotate(doc, parquet_dir, debug=debug, allow_fallbacks=allow_fallbacks)

        # 3) Prune CTE containers
        root = prune_cte(doc["root"])

        # 4) prune single-child Projects
        if not keep_projects:
            root = prune_projects_one_child(root)

        # 5) Strip internal fields and save the final JSON
        doc["root"] = strip_internal(root)
        save_json(doc, out_main)

        # 6) Optionally draw the ASCII tree
        if write_tree:
            tree_text = draw_tree(
                doc,
                show_metrics=tree_metrics,
                show_ms=tree_show_ms,
                show_active_cols=tree_cols,
                decimals=3,
            )
            out_tree.write_text(tree_text)

        return True

    except Exception as e:
        print(f"[skip] {sql_path.name}: {e.__class__.__name__}: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    ap = argparse.ArgumentParser(description="Run DuckDB profiling pipeline over SQL files.")
    ap.add_argument("--db")
    ap.add_argument("--parquet-dir", required=True)
    ap.add_argument("--in-sql-dir", required=True)
    ap.add_argument("--out-json-dir", required=True)
    ap.add_argument("--enable-topn", action="store_true")
    ap.add_argument("--keep-projects", action="store_true")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--write-tree", action="store_true", help="Also write <query>.tree.txt with ASCII plan")
    ap.add_argument("--tree-cols", action="store_true", help="Include active columns in the ASCII plan")
    ap.add_argument(
        "--size-mode",
        choices=["engine", "parquet_uncompressed", "parquet_compressed"],
        default="engine",
        help="How to estimate per-column bytes",
    )
    ap.add_argument(
        "--allow-fallbacks",
        action="store_true",
        help="Allow fallback sizing for scans/columns with unknown tables (strict by default).",
    )

    args = ap.parse_args()
    in_dir = pathlib.Path(args.in_sql_dir)
    out_dir = pathlib.Path(args.out_json_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # con = duckdb.connect(args.db)
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=1")
    register_parquet_views_simple(con, args.parquet_dir)

    sql_files = sorted(in_dir.glob("*.sql"))
    if not sql_files:
        print(f"No .sql files found in {in_dir}")

    ok = 0
    ops_set = set()
    out_ops = out_dir / "observed_ops.txt"
    failed_list = []
    for f in sql_files:
        print(f"[run] {f.name}")
        if run_one(
            con,
            f,
            out_dir,
            args.parquet_dir,
            args.size_mode,
            disable_topn_rule=(not args.enable_topn),
            keep_projects=args.keep_projects,
            debug=args.debug,
            write_tree=args.write_tree,
            tree_cols=args.tree_cols,
            ops_set=ops_set,
            allow_fallbacks=args.allow_fallbacks,
        ):
            ok += 1
            ops_text = ", ".join(sorted(ops_set))
            out_ops.write_text(ops_text + "\n", encoding="utf-8")
        else:
            failed_list.append(f.name)

    con.close()
    print(f"Done. Succeeded: {ok}/{len(sql_files)} — outputs saved only for successful queries.")
    print("failed queries:", failed_list)


if __name__ == "__main__":
    main()

