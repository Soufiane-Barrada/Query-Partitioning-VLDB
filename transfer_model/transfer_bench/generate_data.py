from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pyarrow as pa

import duckdb
from datafusion import SessionContext


# Data generation (Arrow tables)

def make_large_string_col(char_byte: bytes, n: int, strlen: int) -> pa.Array:
    """
    Build LargeString array using Arrow buffers.
    strlen may be 0 (empty strings).
    """
    offsets = (np.arange(n + 1, dtype=np.int64) * strlen)
    offsets_buf = pa.py_buffer(offsets.tobytes())
    data_buf = pa.py_buffer(char_byte * (n * strlen))
    return pa.Array.from_buffers(pa.large_string(), n, [None, offsets_buf, data_buf])


def make_table(rows: int, row_size_bytes: int) -> pa.Table:
    """
    Mixed-type table with schema that deterministically depends on row_size_bytes.

    Always present:
      - id: int64
      - s1: large_string (UTF-8), may be empty-string if no budget

    Added when row_size_bytes is large enough:
      - ts: timestamp[us]
      - v : float64
      - flag: bool
      - s2: large_string

    row_size_bytes is treated as an approcimate "payload budget per row".

    This design allows row_size_bytes as small as 8 (just int64 + empty-string column).
    """
    if rows <= 0:
        raise ValueError("rows must be > 0")
    if row_size_bytes <= 0:
        raise ValueError("row_size_bytes must be > 0")

    # Always include int64 id (8 bytes)
    fixed_bytes = 8

    # Deterministically add more fixed-width columns if budget allows
    include_ts = (row_size_bytes >= (fixed_bytes + 8 + 1))   # leave at least 1 byte for strings
    if include_ts:
        fixed_bytes += 8

    include_v = (row_size_bytes >= (fixed_bytes + 8 + 1))
    if include_v:
        fixed_bytes += 8

    include_flag = (row_size_bytes >= (fixed_bytes + 1 + 1))
    if include_flag:
        fixed_bytes += 1

    # Decide whether to include a second string column (only if we have at least 2 bytes for strings)
    str_total = row_size_bytes - fixed_bytes
    include_s2 = (str_total >= 2)

    # Split string budget across s1/s2 if s2 is present, else all goes to s1.
    if include_s2:
        s1_len = str_total // 2
        s2_len = str_total - s1_len
    else:
        s1_len = max(0, str_total)
        s2_len = 0

    # Strings (ASCII => 1 byte/char). s1_len may be 0.
    s1 = make_large_string_col(b"a", rows, s1_len)

    cols: dict[str, pa.Array] = {}

    # int64 id
    cols["id"] = pa.array(np.arange(rows, dtype=np.int64), type=pa.int64())

    # timestamp[us] (vectorized)
    if include_ts:
        base = np.datetime64("2020-01-01T00:00:00", "us")
        ts_np = base + np.arange(rows, dtype="timedelta64[us]")
        cols["ts"] = pa.array(ts_np, type=pa.timestamp("us"))

    # float64
    if include_v:
        cols["v"] = pa.array(np.arange(rows, dtype=np.float64), type=pa.float64())

    # bool
    if include_flag:
        cols["flag"] = pa.array((np.arange(rows, dtype=np.int64) % 2) == 0, type=pa.bool_())

    # strings
    cols["s1"] = s1
    if include_s2:
        cols["s2"] = make_large_string_col(b"b", rows, s2_len)

    return pa.table(cols)


# Sampling utilities

def geomspace_unique_ints(vmin: int, vmax: int, n: int) -> list[int]:
    """
    Exactly n strictly-increasing integers spanning [vmin, vmax] uniformly in log-space.

    No fallbacks:
      - Raises if the integer range cannot fit n unique integers.
    """
    if vmin <= 0 or vmax <= 0:
        raise ValueError("vmin/vmax must be > 0 for log spacing")
    if vmin > vmax:
        raise ValueError("vmin must be <= vmax")
    if n <= 0:
        raise ValueError("n must be > 0")
    if (vmax - vmin + 1) < n:
        raise ValueError(f"Range [{vmin},{vmax}] too small to fit {n} unique integers")

    xs = np.exp(np.linspace(np.log(vmin), np.log(vmax), n))
    vals = np.rint(xs).astype(np.int64)

    # Enforce endpoints
    vals[0] = vmin
    vals[-1] = vmax

    # Make strictly increasing (forward pass)
    for i in range(1, n):
        if vals[i] <= vals[i - 1]:
            vals[i] = vals[i - 1] + 1

    # If we overshot vmax, fix from the end (backward pass)
    if vals[-1] > vmax:
        vals[-1] = vmax
        for i in range(n - 2, -1, -1):
            if vals[i] >= vals[i + 1]:
                vals[i] = vals[i + 1] - 1

    # Final validation
    if vals[0] < vmin or vals[-1] > vmax:
        raise ValueError("Failed to construct valid unique int grid")
    if not np.all(vals[1:] > vals[:-1]):
        raise ValueError("Failed to construct strictly increasing unique ints")

    return [int(x) for x in vals]


def generate_pairs_grid(
    rows_min: int, rows_max: int, rows_steps: int,
    row_size_min: int, row_size_max: int, row_size_steps: int,
) -> list[tuple[int, int]]:
    rows = geomspace_unique_ints(rows_min, rows_max, rows_steps)
    print(rows)
    sizes = geomspace_unique_ints(row_size_min, row_size_max, row_size_steps)
    return [(r, s) for r in rows for s in sizes]


def generate_pairs_lhs(
    rng: np.random.Generator,
    rows_min: int, rows_max: int,
    row_size_min: int, row_size_max: int,
    n_pairs: int,
) -> list[tuple[int, int]]:
    """
    Discrete Latin-hypercube style:
      - build n_pairs unique rows (log-uniform)
      - build n_pairs unique sizes (log-uniform)
      - permute one axis and zip => exactly n_pairs unique pairs,
        uniformly covering both axes in log-space.
    """
    rows = geomspace_unique_ints(rows_min, rows_max, n_pairs)
    sizes = geomspace_unique_ints(row_size_min, row_size_max, n_pairs)
    perm = rng.permutation(n_pairs)
    return [(rows[i], sizes[int(perm[i])]) for i in range(n_pairs)]


# Engine setup

def setup_duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("SET arrow_large_buffer_size=true")
    return con


def setup_datafusion() -> SessionContext:
    return SessionContext()


def register_input_duckdb(con: duckdb.DuckDBPyConnection, tbl: pa.Table) -> None:
    con.register("input_tbl", tbl)


def register_input_datafusion(ctx: SessionContext, tbl: pa.Table) -> None:
    ctx.register_view("input_tbl", ctx.from_arrow(tbl))


# Cross-engine transfer measure

def build_full_scan_agg_sql(table_name: str, col_names: list[str]) -> str:
    """
    Build an aggregation query that forces a full scan of *all* columns
    but returns only 1 row (so we don't pay receiver output materialization cost).

    """
    allowed = {"id", "ts", "v", "flag", "s1", "s2"}
    unknown = set(col_names) - allowed
    if unknown:
        raise ValueError(f"Unexpected columns in table {table_name}: {sorted(unknown)}")

    exprs: list[str] = []

    if "id" in col_names:
        exprs.append("sum(id) AS sum_id")

    if "ts" in col_names:
    # Force scan of timestamp values WITHOUT casting.
    # max(ts) returns 1 value and works in DuckDB + DataFusion.
        exprs.append("max(ts) AS max_ts")

    if "v" in col_names:
        # Force scan of float values; output remains 1 scalar.
        exprs.append("sum(v) AS sum_v")

    if "flag" in col_names:
        # Force scan of boolean column.
        exprs.append("sum(CASE WHEN flag THEN 1 ELSE 0 END) AS sum_flag_true")

    if "s1" in col_names:
        # length(s1) forces scanning string offsets and (typically) data
        exprs.append("sum(length(s1)) AS sum_len_s1")

    if "s2" in col_names:
        exprs.append("sum(length(s2)) AS sum_len_s2")

    if not exprs:
        raise ValueError("No columns found to scan")

    return f"SELECT {', '.join(exprs)} FROM {table_name}"


def transfer_duckdb_to_datafusion(
    con: duckdb.DuckDBPyConnection,
    ctx: SessionContext,
    input_tbl: pa.Table,
) -> float:
    """
    Measures end-to-end transfer cost for DuckDB -> DataFusion:

    INCLUDED:
      1) DuckDB executes SELECT * and materializes to Arrow.
      2) DataFusion registers the Arrow table.
      3) DataFusion performs a full scan of all columns (via an aggregate),
         returning only 1 row.

    EXCLUDED :
      - Materializing the full dataset again on the receiver.
        (We only materialize a single aggregated row.)
    """
    register_input_duckdb(con, input_tbl)

    t0 = time.perf_counter()

    # 1) producer materialization
    out_tbl = con.execute("SELECT * FROM input_tbl").arrow()

    # 2) receiver ingest
    ctx.register_view("s1", ctx.from_arrow(out_tbl))

    # 3) receiver full read WITHOUT materializing full result:
    scan_sql = build_full_scan_agg_sql("s1", out_tbl.schema.names)
    _ = ctx.sql(scan_sql).collect()  # small (1 row)

    ctx.deregister_table("s1")

    t1 = time.perf_counter()

    con.unregister("input_tbl")
    return (t1 - t0) * 1000.0



def transfer_datafusion_to_duckdb(
    con: duckdb.DuckDBPyConnection,
    ctx: SessionContext,
    input_tbl: pa.Table,
) -> float:
    """
    Measures end-to-end transfer cost for DataFusion -> DuckDB:

    INCLUDED:
      1) DataFusion executes SELECT * and materializes to Arrow (collect + from_batches).
      2) DuckDB registers the Arrow table.
      3) DuckDB performs a full scan of all columns (via an aggregate),
         returning only 1 row.

    EXCLUDED:
      - Materializing the full dataset again on the receiver.
        (We only materialize a single aggregated row.)
    """
    register_input_datafusion(ctx, input_tbl)

    t0 = time.perf_counter()

    # 1) producer materialization (DataFusion -> Arrow Table)
    df = ctx.sql("SELECT * FROM input_tbl")
    batches = df.collect()
    out_tbl = pa.Table.from_batches(batches)

    # 2) receiver ingest
    con.register("s1", out_tbl)

    # 3) receiver full read WITHOUT materializing full result:
    scan_sql = build_full_scan_agg_sql("s1", out_tbl.schema.names)
    _ = con.execute(scan_sql).fetchall()  # small (1 row)

    con.unregister("s1")

    t1 = time.perf_counter()

    ctx.deregister_table("input_tbl")
    return (t1 - t0) * 1000.0



# Plots

def save_plots(df_med: pd.DataFrame, plots_dir: Path) -> None:
    plots_dir.mkdir(parents=True, exist_ok=True)

    for direction in ["duckdb_to_datafusion", "datafusion_to_duckdb"]:
        sub = df_med[df_med["direction"] == direction].copy()
        plt.figure()
        plt.scatter(sub["bytes_feature"], sub["transfer_ms_median"], s=10)
        plt.xscale("log")
        plt.yscale("log")
        plt.xlabel("bytes_feature = rows * row_size_bytes")
        plt.ylabel("transfer_ms (median)")
        plt.title(f"Cross-engine transfer scaling ({direction})")
        plt.tight_layout()
        plt.savefig(plots_dir / f"scatter_bytes_vs_time_{direction}.png", dpi=200)
        plt.close()


# Main

def main() -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--trials", type=int, required=True)

    ap.add_argument("--rows-min", type=int, required=True)
    ap.add_argument("--rows-max", type=int, required=True)

    ap.add_argument("--row-size-min", type=int, required=True)
    ap.add_argument("--row-size-max", type=int, required=True)

    ap.add_argument("--sampling", choices=["grid", "lhs"], required=True)

    # grid sampling
    ap.add_argument("--rows-steps", type=int, default=None)
    ap.add_argument("--row-size-steps", type=int, default=None)

    # lhs sampling
    ap.add_argument("--n-pairs", type=int, default=None)

    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)

    if args.sampling == "grid":
        if args.rows_steps is None or args.row_size_steps is None:
            raise ValueError("grid sampling requires --rows-steps and --row-size-steps")
        pairs = generate_pairs_grid(
            args.rows_min, args.rows_max, args.rows_steps,
            args.row_size_min, args.row_size_max, args.row_size_steps,
        )
    else:
        if args.n_pairs is None:
            raise ValueError("lhs sampling requires --n-pairs")
        pairs = generate_pairs_lhs(
            rng,
            args.rows_min, args.rows_max,
            args.row_size_min, args.row_size_max,
            args.n_pairs,
        )

    num_pairs = len(pairs)
    num_directions = 2
    total_measurements = num_pairs * args.trials * num_directions

    max_bytes_feature = max(float(r) * float(s) for (r, s) in pairs)

    print("=== Dataset plan ===")
    print(f"sampling          : {args.sampling}")
    print(f"unique pairs      : {num_pairs}")
    print(f"trials per pair   : {args.trials}")
    print(f"directions        : {num_directions} (duckdb_to_datafusion, datafusion_to_duckdb)")
    print(f"total measurements: {total_measurements}")
    print(f"max rows*row_size : {max_bytes_feature:.0f} bytes")
    print("====================")

    if args.dry_run:
        return

    con = setup_duckdb()
    ctx = setup_datafusion()

    records: list[dict] = []
    directions = ["duckdb_to_datafusion", "datafusion_to_duckdb"]

    for direction in directions:
        print(f"\n=== Measuring direction={direction} ===")
        for (rows, row_size) in pairs:
            tbl = make_table(rows, row_size)
            bytes_feature = float(rows) * float(row_size)

            for t in range(args.trials):
                if direction == "duckdb_to_datafusion":
                    ms = transfer_duckdb_to_datafusion(con, ctx, tbl)
                else:
                    ms = transfer_datafusion_to_duckdb(con, ctx, tbl)

                records.append(
                    {
                        "direction": direction,
                        "rows": rows,
                        "row_size_bytes": row_size,
                        "bytes_feature": bytes_feature,
                        "trial": t,
                        "transfer_ms": ms,
                    }
                )

            print(f"rows={rows:>10} row_size={row_size:>6} bytes={bytes_feature:>14.0f} done")

    df_raw = pd.DataFrame.from_records(records)
    raw_path = out_dir / "transfer_raw.csv"
    df_raw.to_csv(raw_path, index=False)

    df_med = (
        df_raw.groupby(["direction", "rows", "row_size_bytes"], as_index=False)
        .agg(bytes_feature=("bytes_feature", "first"), transfer_ms_median=("transfer_ms", "median"))
    )
    med_path = out_dir / "transfer_median.csv"
    df_med.to_csv(med_path, index=False)

    plots_dir = out_dir / "plots"
    save_plots(df_med, plots_dir)

    print(f"\nWrote: {raw_path}")
    print(f"Wrote: {med_path}")
    print(f"Wrote plots in: {plots_dir}")


if __name__ == "__main__":
    main()
