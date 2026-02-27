from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Optional

import time

import duckdb
import pyarrow as pa
from datafusion import SessionContext


def _sql_str(val: str) -> str:
    """
    Quote a Python string as a DuckDB SQL string literal.
    Escapes single quotes by doubling them.
    """
    return "'" + val.replace("'", "''") + "'"


def _duckdb_mem_str_from_mb(mem_mb: int) -> str:
    return f"{int(mem_mb)}MB"


def _datafusion_mem_str_from_mb(mem_mb: int) -> str:
    return f"{int(mem_mb)}M"


def register_parquet_views_simple(con: duckdb.DuckDBPyConnection, dirpath: str | Path) -> None:
    """
    """
    p = Path(dirpath)
    if not p.exists():
        raise FileNotFoundError(f"parquet dir not found: {dirpath}")
    for f in sorted(p.glob("*.parquet")):
        view = f.stem  # customers.parquet -> customers
        path_lit = _sql_str(str(f))
        con.execute(
            f'CREATE OR REPLACE TEMP VIEW "{view}" AS '
            f"SELECT * FROM read_parquet({path_lit});"
        )


def setup_duckdb(parquet_dir: str | Path, *, mem_mb: Optional[int] = None) -> duckdb.DuckDBPyConnection:
    """
    Set up DuckDB in-memory and register Parquet files as temp views.
    """
    con = duckdb.connect(":memory:")

    # Allow Arrow to use 64-bit offsets for strings
    con.execute("SET arrow_large_buffer_size=true")

    if mem_mb is not None:
        con.execute(f"SET memory_limit = '{_duckdb_mem_str_from_mb(mem_mb)}'")

    register_parquet_views_simple(con, parquet_dir)
    return con


def setup_datafusion(parquet_dir: str | Path, *, mem_mb: Optional[int] = None) -> SessionContext:
    """
    Set up DataFusion session and register Parquet files as tables.
    """
    parquet_dir = Path(parquet_dir)
    ctx = SessionContext()

    if mem_mb is not None:
        ctx.sql(f"SET datafusion.runtime.memory_limit = '{_datafusion_mem_str_from_mb(mem_mb)}'").collect()

    for entry in parquet_dir.iterdir():
        if entry.is_file() and entry.suffix.lower() == ".parquet":
            table_name = entry.stem
            ctx.register_parquet(table_name, str(entry))

    return ctx


def setup_engines(
    parquet_dir: str | Path,
    *,
    engine_mem_mb: Optional[int] = None,
) -> Tuple[duckdb.DuckDBPyConnection, SessionContext]:
    """
    Convenience: set up both engines using the same Parquet directory.
    """
    con = setup_duckdb(parquet_dir, mem_mb=engine_mem_mb)
    ctx = setup_datafusion(parquet_dir, mem_mb=engine_mem_mb)
    return con, ctx


def run_whole_query_duckdb(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[pa.Table, float]:
    t0 = time.perf_counter()
    tbl = con.execute(sql).arrow()
    return tbl, time.perf_counter() - t0


def run_whole_query_datafusion(ctx: SessionContext, sql: str) -> tuple[pa.Table, float]:
    t0 = time.perf_counter()
    df = ctx.sql(sql)

    batches = df.collect()
    t1 = time.perf_counter()

    if not batches:
        tbl = pa.table({})
        return tbl, (t1 - t0)

    batches, schema = _unify_batch_nullability(batches)

    t2 = time.perf_counter()
    tbl = pa.Table.from_batches(batches, schema=schema)
    t3 = time.perf_counter()

    return tbl, ((t1 - t0) + (t3 - t2))


def _unify_batch_nullability(
    batches: List[pa.RecordBatch],
) -> Tuple[List[pa.RecordBatch], pa.Schema]:
    if not batches:
        return batches, pa.schema([])

    schemas = [b.schema for b in batches]
    base = schemas[0]

    if all(s == base for s in schemas[1:]):
        return batches, base

    ncols = len(base)
    fields = []

    for i in range(ncols):
        f0 = base[i]
        name = f0.name
        typ = f0.type

        for s in schemas[1:]:
            if s[i].type != typ:
                raise ValueError(f"Type mismatch in column {name}: {typ} vs {s[i].type}")

        nullable = any(s[i].nullable for s in schemas)
        fields.append(pa.field(name, typ, nullable=nullable))

    target_schema = pa.schema(fields)
    cast_batches = [b.cast(target_schema) for b in batches]
    return cast_batches, target_schema
