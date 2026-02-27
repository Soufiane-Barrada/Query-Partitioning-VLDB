from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb

# db_path = "/proj/data-integration/distributed_execution/so_12GB_parquet"
# output_path = "/proj/data-integration/distributed_execution/so_dataset_stats/tables_states_LLM.txt"

# db_path = "/Users/sba/databases/job_dba/imdb_parquet"
# output_path = "/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/python/resources/LLM_job_prompt_infos.txt"

# db_path = "/Users/sba/databases/tpch10_dba/tpch_parquet_10gb"
# output_path = "/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/python/resources/LLM_tpch10_prompt_infos.txt"

db_path = "/Users/sba/databases/tpch1_dba/tpch_parquet_1gb"
output_path = "/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/python/resources/LLM_tpch1_prompt_infos.txt"

def _sql_str(val: str) -> str:
    return "'" + val.replace("'", "''") + "'"


def register_parquet_views_simple(con: duckdb.DuckDBPyConnection, parquet_dir: str | Path) -> None:
    p = Path(parquet_dir)
    if not p.is_dir():
        raise FileNotFoundError(f"parquet_dir is not a directory: {p}")

    for f in sorted(p.glob("*.parquet")):
        view = f.stem
        path_lit = _sql_str(str(f))
        con.execute(
            f'CREATE OR REPLACE TEMP VIEW "{view}" AS '
            f"SELECT * FROM read_parquet({path_lit});"
        )



def setup_duckdb(parquet_dir: str | Path, *, mem_mb: Optional[int] = None) -> duckdb.DuckDBPyConnection:
    """
    Set up DuckDB in-memory and register Parquet files as temp views.
    Optionally set DuckDB's internal memory_limit (buffer manager).
    """
    con = duckdb.connect(":memory:")

    # Allow Arrow to use 64-bit offsets for strings (LargeUtf8)
    con.execute("SET arrow_large_buffer_size=true")

    register_parquet_views_simple(con, parquet_dir)
    return con



con = setup_duckdb(db_path)

# Get all tables
tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]

lines = []

for table in tables:
    # Row count
    row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    lines.append(f"\n=== Table: {table} ===")
    lines.append(f"Row count: {row_count}")

    # Get columns and types
    col_info = con.execute(f"PRAGMA table_info('{table}')").fetchdf()

    for _, row in col_info.iterrows():
        col = row["name"]
        col_type = row["type"].upper()

        # Always compute counts
        stats = con.execute(f"""
            SELECT 
                COUNT(*) AS total_rows,
                COUNT(*) - COUNT({col}) AS null_count,
                COUNT(DISTINCT {col}) AS distinct_count
            FROM {table}
        """).fetchdf().iloc[0]

        # Decide whether to include min/max
        if any(t in col_type for t in ["INT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC", "FLOAT", "DATE", "TIME", "TIMESTAMP"]):
            extra = con.execute(f"""
                SELECT MIN({col}) AS min_value, MAX({col}) AS max_value
                FROM {table}
            """).fetchdf().iloc[0]
            lines.append(
                f"  Column: {col} | type={col_type} | distinct={stats['distinct_count']} "
                f"| nulls={stats['null_count']} | min={extra['min_value']} | max={extra['max_value']}"
            )
        else:
            lines.append(
                f"  Column: {col} | type={col_type} | distinct={stats['distinct_count']} | nulls={stats['null_count']}"
            )

# Save all metrics to text
with open(output_path, "w") as f:
    f.write("\n".join(lines))

con.close()
print(f"Saved detailed metrics to {output_path}")