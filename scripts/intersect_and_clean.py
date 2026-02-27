import os
import re
import shutil
from pathlib import Path

def get_numeric_prefixes(directory: str) -> set[str]:
    prefixes = set()
    for filename in os.listdir(directory):
        match = re.match(r'^(\d+)\.', filename)
        if match:
            prefixes.add(match.group(1))
    return prefixes

def clean_directory(directory: str, keep_set: set[str]) -> None:
    kept = 0
    deleted = 0
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            match = re.match(r'^(\d+)\.', filename)
            if match:
                num = match.group(1)
                if num in keep_set:
                    kept += 1
                else:
                    os.remove(os.path.join(directory, filename))
                    deleted += 1
    print(f"In directory: {directory}")
    print(f"Kept .json files: {kept}")
    print(f"Deleted .json files: {deleted}\n")

def _sql_numeric_prefix(filename: str) -> str | None:
    """
    Accepts:
      - '123.sql'
      - '123.anything.sql'
    Returns '123' or None.
    """
    if not filename.lower().endswith(".sql"):
        return None
    m = re.match(r'^(\d+)(?:\.|$)', filename)
    return m.group(1) if m else None

def clean_sql_directory(directory: str, keep_set: set[str]) -> None:
    """
    Delete .sql files in `directory` whose numeric prefix is not in keep_set.
    """
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)

    kept = 0
    deleted = 0
    skipped = 0

    for p in d.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() != ".sql":
            skipped += 1
            continue

        qid = _sql_numeric_prefix(p.name)
        if qid is None:
            skipped += 1
            continue

        if qid in keep_set:
            kept += 1
        else:
            p.unlink()
            deleted += 1

    print(f"In directory: {d}")
    print(f"Kept .sql files: {kept}")
    print(f"Deleted .sql files: {deleted}")
    print(f"Skipped non-matching/non-.sql: {skipped}\n")

def copy_common_queries(src_dir: str, dst_dir: str, keep_set: set[str]) -> None:
    src = Path(src_dir)
    dst = Path(dst_dir)
    dst.mkdir(parents=True, exist_ok=True)

    copied = 0
    skipped = 0

    for p in src.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() != ".sql":
            continue

        qid = _sql_numeric_prefix(p.name)
        if qid is None:
            skipped += 1
            continue

        if qid in keep_set:
            shutil.copy2(p, dst / p.name)
            copied += 1
        else:
            skipped += 1

    print(f"Copied common .sql files: {copied}")
    print(f"Skipped .sql files: {skipped}")
    print(f"Output directory: {dst}\n")


dir1 = "/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/outputs/zuvela/datafusion_outputs"
dir2 = "/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/outputs/zuvela/duckdb_outputs"

queries_src = "/Users/sba/databases/so_dba/sqlstorm_filtered/"
queries_dst = "/Users/sba/Desktop/MasterThesis/flexdata-distributed-execution/outputs/zuvela/common_queries/"

# Get numeric prefixes
nums1 = get_numeric_prefixes(dir1)
nums2 = get_numeric_prefixes(dir2)

# Find intersection
common = nums1 & nums2
print(f"Common query IDs: {len(common)}\n")

# Clean both JSON directories
clean_directory(dir1, common)
clean_directory(dir2, common)

# Clean destination SQL directory, then copy matching queries into it
clean_sql_directory(queries_dst, common)
copy_common_queries(queries_src, queries_dst, common)
