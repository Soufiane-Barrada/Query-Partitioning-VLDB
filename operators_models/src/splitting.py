from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Set, Tuple, Optional

import numpy as np
import pandas as pd

INVALID_CLASS = 0


# parsing helpers

def parse_test_queries_txt(path: Path) -> Set[str]:
    """
    File format:
      14375.json
      13504.json
      ...
    Returns {"14375", "13504", ...} as strings.
    """
    if not path.exists():
        raise FileNotFoundError(f"test_queries.txt not found: {path}")

    out: Set[str] = set()
    for ln in path.read_text().splitlines():
        s = ln.strip()
        if not s:
            continue
        # must be like "<digits>.json" or "<digits>"
        m = re.search(r"(\d+)", s)
        if not m:
            raise ValueError(f"Unparseable line in test_queries.txt: {ln!r}")
        out.add(m.group(1))
    if not out:
        raise ValueError(f"Parsed {path} but got 0 query ids.")
    return out


def parse_failed_queries_from_report(txt_path: Path) -> Set[str]:
    """
    Parses the 'Failed queries:' section from the report and returns IDs as strings.
    """
    if not txt_path.exists():
        raise FileNotFoundError(f"Failed-queries report not found: {txt_path}")

    with open(txt_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    ids: Set[str] = set()
    in_failed_section = False

    for line in lines:
        clean_line = line.strip()

        # 1. Detect the start of the Failed queries section
        if clean_line.startswith("Failed queries:"):
            in_failed_section = True
            continue
        
        # 2. Detect the start of a different section to stop parsing
        if in_failed_section and clean_line and ":" in clean_line and not clean_line.startswith("-"):
            if "Failed queries" not in clean_line:
                break

        # 3. Extract IDs
        if in_failed_section:
            m_id = re.search(r"-?\s*(\d+)", clean_line)
            if m_id:
                ids.add(m_id.group(1))

    if not ids:
        raise ValueError(f"Found 'Failed queries' header in {txt_path.name} but zero IDs were parsed.")
    
    return ids


def normalize_query_id_series(s: pd.Series) -> pd.Series:
    """
    Returns a series of string query IDs.
    """
    if s.isna().any():
        raise ValueError("query_col contains NaN; cannot normalize query ids.")

    if np.issubdtype(s.dtype, np.number):
        # numeric ids must be integer-like
        arr = s.to_numpy()
        if not np.all(np.isfinite(arr)):
            raise ValueError("query_col contains non-finite numbers.")
        if not np.all(np.equal(np.mod(arr, 1), 0)):
            raise ValueError("query_col numeric but contains non-integer values.")
        return s.astype(np.int64).astype(str)

    ss = s.astype(str).str.strip()

    # common: "14375.json"
    m = ss.str.extract(r"(\d+)")[0]
    if m.isna().any():
        bad = ss[m.isna()].head(5).tolist()
        raise ValueError(f"query_col contains unparsable ids; examples: {bad}")
    return m




# artifact paths

def split_artifacts_dir(resources_dir: Path) -> Path:
    d = resources_dir / "split_artifacts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def edges_path(resources_dir: Path, engine: str, op: str) -> Path:
    return split_artifacts_dir(resources_dir) / f"edges_{engine}_{op}.json"


def split_npz_path(resources_dir: Path, engine: str, op: str) -> Path:
    return split_artifacts_dir(resources_dir) / f"split_{engine}_{op}.npz"


def split_report_path(resources_dir: Path, engine: str, op: str) -> Path:
    return split_artifacts_dir(resources_dir) / f"split_{engine}_{op}_report.json"




# binning

def compute_log_edges(y_seconds: np.ndarray, n_bins: int) -> np.ndarray:
    y = np.asarray(y_seconds, float)
    m = np.isfinite(y) & (y > 0)
    if not np.any(m):
        raise ValueError("No positive finite runtimes to compute log edges.")
    t = y[m]
    lo, hi = np.log10(t.min()), np.log10(t.max())
    if hi - lo < 1e-9:
        lo -= 0.15
        hi += 0.15
    edges_log = np.linspace(lo, hi, int(n_bins) + 1)
    edges = 10.0 ** edges_log
    if not np.all(np.diff(edges) > 0):
        raise ValueError("Computed edges are not strictly increasing.")
    return edges


def load_edges(resources_dir: Path, engine: str, op: str, *, n_bins: int) -> Optional[np.ndarray]:
    p = edges_path(resources_dir, engine, op)
    if not p.exists():
        return None
    obj = json.loads(p.read_text())
    edges = np.asarray(obj["edges"], float)

    if int(obj.get("n_bins", len(edges) - 1)) != int(n_bins):
        raise ValueError(f"Edges file {p} has n_bins={obj.get('n_bins')} but config requires n_bins={n_bins}")

    if edges.ndim != 1 or edges.size != (int(n_bins) + 1):
        raise ValueError(f"Bad edges shape in {p}: got {edges.shape}, expected {(n_bins+1,)}")

    if not np.all(np.isfinite(edges)) or not np.all(np.diff(edges) > 0):
        raise ValueError(f"Edges in {p} are not strictly increasing finite values.")

    return edges


def save_edges(resources_dir: Path, engine: str, op: str, edges: np.ndarray, *, n_bins: int) -> Path:
    p = edges_path(resources_dir, engine, op)
    payload = {
        "engine": engine,
        "op": op,
        "n_bins": int(n_bins),
        "unit": "seconds",
        "edges": [float(x) for x in edges],
    }
    p.write_text(json.dumps(payload, indent=2))
    return p


def get_or_create_edges(resources_dir: Path, engine: str, op: str, y_seconds: np.ndarray, *, n_bins: int) -> Tuple[np.ndarray, Path]:
    edges = load_edges(resources_dir, engine, op, n_bins=n_bins)
    if edges is not None:
        return edges, edges_path(resources_dir, engine, op)
    edges = compute_log_edges(y_seconds, n_bins=n_bins)
    p = save_edges(resources_dir, engine, op, edges, n_bins=n_bins)
    return edges, p


def assign_bins(y_seconds: np.ndarray, edges: np.ndarray, *, invalid_class: int = INVALID_CLASS) -> np.ndarray:
    y = np.asarray(y_seconds, float)
    bins = np.full(y.shape, int(invalid_class), dtype=int)
    m = np.isfinite(y) & (y > 0)
    if np.any(m):
        cls = np.digitize(y[m], edges, right=False)
        cls = np.clip(cls, 1, len(edges) - 1)
        bins[m] = cls
    return bins




# splitting

@dataclass
class SplitReport:
    engine: str
    op: str
    splitter: str
    n_bins: int
    val_size: float
    test_size: float
    seed: int
    edges_file: str

    # exclusions
    test_queries_txt: str
    excluded_test_queries: List[str]
    excluded_rows_count: int

    # bucketed only
    failed_queries_txt: str | None
    required_train_queries_missing_after_exclusion: List[str]

    violations: List[str]
    bucket_summary: List[dict]


def _shuffle(idx: np.ndarray, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    out = np.asarray(idx, int).copy()
    rng.shuffle(out)
    return out


def split_random_three_way(n: int, *, val_size: float, test_size: float, seed: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    if n <= 0:
        return np.array([], int), np.array([], int), np.array([], int)
    if val_size <= 0 or test_size <= 0 or (val_size + test_size) >= 0.9:
        raise ValueError("Choose reasonable val_size and test_size (both >0, sum < 0.9).")

    all_idx = np.arange(n, dtype=int)
    rng = np.random.default_rng(seed)
    rng.shuffle(all_idx)

    n_val = int(round(n * float(val_size)))
    n_test = int(round(n * float(test_size)))
    n_val = max(1, min(n - 2, n_val))
    n_test = max(1, min(n - n_val - 1, n_test))

    va = all_idx[:n_val]
    te = all_idx[n_val:n_val + n_test]
    tr = all_idx[n_val + n_test:]
    return np.sort(tr), np.sort(va), np.sort(te)


def _alloc_counts(total: int, train_frac: float, val_frac: float, test_frac: float,
                  minima: Tuple[int, int, int]) -> Tuple[int, int, int]:
    if total <= 0:
        return 0, 0, 0

    targets = np.array([train_frac, val_frac, test_frac], float) * float(total)
    base = np.floor(targets).astype(int)
    rem = total - int(base.sum())

    frac = targets - base
    order = np.argsort(-frac)
    for i in range(rem):
        base[order[i % 3]] += 1

    base = np.maximum(base, np.array(minima, int))

    while int(base.sum()) > total:
        for j in [0, 1, 2]:
            if base[j] > minima[j]:
                base[j] -= 1
                break
        else:
            break

    while int(base.sum()) < total:
        base[0] += 1

    return int(base[0]), int(base[1]), int(base[2])


def split_bucketed_three_way(
    df: pd.DataFrame,
    y_seconds: np.ndarray,
    edges: np.ndarray,
    *,
    query_col: str,
    required_train_queries: Set[str],
    val_size: float,
    test_size: float,
    seed: int,
    invalid_class: int = INVALID_CLASS,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, List[dict], List[str], List[str]]:
    """
    Rules per bucket:
      - size 1 -> test (unless forced to train)
      - size 2 -> one train, one test (unless forced)
      - size >=3 -> try to have at least 1 train/val/test presence and then proportional counts
    """
    n = len(df)
    if n == 0:
        return np.array([], int), np.array([], int), np.array([], int), [], [], []

    train_frac = 1.0 - float(val_size) - float(test_size)
    if train_frac <= 0:
        raise ValueError("val_size + test_size must be < 1.0")

    if query_col not in df.columns:
        raise KeyError(f"query_col='{query_col}' missing in df.")

    qvals = normalize_query_id_series(df[query_col]).to_numpy()
    present_queries = set(qvals.tolist())

    missing_required = sorted(list(required_train_queries - present_queries))
    required_train_queries = set(required_train_queries) & present_queries  # keep only those that exist

    bins = assign_bins(y_seconds, edges, invalid_class=invalid_class)
    n_bins = len(edges) - 1

    req_mask = np.isin(qvals, np.array(list(required_train_queries), dtype=object))
    req_idx = np.where(req_mask)[0]

    train_set = set(int(i) for i in req_idx)
    val_set: set[int] = set()
    test_set: set[int] = set()
    violations: List[str] = []

    for b in range(1, n_bins + 1):
        idx_b = np.where(bins == b)[0]
        total = int(len(idx_b))
        if total == 0:
            continue

        mandatory = np.array([i for i in idx_b if i in train_set], dtype=int)
        rem = np.array([i for i in idx_b if i not in train_set], dtype=int)
        rem = _shuffle(rem, seed + 1000 * b)

        if total == 1:
            if len(mandatory) == 0:
                test_set.add(int(idx_b[0]))
            continue

        if total == 2:
            if len(mandatory) == 2:
                continue
            if len(mandatory) == 1:
                test_set.add(int(rem[0]))
            else:
                train_set.add(int(rem[0]))
                test_set.add(int(rem[1]))
            continue

        # total >= 3
        minima = (1, 1, 1)

        available_for_non_train = total - len(mandatory)
        if available_for_non_train < 2:
            violations.append(f"bucket {b}: only {available_for_non_train} non-mandatory left -> cannot guarantee val+test presence")
            if rem.size >= 1:
                test_set.add(int(rem[0]))
            if rem.size >= 2:
                val_set.add(int(rem[1]))
            for i in rem[2:]:
                train_set.add(int(i))
            continue

        tr_c, va_c, te_c = _alloc_counts(total, train_frac, val_size, test_size, minima=minima)

        if len(mandatory) > tr_c:
            overflow = len(mandatory) - tr_c
            tr_c = len(mandatory)
            for _ in range(overflow):
                if va_c > 1:
                    va_c -= 1
                elif te_c > 1:
                    te_c -= 1
                else:
                    violations.append(f"bucket {b}: mandatory={len(mandatory)} forces shrinking below minima; representation may fail")
                    break

        need_test = te_c
        need_val = va_c
        need_train_from_rem = max(0, tr_c - len(mandatory))

        pos = 0
        for _ in range(need_test):
            if pos < len(rem):
                test_set.add(int(rem[pos])); pos += 1
        for _ in range(need_val):
            if pos < len(rem):
                val_set.add(int(rem[pos])); pos += 1
        for _ in range(need_train_from_rem):
            if pos < len(rem):
                train_set.add(int(rem[pos])); pos += 1

        for i in rem[pos:]:
            train_set.add(int(i))

        got_tr = any(i in train_set for i in idx_b)
        got_va = any(i in val_set for i in idx_b)
        got_te = any(i in test_set for i in idx_b)
        if not (got_tr and got_va and got_te):
            violations.append(f"bucket {b}: missing split presence (train={got_tr}, val={got_va}, test={got_te})")

    # invalid class: assign proportionally (excluding already assigned)
    idx_inv = np.where(bins == invalid_class)[0]
    if len(idx_inv):
        inv = _shuffle(idx_inv, seed + 999999)
        inv = np.array([i for i in inv if (i not in train_set and i not in val_set and i not in test_set)], dtype=int)
        total = len(inv)
        if total:
            tr_c, va_c, te_c = _alloc_counts(total, train_frac, val_size, test_size, minima=(0, 0, 0))
            pos = 0
            for i in inv[pos:pos + te_c]:
                test_set.add(int(i))
            pos += te_c
            for i in inv[pos:pos + va_c]:
                val_set.add(int(i))
            pos += va_c
            for i in inv[pos:]:
                train_set.add(int(i))

    tr = np.array(sorted(train_set), dtype=int)
    va = np.array(sorted(val_set), dtype=int)
    te = np.array(sorted(test_set), dtype=int)

    # strict invariants
    if (np.intersect1d(tr, va).size or np.intersect1d(tr, te).size or np.intersect1d(va, te).size):
        raise RuntimeError("Split sets are not disjoint (bug).")

    covered = len(tr) + len(va) + len(te)
    if covered != n:
        missing = np.setdiff1d(np.arange(n), np.concatenate([tr, va, te]))
        raise RuntimeError(f"Missing split assignments: missing={missing.size}, first={missing[:20]}")

    # required-train rows check (only for those that exist after exclusion)
    if req_idx.size and not np.all(np.isin(req_idx, tr)):
        bad = req_idx[~np.isin(req_idx, tr)]
        raise RuntimeError(f"Some required-train rows not in train (bug). Example idx: {bad[:10]}")

    # summary
    summary = []
    for b in range(invalid_class, n_bins + 1):
        idx_b = np.where(bins == b)[0]
        if len(idx_b) == 0:
            continue
        summary.append({
            "bucket": int(b),
            "count": int(len(idx_b)),
            "train": int(np.sum(np.isin(idx_b, tr))),
            "val":   int(np.sum(np.isin(idx_b, va))),
            "test":  int(np.sum(np.isin(idx_b, te))),
        })

    return tr, va, te, summary, violations, missing_required


def write_split_artifacts(
    resources_dir: Path,
    engine: str,
    op: str,
    *,
    tr: np.ndarray,
    va: np.ndarray,
    te: np.ndarray,
    report: SplitReport,
) -> None:
    np.savez(split_npz_path(resources_dir, engine, op), train=tr, val=va, test=te)
    split_report_path(resources_dir, engine, op).write_text(json.dumps(report.__dict__, indent=2))
