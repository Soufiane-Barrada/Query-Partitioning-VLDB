import json, pathlib, os, tempfile, re
from typing import Any, Dict, List, Optional, Union, Set
import duckdb
import pyarrow.parquet as pq
import pyarrow as pa

# Operator normalization
_NORMALIZE_OP = {
    "SEQ_SCAN": "Scan",
    "TABLE_SCAN": "Scan",
    "COLUMN_DATA_SCAN": "Scan",
    "DELIM_SCAN": "Scan",
    "DUMMY_SCAN": "Scan",
    "PROJECTION": "Project",
    "FILTER": "Filter",
    "HASH_JOIN": "Join",
    "LEFT_DELIM_JOIN": "Join",
    "RIGHT_DELIM_JOIN": "Join",
    "PIECEWISE_MERGE_JOIN": "Join",
    "STREAMING_MERGE_JOIN": "Join",
    "IE_JOIN": "Join",
    "BLOCKWISE_NL_JOIN": "Join",
    "CROSS_PRODUCT": "Join",
    "HASH_GROUP_BY": "Aggregate",
    "AGGREGATE": "Aggregate",
    "PERFECT_HASH_GROUP_BY": "Aggregate",
    "ORDER_BY": "Sort",
    "SORT": "Sort",
    "LIMIT": "Limit",
    "STREAMING_LIMIT": "Limit",
    "WINDOW": "Window",
    "UNNEST": "Unnest",
    "CTE": "CTE",
    "CTE_SCAN": "CTE_SCAN",
}


def _norm_op(op: str) -> str:
    if not op:
        return "Other"
    k = op.upper().strip().replace(" ", "_").rstrip("_")
    return _NORMALIZE_OP.get(k, _NORMALIZE_OP.get(k.split("(")[0], "Other"))


def _ms(x: Optional[float]) -> Optional[float]:
    return None if x is None else float(x) * 1000.0


# parquet catalogs
_TABLE_COLS: Dict[str, Set[str]] = {}
_TABLE_ROWS: Dict[str, int] = {}


def _init_table_cols(parquet_dir: Optional[str]) -> None:
    """Populate _TABLE_COLS and _TABLE_ROWS from <parquet_dir>/*.parquet using PyArrow."""
    _TABLE_COLS.clear()
    _TABLE_ROWS.clear()
    if not parquet_dir:
        return
    base = pathlib.Path(parquet_dir)
    for f in base.glob("*.parquet"):
        table = f.stem.lower()
        pf = pq.ParquetFile(str(f))
        schema = pf.schema_arrow
        cols = {field.name.lower() for field in schema}
        _TABLE_COLS[table] = cols
        # total physical rows in the file
        meta = pf.metadata
        total_rows = 0
        for i in range(meta.num_row_groups):
            total_rows += meta.row_group(i).num_rows
        _TABLE_ROWS[table] = int(total_rows)


def _infer_table_from_columns(cols: List[str], est_rows: Optional[int]) -> Optional[str]:
    """
    Return the table whose columns are a superset of the projected cols.
    If multiple candidates, choose the one with total rows closest to est_rows.
    If est_rows is None or a strict tie remains, return None.
    """
    if not cols:
        return None
    wanted = {c.lower() for c in cols}
    cands = [t for t, tcols in _TABLE_COLS.items() if wanted.issubset(tcols)]
    if len(cands) == 1:
        return cands[0]
    if len(cands) > 1 and est_rows is not None:
        diffs = sorted(((abs(_TABLE_ROWS.get(t, 0) - est_rows), t) for t in cands))
        best_diff, best_tbl = diffs[0]
        if len(diffs) == 1:
            return best_tbl
        next_diff, _ = diffs[1]
        if best_diff < next_diff:
            return best_tbl
    return None


# Identifier parsing
_SQL_KW = {
    "select",
    "from",
    "where",
    "group",
    "by",
    "order",
    "limit",
    "offset",
    "join",
    "on",
    "and",
    "or",
    "not",
    "as",
    "case",
    "when",
    "then",
    "else",
    "end",
    "distinct",
    "all",
    "asc",
    "desc",
    "null",
    "is",
    "in",
    "between",
    "like",
    "exists",
    "true",
    "false",
    "with",
    "over",
    "partition",
    "rows",
    "range",
    "current",
    "row",
    "preceding",
    "following",
    "unbounded",
    "outer",
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "using",
    "union",
    "intersect",
    "except",
    "having",
    "nulls",
    "first",
    "last",
    "timestamp",
}
_INT_FN_PREF = "__internal_"
_SLOT_RE = re.compile(r"#(\d+)")


def _find_functions(expr: str) -> Set[str]:
    return {m.group(1).lower() for m in re.finditer(r"([A-Za-z_][A-Za-z0-9_\.]*)\s*\(", expr or "")}


def _tok(expr: str) -> List[str]:
    s = expr or ""
    s = re.sub(r"'.*?'", " ", s)
    s = re.sub(r'".*?"', " ", s)
    s = re.sub(r"`.*?`", " ", s)
    return re.findall(r"[A-Za-z_][A-Za-z0-9_\.]*", s)


def _slot_idxs(expr: str) -> List[int]:
    return [int(m.group(1)) for m in _SLOT_RE.finditer(expr or "")]


def _named_cols(expr: str) -> List[str]:
    if not expr:
        return []
    fnames = _find_functions(expr)
    out: List[str] = []
    for tok in _tok(expr):
        low = tok.lower()
        if low in _SQL_KW or low in fnames:
            continue
        if tok.startswith(_INT_FN_PREF):
            continue
        out.append(tok)
    seen = set()
    ret = []
    for c in out:
        if c not in seen:
            seen.add(c)
            ret.append(c)
    return ret


def _flat_lineage(child_slots: List[List[str]]) -> List[str]:
    seen = set()
    out = []
    for lst in child_slots or []:
        for c in lst or []:
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out


def _resolve_tokens_against_child(names: List[str], child_slots: List[List[str]]) -> List[str]:
    if not names:
        return []
    # flatten child lineage into a stable list of fully-qualified base columns
    base_cols = _flat_lineage(child_slots)
    # build tail -> [fully.qualified] map preserving order
    suffix_map: Dict[str, List[str]] = {}
    for bc in base_cols:
        tail = str(bc).split(".")[-1]
        suffix_map.setdefault(tail, []).append(bc)

    out: List[str] = []
    seen: set = set()
    for tok in names:
        tail = str(tok).lower().split(".")[-1]
        hits = suffix_map.get(tail, [])
        if len(hits) == 1:
            # emit the fully-qualified base col
            h = hits[0]
            if h not in seen:
                seen.add(h)
                out.append(h)
        elif len(hits) > 1:
            # if ambiguous: emit all fully-qualified candidates (not the raw token)
            for h in hits:
                if h not in seen:
                    seen.add(h)
                    out.append(h)
        # if no hits, emit nothing (don’t push the bare token)

    return out


def _resolve_expr(expr: str, child_slots: List[List[str]]) -> List[str]:
    """Calcite-like: resolve #n to child's output slots, plus resolve named tokens by suffix."""
    if not expr:
        return []
    cols: List[str] = []
    for idx in _slot_idxs(expr):
        if 0 <= idx < len(child_slots):
            cols.extend(child_slots[idx])
    cols.extend(_resolve_tokens_against_child(_named_cols(expr), child_slots))
    seen = set()
    out = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


#  Parse details from the "extra_info" field in the raw jsons
def _parse_details(op: str, extra: Any, raw_type: str, est_rows: Optional[int]) -> Dict[str, Any]:
    d: Dict[str, Any] = {}
    if isinstance(extra, dict):
        if op == "Project" and "Projections" in extra:
            xs = extra["Projections"]
            xs = xs if isinstance(xs, list) else [xs]
            d["project"] = {"expressions": [str(u) for u in xs]}

        if op == "Filter":
            expr = extra.get("Expression")
            if expr is not None:
                d["filter"] = {"predicates": [str(expr)]}

        if op == "Sort":
            keys = extra.get("Order By")
            if keys is not None:
                ks = keys if isinstance(keys, list) else [keys]
                d["sort"] = {
                    "keys": [
                        {"expr": str(k), "asc": ("DESC" not in str(k)), "nulls_first": False} for k in ks
                    ]
                }

        if op == "Aggregate":
            groups = extra.get("Groups")
            aggs = extra.get("Aggregates")
            grp = [str(g) for g in groups] if isinstance(groups, list) else ([groups] if groups else [])
            agg = [str(a) for a in aggs] if isinstance(aggs, list) else ([aggs] if aggs else [])
            algo = "perfect_hash" if (raw_type and "PERFECT_HASH_GROUP_BY" in raw_type.upper()) else None
            d["aggregate"] = {"group_keys": grp, "aggregates": agg, "algorithm": algo}

        if op == "Join":
            jt = extra.get("Join Type")
            join_type = str(jt) if jt is not None else None
            cond = extra.get("Conditions")
            if cond is None:
                cond = extra.get("Condition")
            if (not join_type) and ("CROSS_PRODUCT" in raw_type):
                join_type = "CROSS"
            d["join"] = {
                "join_type": join_type,
                "condition": str(cond) if cond is not None else None,
                "left_keys": [],
                "right_keys": [],
                # Keep the Delim Index (producer side)
                "delim_index": str(extra.get("Delim Index")) if extra.get("Delim Index") is not None else None,
            }

        if op == "Scan":
            projs = extra.get("Projections")
            cols = (
                [str(c) for c in projs]
                if isinstance(projs, list)
                else ([projs] if isinstance(projs, str) else [])
            )
            table = extra.get("Table") or ""
            if not table:
                table = _infer_table_from_columns(cols, est_rows)
            filters = extra.get("Filters")
            push = (
                [str(filters)]
                if isinstance(filters, str)
                else ([str(f) for f in filters] if isinstance(filters, list) else [])
            )
            d["scan"] = {
                "table": (str(table).lower() if table else ""),
                "columns": [c.lower() for c in cols],
                "pushdown_predicates": push,
                # Keep the Delim Index (consumer side — DELIM_SCAN)
                "delim_index": str(extra.get("Delim Index")) if extra.get("Delim Index") is not None else None,
            }

        if op == "Window":
            xs = extra.get("Projections")
            if xs is not None:
                xs = xs if isinstance(xs, list) else [xs]
                d["window"] = {"expressions": [str(u) for u in xs]}

        if op == "CTE":
            name = extra.get("CTE Name") or extra.get("Name")
            idx = extra.get("Table Index")
            idx = str(idx) if idx is not None else None
            d["cte"] = {"name": name, "index": idx}

        elif op == "CTE_SCAN":
            idx = extra.get("CTE Index") or extra.get("Table Index")
            idx = str(idx) if idx is not None else None
            d["cte_scan"] = {"index": idx}

    return d


#  Build raw nodes
def _build_node(n: Dict[str, Any], next_id: List[int], ops_set: set) -> Dict[str, Any]:
    raw_type = (n.get("operator_type") or n.get("operator_name") or "").upper().replace(" ", "_")
    node_id = next_id[0]
    next_id[0] += 1
    raw_name = n.get("operator_name") or n.get("name") or "Unknown"
    op = _norm_op(n.get("operator_type") or raw_name)
    ops_set.add(raw_name)

    elapsed_ms = _ms(n.get("operator_timing"))
    out_rows = n.get("operator_cardinality")
    details = _parse_details(op, n.get("extra_info"), raw_type or "", out_rows)
    children = [_build_node(c, next_id, ops_set) for c in n.get("children", [])]

    delim_deps = []
    if "DELIM_JOIN" in raw_type and len(children) >= 3:
        delim_deps = children[2:]  # dependency subtrees
        children = children[:2]  # true dataflow sides
        details.setdefault("join", {})["algorithm"] = "delim"

    rows_in = None
    if children:
        s = sum((c["metrics"].get("rows_out") or 0) for c in children)
        rows_in = s if s > 0 else None

    node = {
        "id": node_id,
        "op": op,
        "name": str(raw_name).strip(),
        "children": children,
        "metrics": {
            "elapsed_ms": elapsed_ms,
            "rows_in": rows_in,
            "rows_out": out_rows,
        },
        "columns": {"active": []},
        "details": details,
        "_slot_lineage": None,
    }

    if delim_deps:
        node["_deps"] = delim_deps  # side dependencies, not part of dataflow

    if op == "CTE" and children:
        prod = children[0]
        node["metrics"]["rows_in"] = prod["metrics"].get("rows_in")
        node["metrics"]["rows_out"] = prod["metrics"].get("rows_out")

    if op == "Join" and len(children) == 2:
        node["metrics"]["rows_in_left"] = children[0]["metrics"].get("rows_out")
        node["metrics"]["rows_in_right"] = children[1]["metrics"].get("rows_out")

    return node


# Calcite-like lineage
def _scan_slots(details: Dict[str, Any]) -> List[List[str]]:
    scan = details.get("scan") or {}
    cols = [str(c).lower() for c in (scan.get("columns") or [])]
    table = (scan.get("table") or "").split(".")[-1].lower()
    if table:
        return [[f"{table}.{c}"] for c in cols]
    return [[c] for c in cols]


def _project_slots(details: Dict[str, Any], child_slots: List[List[str]]) -> List[List[str]]:
    proj = (details.get("project") or {}).get("expressions") or []
    return [_resolve_expr(p, child_slots) for p in proj]


def _pass(child_slots: List[List[str]]) -> List[List[str]]:
    return child_slots


def _window_slots(details: Dict[str, Any], child_slots: List[List[str]]) -> List[List[str]]:
    exprs = (details.get("window") or {}).get("expressions") or []
    derived = [_resolve_expr(e, child_slots) for e in exprs]
    return child_slots + derived


def _aggregate_slots(details: Dict[str, Any], child_slots: List[List[str]]) -> List[List[str]]:
    agg = details.get("aggregate") or {}
    groups = agg.get("group_keys") or []
    aggs = agg.get("aggregates") or []
    out: List[List[str]] = []
    for g in groups:
        out.append(_resolve_expr(g, child_slots))
    for a in aggs:
        out.append(_resolve_expr(a, child_slots))
    return out


def _join_slots(left: List[List[str]], right: List[List[str]]) -> List[List[str]]:
    return left + right


def _infer_active_columns(op: str, details: Dict[str, Any], child_slots_or_merged: List[List[str]]) -> List[str]:
    exprs: List[str] = []
    if op == "Scan":
        s = details.get("scan") or {}
        cols = [str(c).lower() for c in (s.get("columns") or [])]
        tbl = (s.get("table") or "").split(".")[-1].lower()
        return [f"{tbl}.{c}" for c in cols]

    elif op == "Filter":
        for p in (details.get("filter") or {}).get("predicates") or []:
            exprs.append(str(p))
    elif op == "Aggregate":
        a = details.get("aggregate") or {}
        exprs += list(a.get("group_keys") or []) + list(a.get("aggregates") or [])
    elif op == "Sort":
        for k in (details.get("sort") or {}).get("keys") or []:
            exprs.append(str(k.get("expr")))
    elif op == "Join":
        j = details.get("join") or {}
        if j.get("condition"):
            exprs.append(str(j["condition"]))
        exprs += j.get("left_keys") or []
        exprs += j.get("right_keys") or []
    elif op == "Window":
        exprs += (details.get("window") or {}).get("expressions") or []
    cols: List[str] = []
    for e in exprs:
        cols.extend(_resolve_expr(e, child_slots_or_merged))
    seen = set()
    out: List[str] = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _compute_lineage(
    node: Dict[str, Any],
    _cte_env_stack: Optional[List[Dict[str, List[List[str]]]]] = None,
) -> List[List[str]]:
    if _cte_env_stack is None:
        _cte_env_stack = []

    def _lookup_cte_slots(cte_index: Optional[str]) -> List[List[str]]:
        if cte_index is not None:
            for frame in reversed(_cte_env_stack):
                if cte_index in frame:
                    return frame[cte_index]
        # fallback
        for frame in reversed(_cte_env_stack):
            if len(frame) == 1:
                return next(iter(frame.values()))
        return []

    op = node.get("op")
    kids = node.get("children", [])

    if op == "CTE":
        if not kids:
            node["_slot_lineage"] = []
            node.setdefault("columns", {})["active"] = []
            return []

        prod_slots = _compute_lineage(kids[0], _cte_env_stack)
        node.setdefault("details", {}).setdefault("cte", {})["producer_slots"] = prod_slots

        idx = (node.get("details", {}).get("cte") or {}).get("index")
        frame: Dict[str, List[List[str]]] = {}
        if idx is not None:
            frame[idx] = prod_slots
        else:
            frame["__implicit__"] = prod_slots

        _cte_env_stack.append(frame)

        for ch in kids[1:]:
            _compute_lineage(ch, _cte_env_stack)

        _cte_env_stack.pop()

        out_map = kids[-1].get("_slot_lineage") or prod_slots
        node["_slot_lineage"] = out_map
        node.setdefault("columns", {})["active"] = _flat_lineage(out_map)

        # compute lineage for dependency subtrees with current CTE env
        for dep in node.get("_deps", []):
            _compute_lineage(dep, _cte_env_stack)

        return out_map

    if op == "CTE_SCAN":
        scan_idx = (node.get("details", {}).get("cte_scan") or {}).get("index")
        bound = _lookup_cte_slots(scan_idx)
        node["_slot_lineage"] = bound
        node.setdefault("details", {}).setdefault("cte_scan", {})["bound_index"] = scan_idx
        node.setdefault("columns", {})["active"] = _flat_lineage(bound)

        # Also do deps in the same environment
        for dep in node.get("_deps", []):
            _compute_lineage(dep, _cte_env_stack)

        return bound

    if op == "Join" and len(kids) == 2:
        left_map = _compute_lineage(kids[0], _cte_env_stack)
        right_map = _compute_lineage(kids[1], _cte_env_stack)
        merged = left_map + right_map
        node["columns"]["active"] = _infer_active_columns(op, node.get("details", {}), merged)
        out_map = _join_slots(left_map, right_map)
    else:
        child_map = _compute_lineage(kids[0], _cte_env_stack) if kids else []
        node["columns"]["active"] = _infer_active_columns(op, node.get("details", {}), child_map)

        if op == "Scan":
            out_map = _scan_slots(node.get("details", {}))
        elif op == "Project":
            out_map = _project_slots(node.get("details", {}), child_map)
        elif op == "Filter":
            out_map = _pass(child_map)
        elif op == "Sort":
            out_map = _pass(child_map)
        elif op == "Limit":
            out_map = _pass(child_map)
        elif op == "Window":
            out_map = _window_slots(node.get("details", {}), child_map)
        elif op == "Aggregate":
            out_map = _aggregate_slots(node.get("details", {}), child_map)
        else:
            out_map = _pass(child_map)

    node["_slot_lineage"] = out_map

    for dep in node.get("_deps", []):
        _compute_lineage(dep, _cte_env_stack)

    return out_map


def apply_delim_registry(root: Dict[str, Any], delim_registry: Dict[str, Dict[str, Any]]) -> None:
    """
    Set DELIM_SCAN rows_out from registry and recompute rows_in bottom-up.
    For joins, also refresh rows_in_left/right.
    """

    def _walk(node: Dict[str, Any]) -> None:
        for ch in node.get("children", []):
            _walk(ch)

        # Set rows_out on DELIM_SCAN if registry has it
        if node.get("op") == "Scan" and node.get("name", "").upper() == "DELIM_SCAN":
            idx = ((node.get("details", {}) or {}).get("scan") or {}).get("delim_index")
            if idx and idx in delim_registry and delim_registry[idx].get("rows", 0):
                node.setdefault("metrics", {})["rows_out"] = int(delim_registry[idx]["rows"])

        # Recompute rows_in = sum(children rows_out) when children exist
        kids = node.get("children", [])
        if kids:
            s = sum(((k.get("metrics", {}) or {}).get("rows_out") or 0) for k in kids)
            if s > 0:
                node.setdefault("metrics", {})["rows_in"] = int(s)

        # For joins, also refresh per-side inputs
        if node.get("op") == "Join" and len(kids) == 2:
            l = (kids[0].get("metrics", {}) or {}).get("rows_out") or 0
            r = (kids[1].get("metrics", {}) or {}).get("rows_out") or 0
            node.setdefault("metrics", {})["rows_in_left"] = int(l)
            node.setdefault("metrics", {})["rows_in_right"] = int(r)

    _walk(root)


def _first_aggregate(node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the first Aggregate node under 'node', else None."""
    if node.get("op") == "Aggregate":
        return node
    for ch in node.get("children", []):
        found = _first_aggregate(ch)
        if found:
            return found
    return None


def build_delim_registry(root: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build: delim_index -> {"rows": int, "key_lineage": List[List[str]]}
    - rows: output rows of the dependency pipeline (typically first Aggregate)
    - key_lineage: base-column lineage for group keys (used for sizing)
    """
    registry: Dict[str, Dict[str, Any]] = {}

    def _collect(node: Dict[str, Any]) -> None:
        for ch in node.get("children", []):
            _collect(ch)
        jdet = (node.get("details", {}) or {}).get("join") or {}
        delim_idx = jdet.get("delim_index")
        if node.get("op") == "Join" and delim_idx:
            deps = node.get("_deps", [])
            if not deps:
                return
            dep_root = deps[0]
            agg = _first_aggregate(dep_root) or dep_root
            # prefer aggregate's rows_out; else fallback to dep_root's rows_out; else 0
            rows = (
                (agg.get("metrics", {}) or {}).get("rows_out")
                or (dep_root.get("metrics", {}) or {}).get("rows_out")
                or 0
            )

            key_lineage: List[List[str]] = []
            if agg.get("_slot_lineage"):
                groups = (agg.get("details", {}).get("aggregate") or {}).get("group_keys") or []
                # group keys come first in our _aggregate_slots
                key_lineage = (agg["_slot_lineage"] or [])[: len(groups)]

            registry[str(delim_idx)] = {"rows": int(rows), "key_lineage": key_lineage}

    _collect(root)
    return registry


# API
def collect_unified(
    sql: Union[str, pathlib.Path],
    *,
    connection: Optional["duckdb.DuckDBPyConnection"] = None,
    raw_profile_out: Union[str, pathlib.Path],
    raw_plan_only_out: Optional[Union[str, pathlib.Path]] = None,
    disable_topn_rule: bool = True,
    parquet_dir: Optional[str] = None,
    ops_set: set,
) -> Dict[str, Any]:
    # read SQL (path or text)
    is_path = isinstance(sql, (str, pathlib.Path)) and str(sql).lower().endswith(".sql")
    sql_text = pathlib.Path(sql).read_text() if is_path else str(sql)
    query_name = pathlib.Path(sql).name if is_path else "<inline>"

    # initialize parquet catalogs (schema + row counts)
    _init_table_cols(parquet_dir)

    # profile file path
    raw_profile_out = pathlib.Path(raw_profile_out)
    os.makedirs(raw_profile_out.parent, exist_ok=True)

    # run (simple, file-based profiling)
    con = connection or duckdb.connect()
    if disable_topn_rule:
        con.execute("SET disabled_optimizers='top_n'")
    con.execute("SET enable_profiling='json'")
    con.execute(f"SET profiling_output='{str(raw_profile_out.resolve())}'")
    res = con.execute(sql_text)
    if res.description is not None:
        _ = res.fetchall()
    con.execute("SET enable_profiling='no_output'")
    if disable_topn_rule:
        con.execute("SET disabled_optimizers=''")
    if connection is None:
        con.close()

    # read the profile JSON (DuckDB writes exactly to profiling_output)
    prof = json.loads(raw_profile_out.read_text())

    # optionally write raw plan-only JSON
    if raw_plan_only_out is not None:
        first = prof["children"][0] if prof.get("children") else {}
        p = pathlib.Path(raw_plan_only_out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(first, indent=2))

    # build tree
    next_id = [0]
    if len(prof.get("children", [])) == 1:
        root = _build_node(prof["children"][0], next_id, ops_set)
    else:
        kids = [_build_node(c, next_id, ops_set) for c in prof.get("children", [])]
        nid = next_id[0]
        next_id[0] += 1
        root = {
            "id": nid,
            "op": "Project",
            "name": "ROOT",
            "children": kids,
            "metrics": {"elapsed_ms": None, "rows_in": None, "rows_out": None},
            "columns": {"active": []},
            "details": {},
            "_slot_lineage": None,
        }

    # lineage computation
    _compute_lineage(root)

    # Build registry from *_DELIM_JOIN dependency subtrees
    delim_registry = build_delim_registry(root)

    # Apply registry to DELIM_SCAN nodes and recompute inputs
    apply_delim_registry(root, delim_registry)

    # add total latency
    lat_s = prof.get("latency") or prof.get("execution_time") or prof.get("total_time")
    return {
        "query": query_name,
        "engine": "duckdb",
        "query_latency_ms": _ms(lat_s),
        "root": root,
        "_delim_registry": delim_registry,
    }

