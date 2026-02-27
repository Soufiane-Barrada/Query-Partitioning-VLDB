from typing import Dict, Any, List, Tuple, Set
import pathlib
import re

import pyarrow.parquet as pq
import pyarrow as pa

from .collector import _resolve_expr as _res


STRING_CELL_OVERHEAD = 12.0
NULL_MASK_OVERHEAD_PER_VALUE = 1.0 / 8.0

_AGG_CANON = {
    "COUNT": "COUNT",
    "COUNT_STAR": "COUNT",
    "SUM": "SUM",
    "SUM_NO_OVERFLOW": "SUM",
    "AVG": "AVG",
    "MEAN": "AVG",
    "MIN": "MIN",
    "MAX": "MAX",
    "VAR": "VAR",
    "VARIANCE": "VAR",
    "VAR_SAMP": "VAR",
    "VAR_POP": "VAR",
    "STDDEV": "STDDEV",
    "STDDEV_SAMP": "STDDEV",
    "STDDEV_POP": "STDDEV",
    "ARRAY_AGG": "ARRAY_AGG",
    "STRING_AGG": "STRING_AGG",
}


def _scalar_fixed_size(dt: pa.DataType) -> int:
    if pa.types.is_null(dt):
        return 0
    if pa.types.is_boolean(dt):
        return 1
    if pa.types.is_int8(dt) or pa.types.is_uint8(dt):
        return 1
    if pa.types.is_int16(dt) or pa.types.is_uint16(dt):
        return 2
    if pa.types.is_int32(dt) or pa.types.is_uint32(dt):
        return 4
    if pa.types.is_int64(dt) or pa.types.is_uint64(dt):
        return 8
    if pa.types.is_date32(dt):
        return 4
    if pa.types.is_date64(dt):
        return 8
    if pa.types.is_time32(dt):
        return 4
    if pa.types.is_time64(dt):
        return 8
    if pa.types.is_duration(dt):
        return 8
    if pa.types.is_timestamp(dt):
        return 8
    if pa.types.is_float16(dt):
        return 2
    if pa.types.is_float32(dt):
        return 4
    if pa.types.is_float64(dt):
        return 8
    if pa.types.is_decimal128(dt):
        return 16
    if pa.types.is_decimal256(dt):
        return 32
    if pa.types.is_fixed_size_binary(dt):
        return int(dt.byte_width)
    if (
        pa.types.is_string(dt)
        or pa.types.is_large_string(dt)
        or pa.types.is_binary(dt)
        or pa.types.is_large_binary(dt)
    ):
        return 12
    return 16


def _is_varwidth(dt: pa.DataType) -> bool:
    return (
        pa.types.is_string(dt)
        or pa.types.is_large_string(dt)
        or pa.types.is_binary(dt)
        or pa.types.is_large_binary(dt)
    )


def _read_schema_map(parquet_dir: str) -> Dict[str, Dict[str, pa.DataType]]:
    base = pathlib.Path(parquet_dir)
    out: Dict[str, Dict[str, pa.DataType]] = {}
    for f in base.glob("*.parquet"):
        table = f.stem.lower()
        pf = pq.ParquetFile(str(f))
        schema = pf.schema_arrow
        out[table] = {str(field.name).lower(): field.type for field in schema}
    return out


def _read_payload_and_nulls(parquet_dir: str) -> Tuple[Dict[str, Dict[str, float]], Set[str]]:
    base = pathlib.Path(parquet_dir)
    payload_avg: Dict[str, Dict[str, float]] = {}
    has_nulls: Set[str] = set()

    for f in base.glob("*.parquet"):
        table = f.stem.lower()
        pf = pq.ParquetFile(str(f))
        meta = pf.metadata
        schema = pf.schema_arrow

        name_to_idx = {schema[i].name.lower(): i for i in range(len(schema))}
        totals = {name: [0, 0] for name in name_to_idx.keys()}
        null_any = {name: False for name in name_to_idx.keys()}

        for rg in range(meta.num_row_groups):
            rgm = meta.row_group(rg)
            for name, idx in name_to_idx.items():
                cm = rgm.column(idx)
                totals[name][0] += int(cm.total_uncompressed_size)
                totals[name][1] += int(cm.num_values)
                stats = cm.statistics
                if stats and stats.null_count and stats.null_count > 0:
                    null_any[name] = True

        payload_avg[table] = {}
        for name, (bytes_u, vals) in totals.items():
            avg = float(bytes_u) / float(vals) if vals > 0 else 0.0
            payload_avg[table][name] = avg
            if null_any[name]:
                has_nulls.add(f"{table}.{name}")

    return payload_avg, has_nulls


_AGG_NAME_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def _agg_funcs(texts: List[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for t in texts:
        for m in _AGG_NAME_RE.finditer(t or ""):
            raw = m.group(1).upper()
            if raw.startswith("__"):
                continue
            canon = _AGG_CANON.get(raw)
            if canon and canon not in seen:
                seen.add(canon)
                out.append(canon)
    return out


def annotate(
    doc: Dict[str, Any],
    parquet_dir: str,
    *,
    debug: bool = False,
    allow_fallbacks: bool = False,
    log=print,
) -> Dict[str, Any]:
    schema_map = _read_schema_map(parquet_dir)
    payload_avg, has_nulls = _read_payload_and_nulls(parquet_dir)
    delim_registry = doc.get("_delim_registry", {})

    cte_size_by_index: Dict[str, float] = {}

    def child_slots(n: Dict[str, Any]) -> List[List[str]]:
        kids = n.get("children", [])
        if not kids:
            return []
        if n.get("op") == "Join" and len(kids) == 2:
            return (kids[0].get("_slot_lineage") or []) + (kids[1].get("_slot_lineage") or [])
        return kids[0].get("_slot_lineage") or []

    def width_of_base(base: str) -> float:
        if not base:
            if allow_fallbacks:
                if debug:
                    log("Warning: Empty base column reference, using 8 bytes fallback")
                return 8.0
            raise ValueError("Empty base column reference (strict mode; use --allow-fallbacks)")

        if "." not in base:
            if allow_fallbacks:
                if debug:
                    log(f"Warning: Column '{base}' has no table prefix, using 8 bytes fallback")
                return 8.0
            raise ValueError(f"Column '{base}' has no table prefix (strict mode; use --allow-fallbacks)")

        parts = base.lower().split(".", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            if allow_fallbacks:
                if debug:
                    log(f"Warning: Invalid base column format '{base}', using 8 bytes fallback")
                return 8.0
            raise ValueError(f"Invalid base column format '{base}' (strict mode; use --allow-fallbacks)")

        t, c = parts
        if t not in schema_map:
            if allow_fallbacks:
                if debug:
                    log(f"Warning: Table '{t}' not in schema_map, using 8 bytes fallback")
                return 8.0
            raise KeyError(
                f"Table '{t}' not found in schema_map. Available tables: {list(schema_map.keys())} "
                "(strict mode; use --allow-fallbacks)"
            )

        if c not in schema_map[t]:
            if allow_fallbacks:
                if debug:
                    log(f"Warning: Column '{c}' not in table '{t}', using 8 bytes fallback")
                return 8.0
            raise KeyError(
                f"Column '{c}' not found in table '{t}'. Available columns: {list(schema_map[t].keys())} "
                "(strict mode; use --allow-fallbacks)"
            )

        dt = schema_map[t][c]
        fw = float(_scalar_fixed_size(dt))
        is_var = _is_varwidth(dt)
        payload = payload_avg[t][c] if is_var else 0.0
        null_over = NULL_MASK_OVERHEAD_PER_VALUE if (f"{t}.{c}" in has_nulls) else 0.0
        return fw + payload + null_over

    def width_of_group_key(base: str) -> float:
        if not base:
            if allow_fallbacks:
                if debug:
                    log("Warning: Empty base column in group key, using 8 bytes fallback")
                return 8.0
            raise ValueError("Empty base column in group key (strict mode; use --allow-fallbacks)")

        if "." not in base:
            if allow_fallbacks:
                if debug:
                    log(f"Warning: Group key column '{base}' has no table prefix, using 8 bytes fallback")
                return 8.0
            raise ValueError(f"Group key column '{base}' has no table prefix (strict mode; use --allow-fallbacks)")

        parts = base.lower().split(".", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            if allow_fallbacks:
                if debug:
                    log(f"Warning: Invalid group key format '{base}', using 8 bytes fallback")
                return 8.0
            raise ValueError(f"Invalid group key format '{base}' (strict mode; use --allow-fallbacks)")

        t, c = parts
        if t not in schema_map:
            if allow_fallbacks:
                if debug:
                    log(f"Warning: Group key table '{t}' not in schema_map, using 8 bytes fallback")
                return 8.0
            raise KeyError(
                f"Group key table '{t}' not found in schema_map. Available tables: {list(schema_map.keys())} "
                "(strict mode; use --allow-fallbacks)"
            )

        if c not in schema_map[t]:
            if allow_fallbacks:
                if debug:
                    log(f"Warning: Group key column '{c}' not in table '{t}', using 8 bytes fallback")
                return 8.0
            raise KeyError(
                f"Group key column '{c}' not found in table '{t}'. Available columns: {list(schema_map[t].keys())} "
                "(strict mode; use --allow-fallbacks)"
            )

        dt = schema_map[t][c]
        if _is_varwidth(dt):
            payload = payload_avg[t][c]
            null_over = NULL_MASK_OVERHEAD_PER_VALUE if (f"{t}.{c}" in has_nulls) else 0.0
            return STRING_CELL_OVERHEAD + payload + null_over
        return float(_scalar_fixed_size(dt))

    def _sum_slot_widths_from_lineage(slots: List[List[str]]) -> float:
        total = 0.0
        for slot in slots or []:
            if len(slot) == 1:
                total += width_of_base(slot[0])
            else:
                total += 8.0
        return total

    def walk(n: Dict[str, Any]) -> Tuple[float, float]:
        op = n.get("op")
        kids = n.get("children", [])
        det = n.get("details") or {}

        if op == "CTE":
            child_sizes: List[Tuple[float, float]] = []
            if kids:
                prod_in, prod_out = walk(kids[0])
                child_sizes.append((prod_in, prod_out))
                cte_meta = det.get("cte") or {}
                cte_idx = cte_meta.get("index")
                if cte_idx is not None:
                    cte_size_by_index[str(cte_idx)] = float(prod_out)
            for ch in kids[1:]:
                child_sizes.append(walk(ch))

            child_out = [s[1] for s in child_sizes]
            unary_in = 0.0
            out_size = child_out[-1] if child_out else 0.0

            n.setdefault("metrics", {})["row_size_in_bytes"] = float(unary_in)
            n["metrics"]["row_size_out_bytes"] = float(out_size)
            if debug:
                log(f"CTE#{n.get('id')} in={unary_in} out={out_size}")
            return float(unary_in), float(out_size)

        child_sizes = [walk(ch) for ch in kids]
        child_out = [s[1] for s in child_sizes]
        unary_in = child_out[0] if len(child_out) == 1 else 0.0
        out_size: float

        if op == "Scan":
            scan = det.get("scan") or {}
            table = (scan.get("table") or "").lower()
            cols = [str(c).lower() for c in (scan.get("columns") or [])]

            if n.get("name", "").upper() == "DELIM_SCAN":
                idx = scan.get("delim_index")
                key_bases: List[str] = []
                if idx and idx in delim_registry:
                    for group in (delim_registry[idx].get("key_lineage") or []):
                        for b in group:
                            key_bases.append(b)
                if key_bases:
                    out_size = float(sum(width_of_group_key(b) for b in key_bases))
                else:
                    out_size = 8.0

                n.setdefault("details", {}).setdefault("scan", {})["fixed_widths"] = []
                n["details"]["scan"]["varwidth"] = []

            else:
                if not table or table not in schema_map:
                    if allow_fallbacks:
                        if debug:
                            log(
                                f\"Warning: Scan node has unknown/empty table '{table}' with columns {cols}. "
                                "Using fallback sizing.\"
                            )
                        fixed_list = [8 for _ in cols]
                        varw_list = [False for _ in cols]
                        n.setdefault("details", {}).setdefault("scan", {})["fixed_widths"] = fixed_list
                        n["details"]["scan"]["varwidth"] = varw_list
                        out_size = float(len(cols) * 8.0)
                    else:
                        raise KeyError(
                            f\"Scan node has unknown/empty table '{table}' with columns {cols}. "
                            f\"Available tables: {list(schema_map.keys())} (strict mode; use --allow-fallbacks)\"
                        )
                else:
                    fixed_list: List[int] = []
                    varw_list: List[bool] = []
                    for col in cols:
                        if col not in schema_map[table]:
                            if allow_fallbacks:
                                if debug:
                                    log(f\"Warning: Column '{col}' not found in table '{table}'. Using 8 bytes.\")
                                fixed_list.append(8)
                                varw_list.append(False)
                            else:
                                raise KeyError(
                                    f\"Column '{col}' not found in table '{table}'. "
                                    f\"Available columns: {list(schema_map[table].keys())} "
                                    "(strict mode; use --allow-fallbacks)"
                                )
                        else:
                            dt = schema_map[table][col]
                            fixed_list.append(int(_scalar_fixed_size(dt)))
                            varw_list.append(_is_varwidth(dt))

                    n.setdefault("details", {}).setdefault("scan", {})["fixed_widths"] = fixed_list
                    n["details"]["scan"]["varwidth"] = varw_list

                    per_col = []
                    for col in cols:
                        base = f"{table}.{col}"
                        if col in schema_map[table]:
                            per_col.append(width_of_base(base))
                        else:
                            if allow_fallbacks:
                                per_col.append(8.0)
                            else:
                                raise KeyError(
                                    f\"Column '{col}' not found in table '{table}' (strict mode; use --allow-fallbacks)\"
                                )
                    out_size = float(sum(per_col))

        elif op == "CTE_SCAN":
            cmeta = det.get("cte_scan") or {}
            cidx = cmeta.get("bound_index") or cmeta.get("index")
            got = None
            if cidx is not None and str(cidx) in cte_size_by_index:
                got = float(cte_size_by_index[str(cidx)])
            if got is None:
                slots = n.get("_slot_lineage") or []
                got = _sum_slot_widths_from_lineage(slots)
            out_size = float(got)

        elif op == "Project":
            slots = n.get("_slot_lineage") or []
            out_size = _sum_slot_widths_from_lineage(slots) if slots else unary_in

        elif op in ("Filter", "Sort", "Limit"):
            out_size = unary_in

        elif op == "Window":
            cs = child_slots(n)
            child_slot_cnt = len(cs)
            slots = n.get("_slot_lineage") or []
            extra = 0.0
            for slot in slots[child_slot_cnt:]:
                extra += width_of_base(slot[0]) if len(slot) == 1 else 8.0
            out_size = max(unary_in, unary_in + extra)

        elif op == "Aggregate":
            a = det.get("aggregate") or {}
            cs = child_slots(n)
            agg_exprs = a.get("aggregates") or []
            gk = _res(";".join(a.get("group_keys") or []), cs)
            gk_sz = sum(width_of_group_key(b) for b in gk)

            agg_sz = 0.0
            for expr in agg_exprs:
                fn = _AGG_NAME_RE.search(expr or "")
                canon = _AGG_CANON.get((fn.group(1) if fn else "").upper())
                deps = _res(expr, cs)
                arg_w = width_of_group_key(deps[0]) if deps else 8.0
                if canon in ("COUNT", "SUM", "AVG"):
                    agg_sz += 8.0
                elif canon in ("MIN", "MAX"):
                    agg_sz += arg_w
                elif canon in ("ARRAY_AGG", "STRING_AGG"):
                    agg_sz += 64.0
                else:
                    agg_sz += 16.0
            out_size = gk_sz + agg_sz

        elif op == "Join":
            left_in = child_out[0] if len(child_out) > 0 else 0.0
            right_in = child_out[1] if len(child_out) > 1 else 0.0
            out_size = left_in + right_in
            n.setdefault("metrics", {})["row_size_in_left_bytes"] = float(left_in)
            n.setdefault("metrics", {})["row_size_in_right_bytes"] = float(right_in)

        else:
            out_size = unary_in

        if op != "Join":
            n.setdefault("metrics", {})["row_size_in_bytes"] = float(unary_in)
        n.setdefault("metrics", {})["row_size_out_bytes"] = float(out_size)

        if debug:
            log(f"{op}#{n.get('id')} in={unary_in} out={out_size}")

        return float(unary_in), float(out_size)

    walk(doc["root"])
    return doc

