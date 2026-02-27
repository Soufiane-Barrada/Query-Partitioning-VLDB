from __future__ import annotations

import time
import threading
from typing import Dict, Any, List, Tuple
from collections import deque
import re


import duckdb
import pyarrow as pa
from datafusion import SessionContext, DataFrame as DF

from .models import SubQuery, ColumnSchema




class DistributedRunner:
    """
    Execute a DAG of SubQuery objects on DuckDB / DataFusion.

    - nodes form a DAG
    - inputs: alias_in_sql -> upstream_subquery_id
    - runs nodes in deterministic topo order (serial)
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, ctx: SessionContext):
        self.con = con
        self.ctx = ctx
        self.results: Dict[str, pa.Table] = {}
        self.timings: Dict[str, float] = {}

        # locks to serialize access to each engine (contexts are not thread-safe)
        self._duck_lock = threading.Lock()
        self._df_lock = threading.Lock()

        # per-node cast mappings for DataFusion
        self._cast_mappings: Dict[str, Dict[str, pa.DataType]] = {}
        # track temporary DataFusion views to clean up later
        self._temp_df_views: set[str] = set()




    
    def _prepare_cast_mappings(self, nodes: List[SubQuery]) -> None:
        """
        Build DataFusion cast mappings (column -> PyArrow type) for each node
        """
        self._cast_mappings.clear()
        for n in nodes:
            if not n.schema:
                continue

            mapping: Dict[str, pa.DataType] = {}
            for col in n.schema:
                # DataFusion sees lowercased names
                mapping[col.name.lower()] = _sqltype_to_pa(col.type)

            self._cast_mappings[n.id] = mapping




    def _cleanup_temp_views(self) -> None:
        """
        Deregister temporary DataFusion views (e.g., s1, s2, ...)
        """
        for name in list(self._temp_df_views):
            try:
                self.ctx.deregister_table(name)
            except Exception:
                pass
        self._temp_df_views.clear()



    # registration helpers 
    def _register_input_duckdb(self, name: str, obj: Any) -> None:
        self.con.register(name, obj)

    

    def _register_input_datafusion(self, name: str, obj: Any) -> None:
        # If we've already registered this alias in this run, don't re-register it.
        if name in self._temp_df_views:
            return

        # Track first so that even if register_view raises, we can still
        # attempt to clean it up later.
        self._temp_df_views.add(name)

        df_in = self.ctx.from_arrow(obj)
        # print(f"=== DataFusion schema for {name} ===")
        # print(df_in.schema())
        self.ctx.register_view(name, df_in)



    # engine execution helpers
    def _run_on_duckdb(self, sql: str) -> pa.Table:
        # print("DuckDB: ", sql)
        return self.con.execute(sql).arrow()

    def _run_on_datafusion(self, node: SubQuery) -> pa.Table:

        df = self.ctx.sql(node.sql)

        # If we have an expected schema from Calcite, cast to it
        mapping = self._cast_mappings.get(node.id)
        if mapping:
            df = df.cast(mapping)

        return df.to_arrow_table()



    # DAG helpers 
    @staticmethod
    def _build_graph(nodes: List[SubQuery]) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
        indeg: Dict[str, int] = {n.id: 0 for n in nodes}
        adj: Dict[str, List[str]] = {n.id: [] for n in nodes}
        id_set = set(indeg.keys())

        for n in nodes:
            for upstream in n.inputs.values():
                if upstream not in id_set:
                    raise ValueError(f"Node {n.id} depends on missing node {upstream}")
                indeg[n.id] += 1
                adj[upstream].append(n.id)
        return indeg, adj

    @staticmethod
    def _toposort_from_graph(indeg: Dict[str, int], adj: Dict[str, List[str]]) -> List[str]:
        indeg_local = dict(indeg)
        q = deque([nid for nid, d in indeg_local.items() if d == 0])
        order: List[str] = []
        while q:
            cur = q.popleft()
            order.append(cur)
            for v in adj[cur]:
                indeg_local[v] -= 1
                if indeg_local[v] == 0:
                    q.append(v)
        if len(order) != len(indeg):
            raise ValueError("Cycle detected in subquery DAG")
        return order


    # registration of downstream aliases
    def _register_downstream_aliases(
        self,
        nid: str,
        tbl: pa.Table,
        id_to_node: Dict[str, SubQuery],
        adj: Dict[str, List[str]],
    ) -> None:
        """
        After node `nid` has produced `tbl`, register it under the appropriate
        alias names for all downstream nodes, in the consumer engine.
        """
        for succ_id in adj[nid]:
            succ = id_to_node[succ_id]
            for alias, up_id in succ.inputs.items():
                if up_id != nid:
                    continue
                if succ.engine == "duckdb":
                    self._register_input_duckdb(alias, tbl)
                else:
                    self._register_input_datafusion(alias, tbl)

    # node execution 
    def _run_single_node(self, node: SubQuery) -> tuple[pa.Table, float]:
        t1 = time.perf_counter()
        if node.engine == "duckdb":
            with self._duck_lock:
                out_tbl = self._run_on_duckdb(node.sql)

            # Align DuckDB column names with Calcite/JSON schema if available.
            if node.schema is not None:
                expected_names = [col.name for col in node.schema]
                if len(expected_names) == len(out_tbl.column_names):
                    out_tbl = out_tbl.rename_columns(expected_names)
                else:
                    print(f"[WARN] Schema/column count mismatch for node {node.id}")
                    pass

        else:
            with self._df_lock:
                out_tbl = self._run_on_datafusion(node)

        t2 = time.perf_counter()

        # Normalize to lowercase for both engines, to keep downstream handling simple.
        out_tbl = out_tbl.rename_columns([c.lower() for c in out_tbl.column_names])

        return out_tbl, (t2 - t1)



    #  public API
    def run(
        self,
        nodes: List[SubQuery],
    ) -> Dict[str, pa.Table]:
        if not nodes:
            return {}

        indeg, adj = self._build_graph(nodes)
        self._cleanup_temp_views()

        self.results.clear()
        self.timings.clear()

        self._prepare_cast_mappings(nodes)

        topo_order = self._toposort_from_graph(indeg, adj)
        id_to_node = {n.id: n for n in nodes}

        total_start = time.perf_counter()

        # Serial execution in deterministic topo order
        for nid in topo_order:
            node = id_to_node[nid]
            tbl, elapsed = self._run_single_node(node)
            self.results[nid] = tbl
            self.timings[nid] = elapsed

            # register for downstream nodes immediately
            self._register_downstream_aliases(nid, tbl, id_to_node, adj)

        self.timings["total_distributed"] = time.perf_counter() - total_start
        self._cleanup_temp_views()
        return self.results
    







def _sqltype_to_pa(type_name: str) -> pa.DataType:
    t = type_name.upper()
    if t in ("TINYINT", "SMALLINT"):
        return pa.int32()
    if t in ("INTEGER", "INT", "BIGINT", "LONG"):
        return pa.int64()
    if t in ("FLOAT", "REAL"):
        return pa.float32()
    if t in ("DOUBLE", "DOUBLE_PRECISION", "FLOAT8"):
        return pa.float64()
    if t in ("DECIMAL",):
        return pa.float64()
    if t in ("BOOLEAN", "BIT"):
        return pa.bool_()
    if t in ("CHAR", "VARCHAR", "LONGVARCHAR"):
        return pa.string()
    if t in ("DATE",):
        return pa.date32()
    if t.startswith("TIMESTAMP"):
        return pa.timestamp("us")
    
    print("TYPE CONVERSION PROBLEM: ", t)
    return pa.string()

