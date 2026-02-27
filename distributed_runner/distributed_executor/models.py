from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
import json

Engine = Literal["duckdb", "datafusion"]



@dataclass
class ColumnSchema:
    name: str
    type: str  # "INTEGER", "BIGINT", "VARCHAR", etc.


@dataclass
class SubQuery:
    id: str
    engine: Engine
    sql: str
    inputs: Dict[str, str] = field(default_factory=dict)
    schema: Optional[List[ColumnSchema]] = None


@dataclass
class DpSummary:
    has_cut: bool
    cut_node_id: int
    q1_engine: Engine
    q2_engine: Engine
    cost_all_duckdb: float
    cost_all_datafusion: float
    chosen_cost: float


@dataclass
class Dag:
    final_node_id: str
    nodes: List[SubQuery]


@dataclass
class PlanVariant:
    plan_id: str
    dag: Dag
    dp_summary: Optional[DpSummary] = None
    analysis: Optional[Analysis] = None




@dataclass
class TransferCoeffs:
    a_ms: float
    b_ms_per_row: float
    c_ms_per_row_size_byte: float
    d_ms_per_output_byte: float


@dataclass
class PipelineAnalysis:
    run_id: Optional[str] = None
    do_optimize: Optional[bool] = None
    metadata_provider: Optional[str] = None
    dp_mode: Optional[str] = None
    transfer_extra_constant_ms: Optional[float] = None
    transfer_coeffs_duckdb: Optional[TransferCoeffs] = None
    transfer_coeffs_datafusion: Optional[TransferCoeffs] = None
    decorrelated_plan_sha256: Optional[str] = None


@dataclass
class PlanStats:
    total_nodes: Optional[int] = None
    max_depth: Optional[int] = None
    operator_kind_counts: Dict[str, int] = field(default_factory=dict)
    root_output_rows: Optional[float] = None
    root_row_size_out_bytes: Optional[float] = None


@dataclass
class SubPlanStats:
    engine: Optional[Engine] = None
    node_count: Optional[int] = None
    max_depth: Optional[int] = None
    operator_kind_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class CutAnalysis:
    cut_depth: Optional[int] = None
    cut_node_operator_kind: Optional[str] = None
    cut_output_rows: Optional[float] = None
    cut_row_size_out_bytes: Optional[float] = None
    cut_output_bytes: Optional[float] = None
    transfer_estimated_ms: Optional[float] = None
    q1_stats: Optional[SubPlanStats] = None
    q2_stats: Optional[SubPlanStats] = None


@dataclass
class Analysis:
    pipeline: Optional[PipelineAnalysis] = None
    plan_stats: Optional[PlanStats] = None
    cut: Optional[CutAnalysis] = None


@dataclass
class QueryPlan:
    query_id: str
    original_sql: str
    dag: Dag
    dp_summary: Optional[DpSummary] = None
    analysis: Optional[Analysis] = None
    cut_plans: List[PlanVariant] = field(default_factory=list)


def load_query_plan(path: str | Path) -> QueryPlan:
    path = Path(path)
    with path.open("r", encoding="utf-8") as f:
        raw: Dict[str, Any] = json.load(f)

    def get_key(d: Dict[str, Any], *names: str, default: Any = None) -> Any:
        for name in names:
            if name in d:
                return d[name]
        return default

    def parse_schema(n: Dict[str, Any]) -> Optional[List[ColumnSchema]]:
        schema_raw = n.get("schema")
        if not schema_raw:
            return None
        return [ColumnSchema(name=c["name"], type=c["type"]) for c in schema_raw]

    def parse_dp_summary(dp_obj: Any) -> Optional[DpSummary]:
        if not isinstance(dp_obj, dict):
            return None
        return DpSummary(
            has_cut=bool(get_key(dp_obj, "has_cut", "hasCut", default=False)),
            cut_node_id=int(get_key(dp_obj, "cut_node_id", "cutNodeId", default=-1)),
            q1_engine=get_key(dp_obj, "q1_engine", "q1Engine", default="duckdb"),
            q2_engine=get_key(dp_obj, "q2_engine", "q2Engine", default="duckdb"),
            cost_all_duckdb=float(get_key(dp_obj, "cost_all_duckdb", "costAllDuckdb", default=0.0)),
            cost_all_datafusion=float(get_key(dp_obj, "cost_all_datafusion", "costAllDatafusion", default=0.0)),
            chosen_cost=float(get_key(dp_obj, "chosen_cost", "chosenCost", default=0.0)),
        )

    def parse_dag(dag_obj: Any) -> Dag:
        if not isinstance(dag_obj, dict):
            raise KeyError(f"Missing dag in {path}")
        final_node_id = get_key(dag_obj, "final_node_id", "finalNodeId")
        node_raw_list = dag_obj.get("nodes") or []

        if final_node_id is None:
            raise KeyError(f"Missing dag.finalNodeId in {path}")

        nodes = [
            SubQuery(
                id=n["id"],
                engine=n["engine"],
                sql=n["sql"],
                inputs=n.get("inputs", {}),
                schema=parse_schema(n),
            )
            for n in node_raw_list
        ]

        # pre-normalize SQL for DataFusion nodes
        for n in nodes:
            if n.engine == "datafusion":
                n.sql = n.sql.replace('"', "")

        return Dag(final_node_id=final_node_id, nodes=nodes)

    # parse dpSummary
    dp_raw = raw.get("dp_summary") or raw.get("dpSummary")
    dp: Optional[DpSummary] = parse_dp_summary(dp_raw)

    def parse_analysis(analysis_raw: Any) -> Optional[Analysis]:
        if not isinstance(analysis_raw, dict):
            return None
        # pipeline
        pipe_raw = analysis_raw.get("pipeline")
        pipeline: Optional[PipelineAnalysis] = None
        if isinstance(pipe_raw, dict):
            coeffs_raw = pipe_raw.get("transferCoeffs") or pipe_raw.get("transfer_coeffs") or {}

            def parse_coeffs(obj: Any) -> Optional[TransferCoeffs]:
                if not isinstance(obj, dict):
                    return None
                return TransferCoeffs(
                    a_ms=float(get_key(obj, "aMs", "a_ms", default=0.0)),
                    b_ms_per_row=float(get_key(obj, "bMsPerRow", "b_ms_per_row", default=0.0)),
                    c_ms_per_row_size_byte=float(get_key(obj, "cMsPerRowSizeByte", "c_ms_per_row_size_byte", default=0.0)),
                    d_ms_per_output_byte=float(get_key(obj, "dMsPerOutputByte", "d_ms_per_output_byte", default=0.0)),
                )

            pipeline = PipelineAnalysis(
                run_id=get_key(pipe_raw, "runId", "run_id"),
                do_optimize=get_key(pipe_raw, "doOptimize", "do_optimize"),
                metadata_provider=get_key(pipe_raw, "metadataProvider", "metadata_provider"),
                dp_mode=get_key(pipe_raw, "dpMode", "dp_mode"),
                transfer_extra_constant_ms=get_key(pipe_raw, "transferExtraConstantMs", "transfer_extra_constant_ms"),
                transfer_coeffs_duckdb=parse_coeffs((coeffs_raw or {}).get("duckdb")),
                transfer_coeffs_datafusion=parse_coeffs((coeffs_raw or {}).get("datafusion")),
                decorrelated_plan_sha256=get_key(pipe_raw, "decorrelatedPlanSha256", "decorrelated_plan_sha256"),
            )

        # planStats
        ps_raw = analysis_raw.get("planStats") or analysis_raw.get("plan_stats")
        plan_stats: Optional[PlanStats] = None
        if isinstance(ps_raw, dict):
            okc = ps_raw.get("operatorKindCounts") or ps_raw.get("operator_kind_counts") or {}
            if not isinstance(okc, dict):
                okc = {}
            plan_stats = PlanStats(
                total_nodes=get_key(ps_raw, "totalNodes", "total_nodes"),
                max_depth=get_key(ps_raw, "maxDepth", "max_depth"),
                operator_kind_counts={str(k): int(v) for k, v in okc.items()},
                root_output_rows=get_key(ps_raw, "rootOutputRows", "root_output_rows"),
                root_row_size_out_bytes=get_key(ps_raw, "rootRowSizeOutBytes", "root_row_size_out_bytes"),
            )

        # cut (nullable)
        cut_raw = analysis_raw.get("cut")
        cut: Optional[CutAnalysis] = None
        if isinstance(cut_raw, dict):
            def parse_subplan_stats(obj: Any) -> Optional[SubPlanStats]:
                if not isinstance(obj, dict):
                    return None
                okc2 = obj.get("operatorKindCounts") or obj.get("operator_kind_counts") or {}
                if not isinstance(okc2, dict):
                    okc2 = {}
                return SubPlanStats(
                    engine=get_key(obj, "engine"),
                    node_count=get_key(obj, "nodeCount", "node_count"),
                    max_depth=get_key(obj, "maxDepth", "max_depth"),
                    operator_kind_counts={str(k): int(v) for k, v in okc2.items()},
                )

            cut = CutAnalysis(
                cut_depth=get_key(cut_raw, "cutDepth", "cut_depth"),
                cut_node_operator_kind=get_key(cut_raw, "cutNodeOperatorKind", "cut_node_operator_kind"),
                cut_output_rows=get_key(cut_raw, "cutOutputRows", "cut_output_rows"),
                cut_row_size_out_bytes=get_key(cut_raw, "cutRowSizeOutBytes", "cut_row_size_out_bytes"),
                cut_output_bytes=get_key(cut_raw, "cutOutputBytes", "cut_output_bytes"),
                transfer_estimated_ms=get_key(cut_raw, "transferEstimatedMs", "transfer_estimated_ms"),
                q1_stats=parse_subplan_stats(cut_raw.get("q1Stats") or cut_raw.get("q1_stats")),
                q2_stats=parse_subplan_stats(cut_raw.get("q2Stats") or cut_raw.get("q2_stats")),
            )

        return Analysis(pipeline=pipeline, plan_stats=plan_stats, cut=cut)

    # parse analysis
    analysis = parse_analysis(raw.get("analysis"))

    # top-level fields
    query_id = get_key(raw, "query_id", "queryId")
    original_sql = get_key(raw, "original_sql", "originalSql")

    if query_id is None or original_sql is None:
        raise KeyError(f"Missing queryId/originalSql in {path}")

    # DAG
    dag: Optional[Dag] = None
    if "dag" in raw:
        dag = parse_dag(raw["dag"])

    # cut plans (exhaustive list, excluding main)
    cut_plans: List[PlanVariant] = []
    cut_raw_list = raw.get("cutPlans") or raw.get("cut_plans") or []
    if isinstance(cut_raw_list, list):
        for idx, cp_raw in enumerate(cut_raw_list):
            if not isinstance(cp_raw, dict):
                continue
            plan_id = get_key(cp_raw, "planId", "plan_id", default=f"cut_{idx}")
            cp_dp = parse_dp_summary(cp_raw.get("dpSummary") or cp_raw.get("dp_summary"))
            cp_dag_raw = cp_raw.get("dag")
            if cp_dag_raw is None:
                continue
            try:
                cp_dag = parse_dag(cp_dag_raw)
            except Exception:
                continue
            cp_analysis = parse_analysis(cp_raw.get("analysis"))
            cut_plans.append(
                PlanVariant(
                    plan_id=str(plan_id),
                    dag=cp_dag,
                    dp_summary=cp_dp,
                    analysis=cp_analysis,
                )
            )

    if dag is None:
        if cut_plans:
            dag = cut_plans[0].dag
        else:
            raise KeyError(f"Missing dag in {path}")

    # Always include main plan as the first variant.
    main_variant = PlanVariant(plan_id="main", dag=dag, dp_summary=dp, analysis=analysis)
    if cut_plans:
        cut_plans = [main_variant] + cut_plans
    else:
        cut_plans = [main_variant]

    return QueryPlan(
        query_id=str(query_id),
        original_sql=str(original_sql),
        dag=dag,
        dp_summary=dp,
        analysis=analysis,
        cut_plans=cut_plans,
    )
