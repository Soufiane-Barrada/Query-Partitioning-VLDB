
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UnifiedDoc {
    pub query: String,
    pub engine: String,
    pub query_latency_ms: Option<f64>,
    pub root: Node,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Metrics {
    pub elapsed_ms: Option<f64>,
    pub rows_in: Option<u64>,
    pub rows_out: Option<u64>,

    // join-specific inputs
    pub rows_in_left: Option<u64>,
    pub rows_in_right: Option<u64>,

    // size fields
    pub row_size_in_bytes: Option<f64>,
    pub row_size_out_bytes: Option<f64>,
    pub row_size_in_left_bytes: Option<f64>,
    pub row_size_in_right_bytes: Option<f64>,

    // how much infra time was accumulated into this node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub infra_ms_accumulated: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Columns {
    pub active: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Details {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scan: Option<ScanDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<ProjectDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<AggregateDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<SortDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join: Option<JoinDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<WindowDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cte: Option<CteDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cte_scan: Option<CteScanDet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub op_kind: Option<OpKind>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct OpKind {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comparisons: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregates: Option<Vec<String>>,
    // join-specific:
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalized: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_pairs: Option<Vec<BTreeMap<String, String>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ScanDet {
    pub table: String,
    pub columns: Vec<String>,

    /// For each column, true if it is variable-width (Utf8/LargeUtf8/Binary/LargeBinary/*View)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub varwidth: Option<Vec<bool>>,

    /// For each column, the Arrow fixed scalar width in bytes (e.g., 4 for Int32, 12 for Utf8 header)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed_widths: Option<Vec<u64>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pushdown_predicates: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct ProjectDet {
    pub expressions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct FilterDet {
    pub predicates: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AggregateDet {
    pub group_keys: Vec<String>,
    pub aggregates: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SortKey {
    pub expr: String,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SortDet {
    pub keys: Vec<SortKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct JoinDet {
    pub join_type: Option<String>,
    pub condition: Option<String>,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct WindowDet {
    pub expressions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct CteDet {
    pub name: Option<String>,
    pub index: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer_slots: Option<Vec<Vec<String>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct CteScanDet {
    pub index: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bound_index: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Node {
    pub id: usize,
    pub op: String,
    pub name: String, // DataFusion concrete operator, e.g., "HashJoinExec"
    pub children: Vec<Node>,
    pub metrics: Metrics,
    pub details: Details,
    pub columns: Columns,

    // internal lineage slots (Calcite-like): each output slot -> base column list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _slot_lineage: Option<Vec<Vec<String>>>,
}

impl Node {
    pub fn new(id: usize, op: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id,
            op: op.into(),
            name: name.into(),
            children: vec![],
            metrics: Metrics::default(),
            details: Details::default(),
            columns: Columns::default(),
            _slot_lineage: None,
        }
    }
}
