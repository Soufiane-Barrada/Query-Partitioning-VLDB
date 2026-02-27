
use once_cell::sync::{Lazy, OnceCell};
use parquet::file::reader::{FileReader, SerializedFileReader};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use datafusion::physical_expr::{expressions, PhysicalExpr};
use datafusion::physical_plan::{
    aggregates::AggregateExec,
    execution_plan::ExecutionPlan,
    filter::FilterExec,
    joins::{HashJoinExec, NestedLoopJoinExec, CrossJoinExec},
    metrics::MetricsSet,
    projection::ProjectionExec,
    sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
    windows::WindowAggExec,
};

use crate::annotate::{scalar_size};
use crate::lineage::{collect_child_slots, compute_lineage};
use crate::schema::{
    AggregateDet, FilterDet, JoinDet, Node, OpKind, ProjectDet, ScanDet, SortDet, SortKey,
};


// ────────────────────────────────────────────────────────────────────────────────
// Parquet table -> columns map (built once from PARQUET_DIR)
// ────────────────────────────────────────────────────────────────────────────────

static TABLE_COLS: OnceCell<HashMap<String, BTreeSet<String>>> = OnceCell::new();
static TABLE_ROWS: OnceCell<HashMap<String, u64>> = OnceCell::new();

fn read_parquet_rows(path: &Path) -> Option<u64> {
    let file = File::open(path).ok()?;
    let reader = SerializedFileReader::new(file).ok()?;
    let meta = reader.metadata();
    // Prefer file-level count if available; otherwise sum row groups
    let mut total = meta.file_metadata().num_rows() as u64;
    if total == 0 {
        total = (0..meta.num_row_groups())
            .map(|i| meta.row_group(i).num_rows() as u64)
            .sum();
    }
    Some(total)
}


pub fn init_parquet_table_cols(dir: &Path) {
    let mut cols_map: HashMap<String, BTreeSet<String>> = HashMap::new();
    let mut rows_map: HashMap<String, u64> = HashMap::new();

    if let Ok(rd) = std::fs::read_dir(dir) {
        for ent in rd.flatten() {
            let path = ent.path();
            if path.extension().is_some_and(|e| e == "parquet") {
                if let Some(table) = path.file_stem().and_then(|s| s.to_str()) {
                    let t = table.to_lowercase();
                    if let Some(cols) = read_parquet_cols(&path) {
                        cols_map.insert(t.clone(), cols);
                    }
                    if let Some(nrows) = read_parquet_rows(&path) {
                        rows_map.insert(t, nrows);
                    }
                }
            }
        }
    }

    TABLE_COLS.set(cols_map).expect("init_parquet_table_cols called more than once");
    TABLE_ROWS.set(rows_map).expect("init_parquet_table_cols (rows) called more than once");
}


/// If exactly one Parquet table has this total row count, return it.
fn infer_table_from_rowcount(rows_out: u64) -> Option<String> {
    let map = TABLE_ROWS.get()?;
    let hits: Vec<_> = map
        .iter()
        .filter_map(|(tbl, n)| (*n == rows_out).then_some(tbl.clone()))
        .collect();
    (hits.len() == 1).then(|| hits[0].clone())
}



/// If the output columns uniquely match a Parquet table's columns, return that table name.
fn infer_table_from_columns(cols: &[String]) -> Option<String> {
    if cols.is_empty() {
        return None;
    }
    let wanted: BTreeSet<String> = cols.iter().map(|c| c.to_lowercase()).collect();
    let map = TABLE_COLS.get()?;
    let hits: Vec<_> = map
        .iter()
        .filter_map(|(tbl, tblcols)| (wanted.is_subset(tblcols)).then_some(tbl.clone()))
        .collect();
    (hits.len() == 1).then(|| hits[0].clone())
}





static COL_DBG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"Column\s*\{\s*name:\s*"([^"]+)".*?\}"#).unwrap());


fn read_parquet_cols(path: &Path) -> Option<BTreeSet<String>> {
    let file = File::open(path).ok()?;
    let reader = SerializedFileReader::new(file).ok()?;
    let schema = reader.metadata().file_metadata().schema_descr();

    let mut cols = BTreeSet::new();
    for i in 0..schema.num_columns() {
        cols.insert(schema.column(i).path().string().to_lowercase());
    }
    Some(cols)
}

// ────────────────────────────────────────────────────────────────────────────────
// Small expression helpers
// ────────────────────────────────────────────────────────────────────────────────

/// Prefer logical column name; fall back to Debug for complex exprs.
fn expr_pretty(e: &Arc<dyn PhysicalExpr>) -> String {
    if let Some(c) = e.as_any().downcast_ref::<expressions::Column>() {
        c.name().to_string()
    } else {
        format!("{e:?}")
    }
}

/// Keep Debug form; used for complex Filter predicates (keeps DataFusion tokens).
fn stringify_expr(e: &Arc<dyn PhysicalExpr>) -> String {
    format!("{e:?}")
}

/// Column name + "#<index>" (used later to resolve against slot lineage).
fn expr_with_index(e: &Arc<dyn PhysicalExpr>) -> String {
    if let Some(c) = e.as_any().downcast_ref::<expressions::Column>() {
        return format!("{}#{}", c.name(), c.index());
    }
    format!("{e:?}")
}

/// Adds every Column(name) in an expression into `out` (set).
fn collect_active_columns_from_expr(e: &Arc<dyn PhysicalExpr>, out: &mut BTreeSet<String>) {
    let mut stack: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::clone(e)];
    while let Some(node) = stack.pop() {
        if let Some(c) = node.as_any().downcast_ref::<expressions::Column>() {
            out.insert(c.name().to_string());
        }
        for ch in node.children() {
            stack.push(Arc::clone(ch));
        }
    }
}

/// Derive an OpKind summary from a predicate or aggregate text.
fn op_kind_from_text(s: &str) -> OpKind {
    let su = s.to_uppercase();

    let mut cmp: BTreeSet<String> = BTreeSet::new();
    for (needle, norm) in [
        (" = ", "eq"),
        (" != ", "neq"),
        (" <> ", "neq"),
        (" > ", "gt"),
        (" < ", "lt"),
        (" >= ", "ge"),
        (" <= ", "le"),
        (" IS NULL", "is_null"),
        (" IS NOT NULL", "is_not_null"),
        (" IS DISTINCT FROM", "is_distinct"),
        (" IS NOT DISTINCT FROM", "is_not_distinct"),
        (" OP: EQ", "eq"),
        (" OP: NOTEQ", "neq"),
        (" OP: NEQ", "neq"),
        (" OP: LT", "lt"),
        (" OP: LE", "le"),
        (" OP: GT", "gt"),
        (" OP: GE", "ge"),
        ("ISNULL", "is_null"),
        ("ISNOTNULL", "is_not_null"),
        ("IS DISTINCT FROM", "is_distinct"),
        ("IS NOT DISTINCT FROM", "is_not_distinct"),
    ] {
        if su.contains(needle) {
            cmp.insert(norm.to_string());
        }
    }

    let mut log: BTreeSet<String> = BTreeSet::new();
    for (needle, norm) in [(" AND ", "and"), (" OR ", "or"), (" OP: AND", "and"), (" OP: OR", "or")]
    {
        if su.contains(needle) {
            log.insert(norm.to_string());
        }
    }

    let mut aggs: BTreeSet<String> = BTreeSet::new();
    for cap in AGG_RE.captures_iter(s) {
        let mut name = cap[1].to_lowercase();
        if name == "mean" { name = "avg".to_string(); }
        if name == "variance" { name = "var".to_string(); }
        aggs.insert(name);
    }


    OpKind {
        comparisons: (!cmp.is_empty()).then(|| cmp.clone().into_iter().collect()),
        logical: (!log.is_empty()).then(|| log.into_iter().collect()),
        aggregates: (!aggs.is_empty()).then(|| aggs.clone().into_iter().collect()),
        normalized: (!cmp.is_empty()).then(|| cmp.into_iter().collect()),
        on_pairs: None,
    }

}

/// Normalize DataFusion exec node names to our compact op names.
fn normalize(df_name: &str) -> &'static str {
    let base = df_name.split_once('(').map(|(b, _)| b).unwrap_or(df_name);
    match base {
        "HashJoinExec" | "NestedLoopJoinExec" | "CrossJoinExec" => "Join",
        "ProjectionExec" => "Project",
        "FilterExec" => "Filter",
        "AggregateExec" => "Aggregate",
        "SortExec" | "SortPreservingMergeExec" => "Sort",
        "DataSourceExec" => "Scan",
        "WindowAggExec" => "Window",
        _ => "Other",
    }
}


// ────────────────────────────────────────────────────────────────────────────────
// Column qualification helpers (by unique suffix + optional #index)
// ────────────────────────────────────────────────────────────────────────────────

fn unique_suffix_map(slots: &[Vec<String>]) -> BTreeMap<String, String> {
    // tail -> all matches
    let mut multi: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for slot in slots {
        for bc in slot {
            let tail = bc.split('.').last().unwrap_or(bc).to_string();
            multi.entry(tail).or_default().push(bc.clone());
        }
    }
    // keep only unique tails
    let mut out = BTreeMap::new();
    for (k, v) in multi {
        if v.len() == 1 {
            out.insert(k.to_lowercase(), v.into_iter().next().unwrap());
        }
    }
    out
}

fn unique_suffix_map_from_node(n: &Node) -> BTreeMap<String, String> {
    fn walk(node: &Node, acc: &mut Vec<Vec<String>>) {
        if let Some(sl) = &node._slot_lineage {
            acc.extend(sl.clone());
        }
        for c in &node.children {
            walk(c, acc);
        }
    }
    let mut slots = vec![];
    walk(n, &mut slots);
    unique_suffix_map(&slots)
}

static SIMPLE_TOK_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_\.]*$").unwrap());
#[inline]
fn is_simple_token(s: &str) -> bool {
    SIMPLE_TOK_RE.is_match(s)
}

static AGG_RE: Lazy<Regex> = Lazy::new(|| {
    // only match whole aggregate names, case-insensitive
    Regex::new(r"(?i)\b(count|sum|avg|mean|min|max|variance|var_(?:samp|pop)|stddev|stddev_(?:samp|pop))\b").unwrap()
});


fn qualify_token(token: &str, map: &BTreeMap<String, String>) -> String {
    if token.contains('.') {
        token.to_lowercase()
    } else {
        map.get(&token.to_lowercase())
            .cloned()
            .unwrap_or_else(|| token.to_lowercase())
    }
}

static IDX_TAG_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:^.*index:\s*(\d+).*$|.*#(\d+)$)").unwrap());

/// Resolve a token like `id#index` or DataFusion debug text that contains `index: N`
/// to a fully-qualified base column using child's slot lineage, when unambiguous.
fn resolve_by_index(tok: &str, child: &Node) -> Option<String> {
    let caps = IDX_TAG_RE.captures(tok)?;
    let idx = caps
        .get(1)
        .or_else(|| caps.get(2))
        .and_then(|m| m.as_str().parse::<usize>().ok())?;

    let sl = child._slot_lineage.as_ref()?;
    if idx >= sl.len() {
        return None;
    }
    let bases = &sl[idx];
    if bases.len() == 1 {
        Some(bases[0].to_lowercase())
    } else {
        None
    }
}

/// Remove a trailing `#<digits>` index suffix if present.
fn strip_idx_suffix(s: &str) -> String {
    if let Some(pos) = s.rfind('#') {
        if s[pos + 1..].chars().all(|ch| ch.is_ascii_digit()) {
            return s[..pos].to_string();
        }
    }
    s.to_string()
}

// ────────────────────────────────────────────────────────────────────────────────
// Main: qualify simple column tokens using child (or subtree) lineage.
// ────────────────────────────────────────────────────────────────────────────────

pub fn qualify_tree(n: &mut Node) {
    // Post-order: qualify children first, then this node.
    for ch in n.children.iter_mut() {
        qualify_tree(ch);
    }

    // For non-join ops, build a suffix map from merged child slots.
    let merged_child = collect_child_slots(&n.children);
    let map_all = unique_suffix_map(&merged_child);

    match n.op.as_str() {
        "Aggregate" => {
            if let Some(ag) = n.details.aggregate.as_mut() {
                for g in ag.group_keys.iter_mut() {
                    if is_simple_token(g) {
                        *g = qualify_token(g, &map_all);
                    }
                }
            }
        }
        "Sort" => {
            if let Some(s) = n.details.sort.as_mut() {
                for k in s.keys.iter_mut() {
                    if is_simple_token(&k.expr) {
                        k.expr = qualify_token(&k.expr, &map_all);
                    }
                }
            }
        }
        "Join" => {
            // Build direct + subtree maps for both sides.
            let (l_direct, l_subtree) = match n.children.get(0) {
                Some(c) => (
                    c._slot_lineage
                        .clone()
                        .map(|sl| unique_suffix_map(&sl))
                        .unwrap_or_default(),
                    unique_suffix_map_from_node(c),
                ),
                None => (Default::default(), Default::default()),
            };
            let (r_direct, r_subtree) = match n.children.get(1) {
                Some(c) => (
                    c._slot_lineage
                        .clone()
                        .map(|sl| unique_suffix_map(&sl))
                        .unwrap_or_default(),
                    unique_suffix_map_from_node(c),
                ),
                None => (Default::default(), Default::default()),
            };

            // Prefer primary map; fall back to subtree when not present.
            let qualify_with =
                |tok: &str, primary: &BTreeMap<String, String>, fallback: &BTreeMap<String, String>| {
                    let t = tok.to_lowercase();
                    if t.contains('.') {
                        t
                    } else if let Some(q) = primary.get(&t) {
                        q.clone()
                    } else {
                        fallback.get(&t).cloned().unwrap_or(t)
                    }
                };

            if let Some(j) = n.details.join.as_mut() {
                let left_child = n.children.get(0);
                let right_child = n.children.get(1);

                // Left keys
                for lk in j.left_keys.iter_mut() {
                    if let Some(ch) = left_child {
                        if let Some(resolved) = resolve_by_index(lk, ch) {
                            *lk = resolved;
                            continue;
                        }
                    }
                    *lk = qualify_with(lk, &l_direct, &l_subtree);
                }

                // Right keys
                for rk in j.right_keys.iter_mut() {
                    if let Some(ch) = right_child {
                        if let Some(resolved) = resolve_by_index(rk, ch) {
                            *rk = resolved;
                            continue;
                        }
                    }
                    *rk = qualify_with(rk, &r_direct, &r_subtree);
                }

                // Remove any leftover "#<idx>" tags
                for lk in j.left_keys.iter_mut() {
                    *lk = strip_idx_suffix(lk);
                }
                for rk in j.right_keys.iter_mut() {
                    *rk = strip_idx_suffix(rk);
                }

                // Rebuild condition & op_kind.on_pairs
                if !j.left_keys.is_empty() && j.left_keys.len() == j.right_keys.len() {
                    j.condition = Some(
                        j.left_keys
                            .iter()
                            .zip(j.right_keys.iter())
                            .map(|(l, r)| format!("{l} = {r}"))
                            .collect::<Vec<_>>()
                            .join(" AND "),
                    );

                    if let Some(ok) = n.details.op_kind.as_mut() {
                        ok.on_pairs = Some(
                            j.left_keys
                                .iter()
                                .zip(j.right_keys.iter())
                                .map(|(l, r)| {
                                    let mut m = BTreeMap::new();
                                    m.insert("left".into(), l.clone());
                                    m.insert("op".into(), "=".into());
                                    m.insert("right".into(), r.clone());
                                    m
                                })
                                .collect(),
                        );
                    }
                }
            }
        }
        _ => {}
    }
}


pub fn qualify_active_columns(n: &mut Node) {
    // post-order
    for ch in n.children.iter_mut() {
        qualify_active_columns(ch);
    }

    // Build a unique (by suffix) map from merged child slots
    let merged_child = collect_child_slots(&n.children);
    let map = unique_suffix_map(&merged_child);

    // qualify each token if it's a simple (non dotted) identifier
    let mut out: Vec<String> = Vec::with_capacity(n.columns.active.len());
    let mut seen = std::collections::BTreeSet::new();
    for tok in n.columns.active.iter() {
        let mut q = tok.to_lowercase();
        if is_simple_token(tok) && !tok.contains('.') {
            if let Some(full) = map.get(&tok.to_lowercase()) {
                q = full.clone();
            }
        }
        if seen.insert(q.clone()) {
            out.push(q);
        }
    }
    n.columns.active = out;
}


// ────────────────────────────────────────────────────────────────────────────────
// Plan -> Node builder
// ────────────────────────────────────────────────────────────────────────────────

pub struct BuildCtx {
    next_id: usize,
}

impl BuildCtx {
    pub fn new() -> Self {
        Self { next_id: 0 }
    }
    fn alloc_id(&mut self) -> usize {
        let v = self.next_id;
        self.next_id += 1;
        v
    }
}

fn aggregate_metrics(ms: MetricsSet) -> (Option<u64>, Option<f64>) {
    let a = ms.aggregate_by_name();
    let out = a.output_rows().map(|v| v as u64);
    let ms = a.elapsed_compute().map(|ns| (ns as f64) / 1_000_000.0);
    (out, ms)
}

pub fn build_node(ctx: &mut BuildCtx, plan: Arc<dyn ExecutionPlan>) -> Node {
    // Children first
    let children: Vec<Node> = plan
        .children()
        .iter()
        .map(|ch| build_node(ctx, Arc::clone(ch)))
        .collect();

    // Metrics
    let (rows_out, elapsed_ms) = plan.metrics().map(aggregate_metrics).unwrap_or((None, None));
    let rows_in = {
        let mut sum = 0u64;
        let mut any = false;
        for ch in &children {
            if let Some(v) = ch.metrics.rows_out {
                sum += v;
                any = true;
            }
        }
        any.then_some(sum)
    };

    // Node shell
    let df_name = plan.name().to_string();
    let op = normalize(&df_name).to_string();
    let mut node = Node::new(ctx.alloc_id(), op.clone(), df_name);
    node.children = children;
    node.metrics.elapsed_ms = elapsed_ms;
    node.metrics.rows_out = rows_out;
    node.metrics.rows_in = rows_in;

    // Per-operator details and "active columns"
    let mut active = BTreeSet::new();

    // Projection
    if let Some(px) = plan.as_any().downcast_ref::<ProjectionExec>() {
        let exprs: Vec<String> = px
            .expr()
            .iter()
            .map(|(e, _)| {
                collect_active_columns_from_expr(e, &mut active);
                expr_pretty(e)
            })
            .collect();

        // Slot lineage: each projected expr -> set of base columns referenced
        let slots: Vec<Vec<String>> = px
            .expr()
            .iter()
            .map(|(e, _)| {
                let mut cols = BTreeSet::new();
                collect_active_columns_from_expr(e, &mut cols);
                cols.into_iter().map(|s| s.to_lowercase()).collect()
            })
            .collect();

        node.details.project = Some(ProjectDet { expressions: exprs });
        node._slot_lineage = Some(slots);
    }

    // Filter
    if let Some(fx) = plan.as_any().downcast_ref::<FilterExec>() {
        let pred = fx.predicate();
        collect_active_columns_from_expr(pred, &mut active);

        // Keep Debug for complex predicates; pretty only when it's a single Column
        let pred_s = if pred.as_any().downcast_ref::<expressions::Column>().is_some() {
            expr_pretty(pred)
        } else {
            stringify_expr(pred)
        };
        node.details.filter = Some(FilterDet {
            predicates: vec![pred_s.clone()],
        });
        node.details.op_kind = Some(op_kind_from_text(&pred_s));
    }

    // Aggregate
    if let Some(ax) = plan.as_any().downcast_ref::<AggregateExec>() {
        let groups: Vec<String> = ax
            .group_expr()
            .expr()
            .iter()
            .map(|(ge, _)| {
                collect_active_columns_from_expr(ge, &mut active);
                expr_pretty(ge)
            })
            .collect();

        let aggs: Vec<String> = ax.aggr_expr().iter().map(|a| format!("{a:?}")).collect();

        node.details.aggregate = Some(AggregateDet {
            group_keys: groups.clone(),
            aggregates: aggs.clone(),
            algorithm: None,
        });
        node.details.op_kind = Some(op_kind_from_text(&(groups.join(" ") + " " + &aggs.join(" "))));
    }

    // Join
    if let Some(jx) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        for (l, r) in jx.on() {
            collect_active_columns_from_expr(l, &mut active);
            collect_active_columns_from_expr(r, &mut active);
            left_keys.push(expr_with_index(l));
            right_keys.push(expr_with_index(r));
        }
        let jt = format!("{:?}", jx.join_type()).to_uppercase();

        let condition = (!left_keys.is_empty()).then(|| {
            left_keys
                .iter()
                .zip(right_keys.iter())
                .map(|(l, r)| format!("{l} = {r}"))
                .collect::<Vec<_>>()
                .join(" AND ")
        });

        let on_pairs: Vec<BTreeMap<String, String>> = left_keys
            .iter()
            .zip(right_keys.iter())
            .map(|(l, r)| {
                let mut m = BTreeMap::new();
                m.insert("left".into(), l.clone());
                m.insert("op".into(), "=".into());
                m.insert("right".into(), r.clone());
                m
            })
            .collect();

        node.details.join = Some(JoinDet {
            join_type: Some(jt),
            condition,
            left_keys,
            right_keys,
        });
        node.details.op_kind = Some(OpKind {
            comparisons: Some(vec!["=".into()]),
            logical: None,
            aggregates: None,
            normalized: Some(vec!["eq".into()]),
            on_pairs: Some(on_pairs),
        });
    }


    // NestedLoopJoin
    if let Some(nlj) = plan.as_any().downcast_ref::<NestedLoopJoinExec>() {
        // join type (e.g., INNER/LEFT/RIGHT/FULL)
        let jt = format!("{:?}", nlj.join_type()).to_uppercase();

        // optional non-equi filter (JoinFilter) -> expression() is a PhysicalExpr
        let cond_txt = nlj.filter().map(|f| {
            let e = f.expression();
            // collect active cols for lineage/active-columns
            collect_active_columns_from_expr(&e, &mut active);
            format!("{e:?}")
        });

        node.details.join = Some(JoinDet {
            join_type: Some(jt),
            condition: cond_txt.clone(),
            left_keys: vec![],
            right_keys: vec![],
        });

        // Drive UI tags from condition text (comparisons/logicals)
        if let Some(s) = cond_txt {
            node.details.op_kind = Some(op_kind_from_text(&s));
        }
    }

    // CrossJoin
    if let Some(cx) = plan.as_any().downcast_ref::<CrossJoinExec>() {
        node.details.join = Some(JoinDet {
            join_type: Some("CROSS".into()),
            condition: None,
            left_keys: vec![],
            right_keys: vec![],
        });
        // No op_kind.comparisons for CROSS
    }


    

    // Sort
    if let Some(sx) = plan.as_any().downcast_ref::<SortExec>() {
        let mut keys = Vec::new();
        for se in sx.expr() {
            collect_active_columns_from_expr(&se.expr, &mut active);
            keys.push(SortKey {
                expr: expr_pretty(&se.expr),
                asc: !se.options.descending,
                nulls_first: se.options.nulls_first,
            });
        }
        node.details.sort = Some(SortDet {
            keys,
            fetch: sx.fetch().map(|v| serde_json::json!(v)),
        });
    }

    // SortPreservingMerge
    if let Some(ms) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        let mut keys = Vec::new();
        for se in ms.expr() {
            collect_active_columns_from_expr(&se.expr, &mut active);
            keys.push(SortKey {
                expr: expr_pretty(&se.expr),
                asc: !se.options.descending,
                nulls_first: se.options.nulls_first,
            });
        }
        node.details.sort = Some(SortDet { keys, fetch: None });
    }

     // WindowAggExec
    if let Some(wx) = plan.as_any().downcast_ref::<WindowAggExec>() {
        // Keep Debug text; it carries "index: N" tags we can resolve later.
        let exprs: Vec<String> = wx
            .window_expr()
            .iter()
            .map(|we| {
                // Debug string of the WindowExpr
                let s = format!("{we:?}");
                // Collect active columns by scraping Column{...} from the debug
                for cap in COL_DBG_RE.captures_iter(&s) {
                    // push tail for active; it will be qualified later
                    active.insert(cap[1].to_string());
                }
                s
            })
            .collect();

        node.details.window = Some(crate::schema::WindowDet { expressions: exprs });
    }


    // Scan: synthesize table name guess and initial fixed-width sum
    if node.op == "Scan" {
        let out_cols: Vec<String> = plan.schema().fields().iter().map(|f| f.name().to_string()).collect();

        // var-width flags and per-column fixed widths
        use datafusion::arrow::datatypes::DataType as ADt;
        let mut varw = Vec::with_capacity(out_cols.len());
        let mut fixed = Vec::with_capacity(out_cols.len());
        for f in plan.schema().fields() {
            let dt = f.data_type();
            let is_var = matches!(
                dt,
                ADt::Utf8 | ADt::LargeUtf8 | ADt::Binary | ADt::LargeBinary | ADt::Utf8View | ADt::BinaryView
            );
            varw.push(is_var);
            fixed.push(scalar_size(dt));
        }

        let table_guess = infer_table_from_columns(&out_cols)
        .or_else(|| node.metrics.rows_out.and_then(infer_table_from_rowcount))
        .unwrap_or_default();
        node.details.scan = Some(ScanDet {
            table: table_guess,
            columns: out_cols,
            varwidth: Some(varw.clone()),
            fixed_widths: Some(fixed.clone()),
            pushdown_predicates: None,
        });

        // Initial scan width: sum of fixed widths only (no var payload yet)
        node.metrics.row_size_out_bytes = Some(fixed.into_iter().map(|x| x as f64).sum());
    }

    node.columns.active = active.into_iter().collect();

    // Lineage: postorder from here
    let _ = compute_lineage(&mut node);

    // Refine Projection slot lineage using child column indexes
    if let Some(px) = plan.as_any().downcast_ref::<ProjectionExec>() {
        if let Some(child_slots) = node.children.get(0).and_then(|c| c._slot_lineage.clone()) {
            let existing = node._slot_lineage.clone().unwrap_or_default();
            let mut refined: Vec<Vec<String>> = Vec::with_capacity(px.expr().len());

            for (i, (e, _)) in px.expr().iter().enumerate() {
                if let Some(c) = e.as_any().downcast_ref::<expressions::Column>() {
                    let idx = c.index();
                    if let Some(mapped) = child_slots.get(idx) {
                        if mapped.is_empty() {
                            refined.push(existing.get(i).cloned().unwrap_or_default());
                        } else {
                            refined.push(mapped.clone());
                        }
                    } else {
                        refined.push(existing.get(i).cloned().unwrap_or_default());
                    }
                } else {
                    refined.push(existing.get(i).cloned().unwrap_or_default());
                }
            }
            node._slot_lineage = Some(refined);
        }
    }

    node
}

// ────────────────────────────────────────────────────────────────────────────────
// Infra-time rollup + pruning and row-size reconciliation
// ────────────────────────────────────────────────────────────────────────────────

/// Roll infra time up and drop infra nodes (coalesce/repartition). Preserves behavior.
pub fn rollup_and_prune(mut n: Node) -> Node {
    let is_infra = matches!(
        n.name.as_str(),
        "CoalesceBatchesExec" | "CoalescePartitionsExec" | "RepartitionExec" | "SortPreservingMergeExec"
    );

    // Recurse and absorb children that are infra
    let mut new_children = Vec::new();
    let mut absorbed_ms = 0.0;
    for child in n.children.into_iter().map(rollup_and_prune) {
        let cinfra = matches!(
            child.name.as_str(),
            "CoalesceBatchesExec" | "CoalescePartitionsExec" | "RepartitionExec" | "SortPreservingMergeExec"
        );
        if cinfra {
            absorbed_ms += child.metrics.elapsed_ms.unwrap_or(0.0);
            new_children.extend(child.children);
        } else {
            new_children.push(child);
        }
    }
    n.children = new_children;

    if absorbed_ms > 0.0 {
        let cur = n.metrics.elapsed_ms.unwrap_or(0.0);
        n.metrics.elapsed_ms = Some(cur + absorbed_ms);
        n.metrics.infra_ms_accumulated = Some(absorbed_ms);
    }

    if is_infra {
        let own_ms = n.metrics.elapsed_ms.unwrap_or(0.0);
        if n.children.is_empty() {
            return n;
        }
        let mut pushed = Vec::new();
        for mut ch in n.children {
            ch.metrics.elapsed_ms = Some(ch.metrics.elapsed_ms.unwrap_or(0.0) + own_ms);
            let infra = ch.metrics.infra_ms_accumulated.unwrap_or(0.0);
            ch.metrics.infra_ms_accumulated = Some(infra + own_ms);
            pushed.push(ch);
        }

        // If exactly one child, copy SPME's "final output" onto it.
        if pushed.len() == 1 && n.name == "SortPreservingMergeExec" {
            let mut ch = pushed.remove(0);
            if n.metrics.rows_out.is_some() {
                ch.metrics.rows_out = n.metrics.rows_out;
            }
            if n.metrics.row_size_out_bytes.is_some() {
                ch.metrics.row_size_out_bytes = n.metrics.row_size_out_bytes;
            }
            return ch;
        }

        return if pushed.len() == 1 {
            pushed.into_iter().next().unwrap()
        } else {
            let mut s = Node::new(n.id, "Project", "SyntheticRoot");
            s.children = pushed;
            s.metrics = n.metrics;
            s
        };
    }

    n
}


/// After pruning, recompute rows_in by summing children rows_out (Join also exposes per-side inputs).
pub fn recompute_input_rows(n: &mut Node) -> Option<u64> {
    if n.children.is_empty() {
        return n.metrics.rows_out;
    }
    let mut sum = 0u64;
    let mut any = false;
    for ch in n.children.iter_mut() {
        if let Some(v) = recompute_input_rows(ch) {
            sum += v;
            any = true;
        }
    }
    n.metrics.rows_in = any.then_some(sum);

    if n.op == "Join" && n.children.len() >= 2 {
        n.metrics.rows_in_left = n.children[0].metrics.rows_out;
        n.metrics.rows_in_right = n.children[1].metrics.rows_out;
    } else {
        n.metrics.rows_in_left = None;
        n.metrics.rows_in_right = None;
    }

    n.metrics.rows_out
}

pub fn recompute_row_sizes(n: &mut Node) -> Option<f64> {
    if n.children.is_empty() {
        return n.metrics.row_size_out_bytes;
    }

    for ch in n.children.iter_mut() {
        let _ = recompute_row_sizes(ch);
    }

    let child_out = |n: &Node, i: usize| n.children.get(i).and_then(|c| c.metrics.row_size_out_bytes);

    match n.op.as_str() {

        "Join" => {
            let l = child_out(n, 0);
            let r = child_out(n, 1);
            n.metrics.row_size_in_left_bytes = l;
            n.metrics.row_size_in_right_bytes = r;

            let jt = n
                .details
                .join
                .as_ref()
                .and_then(|j| j.join_type.as_deref())
                .unwrap_or("INNER");

            n.metrics.row_size_out_bytes = match jt {
                "LEFT_SEMI" | "LEFT_ANTI" => l,
                "RIGHT_SEMI" | "RIGHT_ANTI" => r,
                _ => match (l, r) {
                    (Some(a), Some(b)) => Some(a + b),
                    _ => None,
                },
            };
        }
        _ => { /* other ops left as set by annotate_sizes */ }
    }

    n.metrics.row_size_out_bytes
}
