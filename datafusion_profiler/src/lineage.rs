use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};

use crate::schema::{Details, Node};

static SLOT_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"#(\d+)").unwrap());
static INDEX_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"index:\s*(\d+)").unwrap());

fn indices_in_text(expr: &str) -> Vec<usize> {
    INDEX_RE
        .captures_iter(expr)
        .filter_map(|c| c.get(1).and_then(|m| m.as_str().parse::<usize>().ok()))
        .collect()
}

// Merge multiple child-slot lists referenced by indexes, with dedup + stable order.
fn slots_for_indices(child: &[Vec<String>], idxs: &[usize]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for &i in idxs {
        if let Some(slot) = child.get(i) {
            for base in slot {
                if seen.insert(base.clone()) {
                    out.push(base.clone());
                }
            }
        }
    }
    out
}

fn find_functions(expr: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for cap in Regex::new(r"([A-Za-z_][A-Za-z0-9_\.]*)\s*\(")
        .unwrap()
        .captures_iter(expr)
    {
        out.insert(cap[1].to_lowercase());
    }
    out
}

fn tokens(expr: &str) -> Vec<String> {
    let mut s = expr.to_string();
    s = Regex::new(r"'.*?'").unwrap().replace_all(&s, " ").to_string();
    s = Regex::new(r#""[^"]*""#).unwrap().replace_all(&s, " ").to_string();
    s = Regex::new(r"`[^`]*`").unwrap().replace_all(&s, " ").to_string();
    Regex::new(r"[A-Za-z_][A-Za-z0-9_\.]*")
        .unwrap()
        .find_iter(&s)
        .map(|m| m.as_str().to_string())
        .collect()
}

fn slot_idxs(expr: &str) -> Vec<usize> {
    SLOT_RE
        .captures_iter(expr)
        .filter_map(|c| c.get(1).and_then(|m| m.as_str().parse::<usize>().ok()))
        .collect()
}

fn is_sql_kw(s: &str) -> bool {
    matches!(
        s,
        "select" | "from" | "where" | "group" | "by" | "order" | "limit" | "offset" | "join" | "on"
            | "and" | "or" | "not" | "as" | "case" | "when" | "then" | "else" | "end"
            | "distinct" | "all" | "asc" | "desc" | "null" | "is" | "in" | "between" | "like" | "exists"
            | "true" | "false" | "with" | "over" | "partition" | "rows" | "range" | "current" | "row"
            | "preceding" | "following" | "unbounded" | "outer" | "inner" | "left" | "right" | "full"
            | "cross" | "using" | "union" | "intersect" | "except" | "having" | "nulls" | "first" | "last"
            | "timestamp"
    )
}

fn dedup_keep_order(xs: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = vec![];
    for x in xs {
        if seen.insert(x.clone()) {
            out.push(x);
        }
    }
    out
}

fn flat_lineage(child_slots: &[Vec<String>]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = vec![];
    for lst in child_slots {
        for c in lst {
            if seen.insert(c.clone()) {
                out.push(c.clone());
            }
        }
    }
    out
}

// Suffix resolution: prefer unique matches by tail.
fn resolve_tokens_against_child(names: &[String], child_slots: &[Vec<String>]) -> Vec<String> {
    if names.is_empty() {
        return vec![];
    }
    let mut suffix_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for bc in flat_lineage(child_slots) {
        let tail = bc.split('.').last().unwrap_or(&bc).to_string();
        suffix_map.entry(tail).or_default().push(bc);
    }
    let mut resolved = vec![];
    for tok in names {
        let tail = tok.split('.').last().unwrap_or(tok).to_lowercase();
        if let Some(hits) = suffix_map.get(&tail) {
            if hits.len() == 1 {
                resolved.push(hits[0].clone());
            } else {
                resolved.push(tok.to_lowercase()); // ambiguous => keep
            }
        } else {
            resolved.push(tok.to_lowercase());
        }
    }
    dedup_keep_order(resolved)
}

fn named_cols(expr: &str) -> Vec<String> {
    if expr.is_empty() {
        return vec![];
    }
    let fnames = find_functions(expr);
    let mut out = vec![];
    for tok in tokens(expr) {
        let low = tok.to_lowercase();
        if is_sql_kw(&low) || fnames.contains(&low) || low.starts_with("__internal_") {
            continue;
        }
        out.push(tok);
    }
    dedup_keep_order(out)
}

fn resolve_expr(expr: &str, child_slots: &[Vec<String>]) -> Vec<String> {
    if expr.is_empty() {
        return vec![];
    }
    let mut cols = vec![];
    for idx in slot_idxs(expr) {
        if idx < child_slots.len() {
            cols.extend(child_slots[idx].clone());
        }
    }
    cols.extend(resolve_tokens_against_child(&named_cols(expr), child_slots));
    dedup_keep_order(cols)
}

pub fn collect_child_slots(children: &[Node]) -> Vec<Vec<String>> {
    if children.is_empty() {
        return vec![];
    }
    children
        .iter()
        .flat_map(|c| c._slot_lineage.clone().unwrap_or_default())
        .collect()
}

fn scan_slots(det: &Details) -> Vec<Vec<String>> {
    let cols = det.scan.as_ref().map(|s| s.columns.clone()).unwrap_or_default();
    let table = det.scan.as_ref().map(|s| s.table.clone()).unwrap_or_default();
    if table.is_empty() {
        cols.into_iter().map(|c| vec![c.to_lowercase()]).collect()
    } else {
        cols.into_iter()
            .map(|c| vec![format!("{}.{}", table.to_lowercase(), c.to_lowercase())])
            .collect()
    }
}

fn project_slots(det: &Details, child: &[Vec<String>]) -> Vec<Vec<String>> {
    det.project
        .as_ref()
        .map(|p| {
            p.expressions
                .iter()
                .map(|e| {
                    // Prefer index-based mapping (exact) when available
                    let idxs = indices_in_text(e);
                    if !idxs.is_empty() {
                        let cols = slots_for_indices(child, &idxs);
                        if !cols.is_empty() {
                            return cols;
                        }
                    }
                    // Fallback to token / suffix resolution
                    resolve_expr(e, child)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn window_slots(det: &Details, child: &[Vec<String>]) -> Vec<Vec<String>> {
    det.window
        .as_ref()
        .map(|w| {
            let mut out = child.to_vec();
            for e in &w.expressions {
                out.push(resolve_expr(e, child));
            }
            out
        })
        .unwrap_or_else(|| child.to_vec())
}

// replace the aggregate_slots implementation in lineage.rs with:
fn aggregate_slots(det: &Details, child: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut out = vec![];
    if let Some(ag) = det.aggregate.as_ref() {
        // group keys: prefer index mapping when present, else token/suffix
        for g in &ag.group_keys {
            let idxs = indices_in_text(g);
            if !idxs.is_empty() {
                out.push(slots_for_indices(child, &idxs));
            } else {
                out.push(resolve_expr(g, child));
            }
        }
        // aggregates
        for a in &ag.aggregates {
            let idxs = indices_in_text(a);
            if !idxs.is_empty() {
                out.push(slots_for_indices(child, &idxs));
            } else {
                out.push(resolve_expr(a, child));
            }
        }
    }
    out
}


fn join_slots(left: &[Vec<String>], right: &[Vec<String>]) -> Vec<Vec<String>> {
    let mut out = left.to_vec();
    out.extend_from_slice(right);
    out
}


pub fn compute_lineage(node: &mut Node) -> Vec<Vec<String>> {
    fn norm(op: &str) -> &str {
        match op {
            "ProjectionExec" => "Project",
            "FilterExec" => "Filter",
            "HashJoinExec" | "NestedLoopJoinExec" | "CrossJoinExec" => "Join",
            "AggregateExec" => "Aggregate",
            "SortExec" | "SortPreservingMergeExec" => "Sort",
            "DataSourceExec" => "Scan",
            "WindowAggExec" => "Window",

            _ => "Other",
        }
    }

    let op = norm(&node.name).to_string();

    // Join has two children: merge left + right
    if op == "Join" && node.children.len() == 2 {
        let left = compute_lineage(&mut node.children[0]);
        let right = compute_lineage(&mut node.children[1]);
        let mut merged = left.clone();
        merged.extend(right.clone());

        if node.columns.active.is_empty() {
            node.columns.active = infer_active_columns(&op, &node.details, &merged);
        }
        if node._slot_lineage.is_none() {
            node._slot_lineage = Some(join_slots(&left, &right));
        }
        return node._slot_lineage.clone().unwrap();
    }

    // Unary / leaf
    let child_slots = if !node.children.is_empty() {
        compute_lineage(&mut node.children[0])
    } else {
        vec![]
    };

    if node.columns.active.is_empty() {
        node.columns.active = infer_active_columns(&op, &node.details, &child_slots);
    }

    if node._slot_lineage.is_none() {
        let out = match op.as_str() {
            "Scan" => scan_slots(&node.details),
            "Project" => project_slots(&node.details, &child_slots),
            "Filter" | "Sort" | "Limit" | "Other" => child_slots.clone(),
            "Window" => window_slots(&node.details, &child_slots),
            "Aggregate" => aggregate_slots(&node.details, &child_slots),
            _ => child_slots.clone(),
        };
        node._slot_lineage = Some(out);
    }

    node._slot_lineage.clone().unwrap()
}

/// Collect expressions used by each operator and resolve to base columns.
pub fn infer_active_columns(op: &str, det: &Details, child_or_merged: &[Vec<String>]) -> Vec<String> {
    let mut exprs: Vec<String> = vec![];
    match op {
        "Scan" => {
            if let Some(s) = &det.scan {
                if let Some(ps) = &s.pushdown_predicates {
                    exprs.extend(ps.clone());
                }
                exprs.extend(s.columns.clone());
            }
        }
        "Filter" => {
            if let Some(f) = &det.filter {
                exprs.extend(f.predicates.clone());
            }
        }
        "Aggregate" => {
            if let Some(a) = &det.aggregate {
                exprs.extend(a.group_keys.clone());
                exprs.extend(a.aggregates.clone());
            }
        }
        "Sort" => {
            if let Some(s) = &det.sort {
                for k in &s.keys {
                    exprs.push(k.expr.clone());
                }
            }
        }
        "Join" => {
            if let Some(j) = &det.join {
                if let Some(c) = &j.condition {
                    exprs.push(c.clone());
                }
                exprs.extend(j.left_keys.clone());
                exprs.extend(j.right_keys.clone());
            }
        }
        "Window" => {
            if let Some(w) = &det.window {
                exprs.extend(w.expressions.clone());
            }
        }
        _ => {}
    }
    let mut cols = vec![];
    for e in exprs {
        cols.extend(resolve_expr(&e, child_or_merged));
    }
    dedup_keep_order(cols)
}
