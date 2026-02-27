use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::BTreeSet;

use crate::schema::Node;

/// Normalize operator names
pub fn norm_op(raw_name: &str) -> &'static str {
    let base = raw_name.split_once('(').map(|(b, _)| b).unwrap_or(raw_name);
    match base {
        "ProjectionExec"                    => "Project",
        "FilterExec"                        => "Filter",
        "HashJoinExec" | "NestedLoopJoinExec" | "CrossJoinExec"              => "Join",
        "AggregateExec"                     => "Aggregate",
        "SortExec"                          => "Sort",
        "GlobalLimitExec" | "LocalLimitExec"=> "Limit",
        "ParquetExec" | "DataSourceExec"    => "Scan",
        "CoalesceBatchesExec" | "CoalescePartitionsExec" | "RepartitionExec" => "Other",
        _ => "Other",
    }
}

/// Matches DataFusion's Debug formatting for a column physical expr, e.g.:
///   Column { name: "owneruserid", index: 0 }
static COL_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"Column\s*\{\s*name:\s*"([^"]+)"(?:,\s*index:\s*\d+)?\s*\}"#).unwrap()
});

/// Collect (table -> set(cols)) from Scan nodes in a subtree.
fn collect_scans(n: &Node) -> Vec<(String, BTreeSet<String>)> {
    let mut out = vec![];
    if n.op == "Scan" {
        if let Some(s) = &n.details.scan {
            if !s.table.is_empty() {
                let cols = s
                    .columns
                    .iter()
                    .map(|c| c.to_lowercase())
                    .collect::<BTreeSet<_>>();
                out.push((s.table.to_lowercase(), cols));
            }
        }
    }
    for ch in &n.children {
        out.extend(collect_scans(ch));
    }
    out
}

/// If exactly one scanned table on this side has `col`, qualify it.
/// Otherwise, return `col` unchanged.
fn qualify(col: &str, scans: &[(String, BTreeSet<String>)]) -> String {
    let c = col.to_lowercase();
    let mut hits: Vec<&str> = vec![];
    for (t, cols) in scans {
        if cols.contains(&c) {
            hits.push(t.as_str());
        }
    }
    if hits.len() == 1 {
        format!("{}.{}", hits[0], c)
    } else {
        col.to_string()
    }
}

/// Walk the plan and normalize join key text:
///   - left/right key arrays
///   - readqble `condition`
///   - `op_kind.on_pairs`
/// Only affects Join nodes. only rewrites when unambiguous.
pub fn normalize_join_keys_text(n: &mut Node) {
    for ch in &mut n.children {
        normalize_join_keys_text(ch);
    }
    if n.op != "Join" || n.children.len() < 2 {
        return;
    }

    let left_scans  = collect_scans(&n.children[0]);
    let right_scans = collect_scans(&n.children[1]);

    // Update details.join
    if let Some(j) = &mut n.details.join {
        // Normalize arrays
        let norm_left: Vec<String> = j.left_keys.iter().map(|s| {
            if let Some(c) = COL_RE.captures(s) {
                qualify(&c[1], &left_scans)
            } else {
                s.clone()
            }
        }).collect();

        let norm_right: Vec<String> = j.right_keys.iter().map(|s| {
            if let Some(c) = COL_RE.captures(s) {
                qualify(&c[1], &right_scans)
            } else {
                s.clone()
            }
        }).collect();

        j.left_keys = norm_left.clone();
        j.right_keys = norm_right.clone();

        // Rebuild condition
        if !norm_left.is_empty() && norm_left.len() == norm_right.len() {
            j.condition = Some(
                norm_left.iter()
                    .zip(norm_right.iter())
                    .map(|(l, r)| format!("{l} = {r}"))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            );
        }
    }

    // Update op_kind.on_pairs
    if let Some(ok) = &mut n.details.op_kind {
        if let Some(pairs) = &mut ok.on_pairs {
            for p in pairs {
                if let Some(l) = p.get("left").cloned() {
                    if let Some(c) = COL_RE.captures(&l) {
                        p.insert("left".into(), qualify(&c[1], &left_scans));
                    }
                }
                if let Some(r) = p.get("right").cloned() {
                    if let Some(c) = COL_RE.captures(&r) {
                        p.insert("right".into(), qualify(&c[1], &right_scans));
                    }
                }
            }
        }
    }
}
