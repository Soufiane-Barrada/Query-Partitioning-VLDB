use parquet::file::reader::{FileReader, SerializedFileReader};
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use regex::Regex;


use datafusion::arrow::datatypes::DataType;
use parquet::file::statistics::Statistics as ParquetStats;



use crate::schema::Node;


static CATALOG: OnceCell<ParquetWidthCatalog> = OnceCell::new();

pub fn init_parquet_catalog(dir: &Path) {
    let cat = build_catalog(dir.to_string_lossy().as_ref());
    CATALOG.set(cat).expect("init_parquet_catalog called more than once");
}

/// include a per-column null validity bitmap overhead (0.125 bytes/value)
const INCLUDE_NULL_BITMAP: bool = true;

/// Public so adapter can reuse the fixed-width sizes for Arrow types
pub(crate) fn scalar_size(dt: &DataType) -> u64 {
    use datafusion::arrow::datatypes::DataType::*;
    match dt {
        // fixed-width scalars
        Null => 0,
        Boolean => 1,
        Int8 | UInt8 => 1,
        Int16 | UInt16 => 2,
        Int32 | UInt32 | Date32 | Time32(_) | Interval(_) => 4,
        Int64 | UInt64 | Date64 | Time64(_) | Duration(_) | Timestamp(_, _) => 8,
        Float16 => 2,
        Float32 => 4,
        Float64 => 8,

        Decimal128(_, _) => 16,
        Decimal256(_, _) => 32,

        // variable-width “header”
        Binary | LargeBinary | Utf8 | LargeUtf8 => 12,
        BinaryView | Utf8View => 12,

        List(_)
        | LargeList(_)
        | FixedSizeList(_, _)
        | Struct(_)
        | Map(_, _)
        | Union(_, _) 
        | Dictionary(_, _)
        | ListView(_)
        | LargeListView(_)
        | RunEndEncoded(_, _) => 16,

        FixedSizeBinary(n) => *n as u64,

        _ => 12,
    }
}


// Parquet width catalog (avg uncompressed bytes per cell)
#[derive(Debug, Default)]
struct ParquetWidthCatalog {
    // table -> col -> avg_uncompressed_bytes
    table_col: HashMap<String, HashMap<String, f64>>,
    // col -> avg if unique across tables
    unique_col: HashMap<String, f64>,
    // table.col -> has_nulls (any row group has null_count > 0)
    has_nulls: BTreeSet<String>,
}


fn build_catalog(dir: &str) -> ParquetWidthCatalog {
    let mut cat = ParquetWidthCatalog::default();
    let p = Path::new(dir);
    if !p.exists() {
        return cat;
    }

    let mut global_by_col: HashMap<String, Vec<(String, f64)>> = HashMap::new();

    if let Ok(rd) = std::fs::read_dir(p) {
        for ent in rd.flatten() {
            let path = ent.path();
            if !path.is_file() || path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                continue;
            }
            let table = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s.to_lowercase(),
                None => continue,
            };
            if let Some(per_col) = read_parquet_avgs_and_nulls(&path, &mut cat.has_nulls, &table) {
                let mut m = HashMap::new();
                for (col, avg) in per_col {
                    m.insert(col.clone(), avg);
                    global_by_col.entry(col).or_default().push((table.clone(), avg));
                }
                cat.table_col.insert(table, m);
            }
        }
    }

    // populate unique_col only if a name appears in exactly one table
    for (col, vecs) in global_by_col {
        if vecs.len() == 1 {
            cat.unique_col.insert(col, vecs[0].1);
        }
    }

    cat
}

fn read_parquet_avgs_and_nulls(
    path: &Path,
    has_nulls_out: &mut BTreeSet<String>,
    table: &str,
) -> Option<HashMap<String, f64>> {
    let file = File::open(path).ok()?;
    let reader = SerializedFileReader::new(file).ok()?;
    let meta = reader.metadata();
    let file_meta = meta.file_metadata();

    // physical column order
    let schema = file_meta.schema_descr();
    let mut names = Vec::new();
    for i in 0..schema.num_columns() {
        names.push(schema.column(i).path().string().to_lowercase());
    }

    let mut totals: Vec<(u64, u64)> = vec![(0, 0); names.len()]; // (bytes, values)
    let mut null_any: Vec<bool> = vec![false; names.len()];

    for i in 0..meta.num_row_groups() {
        let rg = meta.row_group(i);
        for c in 0..rg.num_columns() {
            let cc = rg.column(c);
            let bytes = cc.uncompressed_size() as u64;
            let vals = cc.num_values() as u64;
            totals[c].0 += bytes;
            totals[c].1 += vals;

            if let Some(stats) = cc.statistics() {
                if let Some(nc) = parquet_null_count(stats) {
                    if nc > 0 {
                        null_any[c] = true;
                    }
                }
            }
        }
    }

    let mut out = HashMap::new();
    for (idx, (bytes, vals)) in totals.into_iter().enumerate() {
        if vals > 0 {
            out.insert(names[idx].clone(), (bytes as f64) / (vals as f64));
        }
        if null_any[idx] {
            has_nulls_out.insert(format!("{}.{}", table, names[idx]));
        }
    }
    Some(out)
}

fn parquet_null_count(stats: &ParquetStats) -> Option<i64> {
    // In current parquet, use the *_opt variant which returns Option<u64>
    macro_rules! get_nc {
        ($s:expr) => {
            $s.null_count_opt().map(|v| v as i64)
        };
    }
    match stats {
        ParquetStats::Boolean(s)           => get_nc!(s),
        ParquetStats::Int32(s)             => get_nc!(s),
        ParquetStats::Int64(s)             => get_nc!(s),
        ParquetStats::Int96(s)             => get_nc!(s),
        ParquetStats::Float(s)             => get_nc!(s),
        ParquetStats::Double(s)            => get_nc!(s),
        ParquetStats::ByteArray(s)         => get_nc!(s),
        ParquetStats::FixedLenByteArray(s) => get_nc!(s),
    }
}



// -------- width helpers --------

/// Payload-only, averaged uncompressed bytes/value from parquet.
fn payload_avg_for_base_col(token: &str) -> Option<f64> {
    let cat = CATALOG.get()?; // requires init_parquet_catalog() to have been called
    let t = token.to_lowercase();
    if let Some((tbl, col)) = t.split_once('.') {
        return cat.table_col.get(tbl).and_then(|m| m.get(col)).copied();
    }
    cat.unique_col.get(&t).copied()
}


fn has_nulls(base: &str) -> bool {
    if !INCLUDE_NULL_BITMAP {
        return false;
    }
    let cat = match CATALOG.get() {
        Some(c) => c,
        None => return false,
    };
    let t = base.to_lowercase();
    if let Some((tbl, col)) = t.split_once('.') {
        cat.has_nulls.contains(&format!("{}.{}", tbl, col))
    } else {
        // name-only checks are ambiguous; keep conservative default (false)
        false
    }
}


/// Build map "table.col" -> (is_varwidth, fixed_width_bytes) by walking Scan nodes.
fn build_scan_width_map(n: &Node) -> BTreeMap<String, (bool, f64)> {
    fn walk(node: &Node, m: &mut BTreeMap<String, (bool, f64)>) {
        if let Some(s) = node.details.scan.as_ref() {
            let tbl = s.table.to_lowercase();
            let varw = s
                .varwidth
                .clone()
                .unwrap_or_else(|| vec![false; s.columns.len()]);
            let fixed = s.fixed_widths.clone().unwrap_or_default();
            for (i, c) in s.columns.iter().enumerate() {
                let base = if tbl.is_empty() {
                    c.to_lowercase()
                } else {
                    format!("{}.{}", tbl, c.to_lowercase())
                };
                let is_var = varw.get(i).copied().unwrap_or(false);
                let fw = fixed.get(i).copied().unwrap_or(12) as f64;
                m.insert(base, (is_var, fw));
            }
        }
        for ch in &node.children {
            walk(ch, m);
        }
    }
    let mut m = BTreeMap::new();
    walk(n, &mut m);
    m
}

/// Width of one base column, using the scan map for fixed vs var-width.
/// For var-width: header + parquet payload + plus 0.125 if has_nulls.
fn width_of_base(base: &str, scan_map: &BTreeMap<String, (bool, f64)>) -> f64 {
    let mut w = if let Some((is_var, fixed)) = scan_map.get(&base.to_lowercase()) {
        if *is_var {
            let payload = payload_avg_for_base_col(base).unwrap_or(16.0);
            fixed + payload
        } else {
            *fixed
        }
    } else {
        // Fallbacks if we couldn't find a typed mapping: treat as var-width with default payload.
        payload_avg_for_base_col(base).map(|p| 12.0 + p).unwrap_or(24.0)
    };

    if has_nulls(base) {
        w += 0.125;
    }
    w
}

// annotate_sizes (main)

pub fn annotate_sizes(root: &mut Node, _mode: super::annotate::SizeMode) {
    let scan_map = build_scan_width_map(root);

    fn visit(n: &mut Node, scan_map: &BTreeMap<String, (bool, f64)>) -> f64 {
        let mut child_sizes = Vec::new();
        for c in n.children.iter_mut() {
            child_sizes.push(visit(c, scan_map));
        }

        let op = n.op.as_str();
        let mut out_bytes: f64 = 0.0;

        match op {
            "Scan" => {
                // Start from initial fixed widths sum (set in adapter), then add ONLY var-width payloads
                let mut sum = n.metrics.row_size_out_bytes.unwrap_or(0.0);
                if let Some(scan) = &n.details.scan {
                    let tbl = scan.table.to_lowercase();
                    let cols = &scan.columns;
                    let varw = scan
                        .varwidth
                        .clone()
                        .unwrap_or_else(|| vec![false; cols.len()]);
                    for (i, c) in cols.iter().enumerate() {
                        let base = if tbl.is_empty() {
                            c.to_lowercase()
                        } else {
                            format!("{}.{}", tbl, c.to_lowercase())
                        };
                        // add payload only for var-width (header already in fixed sum)
                        if varw.get(i).copied().unwrap_or(false) {
                            let payload = payload_avg_for_base_col(&base).unwrap_or(16.0);
                            sum += payload;
                        }
                        // add null bitmap overhead if applicable
                        if has_nulls(&base) {
                            sum += 0.125;
                        }
                    }
                }
                n.metrics.row_size_out_bytes = Some(sum);
                out_bytes = sum;
            }

            // Project: per expression:
            //   if exactly 1 base dependency -> width(base)
            //   else (0 or >=2 deps) -> 8 bytes
            "Project" => {
                let mut sum = 0.0_f64;
                if let Some(slots) = &n._slot_lineage {
                    for slot in slots {
                        let slot_w = if slot.len() == 1 {
                            width_of_base(&slot[0], scan_map)
                        } else {
                            8.0
                        };
                        sum += slot_w;
                    }
                } else if let Some(child) = child_sizes.get(0).copied() {
                    sum = child;
                }
                n.metrics.row_size_in_bytes = child_sizes.get(0).copied();
                n.metrics.row_size_out_bytes = Some(sum);
                out_bytes = sum;
            }

            // Aggregate:
            //   out = sum(widths of all group keys) + sum(widths of aggregates)
            //   COUNT/SUM/AVG => 8
            //   MIN/MAX => width(arg)
            //   ARRAY_AGG/STRING_AGG => 64
            //   else => 16
            "Aggregate" => {
                let mut gk_bytes = 0.0_f64;
                let mut agg_bytes = 0.0_f64;

                let gk_count = n
                    .details
                    .aggregate
                    .as_ref()
                    .map(|a| a.group_keys.len())
                    .unwrap_or(0);

                // group keys come first in slot lineage — sum widths of all their base columns
                if let Some(slots) = &n._slot_lineage {
                    for i in 0..gk_count.min(slots.len()) {
                        for base in &slots[i] {
                            gk_bytes += width_of_base(base, scan_map);
                        }
                    }
                }

                // Aggregate functions
                let fn_names: Vec<String> = if let Some(ok) = &n.details.op_kind {
                    ok.aggregates
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .map(|s| s.to_lowercase())
                        .collect()
                } else if let Some(a) = &n.details.aggregate {
                    // fallback: try to parse function names from debug strings
                    a.aggregates
                        .iter()
                        .map(|s| detect_agg_name(s))
                        .map(|s| s.to_lowercase())
                        .collect()
                } else {
                    Vec::new()
                };

                // Find first arg base width for MIN/MAX if we can (best-effort)
                let mut minmax_arg_width: Option<f64> = None;
                if fn_names.iter().any(|f| f == "min" || f == "max") {
                    if let Some(arg_base) = first_base_arg_from_aggregate_text(
                        n,
                        scan_map,
                        n.details.aggregate.as_ref().map(|a| &a.aggregates),
                    ) {
                        minmax_arg_width = Some(width_of_base(&arg_base, scan_map));
                    }
                }

                for fname in fn_names {
                    let w = match fname.as_str() {
                        "count" | "sum" | "avg" => 8.0,
                        "min" | "max" => minmax_arg_width.unwrap_or(8.0),
                        "array_agg" | "string_agg" => 64.0,
                        _ => 16.0,
                    };
                    agg_bytes += w;
                }

                let sum = gk_bytes + agg_bytes;
                n.metrics.row_size_in_bytes = child_sizes.get(0).copied();
                n.metrics.row_size_out_bytes = Some(sum);
                out_bytes = sum;
            }

            // Pass-through operators
            "Filter" | "Sort" | "Limit" => {
                out_bytes = child_sizes.get(0).copied().unwrap_or(0.0);
                n.metrics.row_size_in_bytes = child_sizes.get(0).copied();
                n.metrics.row_size_out_bytes = Some(out_bytes);
            }

            // Window: 
            "Window" => {
                let input = child_sizes.get(0).copied().unwrap_or(0.0);

                // Add widths for derived window outputs if present
                let mut extra = 0.0_f64;
                if n.details.window.is_some() {
                    if let Some(slots) = &n._slot_lineage {
                        // how many slots come from the child?
                        let child_slot_count = n.children
                            .get(0)
                            .and_then(|c| c._slot_lineage.clone())
                            .map(|v| v.len())
                            .unwrap_or(0);

                        for slot in slots.iter().skip(child_slot_count) {
                            // rule: width(base) if exactly one base dep; else 8.0
                            let w = if slot.len() == 1 {
                                width_of_base(&slot[0], scan_map)
                            } else {
                                8.0
                            };
                            extra += w;
                        }
                    }
                }

                let out = input + extra;
                n.metrics.row_size_in_bytes = Some(input);
                n.metrics.row_size_out_bytes = Some(out);
                out_bytes = out;
            }



            // Join: out = left + right; expose per-side sizes
            "Join" => {
                let l = child_sizes.get(0).copied().unwrap_or(0.0);
                let r = child_sizes.get(1).copied().unwrap_or(0.0);
                n.metrics.row_size_out_bytes = Some(l + r);
                out_bytes = l + r;
            }

            // Other: pass-through
            _ => {
                out_bytes = child_sizes.get(0).copied().unwrap_or(0.0);
                n.metrics.row_size_out_bytes = Some(out_bytes);
            }
        }

        out_bytes
    }

    visit(root, &scan_map);
}

// helpers to parse aggregate text (best-effort)

fn detect_agg_name(s: &str) -> String {
    let u = s.to_uppercase();
    for (needle, norm) in [
        ("COUNT", "count"),
        ("SUM", "sum"),
        ("AVG", "avg"),
        ("MEAN", "avg"),
        ("MIN", "min"),
        ("MAX", "max"),
        ("ARRAY_AGG", "array_agg"),
        ("STRING_AGG", "string_agg"),
        ("VAR", "var"),
        ("STDDEV", "stddev"),
    ] {
        if u.contains(needle) {
            return norm.to_string();
        }
    }
    "other".to_string()
}


fn first_base_arg_from_aggregate_text(
    node: &Node,
    _scan_map: &BTreeMap<String, (bool, f64)>,
    agg_texts: Option<&Vec<String>>,
) -> Option<String> {
    let texts = agg_texts?;
    if texts.is_empty() {
        return None;
    }

    // collect all visible base columns in the subtree
    let mut slots: Vec<Vec<String>> = Vec::new();
    fn walk_slots(n: &Node, acc: &mut Vec<Vec<String>>) {
        if let Some(sl) = &n._slot_lineage {
            acc.extend(sl.clone());
        }
        for c in &n.children {
            walk_slots(c, acc);
        }
    }
    walk_slots(node, &mut slots);
    let unique = unique_suffix_map(&slots);

    // From DataFusion's Debug: Column { name: "owneruserid", index: 4 }
    static COL_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"Column\s*\{\s*name:\s*"([^"]+)".*?\}"#).unwrap()
    });

    for s in texts {
        if let Some(cap) = COL_RE.captures(s) {
            let tail = cap[1].to_lowercase();
            if let Some(full) = unique.get(&tail) {
                return Some(full.clone());
            }
        }
    }
    None
}

fn unique_suffix_map(slots: &[Vec<String>]) -> BTreeMap<String, String> {
    let mut multi: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for slot in slots {
        for bc in slot {
            let tail = bc.split('.').last().unwrap_or(bc).to_lowercase();
            multi.entry(tail).or_default().push(bc.to_lowercase());
        }
    }
    let mut out = BTreeMap::new();
    for (k, v) in multi {
        if v.len() == 1 {
            out.insert(k, v.into_iter().next().unwrap());
        }
    }
    out
}

/// The annotate module exports this for the caller, keep the enum visible.
#[derive(Debug, Clone, Copy)]
pub enum SizeMode {
    Engine,
}
