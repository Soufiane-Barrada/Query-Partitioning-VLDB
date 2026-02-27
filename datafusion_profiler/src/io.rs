use anyhow::Result;
use std::path::Path;

use crate::schema::UnifiedDoc;

/// Write the JSON document to path, pretty-printed. Creates directories as needed.
pub fn save_json(doc: &UnifiedDoc, path: &str) -> Result<()> {
    let p = Path::new(path);
    if let Some(par) = p.parent() {
        std::fs::create_dir_all(par)?;
    }
    let txt = serde_json::to_string_pretty(doc)?;
    std::fs::write(p, txt)?;
    Ok(())
}

/// Pretty, single-string ASCII tree based on the Node graph.
pub fn draw_tree(
    doc: &UnifiedDoc,
    show_metrics: bool,
    show_ms: bool,
    show_active_cols: bool,
    decimals: usize,
) -> String {
    fn fmt(
        n: &crate::schema::Node,
        show_metrics: bool,
        show_ms: bool,
        show_active_cols: bool,
        decimals: usize,
    ) -> String {
        let mut parts = vec![n.op.clone(), format!("[{}]", n.name)];
        if show_metrics {
            let m = &n.metrics;
            let rin = m.rows_in.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
            let rout = m.rows_out.map(|v| v.to_string()).unwrap_or_else(|| "-".into());
            let mut metric_line = format!("in:{rin} out:{rout}");
            if show_ms {
                let t = m
                    .elapsed_ms
                    .map(|v| format!("{:.dec$}", v, dec = decimals))
                    .unwrap_or_else(|| "-".into());
                metric_line.push_str(&format!(" ms:{t}"));
            }
            parts.push(metric_line);
            if n.op == "Join" {
                parts.push(format!(
                    "L_in:{} R_in:{}",
                    m.rows_in_left.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
                    m.rows_in_right.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
                ));
            }
        }
        if show_active_cols && !n.columns.active.is_empty() {
            parts.push(format!("cols:{:?}", n.columns.active));
        }
        parts.join(" ")
    }

    fn walk(
        n: &crate::schema::Node,
        pref: &str,
        last: bool,
        out: &mut Vec<String>,
        show_m: bool,
        show_ms: bool,
        show_c: bool,
        dec: usize,
    ) {
        out.push(format!(
            "{}{} {}",
            pref,
            if last { "└─" } else { "├─" },
            fmt(n, show_m, show_ms, show_c, dec)
        ));
        let kids = &n.children;
        for (i, ch) in kids.iter().enumerate() {
            let next_pref = format!("{}{}", pref, if last { "   " } else { "│  " });
            walk(
                ch,
                &next_pref,
                i == kids.len() - 1,
                out,
                show_m,
                show_ms,
                show_c,
                dec,
            );
        }
    }

    let mut lines = vec![];
    walk(
        &doc.root,
        "",
        true,
        &mut lines,
        show_metrics,
        show_ms,
        show_active_cols,
        decimals,
    );
    if let Some(ms) = doc.query_latency_ms {
        lines.push(format!("\nTotal query latency: {:.dec$} ms", ms, dec = decimals));
    }
    lines.join("\n")
}
