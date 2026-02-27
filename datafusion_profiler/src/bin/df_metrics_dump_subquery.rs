use anyhow::{anyhow, Result};
use datafusion::execution::context::SessionConfig;
use datafusion::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

use df_metrics_dump::adapter_datafusion::{
    build_node, recompute_input_rows, recompute_row_sizes, rollup_and_prune, BuildCtx,
};
use df_metrics_dump::annotate::{annotate_sizes, SizeMode};
use df_metrics_dump::io::{draw_tree, save_json};
use df_metrics_dump::prune::{prune_projects_one_child, strip_internal};
use df_metrics_dump::schema::{Node, UnifiedDoc};

#[derive(Debug, Clone)]
struct RunOpts {
    in_sql_file: Option<PathBuf>,
    in_sql_dir: Option<PathBuf>,
    in_parquet_dir: PathBuf,
    out_json_dir: PathBuf,
    write_tree: bool,
    tree_cols: bool,
    tree_plain: bool,
    observed_ops_out: Option<PathBuf>,
    failed_out: Option<PathBuf>,
    extra_parquet: Vec<(String, PathBuf)>,
    target_partitions: Option<usize>,
}

fn parse_args() -> Result<RunOpts> {
    let mut in_sql_file = None;
    let mut in_sql_dir = None;
    let mut parquet_dir = None;
    let mut out_dir = None;
    let mut write_tree = false;
    let mut tree_cols = false;
    let mut tree_plain = false;
    let mut observed_ops_out = None;
    let mut failed_out = None;
    let mut extra_parquet: Vec<(String, PathBuf)> = vec![];
    let mut target_partitions = None;

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--in-sql-file" => in_sql_file = it.next().map(PathBuf::from),
            "--in-sql-dir" => in_sql_dir = it.next().map(PathBuf::from),
            "--in-parquet-dir" => parquet_dir = it.next().map(PathBuf::from),
            "--out-json-dir" => out_dir = it.next().map(PathBuf::from),
            "--write-tree" => write_tree = true,
            "--tree-cols" => tree_cols = true,
            "--tree-plain" => tree_plain = true,
            "--observed-ops-out" => observed_ops_out = it.next().map(PathBuf::from),
            "--failed-out" => failed_out = it.next().map(PathBuf::from),
            "--extra-parquet" => {
                if let Some(v) = it.next() {
                    let parts: Vec<&str> = v.splitn(2, '=').collect();
                    if parts.len() != 2 {
                        return Err(anyhow!("Invalid --extra-parquet value: {}", v));
                    }
                    extra_parquet.push((parts[0].to_string(), PathBuf::from(parts[1])));
                }
            }
            "--target-partitions" => {
                if let Some(v) = it.next() {
                    target_partitions = v.parse::<usize>().ok();
                }
            }
            "--size-mode" => {
                let _ = it.next();
            }
            _ => {}
        }
    }

    let parquet_dir = parquet_dir.ok_or_else(|| anyhow!("--in-parquet-dir is required"))?;
    let out_json_dir = out_dir.ok_or_else(|| anyhow!("--out-json-dir is required"))?;

    if in_sql_file.is_none() && in_sql_dir.is_none() {
        return Err(anyhow!("--in-sql-file or --in-sql-dir is required"));
    }

    Ok(RunOpts {
        in_sql_file,
        in_sql_dir,
        in_parquet_dir: parquet_dir,
        out_json_dir,
        write_tree,
        tree_cols,
        tree_plain,
        observed_ops_out,
        failed_out,
        extra_parquet,
        target_partitions,
    })
}

fn collect_queries(in_sql_file: Option<PathBuf>, in_sql_dir: Option<PathBuf>) -> Result<Vec<PathBuf>> {
    if let Some(p) = in_sql_file {
        return Ok(vec![p]);
    }
    let root = in_sql_dir.ok_or_else(|| anyhow!("Missing in_sql_dir"))?;
    let mut out = vec![];
    if root.is_file() && root.extension().is_some_and(|e| e == "sql") {
        out.push(root.to_path_buf());
    } else {
        for e in WalkDir::new(root).into_iter().flatten() {
            let p = e.path();
            if p.is_file() && p.extension().is_some_and(|x| x == "sql") {
                out.push(p.to_path_buf());
            }
        }
    }
    out.sort();
    Ok(out)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let opts = parse_args()?;

    std::fs::create_dir_all(&opts.out_json_dir)?;

    df_metrics_dump::annotate::init_parquet_catalog(&opts.in_parquet_dir);
    df_metrics_dump::adapter_datafusion::init_parquet_table_cols(&opts.in_parquet_dir);

    let mut cfg = SessionConfig::new();
    if let Some(tp) = opts.target_partitions {
        cfg = cfg.with_target_partitions(tp);
    }
    let ctx = SessionContext::new_with_config(cfg);

    // Base parquet tables
    if Path::new(&opts.in_parquet_dir).exists() {
        for entry in std::fs::read_dir(&opts.in_parquet_dir)? {
            let p = entry?.path();
            if p.extension().is_some_and(|e| e == "parquet") {
                let table = p.file_stem().unwrap().to_string_lossy().to_string();
                ctx.register_parquet(&table, p.to_str().unwrap(), ParquetReadOptions::default())
                    .await?;
            }
        }
    }

    // Extra parquet tables (e.g., s1)
    for (name, path) in &opts.extra_parquet {
        if path.exists() {
            ctx.register_parquet(name, path.to_str().unwrap(), ParquetReadOptions::default())
                .await?;
        } else {
            return Err(anyhow!("Extra parquet table not found: {}", path.display()));
        }
    }

    let queries = collect_queries(opts.in_sql_file, opts.in_sql_dir)?;
    if queries.is_empty() {
        eprintln!("No .sql files found");
        return Ok(());
    }

    let mut observed_ops = BTreeSet::new();
    let mut failed: BTreeMap<String, String> = BTreeMap::new();

    for q in queries {
        let base = q.file_stem().unwrap().to_string_lossy().to_string();
        let out_main = opts.out_json_dir.join(format!("{base}.json"));
        let out_tree = opts.out_json_dir.join(format!("{base}.tree.txt"));

        let sql_raw = std::fs::read_to_string(&q)?;
        let mut sql = sql_raw.clone();
        if sql.trim_end().ends_with(';') {
            sql = sql
                .trim_end_matches(|c: char| c == ';' || c.is_whitespace())
                .to_string();
        }

        let df = match ctx.sql(&sql).await {
            Ok(df) => df,
            Err(e) => {
                failed.insert(base.clone(), format!("plan: {e}"));
                continue;
            }
        };
        let plan = match df.create_physical_plan().await {
            Ok(p) => p,
            Err(e) => {
                failed.insert(base.clone(), format!("phys-plan: {e}"));
                continue;
            }
        };

        let t0 = Instant::now();
        let run = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx()).await;
        let query_ms = t0.elapsed().as_secs_f64() * 1000.0;
        if let Err(e) = run {
            failed.insert(base.clone(), format!("exec: {e}"));
            continue;
        }

        let mut build_ctx = BuildCtx::new();
        let mut root = build_node(&mut build_ctx, plan.clone());

        fn collect_ops(n: &Node, set: &mut BTreeSet<String>) {
            set.insert(n.name.clone());
            for c in &n.children {
                collect_ops(c, set);
            }
        }
        collect_ops(&root, &mut observed_ops);

        df_metrics_dump::adapter_datafusion::qualify_tree(&mut root);
        df_metrics_dump::adapter_datafusion::qualify_active_columns(&mut root);
        df_metrics_dump::normalize::normalize_join_keys_text(&mut root);

        annotate_sizes(&mut root, SizeMode::Engine);

        let rolled = rollup_and_prune(root);
        let mut root2 = rolled;
        recompute_input_rows(&mut root2);

        let mut final_root = prune_projects_one_child(root2);
        final_root = strip_internal(final_root);

        let _ = recompute_row_sizes(&mut final_root);

        let doc = UnifiedDoc {
            query: base.clone(),
            engine: "datafusion".into(),
            query_latency_ms: Some(query_ms),
            root: final_root,
        };

        if let Err(e) = save_json(&doc, out_main.to_str().unwrap()) {
            eprintln!("[code-error] save_json {}: {e}", base);
            continue;
        }

        if opts.write_tree {
            let show_metrics = true;
            let show_ms = !opts.tree_plain;
            let show_cols = opts.tree_cols && !opts.tree_plain;
            let tree = draw_tree(&doc, show_metrics, show_ms, show_cols, 3);
            if let Err(e) = std::fs::write(&out_tree, tree) {
                eprintln!("[code-error] write_tree {}: {e}", base);
            }
        }

        if let Some(p) = &opts.observed_ops_out {
            if let Some(parent) = p.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let ops_snapshot = observed_ops.iter().cloned().collect::<Vec<_>>().join("\n");
            if let Err(e) = std::fs::write(p, ops_snapshot) {
                eprintln!("[warn] could not write observed ops: {e}");
            }
        }
    }

    if let Some(p) = &opts.failed_out {
        if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = std::fs::write(p, serde_json::to_string_pretty(&failed)?) {
            eprintln!("[warn] could not write failed.json: {e}");
        }
    }

    Ok(())
}
