from __future__ import annotations
import argparse
from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np



def load_runs(root: Path):
    rows = []
    for p in sorted(root.glob("**/metrics.json")):
        with open(p) as f:
            m = json.load(f)
        meta = m.get("config", {})
        train = m.get("train", {})
        val = m.get("val", {})
        rows.append({"run": p.parent.name, **meta, **{f"train_{k}": v for k, v in train.items()}, **{f"val_{k}": v for k, v in val.items()}})
    return pd.DataFrame(rows)


# plots


# plot 1
def plot_actual_vs_pred(y_true, y_pred, path: Path, title: str, unit: str = "s"):
    plt.figure()
    plt.scatter(y_true, y_pred, s=8, alpha=0.6)
    lim = max(float(np.max(y_true)), float(np.max(y_pred)))
    plt.plot([0, lim], [0, lim], linestyle="--", linewidth=1.2)
    plt.xlabel(f"Actual time [{unit}]")
    plt.ylabel(f"Predicted time [{unit}]")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()




# plot 2
def plot_feature_importance(feature_names, importances, path: Path, title: str):
    order = np.argsort(importances)[::-1]
    names = [feature_names[i] for i in order]
    vals = importances[order]
    plt.figure()
    plt.barh(range(len(names)), vals)
    plt.gca().invert_yaxis()
    plt.yticks(range(len(names)), names)
    plt.xlabel("Gain (model-specific)")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()



# plot 3
def plot_qerror_distribution(qe: np.ndarray, path: Path, title: str,
                             clip_q=(0.005, 0.995), show_kde=True):

    qe = np.asarray(qe, float)
    qe = qe[np.isfinite(qe) & (qe > 0)]
    if qe.size == 0:
        return

    # clip extremes for plotting only
    lo, hi = np.quantile(qe, clip_q)
    qe_p = qe[(qe >= lo) & (qe <= hi)]
    qe_log = np.log10(qe_p)

    # Common bins (Freedman–Diaconis in log-space)
    bins = np.histogram_bin_edges(qe_log, bins="fd")
    if bins.size < 10:
        bins = np.linspace(qe_log.min(), qe_log.max(), 30)

    # Percent weights so bars show % per bin
    w = np.full_like(qe_log, 100.0 / max(1, qe_log.size), dtype=float)

    fig, ax = plt.subplots(figsize=(8, 5), dpi=150)
    ax.hist(qe_log, bins=bins, weights=w, histtype="stepfilled", alpha=0.25, label="Q-error %/bin")
    ax.hist(qe_log, bins=bins, weights=w, histtype="step", linewidth=1.8)

    # Optional KDE scaled to "% per bin"
    if show_kde:
        def kde(grid, x):
            n = x.size
            std = x.std(ddof=1) if n > 1 else 0.0
            bw = 1.06 * std * n**(-1/5) if (n > 1 and std > 0) else 0.1
            U = (grid[:, None] - x[None, :]) / bw
            return np.exp(-0.5 * U * U).sum(axis=1) / (n * bw * np.sqrt(2*np.pi))
        widths = np.diff(bins)
        eq = np.allclose(widths, widths[0], rtol=1e-6, atol=1e-12)
        grid = np.linspace(bins[0], bins[-1], 600)
        bin_width = widths[0] if eq else 1.0
        scale = 100.0 * bin_width
        ax.plot(grid, kde(grid, qe_log) * scale, linewidth=1.5, label="KDE (scaled)")

    # Pretty x-axis: show ×-multipliers instead of log values
    lo10, hi10 = qe_log.min(), qe_log.max()
    # suggest ticks at common multipliers if within range
    candidates = np.array([1.0, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0])
    cand_log = np.log10(candidates)
    xticks = cand_log[(cand_log >= lo10) & (cand_log <= hi10)]
    if xticks.size >= 3:
        ax.set_xticks(xticks)
        ax.set_xticklabels([f"{float(10**x):g}×" for x in xticks])

    ax.set_xlabel("Q-error (× multiplicative)")
    ax.set_ylabel("Share of points per bin (%)")
    ax.set_title(title)
    ax.legend()
    ax.grid(True, linewidth=0.3, alpha=0.5)
    plt.tight_layout()
    plt.savefig(path)
    plt.close()





# plot 4
def _format_time_tick(v: float) -> str:
    """Nice tick label for seconds-scale values."""
    if v < 1e-3:
        return f"{v*1e6:.0f} µs"
    if v < 1.0:
        return f"{v*1e3:.0f} ms"
    if v < 60.0:
        return f"{v:.1f} s"
    if v < 3600.0:
        return f"{v/60:.1f} min"
    return f"{v/3600:.1f} h"


def summarize_qerror_by_time_bins(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    n_bins: int = 20,
    log_bins: bool = True,
) -> pd.DataFrame:
    """
    Summarize Q-error by bins of ACTUAL runtime.
    Returns a pandas DataFrame with bin_left/bin_right/bin_center in the same scale as y_true,
    plus count and (q_mean, q_median, q_p95).
    """
    y_true = np.asarray(y_true, float)
    y_pred = np.asarray(y_pred, float)

    mask = np.isfinite(y_true) & np.isfinite(y_pred) & (y_true > 0)
    t = y_true[mask]
    p = y_pred[mask]

    if t.size == 0:
        return pd.DataFrame(columns=["bin_left", "bin_right", "bin_center", "count", "q_mean", "q_median", "q_p95"])

    # Q-error
    eps = 1e-12
    qe = np.maximum((p + eps) / (t + eps), (t + eps) / (p + eps))

    # Bin edges over ACTUAL time
    if log_bins:
        lo, hi = np.log10(t.min()), np.log10(t.max())
        if hi - lo < 1e-9:  # degenerate range -> widen a touch
            lo -= 0.15
            hi += 0.15
        edges_log = np.linspace(lo, hi, n_bins + 1)
        edges = 10.0 ** edges_log
        centers = np.sqrt(edges[:-1] * edges[1:])  # geometric center
    else:
        lo, hi = float(t.min()), float(t.max())
        if hi - lo < 1e-12:
            span = max(1e-9, lo * 0.1)
            lo -= span
            hi += span
        edges = np.linspace(lo, hi, n_bins + 1)
        centers = 0.5 * (edges[:-1] + edges[1:])  # arithmetic center

    # Assign bins
    b = np.digitize(t, edges, right=False)

    # Aggregate
    rows = []
    for i in range(1, n_bins + 1):
        left, right = edges[i - 1], edges[i]
        sel = (b == i)
        q_vals = qe[sel]
        rows.append(
            dict(
                bin_left=float(left),
                bin_right=float(right),
                bin_center=float(centers[i - 1]),
                count=int(q_vals.size),
                q_mean=(float(np.mean(q_vals)) if q_vals.size else np.nan),
                q_median=(float(np.median(q_vals)) if q_vals.size else np.nan),
                q_p95=(float(np.percentile(q_vals, 95)) if q_vals.size else np.nan),
            )
        )

    return pd.DataFrame(rows)


def plot_qerror_vs_time_binned(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    path: Path,
    title: str,
    n_bins: int = 20,
    log_x: bool = True,
    show_counts: bool = True,
):
    """
    Visualizes mean/median/p95 Q-error per bin of "actual" runtime.
    - X axis shows actual time (same unit as y_true) with optional log scaling.
    - Count per bin drawn as translucent bars on a secondary y-axis.
    - Zero-count bins get a faint background stripe so bin ranges remain visible.
    Also writes a CSV with the per-bin stats alongside the plot png.
    """

    stats = summarize_qerror_by_time_bins(y_true, y_pred, n_bins=n_bins, log_bins=log_x)
    if stats.empty:
        return

    x_left = stats["bin_left"].to_numpy()
    x_right = stats["bin_right"].to_numpy()
    x_ctr = stats["bin_center"].to_numpy()
    cnt = stats["count"].to_numpy()
    q_mean = stats["q_mean"].to_numpy()
    q_med = stats["q_median"].to_numpy()
    q_p95 = stats["q_p95"].to_numpy()

    # Mask NaNs so lines don't connect across empty bins
    m_mean = np.isfinite(q_mean)
    m_med = np.isfinite(q_med)
    m_p95 = np.isfinite(q_p95)

    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)

    # Secondary axis for counts (bars spanning [left, right] so bin extents are visible)
    if show_counts:
        ax2 = ax.twinx()
        widths = x_right - x_left
        ax2.bar(
            x_left,
            cnt,
            width=widths,
            align="edge",
            alpha=0.25,
            edgecolor="none",
            label="Count per bin",
        )
        ax2.set_ylabel("Count")

        # Lightly shade zero-count bins so their ranges are still visible
        for left, right, c in zip(x_left, x_right, cnt):
            if c == 0:
                ax.axvspan(left, right, facecolor="0.9", alpha=0.4, zorder=0)

    # Q-error lines
    if log_x:
        ax.set_xscale("log")
    ax.plot(x_ctr[m_mean], q_mean[m_mean], marker="o", linewidth=1.8, label="mean Q-error")
    ax.plot(x_ctr[m_med],  q_med[m_med],   marker="s", linewidth=1.8, label="median Q-error")
    ax.plot(x_ctr[m_p95],  q_p95[m_p95],   marker="^", linewidth=1.8, label="p95 Q-error")

    # Force q-error y-axis to be 0..500 so the whole graph stays consistent
    TOP_QERROR = 500.0
    ax.set_ylim(0.0, TOP_QERROR)

    # show clipped markers for any points above the limit so we know were larger values
    clipped_x = []
    for xs, ys in [(x_ctr[m_mean], q_mean[m_mean]), (x_ctr[m_med], q_med[m_med]), (x_ctr[m_p95], q_p95[m_p95])]:
        for xv, yv in zip(xs, ys):
            if np.isfinite(yv) and yv > TOP_QERROR:
                clipped_x.append(xv)
    if clipped_x:
        ax.scatter(clipped_x, [TOP_QERROR] * len(clipped_x), marker="v", color="red", s=30, zorder=6,
                   label=f"values > {int(TOP_QERROR)} (clipped)")
        ax.text(
            0.99,
            0.98,
            f"{len(clipped_x)} point(s) clipped at {int(TOP_QERROR)}",
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=8,
            color="red",
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.8),
        )

    # Annotate q-mean points only (same as your current behavior)
    fmt = "{:.2f}"
    y_offset_pts = 6
    fontsize = 7
    color = "black"

    if np.any(m_mean):
        xs = x_ctr[m_mean]
        ys = q_mean[m_mean]
        for x, y in zip(xs, ys):
            if np.isfinite(y):
                ax.annotate(
                    fmt.format(y),
                    xy=(x, y),
                    xytext=(0, y_offset_pts),
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                    fontsize=fontsize,
                    color=color,
                    alpha=0.9,
                    clip_on=True,
                )

    # Formatting
    ax.set_title(title)
    ax.set_xlabel("Actual runtime")
    ax.set_ylabel("Q-error (×)")
    ax.grid(True, linewidth=0.3, alpha=0.5)

    # Show bin edges as thin vertical gridlines
    for x in np.r_[x_left, x_right[-1]]:
        ax.axvline(x, color="0.92", linewidth=0.6, zorder=0)

    # x tick labels (normal time units)
    xticks = ax.get_xticks()
    try:
        ax.set_xticklabels([_format_time_tick(float(v)) for v in xticks])
    except Exception:
        pass

    # Unified legend (primary + secondary)
    handles, labels = ax.get_legend_handles_labels()
    if show_counts:
        h2, l2 = ax2.get_legend_handles_labels()
        handles += h2
        labels += l2
    if handles:
        ax.legend(handles, labels, loc="best")

    plt.tight_layout()
    plt.savefig(path)
    plt.close()

    # Save CSV right next to PNG
    csv_path = Path(str(path).rsplit(".", 1)[0] + ".csv")
    stats.to_csv(csv_path, index=False)






# plot 5

def summarize_over_under_by_time_bins(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    n_bins: int = 20,
    log_bins: bool = True,
):
    """
    Summarize over/under prediction counts in bins of ACTUAL runtime.
    Returns a DataFrame with:
      bin_left, bin_right, bin_center, count, under_count, over_count,
      under_pct, over_pct
    """

    y_true = np.asarray(y_true, float)
    y_pred = np.asarray(y_pred, float)

    m = np.isfinite(y_true) & np.isfinite(y_pred) & (y_true > 0)
    t = y_true[m]
    p = y_pred[m]
    if t.size == 0:
        return pd.DataFrame(columns=[
            "bin_left","bin_right","bin_center","count",
            "under_count","over_count","under_pct","over_pct"
        ])

    # Build edges in ACTUAL time
    if log_bins:
        lo, hi = np.log10(t.min()), np.log10(t.max())
        if hi - lo < 1e-9:
            lo -= 0.15; hi += 0.15
        edges_log = np.linspace(lo, hi, n_bins + 1)
        edges = 10.0 ** edges_log
        centers = np.sqrt(edges[:-1] * edges[1:])
    else:
        lo, hi = float(t.min()), float(t.max())
        if hi - lo < 1e-12:
            span = max(1e-9, lo * 0.1)
            lo -= span; hi += span
        edges = np.linspace(lo, hi, n_bins + 1)
        centers = 0.5 * (edges[:-1] + edges[1:])

    # Bin assignment
    b = np.digitize(t, edges, right=False)

    rows = []
    for i in range(1, n_bins + 1):
        left, right = edges[i-1], edges[i]
        sel = (b == i)
        ti = t[sel]
        pi = p[sel]
        c = int(ti.size)
        if c > 0:
            # Over/Under split (ties -> count as under)
            over = int(np.sum(pi > ti))
            under = int(c - over)
            over_pct = 100.0 * over / c
            under_pct = 100.0 * under / c
        else:
            over = under = 0
            over_pct = under_pct = 0.0

        rows.append(dict(
            bin_left=float(left),
            bin_right=float(right),
            bin_center=float(centers[i-1]),
            count=c,
            under_count=under,
            over_count=over,
            under_pct=under_pct,
            over_pct=over_pct,
        ))
    return pd.DataFrame(rows)



def plot_over_under_by_time_bins(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    path: Path,
    title: str,
    n_bins: int = 20,
    log_x: bool = True,
    count_axis: str = "log",     # "log", "linear", or "none"
    annotate_counts: bool = True,
    under_color: str = "#1b7837",
    over_color: str = "#1f4e79",
    empty_fill: str = "#555555",
    empty_alpha: float = 0.4,
):
    """
    100%-stacked bars per ACTUAL-time bin (under% vs over%), with total count overlaid.
    - Empty bins: shaded dark grey and annotated "0"
    - Count line: secondary axis (log/linear), with numeric labels (including zeros)
    Saves PNG and a CSV with per-bin stats.
    """

    stats = summarize_over_under_by_time_bins(y_true, y_pred, n_bins=n_bins, log_bins=log_x)
    if stats.empty:
        return

    x_left  = stats["bin_left"].to_numpy()
    x_right = stats["bin_right"].to_numpy()
    x_ctr   = stats["bin_center"].to_numpy()
    cnt     = stats["count"].to_numpy()
    u_pct   = stats["under_pct"].to_numpy()
    o_pct   = stats["over_pct"].to_numpy()

    widths = x_right - x_left

    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)

    # Shade empty bins first
    for left, right, c in zip(x_left, x_right, cnt):
        if c == 0:
            ax.axvspan(left, right, facecolor=empty_fill, alpha=empty_alpha, zorder=0)

    # 100% stacked percentage bars (under + over)
    ax.bar(x_left, u_pct, width=widths, align="edge", label="Under (%)", color=under_color, zorder=2)
    ax.bar(x_left, o_pct, width=widths, align="edge", bottom=u_pct, label="Over (%)", color=over_color, zorder=2)

    # X scale
    if log_x:
        ax.set_xscale("log")

    ax.set_ylim(0, 100)
    ax.set_ylabel("Share per bin (%)")
    ax.set_xlabel("Actual runtime")
    ax.set_title(title)
    ax.grid(True, linewidth=0.3, alpha=0.5, zorder=1)

    # Secondary axis: total counts
    legend_handles, legend_labels = ax.get_legend_handles_labels()
    if count_axis.lower() != "none":
        ax2 = ax.twinx()
        if count_axis.lower() == "log":
            ax2.set_yscale("log")
        # so zero shows at 1 on log scale
        ax2.plot(x_ctr, np.maximum(cnt, 1), marker="o", linewidth=1.4, label="Count (per bin)", color="#333333", zorder=3)
        ax2.set_ylabel("Count (per bin)")

        # Count annotations — include zeros
        if annotate_counts and len(cnt) <= 200:
            for xi, c in zip(x_ctr, cnt):
                y = 1.0 if (count_axis.lower() == "log" and c == 0) else c
                ax2.text(xi, max(y, 1e-12), f"{int(c)}", ha="center", va="bottom", fontsize=8, color="#222222")

        h2, l2 = ax2.get_legend_handles_labels()
        legend_handles += h2
        legend_labels  += l2
    else:
        if annotate_counts and len(cnt) <= 200:
            for left, width, c in zip(x_left, widths, cnt):
                ax.text(left + width*0.5, 102, f"{int(c)}", ha="center", va="bottom", fontsize=8, color="#222222")

    # Show bin edges as subtle vertical lines
    for x in np.r_[x_left, x_right[-1]]:
        ax.axvline(x, color="0.9", linewidth=0.6, zorder=0)

    # Pretty x tick labels in normal units
    xticks = ax.get_xticks()
    try:
        ax.set_xticklabels([_format_time_tick(float(v)) for v in xticks])
    except Exception:
        pass

    if legend_handles:
        ax.legend(legend_handles, legend_labels, loc="best")

    plt.tight_layout()
    plt.savefig(path)
    plt.close()

    # Save CSV next to the image
    csv_path = Path(str(path).rsplit(".", 1)[0] + ".csv")
    stats.to_csv(csv_path, index=False)




#******************************


# Top-10 best runs by test Q-error

def _load_all_runs_recursive(root: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(root.glob("**/metrics.json")):
        try:
            with open(p) as f:
                m = json.load(f)
            meta = m.get("config", {})
            train = m.get("train", {})
            val   = m.get("val", {})
            test  = m.get("test", {})
            rows.append({
                "run_path": str(p.parent),
                "run": p.parent.name,
                **meta,
                **{f"train_{k}": v for k, v in train.items()},
                **{f"val_{k}":   v for k, v in val.items()},
                **{f"test_{k}":  v for k, v in test.items()},
            })
        except Exception:
            continue
    return pd.DataFrame(rows)

def rank_top_runs_by_test(df: pd.DataFrame, engine: str, op: str, top_k: int = 10) -> pd.DataFrame:
    sub = df[(df["engine"] == engine) & (df["op"] == op)].copy()
    needed = ["test_Q-mean", "test_Q-median", "test_Q-p95"]
    if sub.empty or any(c not in sub.columns for c in needed):
        return pd.DataFrame(columns=["run","rank_sum",*needed])

    # drop runs missing any test metric
    sub = sub.dropna(subset=needed).copy()
    if sub.empty:
        return sub

    # lower is better
    for c in needed:
        sub[f"rank_{c}"] = sub[c].rank(method="min", ascending=True)

    sub["rank_sum"] = sub[[f"rank_{c}" for c in needed]].sum(axis=1)
    sub = sub.sort_values(["rank_sum", "test_Q-mean", "test_Q-p95", "test_Q-median"], ascending=True)
    return sub.head(top_k)

def plot_top_runs_by_test(df: pd.DataFrame, engine: str, op: str, out_path: Path, top_k: int = 10):
    top = rank_top_runs_by_test(df, engine, op, top_k=top_k)
    if top.empty:
        print("No eligible runs with test metrics found.")
        return

    labels = top["run"].tolist()
    means  = top["test_Q-mean"].to_numpy()
    med    = top["test_Q-median"].to_numpy()
    p95    = top["test_Q-p95"].to_numpy()
    ranks  = top["rank_sum"].to_numpy()

    x = np.arange(len(labels))
    width = 0.25

    fig, ax = plt.subplots(figsize=(12, 6), dpi=150)
    ax.bar(x - width, means, width, label="Q-mean")
    ax.bar(x,         med,   width, label="Q-median")
    ax.bar(x + width, p95,   width, label="Q-p95")

    ax.set_ylabel("Q-error (×)")
    ax.set_title(f"Top {len(labels)} runs by TEST Q-error — {engine}/{op} (rank-sum over mean/median/p95)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=35, ha="right")

    # annotate rank_sum above bars (center group)
    for xi, r in zip(x, ranks):
        ax.text(xi, max(means[xi] if xi < len(means) else 0, med[xi], p95[xi]) * 1.02,
                f"rankΣ={int(r)}", ha="center", va="bottom", fontsize=8, color="#333")

    ax.grid(True, axis="y", alpha=0.3, linewidth=0.6)
    ax.legend(loc="best")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def cli_top10(runs_root: str, engine: str, op: str, top_k: int = 10):
    root = Path(runs_root)
    df = _load_all_runs_recursive(root / engine)
    out = root / f"top{top_k}_{engine}_{op}_test_q.png"
    plot_top_runs_by_test(df, engine, op, out, top_k=top_k)
    # also write a CSV summary
    top = rank_top_runs_by_test(df, engine, op, top_k=top_k)
    if not top.empty:
        top[["run","rank_sum","test_Q-mean","test_Q-median","test_Q-p95","run_path"]].to_csv(
            root / f"top{top_k}_{engine}_{op}_test_q.csv", index=False
        )
    print(f"Wrote: {out}")





#******************************


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir", nargs="?", default="runs")
    ap.add_argument("--top10", action="store_true", help="Show top-10 by TEST Q metrics for an engine/op.")
    ap.add_argument("--engine", default=None)
    ap.add_argument("--op", default=None)
    ap.add_argument("--k", type=int, default=10)
    args = ap.parse_args()

    if args.top10:
        if not args.engine or not args.op:
            raise SystemExit("--top10 requires --engine and --op")
        cli_top10(args.run_dir, args.engine, args.op, top_k=args.k)
