import json, pathlib
from typing import Dict, Any, List, Optional


def save_json(doc: Dict[str, Any], path: str) -> None:
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, indent=2))


def load_json(path: str) -> Dict[str, Any]:
    return json.loads(pathlib.Path(path).read_text())


def draw_tree(
    doc: Dict[str, Any],
    *,
    show_metrics: bool = True,
    show_ms: bool = True,
    show_active_cols: bool = False,
    decimals: int = 3,
) -> str:
    def fmt(n: Dict[str, Any]) -> str:
        op = n.get("op")
        name = n.get("name", "")
        m = n.get("metrics", {})
        cols = (n.get("columns") or {}).get("active") or []
        parts = [op]
        if name:
            parts.append(f"[{name}]")
        if show_metrics:
            t = m.get("elapsed_ms")
            rin = m.get("rows_in")
            rout = m.get("rows_out")
            parts += [
                f"in:{rin if rin is not None else '-'}",
                f"out:{rout if rout is not None else '-'}",
            ]
            if show_ms:
                parts.append(f"ms:{f'{t:.{decimals}f}' if isinstance(t, (int, float)) else '-'}")
            if op == "Join":
                parts.append(f"L_in:{m.get('rows_in_left','-')} R_in:{m.get('rows_in_right','-')}")
        if show_active_cols and cols:
            parts.append(f"cols:{cols}")

        dep_cnt = len(n.get("_deps", [])) if isinstance(n.get("_deps"), list) else 0
        if dep_cnt:
            parts.append(f"deps:{dep_cnt}")

        return " ".join(parts)

    lines: List[str] = []

    def walk(n: Dict[str, Any], pref: str, last: bool):
        lines.append(pref + ("└─ " if last else "├─ ") + fmt(n))
        kids = n.get("children", [])
        for i, ch in enumerate(kids):
            walk(ch, pref + ("   " if last else "│  "), i == len(kids) - 1)

    walk(doc.get("root", {}), "", True)
    qms = doc.get("query_latency_ms")
    if isinstance(qms, (int, float)):
        lines.append(f"\nTotal query latency: {qms:.{decimals}f} ms")
    return "\n".join(lines)


def save_tree_text(
    doc: Dict[str, Any],
    path: str,
    *,
    show_metrics: bool = True,
    show_ms: bool = True,
    show_active_cols: bool = False,
    decimals: int = 3,
) -> None:
    txt = draw_tree(
        doc,
        show_metrics=show_metrics,
        show_ms=show_ms,
        show_active_cols=show_active_cols,
        decimals=decimals,
    )
    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(txt)

