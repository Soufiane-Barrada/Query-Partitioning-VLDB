from typing import Dict, Any


def prune_projects_one_child(node: Dict[str, Any]) -> Dict[str, Any]:
    kids = [prune_projects_one_child(ch) for ch in node.get("children", [])]
    node = dict(node)
    node["children"] = kids
    if node.get("op") == "Project" and len(kids) == 1:
        child = dict(kids[0])
        if node.get("_slot_lineage") is not None:
            child["_slot_lineage"] = node["_slot_lineage"]
        return child
    return node


def strip_internal(node: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: v for k, v in node.items() if k != "_slot_lineage"}
    out["children"] = [strip_internal(ch) for ch in node.get("children", [])]
    return out


def prune_cte(node: Dict[str, Any]) -> Dict[str, Any]:
    kids = [prune_cte(ch) for ch in node.get("children", [])]

    if node.get("op") == "CTE":
        if len(kids) == 1:
            return kids[0]
        out = dict(node)
        out["children"] = kids
        return out

    out = dict(node)
    out["children"] = kids
    return out

