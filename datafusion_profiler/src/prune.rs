
use crate::schema::Node;

/// Remove `Project` nodes with exactly one child
pub fn prune_projects_one_child(node: Node) -> Node {
    let kids = node
        .children
        .into_iter()
        .map(prune_projects_one_child)
        .collect::<Vec<_>>();
    let mut node = Node { children: kids, ..node };
    if node.op == "Project" && node.children.len() == 1 {
        let mut child = node.children.remove(0);
        if let Some(sl) = node._slot_lineage.clone() {
            child._slot_lineage = Some(sl);
        }
        return child;
    }
    node
}

/// Remove internal `_slot_lineage` and infra_ms_accumulated from the final tree for cleanliness.
pub fn strip_internal(mut node: Node) -> Node {
    // Recurse
    let kids = node.children.into_iter().map(strip_internal).collect::<Vec<_>>();

    // Drop internal slot lineage
    node._slot_lineage = None;
    node.children = kids;

    // Strip infra accumulation field
    node.metrics.infra_ms_accumulated = None;

    node
}
