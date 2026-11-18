"""
SQL compiler for Hyper materialisation.

MATERIALIZED types (stored in Hyper): source, join, union, aggregate
FOLD types (computed on demand, never stored): filter, sort, clean, calculate, output
"""
from __future__ import annotations

import hashlib
import json

MATERIALIZED_TYPES = frozenset({"source", "join", "union", "aggregate"})
FOLD_TYPES = frozenset({"filter", "sort", "clean", "calculate", "output"})


def is_materialized_type(ntype: str) -> bool:
    return ntype in MATERIALIZED_TYPES


def compute_node_hash(node: dict) -> str:
    """8-char MD5 of node content — excludes id, hash, and position keys."""
    content = {k: v for k, v in node.items() if k not in ("id", "hash", "x", "y")}
    return hashlib.md5(json.dumps(content, sort_keys=True).encode()).hexdigest()[:8]


def nodes_by_id(nodes: list[dict]) -> dict[str, dict]:
    return {n["id"]: n for n in nodes}


def materialized_ref(node: dict) -> str:
    """Fully-quoted Hyper table reference for a node's stored output."""
    if node.get("type") == "source":
        table = node.get("table", node["id"])
        return f'"sources"."{table}"'
    return f'"materialized"."{node["id"]}"'


# ── Materialized node SQL ────────────────────────────────────────────────────

def _parse_join_condition(condition: str) -> list[tuple[str, str]]:
    """'t1.col = t2.col AND ...' → [('col', 'col'), ...]"""
    pairs: list[tuple[str, str]] = []
    for part in condition.split(" AND "):
        part = part.strip()
        if "=" not in part:
            continue
        lhs, rhs = [s.strip() for s in part.split("=", 1)]
        pairs.append((
            lhs.split(".")[-1].strip('"').strip("`"),
            rhs.split(".")[-1].strip('"').strip("`"),
        ))
    return pairs


def compile_materialized(node: dict, nbi: dict[str, dict]) -> str:
    """
    Return a SELECT whose result should be stored in Hyper.
    Call only for join / union / aggregate nodes.
    source nodes are handled separately (live DB → Hyper).
    """
    ntype = node.get("type")

    if ntype == "aggregate":
        input_id = node.get("input")
        upstream = materialized_ref(nbi[input_id]) if input_id and input_id in nbi else '"sources"."unknown"'
        group_by = node.get("group_by", [])
        aggregations = node.get("aggregations", [])
        group_cols = ", ".join(f'"{c}"' for c in group_by)
        agg_exprs = ", ".join(
            f'{a.get("function", a.get("func", "COUNT"))}("{a["column"]}") AS "{a.get("alias", a["column"])}"'
            for a in aggregations
        )
        select = ", ".join(p for p in [group_cols, agg_exprs] if p) or "*"
        sql = f"SELECT {select} FROM {upstream}"
        if group_cols:
            sql += f" GROUP BY {group_cols}"
        return sql

    if ntype == "join":
        left_id = node.get("left_input")
        right_id = node.get("right_input")
        if not left_id or not right_id:
            raise ValueError(f"Join {node['id']!r}: missing left_input / right_input")
        left_ref = materialized_ref(nbi[left_id])
        right_ref = materialized_ref(nbi[right_id])
        join_type = node.get("join_type", "INNER").upper()
        condition = node.get("condition", "")
        pairs = _parse_join_condition(condition)
        on_clause = (
            " AND ".join(f'l."{l}" = r."{r}"' for l, r in pairs)
            if pairs else (condition or "1=1")
        )
        return (
            f"SELECT l.*, r.* FROM {left_ref} AS l "
            f"{join_type} JOIN {right_ref} AS r ON {on_clause}"
        )

    if ntype == "union":
        input_ids = node.get("inputs", [])
        if not input_ids:
            raise ValueError(f"Union {node['id']!r}: no inputs")
        refs = [materialized_ref(nbi[nid]) for nid in input_ids if nid in nbi]
        return " UNION ALL ".join(f"SELECT * FROM {ref}" for ref in refs)

    raise ValueError(f"compile_materialized called on non-materialized type: {ntype!r}")


# ── Fold node SQL chain ──────────────────────────────────────────────────────

def _fold_step_sql(node: dict, upstream_expr: str) -> str:
    """Single fold transformation applied on top of upstream_expr."""
    ntype = node.get("type")

    if ntype == "filter":
        cond = node.get("condition", "1=1")
        return f"SELECT * FROM ({upstream_expr}) AS _f WHERE {cond}"

    if ntype == "sort":
        order_by = node.get("order_by", [])
        if not order_by:
            return upstream_expr
        clause = ", ".join(
            f'"{o["column"]}" {o.get("direction", "ASC").upper()}' for o in order_by
        )
        return f"SELECT * FROM ({upstream_expr}) AS _s ORDER BY {clause}"

    if ntype == "calculate":
        calcs = node.get("calculations", [])
        if not calcs:
            return upstream_expr
        exprs = ", ".join(f'{c["expression"]} AS "{c["alias"]}"' for c in calcs)
        return f"SELECT *, {exprs} FROM ({upstream_expr}) AS _c"

    if ntype == "clean":
        ops = node.get("operations", [])
        renames: dict[str, str] = {}
        for op in ops:
            if op.get("op") == "rename":
                renames.update(op.get("mapping", {}))
        if renames:
            exprs = ", ".join(f'"{old}" AS "{new}"' for old, new in renames.items())
            return f"SELECT *, {exprs} FROM ({upstream_expr}) AS _cl"
        return upstream_expr

    # output or unknown: passthrough
    return upstream_expr


def build_fold_preview_sql(node: dict, nbi: dict[str, dict], limit: int = 100) -> str:
    """
    Build a SELECT for a fold node by chaining from its nearest upstream
    materialized table. The result is NOT stored in Hyper.
    """
    chain: list[dict] = []
    current = node
    while True:
        chain.append(current)
        if is_materialized_type(current.get("type", "")):
            break
        input_id = current.get("input")
        if not input_id or input_id not in nbi:
            break
        current = nbi[input_id]

    chain.reverse()  # [materialized_base, fold1, fold2, ..., target]

    sql = f"SELECT * FROM {materialized_ref(chain[0])}"
    for fold_node in chain[1:]:
        sql = _fold_step_sql(fold_node, sql)

    return f"SELECT * FROM ({sql}) AS _preview LIMIT {limit}"


# ── Topological ordering ─────────────────────────────────────────────────────

def topological_order(nodes: list[dict]) -> list[dict]:
    """Return nodes sorted sources-first, output-last."""
    nbi = nodes_by_id(nodes)
    visited: set[str] = set()
    order: list[dict] = []

    def visit(n: dict) -> None:
        nid = n["id"]
        if nid in visited:
            return
        visited.add(nid)
        for key in ("input", "left_input", "right_input"):
            parent_id = n.get(key)
            if parent_id and parent_id in nbi:
                visit(nbi[parent_id])
        for parent_id in n.get("inputs", []):
            if parent_id in nbi:
                visit(nbi[parent_id])
        order.append(n)

    for n in nodes:
        visit(n)
    return order


def downstream_materialized_ids(
    changed_node_id: str,
    nodes: list[dict],
    edges: list[dict],
) -> list[str]:
    """
    Return node_ids of all downstream MATERIALIZED nodes reachable from
    changed_node_id, in topological order (closest first).
    """
    adj: dict[str, list[str]] = {}
    for e in edges:
        frm = e.get("from") or e.get("source", "")
        to = e.get("to") or e.get("target", "")
        if frm and to:
            adj.setdefault(frm, []).append(to)

    nbi = nodes_by_id(nodes)
    visited: set[str] = set()
    result: list[str] = []

    def dfs(node_id: str) -> None:
        if node_id in visited:
            return
        visited.add(node_id)
        for child_id in adj.get(node_id, []):
            if child_id not in nbi:
                continue
            child = nbi[child_id]
            if is_materialized_type(child.get("type", "")):
                result.append(child_id)
            dfs(child_id)

    dfs(changed_node_id)
    topo = [n["id"] for n in topological_order(nodes)]
    result.sort(key=lambda nid: topo.index(nid) if nid in topo else 9999)
    return result
