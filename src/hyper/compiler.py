"""
SQL compiler for Hyper materialisation with FOLD fusion.

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


# ── Step 1: Upstream Traversal to Nearest MAT Node ──────────────────────────

def resolve_upstream_mat(start_id: str | None, nbi: dict[str, dict]) -> tuple[dict | None, list[dict]]:
    """
    Walk backwards from start_id until hitting the nearest MATERIALIZED node.
    Returns: (nearest_mat_node, [fold1, fold2, ... in execution order])
    """
    if not start_id or start_id not in nbi:
        return None, []

    folds_reversed: list[dict] = []
    current = nbi[start_id]

    while not is_materialized_type(current.get("type", "")):
        folds_reversed.append(current)
        parent_id = current.get("input")
        if not parent_id or parent_id not in nbi:
            return None, []
        current = nbi[parent_id]

    return current, list(reversed(folds_reversed))


# ── Step 2: Fusing FOLD Nodes into a Single SELECT ──────────────────────────

def _clean_op_to_sql(
    op: dict,
    where_clauses: list[str],
    calc_exprs: list[str],
    rename_exprs: list[str],
    dedup_cols: list[list[str]],
) -> None:
    """Translate one clean sub-operation into its SQL fragment lists (mutates in place)."""
    kind = op.get("op")

    if kind == "rename":
        for old, new in op.get("mapping", {}).items():
            rename_exprs.append(f'"{old}" AS "{new}"')

    elif kind == "filter_values":
        conditions = op.get("conditions", [])
        logic = op.get("logic", "AND").upper()
        parts: list[str] = []
        for c in conditions:
            col = c.get("column", "")
            operator = str(c.get("operator", "=")).upper()
            val = c.get("value")
            if val is None:
                parts.append(f'"{col}" IS NULL')
            elif isinstance(val, str):
                safe = val.replace("'", "''")
                parts.append(f'"{col}" {operator} \'{safe}\'')
            else:
                parts.append(f'"{col}" {operator} {val}')
        if parts:
            joined = f" {logic} ".join(f"({p})" for p in parts)
            where_clauses.append(f"({joined})")

    elif kind == "calculate":
        name = op.get("name", "")
        expr = op.get("expression", "")
        if name and expr:
            calc_exprs.append(f'{expr} AS "{name}"')

    elif kind == "dedup":
        cols = op.get("columns") or []
        dedup_cols.append(cols)


def fuse_folds_sql(mat_node: dict, folds: list[dict], select_cols: str = "*") -> str:
    """
    Fuses a chain of fold steps on top of a materialized node into ONE flat SELECT.
    Handles: filter, sort, calculate (standalone fold types)
             rename, filter_values, calculate, dedup (clean sub-operations)
    """
    base_ref = materialized_ref(mat_node)
    if not folds:
        return f"SELECT {select_cols} FROM {base_ref}"

    where_clauses: list[str] = []
    order_by_clauses: list[str] = []
    calc_exprs: list[str] = []
    rename_exprs: list[str] = []
    dedup_cols: list[list[str]] = []  # each entry = one dedup op's column list

    for fold in folds:
        ftype = fold.get("type")

        if ftype == "filter":
            cond = fold.get("condition")
            if cond and cond.strip():
                where_clauses.append(f"({cond})")

        elif ftype == "calculate":
            for calc in fold.get("calculations", []):
                calc_exprs.append(f'{calc["expression"]} AS "{calc["alias"]}"')

        elif ftype == "sort":
            for o in fold.get("order_by", []):
                order_by_clauses.append(f'"{o["column"]}" {o.get("direction", "ASC").upper()}')

        elif ftype == "clean":
            for op in fold.get("operations", []):
                _clean_op_to_sql(op, where_clauses, calc_exprs, rename_exprs, dedup_cols)

    select_parts = [select_cols]
    if calc_exprs:
        select_parts.extend(calc_exprs)
    if rename_exprs:
        select_parts.extend(rename_exprs)
    select_sql = ", ".join(select_parts)

    sql = f"SELECT {select_sql} FROM {base_ref}"
    if where_clauses:
        sql += f" WHERE {' AND '.join(where_clauses)}"

    # Wrap in DISTINCT / QUALIFY for dedup if requested
    if dedup_cols:
        # Last dedup op wins; empty cols list = full-row dedup
        cols = dedup_cols[-1]
        if cols:
            partition = ", ".join(f'"{c}"' for c in cols)
            sql = (
                f"SELECT * FROM ({sql}) AS _dedup_src"
                f" WHERE ROW_NUMBER() OVER (PARTITION BY {partition}) = 1"
            )
        else:
            sql = f"SELECT DISTINCT * FROM ({sql}) AS _dedup_src"

    if order_by_clauses:
        sql += f" ORDER BY {', '.join(order_by_clauses)}"

    return sql


# ── Step 3: Materialized Node Compilation (Max 1-2 CTEs) ────────────────────

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

    Fuses any FOLD nodes between upstream MAT nodes and this node into CTEs
    so intermediate fold results are never referenced as Hyper tables.
    """
    ntype = node.get("type")

    # ── AGGREGATE ────────────────────────────────────────────────────────────
    if ntype == "aggregate":
        mat_parent, folds = resolve_upstream_mat(node.get("input"), nbi)
        if not mat_parent:
            raise ValueError(f"Aggregate {node['id']!r}: upstream materialized node not found")

        group_by = node.get("group_by", [])
        aggregations = node.get("aggregations", [])
        group_cols = ", ".join(f'"{c}"' for c in group_by)
        agg_exprs = ", ".join(
            f'{a.get("function", a.get("func", "COUNT"))}("{a["column"]}") AS "{a.get("alias", a["column"])}"'
            for a in aggregations
        )
        select = ", ".join(p for p in [group_cols, agg_exprs] if p) or "*"
        group_sql = f" GROUP BY {group_cols}" if group_cols else ""

        if folds:
            fused_stage = fuse_folds_sql(mat_parent, folds)
            return f"WITH _stage AS ({fused_stage}) SELECT {select} FROM _stage{group_sql}"
        else:
            return f"SELECT {select} FROM {materialized_ref(mat_parent)}{group_sql}"

    # ── JOIN ─────────────────────────────────────────────────────────────────
    if ntype == "join":
        left_mat, left_folds = resolve_upstream_mat(node.get("left_input"), nbi)
        right_mat, right_folds = resolve_upstream_mat(node.get("right_input"), nbi)

        if not left_mat or not right_mat:
            raise ValueError(f"Join {node['id']!r}: missing left or right materialized parent")

        ctes: list[str] = []
        left_table = materialized_ref(left_mat)
        if left_folds:
            ctes.append(f"_left_stage AS ({fuse_folds_sql(left_mat, left_folds)})")
            left_table = "_left_stage"

        right_table = materialized_ref(right_mat)
        if right_folds:
            ctes.append(f"_right_stage AS ({fuse_folds_sql(right_mat, right_folds)})")
            right_table = "_right_stage"

        join_type = node.get("join_type", "INNER").upper()
        condition = node.get("condition", "")
        pairs = _parse_join_condition(condition)
        on_clause = (
            " AND ".join(f'l."{l}" = r."{r}"' for l, r in pairs)
            if pairs else (condition or "1=1")
        )

        with_prefix = f"WITH {', '.join(ctes)} " if ctes else ""
        return (
            f"{with_prefix}SELECT l.*, r.* FROM {left_table} AS l "
            f"{join_type} JOIN {right_table} AS r ON {on_clause}"
        )

    # ── UNION ─────────────────────────────────────────────────────────────────
    if ntype == "union":
        input_ids = node.get("inputs", [])
        if not input_ids:
            raise ValueError(f"Union {node['id']!r}: no inputs")

        ctes: list[str] = []
        branch_selects: list[str] = []

        for idx, iid in enumerate(input_ids):
            mat_parent, folds = resolve_upstream_mat(iid, nbi)
            if not mat_parent:
                continue
            if folds:
                stage_name = f"_u_stage_{idx}"
                ctes.append(f"{stage_name} AS ({fuse_folds_sql(mat_parent, folds)})")
                branch_selects.append(f"SELECT * FROM {stage_name}")
            else:
                branch_selects.append(f"SELECT * FROM {materialized_ref(mat_parent)}")

        with_prefix = f"WITH {', '.join(ctes)} " if ctes else ""
        return f"{with_prefix}" + " UNION ALL ".join(branch_selects)

    raise ValueError(f"compile_materialized called on non-materialized type: {ntype!r}")


# ── Step 4: Fold Preview Query (Zero CTEs, Reads Nearest MAT) ────────────────

def build_fold_preview_sql(node: dict, nbi: dict[str, dict], limit: int = 100) -> str:
    """
    Called when user clicks a FOLD node.
    Traces back to nearest MAT node, fuses all folds up to this node,
    and returns a single flat SELECT. Zero Hyper writes.
    """
    mat_parent, folds = resolve_upstream_mat(node["id"], nbi)
    if not mat_parent:
        return f"SELECT 'Error: no upstream source' AS error LIMIT {limit}"

    fused_query = fuse_folds_sql(mat_parent, folds)
    return f"{fused_query} LIMIT {limit}"


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
