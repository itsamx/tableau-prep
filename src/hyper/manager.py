"""
HyperManager — singleton that owns the HyperProcess and per-flow connections.

Materialization strategy:
  - DIRTY  = data not in Hyper yet, or config changed since last fetch
  - CLEAN  = Hyper table exists and hash matches last materialization

On flow open  → materialize all source nodes from live DB → all transform
                nodes (aggregate/join/union) start as DIRTY
On node click → walk ancestors of that node in topo order
                → only materialize DIRTY ancestors
                → source nodes hit live DB; transforms use Hyper upstream tables
                → fold nodes (filter/sort/clean/calculate) are never stored,
                  always computed on demand from nearest materialized ancestor
On node edit  → drop Hyper table for changed node + all downstream materialized
                nodes → mark them DIRTY (materialization deferred to next click)
"""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any

import logging

log = logging.getLogger(__name__)

from .compiler import (
    MATERIALIZED_TYPES,
    build_fold_preview_sql,
    compile_materialized,
    compute_node_hash,
    downstream_materialized_ids,
    is_materialized_type,
    materialized_ref,
    nodes_by_id,
    topological_order,
)


def _hyper_dir() -> Path:
    return Path(os.getenv("HYPER_STORAGE_DIR", "storage/hyper"))


def _map_py_to_hyper_type(value: Any):
    from tableauhyperapi import SqlType
    if isinstance(value, bool):
        return SqlType.bool()
    if isinstance(value, int):
        return SqlType.big_int()
    if isinstance(value, float):
        return SqlType.double()
    return SqlType.text()


def _db_type_str_to_hyper(type_str: str) -> Any:
    """Map SQLAlchemy type string to Hyper SqlType."""
    from tableauhyperapi import SqlType
    t = type_str.upper().split("(")[0].strip()
    if t in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "INT2", "INT8"):
        return SqlType.big_int()
    if t in ("REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "NUMERIC", "DECIMAL", "NUMBER"):
        return SqlType.double()
    if t in ("BOOLEAN", "BOOL"):
        return SqlType.bool()
    return SqlType.text()


def _ancestor_ids(node: dict, nbi: dict[str, dict]) -> set[str]:
    """All ancestor node_ids of a node (not including the node itself)."""
    result: set[str] = set()
    stack: list[str] = []
    for key in ("input", "left_input", "right_input"):
        pid = node.get(key)
        if pid and pid in nbi:
            stack.append(pid)
    for pid in node.get("inputs", []):
        if pid in nbi:
            stack.append(pid)
    while stack:
        nid = stack.pop()
        if nid in result:
            continue
        result.add(nid)
        parent = nbi[nid]
        for key in ("input", "left_input", "right_input"):
            pid = parent.get(key)
            if pid and pid in nbi:
                stack.append(pid)
        for pid in parent.get("inputs", []):
            if pid in nbi:
                stack.append(pid)
    return result


class HyperManager:
    _instance: HyperManager | None = None
    _class_lock = threading.Lock()

    def __init__(self) -> None:
        self._process: Any = None
        self._connections: dict[str, Any] = {}       # flow_id → Connection
        self._conn_lock = threading.Lock()
        self._hashes: dict[str, dict[str, str]] = {} # flow_id → {node_id → hash}
        self._dirty: dict[str, set[str]] = {}         # flow_id → set of dirty node_ids

    # ── Singleton ────────────────────────────────────────────────────────────

    @classmethod
    def get(cls) -> "HyperManager":
        if cls._instance is None:
            with cls._class_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._process is not None:
            return
        try:
            from tableauhyperapi import HyperProcess, Telemetry
            self._process = HyperProcess(
                telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
            )
        except ImportError:
            self._process = None

    def stop(self) -> None:
        with self._conn_lock:
            for conn in list(self._connections.values()):
                try:
                    conn.close()
                except Exception:
                    pass
            self._connections.clear()
        if self._process is not None:
            try:
                self._process.close()
            except Exception:
                pass
            self._process = None

    # ── Connection management ─────────────────────────────────────────────────

    def _require_process(self) -> Any:
        if self._process is None:
            self.start()
        if self._process is None:
            raise RuntimeError(
                "tableauhyperapi is not installed. Run: pip install tableauhyperapi"
            )
        return self._process

    def _get_connection(self, flow_id: str) -> Any:
        with self._conn_lock:
            if flow_id not in self._connections:
                from tableauhyperapi import Connection, CreateMode
                path = _hyper_dir() / f"{flow_id}.hyper"
                path.parent.mkdir(parents=True, exist_ok=True)
                process = self._require_process()
                conn = Connection(
                    process.endpoint, str(path), CreateMode.CREATE_IF_NOT_EXISTS
                )
                conn.catalog.create_schema_if_not_exists("sources")
                conn.catalog.create_schema_if_not_exists("materialized")
                self._connections[flow_id] = conn
            return self._connections[flow_id]

    def close_flow(self, flow_id: str) -> None:
        with self._conn_lock:
            conn = self._connections.pop(flow_id, None)
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        self._hashes.pop(flow_id, None)
        self._dirty.pop(flow_id, None)

    def hyper_path(self, flow_id: str) -> Path:
        return _hyper_dir() / f"{flow_id}.hyper"

    # ── Table helpers ─────────────────────────────────────────────────────────

    def _table_exists(self, conn: Any, schema: str, table: str) -> bool:
        try:
            with conn.execute_query(f'SELECT 1 FROM "{schema}"."{table}" LIMIT 0'):
                pass
            return True
        except Exception:
            return False

    def _drop_table(self, conn: Any, schema: str, table: str) -> None:
        try:
            conn.execute_command(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
        except Exception:
            pass

    # ── Source materialisation (live DB → Hyper "sources") ───────────────────

    def _materialize_source(self, flow_id: str, node: dict, connection_id: str) -> dict:
        from tableauhyperapi import TableDefinition, TableName, Inserter

        table_name = node.get("table", node["id"])
        node_id = node["id"]
        new_hash = compute_node_hash(node)

        conn = self._get_connection(flow_id)

        # Skip if hash matches (from node JSON — survives backend restarts) and table exists
        stored_hash = node.get("hash") or self._hashes.get(flow_id, {}).get(node_id)
        table_exists = self._table_exists(conn, "sources", table_name)
        log.info("hyper source check node=%s stored_hash=%s new_hash=%s table_exists=%s",
                 node_id, stored_hash, new_hash, table_exists)
        if stored_hash == new_hash and table_exists:
            self._dirty.get(flow_id, set()).discard(node_id)
            return {"ok": True, "node_id": node_id, "table": table_name, "skipped": True}

        try:
            from ..connectors.registry import get_connector
            with get_connector(connection_id) as db_conn:
                col_info = db_conn.get_column_info(table_name)
                rows = db_conn.execute_query(f'SELECT * FROM "{table_name}"')
        except Exception as exc:
            return {"ok": False, "node_id": node_id, "error": f"DB fetch failed: {exc}"}

        if not rows:
            return {"ok": False, "node_id": node_id, "error": "Source table is empty"}

        # Build column definitions from DB schema; fall back to row inference if unavailable
        if col_info:
            columns = [
                TableDefinition.Column(c["name"], _db_type_str_to_hyper(c["type_str"]))
                for c in col_info
            ]
        else:
            columns = [
                TableDefinition.Column(col, _map_py_to_hyper_type(val))
                for col, val in rows[0].items()
            ]

        hyper_table = TableName("sources", table_name)
        table_def = TableDefinition(hyper_table, columns)

        try:
            self._drop_table(conn, "sources", table_name)
            conn.catalog.create_table(table_def)
            col_names = [c.name.unescaped for c in columns]
            with Inserter(conn, table_def) as ins:
                for row in rows:
                    ins.add_row([row.get(name) for name in col_names])
                ins.execute()
        except Exception as exc:
            log.error("hyper create_table/insert failed node=%s table=%s error=%s", node_id, table_name, exc)
            return {"ok": False, "node_id": node_id, "error": f"Hyper write failed: {exc}"}

        self._hashes.setdefault(flow_id, {})[node_id] = new_hash
        self._dirty.get(flow_id, set()).discard(node_id)
        return {"ok": True, "node_id": node_id, "table": table_name, "row_count": len(rows)}

    # ── Join column deduplication ─────────────────────────────────────────────

    def _resolve_join_columns(self, conn: Any, node: dict, nbi: dict[str, dict]) -> str | None:
        """
        Returns an explicit SELECT column list for a join that renames right-side
        columns that clash with left-side columns (appends '_right' suffix).
        Returns None if schema introspection fails (caller keeps l.*, r.*).
        """
        from .compiler import resolve_upstream_mat, fuse_folds_sql, materialized_ref

        left_mat, left_folds = resolve_upstream_mat(node.get("left_input"), nbi)
        right_mat, right_folds = resolve_upstream_mat(node.get("right_input"), nbi)
        if not left_mat or not right_mat:
            return None

        def _cols(mat_node: dict, folds: list) -> list[str] | None:
            if folds:
                sql = fuse_folds_sql(mat_node, folds) + " LIMIT 0"
            else:
                sql = f"SELECT * FROM {materialized_ref(mat_node)} LIMIT 0"
            try:
                with conn.execute_query(sql) as result:
                    return [c.name.unescaped for c in result.schema.columns]
            except Exception:
                return None

        left_cols = _cols(left_mat, left_folds)
        right_cols = _cols(right_mat, right_folds)
        if not left_cols or not right_cols:
            return None

        left_set = set(left_cols)
        left_select = ", ".join(f'l."{c}"' for c in left_cols)
        right_parts: list[str] = []
        for c in right_cols:
            if c in left_set:
                right_parts.append(f'r."{c}" AS "{c}_right"')
            else:
                right_parts.append(f'r."{c}"')
        return f"{left_select}, {', '.join(right_parts)}"

    # ── Transform materialisation (Hyper → Hyper "materialized") ─────────────

    def _materialize_transform(self, flow_id: str, node: dict, nbi: dict[str, dict]) -> dict:
        node_id = node["id"]
        new_hash = compute_node_hash(node)
        conn = self._get_connection(flow_id)

        stored_hash = node.get("hash") or self._hashes.get(flow_id, {}).get(node_id)
        if stored_hash == new_hash and self._table_exists(conn, "materialized", node_id):
            self._dirty.get(flow_id, set()).discard(node_id)
            return {"ok": True, "node_id": node_id, "skipped": True}

        try:
            select_sql = compile_materialized(node, nbi)
        except Exception as exc:
            return {"ok": False, "node_id": node_id, "error": f"Compile error: {exc}"}

        # Resolve duplicate column names in join output before writing to Hyper
        if node.get("type") == "join":
            explicit_cols = self._resolve_join_columns(conn, node, nbi)
            if explicit_cols:
                select_sql = select_sql.replace("l.*, r.*", explicit_cols)
                log.debug("join dedup cols node=%s cols=%s", node_id, explicit_cols[:120])

        self._drop_table(conn, "materialized", node_id)
        try:
            conn.execute_command(
                f'CREATE TABLE "materialized"."{node_id}" AS ({select_sql})'
            )
        except Exception as exc:
            return {"ok": False, "node_id": node_id, "error": f"Hyper execute error: {exc}"}

        try:
            with conn.execute_query(f'SELECT COUNT(*) FROM "materialized"."{node_id}"') as result:
                row_count = next(iter(result))[0]
        except Exception:
            row_count = -1

        self._hashes.setdefault(flow_id, {})[node_id] = new_hash
        self._dirty.get(flow_id, set()).discard(node_id)
        return {"ok": True, "node_id": node_id, "row_count": row_count}

    # ── Ensure ancestors are materialized before a preview ───────────────────

    def _ensure_materialized(
        self,
        flow_id: str,
        node: dict,
        nbi: dict[str, dict],
        connection_id: str,
    ) -> list[dict]:
        """
        Walk ancestors of `node` in topological order.
        For each ancestor that is dirty or missing from Hyper:
          - source node  → fetch from live DB (only DB touch in the whole chain)
          - join/union/aggregate → run SQL against upstream Hyper tables
        Fold nodes are skipped (never stored).
        Returns list of materialization results.
        """
        ancestor_ids = _ancestor_ids(node, nbi)
        # Include the node itself if it is a materialized type
        if is_materialized_type(node.get("type", "")):
            ancestor_ids.add(node["id"])

        all_nodes = list(nbi.values())
        topo = topological_order(all_nodes)
        dirty = self._dirty.get(flow_id, set())
        conn = self._get_connection(flow_id)

        results: list[dict] = []
        for n in topo:
            nid = n["id"]
            if nid not in ancestor_ids:
                continue
            ntype = n.get("type", "")
            if not is_materialized_type(ntype):
                continue  # fold nodes: never stored

            # Decide if we need to materialize
            schema = "sources" if ntype == "source" else "materialized"
            tbl = n.get("table", nid) if ntype == "source" else nid
            is_dirty = nid in dirty
            is_missing = not self._table_exists(conn, schema, tbl)

            if not is_dirty and not is_missing:
                continue  # already clean and in Hyper → skip

            if ntype == "source":
                r = self._materialize_source(flow_id, n, connection_id)
            else:
                r = self._materialize_transform(flow_id, n, nbi)

            results.append(r)
            if not r.get("ok"):
                break  # stop chain on first failure

        return results

    # ── Open flow: materialize sources, mark transforms dirty ─────────────────

    def materialize_all_sources(
        self, flow_id: str, nodes: list[dict], connection_id: str
    ) -> dict:
        """
        Called when a flow is first opened.
        Fetches all source tables from live DB into Hyper "sources" schema.
        All transform nodes (aggregate/join/union) are marked DIRTY —
        they will be materialized lazily on first preview click.
        """
        source_nodes = [n for n in nodes if n.get("type") == "source"]
        log.info("hyper/open flow=%s sources=%s nodes_have_hash=%s",
                 flow_id,
                 [n.get("table", n["id"]) for n in source_nodes],
                 [bool(n.get("hash")) for n in source_nodes])

        results = []
        dirty = self._dirty.setdefault(flow_id, set())

        for node in nodes:
            ntype = node.get("type", "")
            if ntype == "source":
                r = self._materialize_source(flow_id, node, connection_id)
                results.append(r)
            elif ntype in MATERIALIZED_TYPES:
                dirty.add(node["id"])

        failed = [r for r in results if not r["ok"]]
        return {"ok": len(failed) == 0, "results": results, "failed": failed}

    # ── Preview ───────────────────────────────────────────────────────────────

    def get_preview(
        self,
        flow_id: str,
        node: dict,
        nbi: dict[str, dict],
        connection_id: str,
        limit: int = 100,
    ) -> dict:
        node_id = node["id"]
        ntype = node.get("type", "")

        # Ensure ancestors + this node are ready in Hyper
        chain_results = self._ensure_materialized(flow_id, node, nbi, connection_id)
        failed = [r for r in chain_results if not r.get("ok")]
        if failed:
            return {"ok": False, "node_id": node_id, "error": failed[0]["error"]}

        conn = self._get_connection(flow_id)

        if is_materialized_type(ntype):
            schema = "sources" if ntype == "source" else "materialized"
            tbl = node.get("table", node_id) if ntype == "source" else node_id
            sql = f'SELECT * FROM "{schema}"."{tbl}" LIMIT {limit}'
        else:
            # Fold node — build SQL chain from nearest upstream materialized table
            sql = build_fold_preview_sql(node, nbi, limit=limit)

        log.info("hyper preview sql node=%s: %s", node_id, sql)
        try:
            with conn.execute_query(sql) as result:
                col_names = [c.name.unescaped for c in result.schema.columns]
                rows = [dict(zip(col_names, r)) for r in result]
        except Exception as exc:
            log.error("hyper query failed node=%s sql=%s error=%s", node_id, sql, exc)
            return {"ok": False, "node_id": node_id, "error": str(exc)}

        return {"ok": True, "node_id": node_id, "rows": rows, "row_count": len(rows)}

    # ── Hash-based sync on node edit (called from PATCH /flows/{id}) ──────────

    def sync_changes(
        self,
        flow_id: str,
        old_nodes: list[dict],
        new_nodes: list[dict],
        edges: list[dict],
        connection_id: str,
    ) -> dict:
        """
        Called after nodes are saved to the DB.

        For each node whose payload hash changed:
          1. Drop its Hyper table immediately (data is stale).
          2. Drop all downstream materialized nodes' tables.
          3. Mark all of them as DIRTY.
          Actual re-materialization is deferred to the next preview click.

        Fold nodes are never stored so there is nothing to drop for them.
        """
        old_nbi = nodes_by_id(old_nodes)
        nbi = nodes_by_id(new_nodes)
        conn = self._get_connection(flow_id)
        dirty = self._dirty.setdefault(flow_id, set())

        changed_ids: list[str] = []
        for node in new_nodes:
            nid = node["id"]
            new_hash = compute_node_hash(node)
            old_node = old_nbi.get(nid)
            old_hash = old_node.get("hash") if old_node else None
            # Also treat nodes not yet in hash registry as changed
            stored_hash = self._hashes.get(flow_id, {}).get(nid)
            if new_hash != old_hash or new_hash != stored_hash:
                changed_ids.append(nid)

        if not changed_ids:
            return {"ok": True, "dirty": [], "skipped": "no changes detected"}

        # Collect everything that needs dropping
        to_drop: set[str] = set()
        for cid in changed_ids:
            node = nbi.get(cid)
            if node and is_materialized_type(node.get("type", "")):
                to_drop.add(cid)
            for did in downstream_materialized_ids(cid, new_nodes, edges):
                to_drop.add(did)

        # Drop Hyper tables and mark dirty — DO NOT re-materialize yet
        for nid in to_drop:
            n = nbi.get(nid)
            if not n:
                continue
            ntype = n.get("type", "")
            schema = "sources" if ntype == "source" else "materialized"
            tbl = n.get("table", nid) if ntype == "source" else nid
            self._drop_table(conn, schema, tbl)
            self._hashes.get(flow_id, {}).pop(nid, None)
            dirty.add(nid)

        return {"ok": True, "dirty": list(to_drop)}

    # ── Force invalidate ──────────────────────────────────────────────────────

    def invalidate_node(
        self,
        flow_id: str,
        node_id: str,
        nodes: list[dict],
        edges: list[dict],
    ) -> None:
        """Drop a node's Hyper table and all downstream materialized tables, mark dirty."""
        nbi = nodes_by_id(nodes)
        conn = self._get_connection(flow_id)
        dirty = self._dirty.setdefault(flow_id, set())

        node = nbi.get(node_id)
        if node:
            schema = "sources" if node.get("type") == "source" else "materialized"
            tbl = node.get("table", node_id) if node.get("type") == "source" else node_id
            self._drop_table(conn, schema, tbl)
            self._hashes.get(flow_id, {}).pop(node_id, None)
            dirty.add(node_id)

        for did in downstream_materialized_ids(node_id, nodes, edges):
            dn = nbi.get(did)
            if dn:
                self._drop_table(conn, "materialized", did)
                self._hashes.get(flow_id, {}).pop(did, None)
                dirty.add(did)
