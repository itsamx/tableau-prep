"""Flow endpoints — CRUD, checkpoints, run, execute."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from ...store import crud
from ...connectors.registry import get_connector, get_connection_details
from ...validation.sql import check_sql
from ..deps import get_actor

router = APIRouter(prefix="/flows", tags=["flows"])


# ── Request bodies ────────────*=*=*────────-+-+-+-+-───────────────────────────────────

class CreateFlowBody(BaseModel):
    name: str
    connection_id: str

class PatchFlowBody(BaseModel):
    nodes: list
    edges: list
    sql: str
    node_sql: dict = {}
    node_positions: dict = {}


class CheckpointBody(BaseModel):
    nodes: list
    edges: list
    sql: str
    node_sql: dict = {}
    node_positions: dict = {}
    change_summary: str | None = None


# ── Flow CRUD ────────────────────────────────────────────────────────────────

@router.post("")
def create_flow(body: CreateFlowBody, actor: str = Depends(get_actor)):
    """Create a new empty flow (called when user first drags a table to canvas)."""
    flow_id = str(uuid.uuid4())
    flow = crud.save_flow(
        flow_id=flow_id,
        name=body.name,
        connection_id=body.connection_id,
        sql="",
        nodes=[],
        edges=[],
        source_tables=[],
    )
    return flow


@router.get("")
def list_flows(actor: str = Depends(get_actor)):
    return crud.list_flows()


@router.get("/{flow_id}")
def get_flow(flow_id: str, actor: str = Depends(get_actor)):
    flow = crud.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    return flow


@router.patch("/{flow_id}")
def update_flow(flow_id: str, body: PatchFlowBody, actor: str = Depends(get_actor)):
    """Update the live nodes/edges of a flow (canvas edits)."""
    flow = crud.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    old_nodes = flow["nodes"]
    updated = crud.update_flow_nodes(flow_id, body.nodes, body.edges, body.sql, body.node_sql, body.node_positions)

    # Trigger hash-based Hyper re-materialisation for changed materialized nodes
    try:
        from ...hyper.manager import HyperManager
        HyperManager.get().sync_changes(
            flow_id=flow_id,
            old_nodes=old_nodes,
            new_nodes=body.nodes,
            edges=body.edges,
            connection_id=flow["connection_id"],
        )
    except Exception:
        pass  # Hyper sync is best-effort; never block the PATCH response

    return updated


# ── Checkpoints ──────────────────────────────────────────────────────────────

@router.get("/{flow_id}/checkpoints")
def list_checkpoints(flow_id: str, actor: str = Depends(get_actor)):
    return crud.list_checkpoints(flow_id)


@router.post("/{flow_id}/checkpoints")
def save_checkpoint(flow_id: str, body: CheckpointBody, actor: str = Depends(get_actor)):
    """Save a named checkpoint of the current canvas state."""
    flow = crud.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    cp = crud.save_checkpoint(
        flow_id,
        body.nodes,
        body.edges,
        body.sql,
        node_sql=body.node_sql,
        node_positions=body.node_positions,
        change_summary=body.change_summary,
        created_by=actor,
    )
    return cp


@router.post("/{flow_id}/checkpoints/{cp_id}/restore")
def restore_checkpoint(flow_id: str, cp_id: str, actor: str = Depends(get_actor)):
    """Restore a checkpoint as the active flow state."""
    result = crud.restore_checkpoint(flow_id, cp_id)
    if not result:
        raise HTTPException(status_code=404, detail="Checkpoint not found")
    return result


# ── Run (execute SQL, store results) ────────────────────────────────────────

@router.post("/{flow_id}/run")
def run_flow(flow_id: str, actor: str = Depends(get_actor)):
    """
    Validate and execute the flow's SQL against the live database.
    Stores results in query_runs table.
    """
    flow = crud.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    sql = flow["sql"]
    if not sql or not sql.strip():
        return {"ok": False, "run": None,
                "error": "No SQL for this flow. Add a table to the canvas first."}

    connection_id = flow["connection_id"]

    # sqlglot check
    details = get_connection_details(connection_id)
    dialect_map = {"sqlite": "sqlite", "mysql": "mysql", "postgresql": "postgres"}
    dialect = dialect_map.get(details.get("type", "sqlite"), "sqlite")
    check = check_sql(sql, dialect=dialect)
    if not check["ok"]:
        run = crud.save_query_run(
            flow_id, sql, [], 0,
            status="failed",
            error=f"SQL check failed: {check['report']}",
        )
        return {"ok": False, "run": run, "error": check["report"]}

    # Execute
    row_limit = 1000
    try:
        with get_connector(connection_id) as conn:
            rows = conn.execute_query(check["corrected_sql"], row_limit=row_limit)
        run = crud.save_query_run(flow_id, sql, rows, len(rows), status="ok")
        return {"ok": True, "run": run, "rows": rows, "row_count": len(rows)}
    except Exception as exc:
        run = crud.save_query_run(flow_id, sql, [], 0, status="failed", error=str(exc))
        return {"ok": False, "run": run, "error": str(exc)}



# ── Execute (materialize into Hyper via HyperManager) ───────────────────────

@router.post("/{flow_id}/execute")
def execute_flow(flow_id: str, actor: str = Depends(get_actor)):
    """
    Materialize all source nodes into the flow's Hyper file and persist the
    extract record. Uses HyperManager — same pipeline as the canvas preview.
    """
    flow = crud.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    from ...hyper.manager import HyperManager
    mgr = HyperManager.get()
    result = mgr.materialize_all_sources(
        flow_id=flow_id,
        nodes=flow["nodes"],
        connection_id=flow["connection_id"],
    )

    if result.get("ok"):
        row_count = sum(r.get("row_count", 0) for r in result.get("results", []) if r.get("ok"))
        extract = crud.save_hyper_extract(
            flow_id,
            str(mgr.hyper_path(flow_id)),
            row_count,
        )
        return {"ok": True, "hyper": extract, "row_count": row_count}
    else:
        return {"ok": False, "error": result.get("failed")}
