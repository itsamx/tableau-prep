"""Hyper materialisation endpoints."""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

log = logging.getLogger(__name__)

from ...store import crud
from ...hyper.manager import HyperManager
from ...hyper.compiler import nodes_by_id

router = APIRouter(prefix="/flows", tags=["hyper"])


class InvalidateBody(BaseModel):
    node_id: str


# ── Open: materialize all source nodes ───────────────────────────────────────

@router.post("/{flow_id}/hyper/open")
def open_flow_hyper(flow_id: str):
    """
    Fetch all source tables from the live DB and write them into the flow's
    Hyper file ("sources" schema). Call this once after loading a flow.
    """
    flow = crud.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    mgr = HyperManager.get()
    result = mgr.materialize_all_sources(
        flow_id=flow_id,
        nodes=flow["nodes"],
        connection_id=flow["connection_id"],
    )
    return result


# ── Preview: get rows for any node ────────────────────────────────────────────

@router.get("/{flow_id}/hyper/{node_id}/preview")
def node_preview(flow_id: str, node_id: str, limit: int = 100):
    """
    Return up to `limit` rows for a node.

    - Materialized nodes (source/join/union/aggregate): queries the stored Hyper table.
    - Fold nodes (filter/sort/clean/calculate/output): builds SQL from the nearest
      upstream materialized table, runs it, returns rows WITHOUT storing them.
    """
    flow = crud.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    nbi = nodes_by_id(flow["nodes"])
    node = nbi.get(node_id)
    if not node:
        # Fallback: find source node whose table matches node_id (handles table-name aliases)
        node = next(
            (n for n in flow["nodes"] if n.get("type") == "source" and n.get("table") == node_id),
            None,
        )
    if not node:
        raise HTTPException(status_code=404, detail=f"Node {node_id!r} not found in flow")

    mgr = HyperManager.get()
    result = mgr.get_preview(
        flow_id, node, nbi,
        connection_id=flow["connection_id"],
        limit=min(limit, 1000),
    )
    if not result["ok"]:
        log.error("hyper preview failed node=%s flow=%s error=%s", node_id, flow_id, result.get("error"))
        raise HTTPException(status_code=422, detail=result["error"])
    return result


# ── Invalidate: force re-materialize a node + downstream ─────────────────────

@router.post("/{flow_id}/hyper/invalidate")
def invalidate_node(flow_id: str, body: InvalidateBody):
    """
    Drop the Hyper table for `node_id` and all downstream materialized nodes,
    then re-materialize them from the live DB / upstream Hyper tables.
    """
    flow = crud.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    mgr = HyperManager.get()
    # Drop stale tables
    mgr.invalidate_node(
        flow_id=flow_id,
        node_id=body.node_id,
        nodes=flow["nodes"],
        edges=flow["edges"],
    )
    # Re-open sources so they are fresh; transforms stay dirty until clicked
    result = mgr.materialize_all_sources(
        flow_id=flow_id,
        nodes=flow["nodes"],
        connection_id=flow["connection_id"],
    )
    return result


# ── Close: release the Hyper connection for a flow ───────────────────────────

@router.delete("/{flow_id}/hyper")
def close_flow_hyper(flow_id: str):
    """Close the Hyper connection for a flow (does not delete the .hyper file)."""
    HyperManager.get().close_flow(flow_id)
    return {"ok": True, "flow_id": flow_id}
