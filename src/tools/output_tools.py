"""MCP tool implementations for Flow JSON and Hyper extract generation."""
from __future__ import annotations

from langsmith import traceable

from ..outputs import flow_json


@traceable(name="generate_flow_json", run_type="tool")
def tool_generate_flow_json(plan: dict, name: str | None = None) -> dict:
    """
    Serialize a validated flow plan into Flow JSON.

    The plan must have already passed validate_flow_plan.

    Args:
        plan: Validated plan dict with nodes, edges, sql, connection_id.
        name: Optional display name for the flow.

    Returns:
        {"ok": true, "flow_json": {...}}
        {"ok": false, "error": "..."}
    """
    try:
        result = flow_json.generate(plan, name=name)
        return {"ok": True, "flow_json": result}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@traceable(name="materialize_flow_hyper", run_type="tool")
def tool_materialize_flow_hyper(flow_id: str, nodes: list[dict], connection_id: str) -> dict:
    """
    Materialize all source nodes into the flow's Hyper file via HyperManager.
    Transform nodes (join/union/aggregate) are marked dirty and materialized
    lazily on first preview. Uses the same pipeline as the frontend canvas.

    Args:
        flow_id: The flow's UUID.
        nodes: The flow's node list (must include source nodes with 'table' field).
        connection_id: The live DB connection to fetch source data from.

    Returns:
        {"ok": bool, "results": [...], "failed": [...], "hyper_path": "..."}
    """
    try:
        from ..hyper.manager import HyperManager
        mgr = HyperManager.get()
        mgr.start()
        result = mgr.materialize_all_sources(flow_id, nodes, connection_id)
        result["hyper_path"] = str(mgr.hyper_path(flow_id))
        return result
    except Exception as exc:
        return {"ok": False, "error": str(exc), "results": [], "failed": []}
