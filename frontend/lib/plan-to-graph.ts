import { Node, Edge } from "@xyflow/react";
import type { PlanDraft, PlanStep, SourceTableRef } from "@/lib/types";
import type { FoldBadgeInfo } from "@/components/prep-flow-edge";

// ── Types for our backend's native nodes/edges format ────────────────────────

// Fold node types — hidden from canvas, shown as edge annotation badges.
// Matches FOLD_TYPES in src/hyper/compiler.py (minus "output" which stays on canvas).
const FOLD_BADGE_TYPES = new Set(["filter", "sort", "clean", "calculate", "rename"]);

function _foldBadgeLabel(n: BackendNode): string {
  if (n.type === "filter") return "Filter";
  if (n.type === "calculate") return n.alias ? `Calc: ${String(n.alias).slice(0, 10)}` : "Calc";
  if (n.type === "sort") return "Sort";
  if (n.type === "clean") return "Clean";
  if (n.type === "rename") return "Rename";
  return n.type;
}

function _foldBadgeDesc(n: BackendNode): string {
  if (n.type === "filter") return n.condition ? `WHERE ${String(n.condition).slice(0, 60)}` : "Filter step";
  if (n.type === "calculate") return `${n.alias ?? ""} = ${String(n.expression ?? "").slice(0, 40)}`;
  if (n.type === "sort") return "Sort step";
  if (n.type === "clean") {
    const ops = (n as Record<string, unknown>).operations as any[] | undefined ?? [];
    return ops.length ? `${ops.length} op(s): ${ops.map((o: any) => o.op).join(", ")}` : "Clean step";
  }
  if (n.type === "rename") return `Rename: ${Object.keys(n.mappings ?? {}).slice(0, 2).join(", ")}`;
  return n.type;
}

// Walks backward from startId through fold nodes until reaching the nearest
// materialized (non-fold) node. Returns that MAT node + the fold chain.
function _traceToMat(
  startId: string,
  nbi: Record<string, BackendNode>,
): { mat: BackendNode | null; folds: BackendNode[] } {
  const foldsReversed: BackendNode[] = [];
  let current: BackendNode | undefined = nbi[startId];
  while (current && FOLD_BADGE_TYPES.has(current.type)) {
    foldsReversed.push(current);
    const parentId = current.input as string | undefined;
    if (!parentId || !nbi[parentId]) return { mat: null, folds: [] };
    current = nbi[parentId];
  }
  return { mat: current ?? null, folds: foldsReversed.reverse() };
}

export interface BackendNode {
  id: string;
  type: "source" | "clean" | "script" | "filter" | "join" | "aggregate" | "rename" | "calculate" | "sort" | "union" | "output";
  table?: string;
  input?: string;
  left_input?: string;
  right_input?: string;
  condition?: string;
  group_by?: string[];
  aggregations?: Array<{ column: string; func?: string; function?: string; alias: string }>;
  mappings?: Record<string, string>;
  expression?: string;
  alias?: string;
  inputs?: string[];
  name?: string;
  [key: string]: unknown;
}

export interface BackendFlow {
  flow_id: string;
  name: string;
  connection_id: string;
  sql: string;
  nodes: BackendNode[];
  edges: Array<{ from: string; to: string }>;
  source_tables?: string[];
}

// ── Adapter: backend nodes/edges → React Flow graph ─────────────────────────

export function backendFlowToGraph(
  flow: BackendFlow,
  savedPositions?: Record<string, { x: number; y: number }>,
): { nodes: Node<PrepNodeData>[]; edges: Edge[] } {
  const rfNodes: Node<PrepNodeData>[] = [];
  const rfEdges: Edge[] = [];

  const startX = 50;
  const startY = 80;
  const MAT_COL_WIDTH = 300; // base MAT-to-MAT distance
  const FOLD_SLOT = 110;     // minimum pixels per fold node slot
  const rowHeight = 115;

  // ── Index all nodes ──────────────────────────────────────────────────────────
  const nbi: Record<string, BackendNode> = {};
  flow.nodes.forEach((n) => { nbi[n.id] = n; });

  // ── MAT-layer depth (fold nodes are transparent) ─────────────────────────────
  const matDepth: Record<string, number> = {};
  flow.nodes.filter((n) => n.type === "source").forEach((n) => { matDepth[n.id] = 0; });

  let changed = true;
  while (changed) {
    changed = false;
    flow.nodes.forEach((n) => {
      if (n.type === "source" || FOLD_BADGE_TYPES.has(n.type)) return;
      const inputIds = [n.input, n.left_input, n.right_input, ...(n.inputs ?? [])]
        .filter(Boolean) as string[];
      const matAncestorDepths = inputIds.map((id) => {
        const { mat } = _traceToMat(id, nbi);
        return mat ? (matDepth[mat.id] ?? -1) : -1;
      });
      const maxD = Math.max(-1, ...matAncestorDepths);
      if (maxD >= 0) {
        const desired = maxD + 1;
        if ((matDepth[n.id] ?? -1) < desired) {
          matDepth[n.id] = desired;
          changed = true;
        }
      }
    });
  }

  // ── MAT node Y (stack same-depth) ────────────────────────────────────────────
  const matYCounters: Record<number, number> = {};
  const matYPos: Record<string, number> = {};
  flow.nodes.forEach((n) => {
    if (FOLD_BADGE_TYPES.has(n.type) || n.type === "output") return;
    const depth = matDepth[n.id] ?? 0;
    const yIdx = matYCounters[depth] ?? 0;
    matYCounters[depth] = yIdx + 1;
    matYPos[n.id] = startY + yIdx * rowHeight;
  });

  // ── MAT node X — edge length expands when fold nodes need room ───────────────
  // Traverse in topological order: sources first, then each MAT node after its
  // upstream MAT ancestor is positioned.
  const matXPos: Record<string, number> = {};
  flow.nodes.filter((n) => n.type === "source").forEach((n) => { matXPos[n.id] = startX; });

  let matChanged = true;
  while (matChanged) {
    matChanged = false;
    flow.nodes.forEach((n) => {
      if (n.type === "source" || FOLD_BADGE_TYPES.has(n.type) || n.type === "output") return;
      const inputIds = [n.input, n.left_input, n.right_input, ...(n.inputs ?? [])]
        .filter(Boolean) as string[];
      for (const inputId of inputIds) {
        const { mat: upMat, folds } = _traceToMat(inputId, nbi);
        if (!upMat || !(upMat.id in matXPos)) continue;
        // Edge length = max of base width and fold-driven minimum
        const edgeLen = Math.max(MAT_COL_WIDTH, (folds.length + 1) * FOLD_SLOT + 80);
        const candidateX = matXPos[upMat.id] + edgeLen;
        if ((matXPos[n.id] ?? 0) < candidateX) {
          matXPos[n.id] = candidateX;
          matChanged = true;
        }
      }
    });
  }

  // ── Build fold chain info (which MAT nodes bracket each fold node) ───────────
  interface FoldInfo { upMat: BackendNode; downMat: BackendNode; chainIdx: number; chainLen: number }
  const foldInfo: Record<string, FoldInfo> = {};

  flow.nodes.forEach((n) => {
    if (FOLD_BADGE_TYPES.has(n.type) || n.type === "output") return;
    const inputIds = [n.input, n.left_input, n.right_input, ...(n.inputs ?? [])]
      .filter(Boolean) as string[];
    inputIds.forEach((inputId) => {
      const { mat: upMat, folds } = _traceToMat(inputId, nbi);
      if (!upMat) return;
      folds.forEach((fold, idx) => {
        foldInfo[fold.id] = { upMat, downMat: n, chainIdx: idx, chainLen: folds.length };
      });
    });
  });

  // ── Fold node positions ───────────────────────────────────────────────────────
  const foldXPos: Record<string, number> = {};
  const foldYPos: Record<string, number> = {};

  flow.nodes.forEach((n) => {
    if (!FOLD_BADGE_TYPES.has(n.type)) return;
    const info = foldInfo[n.id];
    if (info) {
      // Fold sits between two MAT nodes — distribute evenly
      const x1 = matXPos[info.upMat.id] ?? startX;
      const x2 = matXPos[info.downMat.id] ?? (startX + MAT_COL_WIDTH);
      const y1 = matYPos[info.upMat.id] ?? startY;
      foldXPos[n.id] = x1 + (info.chainIdx + 1) * (x2 - x1) / (info.chainLen + 1);
      foldYPos[n.id] = y1;
    } else {
      // Terminal fold — no downstream MAT yet; position after upstream MAT
      const { mat: upMat, folds: allFolds } = _traceToMat(n.id, nbi);
      const myIdx = allFolds.findIndex((f) => f.id === n.id);
      foldXPos[n.id] = (upMat ? (matXPos[upMat.id] ?? startX) : startX) + (myIdx + 1) * FOLD_SLOT;
      foldYPos[n.id] = upMat ? (matYPos[upMat.id] ?? startY) : startY;
    }
  });

  // ── Combined posMap for all non-output nodes ──────────────────────────────────
  const posMap: Record<string, { x: number; y: number }> = {};
  flow.nodes.forEach((n) => {
    if (FOLD_BADGE_TYPES.has(n.type)) {
      posMap[n.id] = { x: foldXPos[n.id] ?? startX, y: foldYPos[n.id] ?? startY };
    } else {
      posMap[n.id] = { x: matXPos[n.id] ?? startX, y: matYPos[n.id] ?? startY };
    }
  });

  // Build canvas nodes — all node types visible (fold nodes shown with compact style)
  flow.nodes.forEach((n) => {
    if (n.type === "output") return; // rendered separately below

    const pos = savedPositions?.[n.id] ?? posMap[n.id] ?? { x: startX, y: startY };
    const style = STEP_COLORS[n.type] ?? STEP_COLORS.filter;

    const label = n.type === "source" ? (n.table ?? n.id) : n.id;
    let description = "";
    if (n.type === "source") description = n.table ?? "";
    else if (n.type === "filter") description = n.condition ? `WHERE ${String(n.condition).slice(0, 60)}` : "Filter step";
    else if (n.type === "join") description = `${((n as any).join_type ?? "INNER").toUpperCase()} JOIN`;
    else if (n.type === "aggregate") description = `GROUP BY ${(n.group_by ?? []).join(", ") || "all"}`;
    else if (n.type === "rename") description = `Rename: ${Object.keys(n.mappings ?? {}).slice(0, 2).join(", ")}`;
    else if (n.type === "calculate") description = `Calc: ${n.alias ?? ""} = ${String(n.expression ?? "").slice(0, 40)}`;
    else if (n.type === "sort") description = "Sort step";
    else if (n.type === "union") description = `Stack ${(n.inputs ?? []).length} inputs`;
    else if (n.type === "clean") {
      const ops = ((n as Record<string, unknown>).operations as any[] | undefined) ?? [];
      description = ops.length ? `${ops.length} operation(s): ${ops.map((o: any) => o.op).join(", ")}` : "Clean step";
    }
    else if (n.type === "script") description = "Script (passthrough)";

    rfNodes.push({
      id: n.id,
      type: "prepStep",
      position: pos,
      data: {
        label,
        type: n.type,
        alias: n.id,
        description,
        badgeColor: style.badge,
        changesCount: _changesCount(n),
        details: n as unknown as Record<string, unknown>,
        // isFold signals the node renderer to use compact styling
        isFold: FOLD_BADGE_TYPES.has(n.type),
      },
    });
  });

  // Add final output node
  const outputNode = flow.nodes.find((n) => n.type === "output");
  if (outputNode) {
    const parentId = outputNode.input ?? flow.nodes[flow.nodes.length - 2]?.id;
    const parentPos = posMap[parentId ?? ""] ?? { x: startX, y: startY };
    const outputPos = savedPositions?.[outputNode.id] ?? { x: parentPos.x + MAT_COL_WIDTH, y: parentPos.y };
    rfNodes.push({
      id: outputNode.id,
      type: "prepStep",
      position: outputPos,
      data: {
        label: outputNode.name ?? "Tableau Extract",
        type: "output",
        alias: outputNode.id,
        description: "Ready to publish",
        badgeColor: STEP_COLORS.output.badge,
        details: outputNode as unknown as Record<string, unknown>,
      },
    });
  }

  // Build edges from backend edge list — all nodes are on canvas so no fold-chain collapsing needed
  flow.edges.forEach((e, idx) => {
    const target = e.to;
    if (!rfNodes.find((n) => n.id === e.from) || !rfNodes.find((n) => n.id === target)) return;
    rfEdges.push({
      id: `e-${idx}-${e.from}-${target}`,
      source: e.from,
      target,
      type: "prepEdge",
      animated: true,
      style: { stroke: "#6366f1", strokeWidth: 2 },
      data: {},
    });
  });

  return { nodes: rfNodes, edges: rfEdges };
}

function _changesCount(n: BackendNode): number {
  if (n.type === "filter") return 1;
  if (n.type === "aggregate") return (n.aggregations ?? []).length;
  if (n.type === "rename") return Object.keys(n.mappings ?? {}).length;
  if (n.type === "clean") return ((n as Record<string, unknown>).operations as any[] | undefined)?.length ?? 1;
  return 1;
}

export interface PrepNodeData extends Record<string, unknown> {
  label: string;
  type: string;
  alias: string;
  description: string;
  badgeColor?: string;
  details?: Record<string, unknown>;
  columnsCount?: number;
  changesCount?: number;
  isSelected?: boolean;
  isFold?: boolean;
  onDeleteStep?: (alias: string) => void;
  onAddStep?: (sourceAlias: string, stepType?: string) => void;
  onRun?: (nodeId: string) => void;
}

const STEP_COLORS: Record<string, { bg: string; border: string; text: string; badge: string; headerBg: string }> = {
  source: { bg: "bg-blue-50/50", border: "border-blue-500", text: "text-blue-700", badge: "bg-blue-100 text-blue-800", headerBg: "bg-blue-600 text-white" },
  clean: { bg: "bg-teal-50/50", border: "border-teal-500", text: "text-teal-700", badge: "bg-teal-100 text-teal-800", headerBg: "bg-teal-600 text-white" },
  filter: { bg: "bg-teal-50/50", border: "border-teal-500", text: "text-teal-700", badge: "bg-teal-100 text-teal-800", headerBg: "bg-teal-600 text-white" },
  join: { bg: "bg-purple-50/50", border: "border-purple-500", text: "text-purple-700", badge: "bg-purple-100 text-purple-800", headerBg: "bg-purple-600 text-white" },
  aggregate: { bg: "bg-orange-50/50", border: "border-orange-500", text: "text-orange-700", badge: "bg-orange-100 text-orange-800", headerBg: "bg-orange-600 text-white" },
  calculate: { bg: "bg-amber-50/50", border: "border-amber-500", text: "text-amber-800", badge: "bg-amber-100 text-amber-800", headerBg: "bg-amber-600 text-white" },
  drop_nulls: { bg: "bg-teal-50/50", border: "border-teal-500", text: "text-teal-700", badge: "bg-teal-100 text-teal-800", headerBg: "bg-teal-600 text-white" },
  dedupe: { bg: "bg-teal-50/50", border: "border-teal-500", text: "text-teal-700", badge: "bg-teal-100 text-teal-800", headerBg: "bg-teal-600 text-white" },
  rename: { bg: "bg-teal-50/50", border: "border-teal-500", text: "text-teal-700", badge: "bg-teal-100 text-teal-800", headerBg: "bg-teal-600 text-white" },
  select_columns: { bg: "bg-teal-50/50", border: "border-teal-500", text: "text-teal-700", badge: "bg-teal-100 text-teal-800", headerBg: "bg-teal-600 text-white" },
  union: { bg: "bg-yellow-50/50", border: "border-yellow-500", text: "text-yellow-800", badge: "bg-yellow-100 text-yellow-900", headerBg: "bg-yellow-600 text-white" },
  cast: { bg: "bg-teal-50/50", border: "border-teal-500", text: "text-teal-700", badge: "bg-teal-100 text-teal-800", headerBg: "bg-teal-600 text-white" },
  text_clean: { bg: "bg-teal-50/50", border: "border-teal-500", text: "text-teal-700", badge: "bg-teal-100 text-teal-800", headerBg: "bg-teal-600 text-white" },
  script: { bg: "bg-slate-50/50", border: "border-slate-400", text: "text-slate-700", badge: "bg-slate-200 text-slate-700", headerBg: "bg-slate-600 text-white" },
  output: { bg: "bg-slate-900", border: "border-blue-900", text: "text-slate-100", badge: "bg-blue-950 text-blue-200", headerBg: "bg-slate-950 text-white" },
};

export function planToFlowGraph(plan: PlanDraft): { nodes: Node<PrepNodeData>[]; edges: Edge[] } {
  const nodes: Node<PrepNodeData>[] = [];
  const edges: Edge[] = [];

  // Alias -> position / id map
  const aliasToNodeId: Record<string, string> = {};

  // Step 1: Layout Sources vertically on the left
  const startX = 50;
  const startY = 80;
  const colWidth = 260;
  const rowHeight = 110;

  plan.sources.forEach((src: SourceTableRef, idx: number) => {
    const nodeId = `source-${src.alias}`;
    aliasToNodeId[src.alias] = nodeId;

    nodes.push({
      id: nodeId,
      type: "prepStep",
      position: { x: startX, y: startY + idx * rowHeight },
      data: {
        label: src.table_name,
        type: "source",
        alias: src.alias,
        description: `${src.schema_name}.${src.table_name}`,
        badgeColor: STEP_COLORS.source.badge,
        details: { schema: src.schema_name, table: src.table_name },
      },
    });
  });

  // Step 2: Traverse steps and position downstream
  let currentX = startX + colWidth + 60;
  const currentY = startY;

  plan.steps.forEach((step: PlanStep, idx: number) => {
    const stepType = step.type;
    const outputAlias = step.output_alias;
    const nodeId = `step-${idx}-${outputAlias}`;
    aliasToNodeId[outputAlias] = nodeId;

    let description = "";
    if (step.type === "filter") {
      description = `${step.conditions.length} condition(s) on '${step.target}'`;
    } else if (step.type === "join") {
      description = `${step.join_type.toUpperCase()} join with '${step.right}'`;
    } else if (step.type === "drop_nulls") {
      description = `Drop nulls in: ${step.columns.join(", ")}`;
    } else if (step.type === "aggregate") {
      description = `Group by ${step.group_by.join(", ") || "all"}`;
    } else if (step.type === "calculate") {
      const calcs = step.calculations || [];
      description = `Calc: ${calcs.slice(0, 2).map((c) => `${c.name} = ${c.expression}`).join(", ")}`;
    } else if (step.type === "dedupe") {
      description = "Deduplicate rows";
    } else if (step.type === "rename") {
      const keys = Object.keys(step.mapping || {});
      description = `Rename: ${keys.slice(0, 2).map((k) => `${k} ➔ ${step.mapping[k]}`).join(", ")}`;
    } else if (step.type === "select_columns") {
      description = `Select ${step.columns.length} columns`;
    } else if (step.type === "union") {
      description = `Stack ${step.inputs.length} datasets (${step.distinct ? "UNION" : "UNION ALL"})`;
    } else if ((step as any).type === "cast") {
      const keys = Object.keys((step as any).mapping || {});
      description = `Cast: ${keys.slice(0, 2).map((k) => `${k} as ${(step as any).mapping[k]}`).join(", ")}`;
    } else if ((step as any).type === "text_clean") {
      const keys = Object.keys((step as any).operations || {});
      description = `Clean: ${keys.slice(0, 2).map((k) => `${(step as any).operations[k]}(${k})`).join(", ")}`;
    }

    // Determine Y position based on parent inputs
    let targetY = currentY;
    if (step.type === "join") {
      const leftParentId = aliasToNodeId[step.left];
      const rightParentId = aliasToNodeId[step.right];
      const leftNode = nodes.find((n) => n.id === leftParentId);
      const rightNode = nodes.find((n) => n.id === rightParentId);
      if (leftNode && rightNode) {
        targetY = (leftNode.position.y + rightNode.position.y) / 2;
      }
    } else if (step.type === "union" && step.inputs.length > 0) {
      const parentNodes = step.inputs
        .map((inp) => nodes.find((n) => n.id === aliasToNodeId[inp]))
        .filter(Boolean);
      if (parentNodes.length > 0) {
        const sumY = parentNodes.reduce((acc, n) => acc + (n?.position.y || 0), 0);
        targetY = sumY / parentNodes.length;
      }
    }

    let changesCount = 1;
    if (step.type === "filter") {
      changesCount = step.conditions.length;
    } else if (step.type === "calculate") {
      changesCount = (step.calculations || []).length;
    } else if (step.type === "rename") {
      changesCount = Object.keys(step.mapping || {}).length;
    } else if ((step as any).type === "cast") {
      changesCount = Object.keys((step as any).mapping || {}).length;
    } else if ((step as any).type === "text_clean") {
      changesCount = Object.keys((step as any).operations || {}).length;
    } else if (step.type === "drop_nulls") {
      changesCount = step.columns.length;
    } else if (step.type === "select_columns") {
      changesCount = step.columns.length;
    } else if (step.type === "aggregate") {
      changesCount = step.aggregations.length;
    }

    nodes.push({
      id: nodeId,
      type: "prepStep",
      position: { x: currentX, y: targetY },
      data: {
        label: outputAlias,
        type: stepType,
        alias: outputAlias,
        description,
        badgeColor: (STEP_COLORS[stepType] || STEP_COLORS.filter).badge,
        changesCount,
        details: step as unknown as Record<string, unknown>,
      },
    });

    // Create Edges
    if (step.type === "join") {
      if (aliasToNodeId[step.left]) {
        edges.push({
          id: `e-${aliasToNodeId[step.left]}-${nodeId}-left`,
          source: aliasToNodeId[step.left],
          target: nodeId,
          type: "prepEdge",
          animated: true,
          style: { stroke: "#10b981", strokeWidth: 2 },
          data: { sourceAlias: step.left, targetAlias: outputAlias },
        });
      }
      if (aliasToNodeId[step.right] && aliasToNodeId[step.right] !== aliasToNodeId[step.left]) {
        edges.push({
          id: `e-${aliasToNodeId[step.right]}-${nodeId}-right`,
          source: aliasToNodeId[step.right],
          target: nodeId,
          type: "prepEdge",
          animated: true,
          style: { stroke: "#10b981", strokeWidth: 2 },
          data: { sourceAlias: step.right, targetAlias: outputAlias },
        });
      }
    } else if (step.type === "union") {
      step.inputs.forEach((inp, iIdx) => {
        if (aliasToNodeId[inp]) {
          edges.push({
            id: `e-${aliasToNodeId[inp]}-${nodeId}-${iIdx}`,
            source: aliasToNodeId[inp],
            target: nodeId,
            type: "prepEdge",
            animated: true,
            style: { stroke: "#d946ef", strokeWidth: 2 },
            data: { sourceAlias: inp, targetAlias: outputAlias },
          });
        }
      });
    } else if ("target" in step && step.target && aliasToNodeId[step.target]) {
      edges.push({
        id: `e-${aliasToNodeId[step.target]}-${nodeId}`,
        source: aliasToNodeId[step.target],
        target: nodeId,
        type: "prepEdge",
        animated: true,
        style: { stroke: "#6366f1", strokeWidth: 2 },
        data: { sourceAlias: (step as any).target, targetAlias: outputAlias },
      });
    }

    currentX += colWidth;
  });

  // Step 3: Add Final Output Node
  const finalOutputAlias = plan.output_alias;
  const parentNodeId = aliasToNodeId[finalOutputAlias];
  if (parentNodeId) {
    const parentNode = nodes.find((n) => n.id === parentNodeId);
    const outputNodeId = "output-final";
    nodes.push({
      id: outputNodeId,
      type: "prepStep",
      position: { x: currentX, y: parentNode ? parentNode.position.y : startY },
      data: {
        label: "Tableau Hyper Extract",
        type: "output",
        alias: finalOutputAlias,
        description: "Ready to Publish",
        badgeColor: STEP_COLORS.output.badge,
        details: { output_alias: finalOutputAlias },
      },
    });

    edges.push({
      id: `e-${parentNodeId}-${outputNodeId}`,
      source: parentNodeId,
      target: outputNodeId,
      type: "prepEdge",
      animated: true,
      style: { stroke: "#ea580c", strokeWidth: 3 },
      data: { sourceAlias: finalOutputAlias, targetAlias: "output" },
    });
  }

  return { nodes, edges };
}
