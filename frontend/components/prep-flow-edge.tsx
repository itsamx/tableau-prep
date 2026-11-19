"use client";

import React from "react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type EdgeProps,
} from "@xyflow/react";

export interface FoldBadgeInfo {
  id: string;
  type: string;
  label: string;
  description: string;
  details: Record<string, unknown>;
}

export interface PrepEdgeData {
  sourceAlias?: string;
  targetAlias?: string;
  foldedChanges?: FoldBadgeInfo[];
  onAddStep?: (sourceAlias: string, targetAlias: string) => void;
  onFoldClick?: (badge: FoldBadgeInfo) => void;
}

const BADGE_COLORS: Record<string, { bg: string; text: string; dot: string; ring: string }> = {
  filter:    { bg: "bg-teal-100",   text: "text-teal-800",   dot: "bg-teal-500",   ring: "ring-teal-300"   },
  sort:      { bg: "bg-sky-100",    text: "text-sky-800",    dot: "bg-sky-500",    ring: "ring-sky-300"    },
  clean:     { bg: "bg-teal-100",   text: "text-teal-800",   dot: "bg-teal-500",   ring: "ring-teal-300"   },
  calculate: { bg: "bg-amber-100",  text: "text-amber-800",  dot: "bg-amber-500",  ring: "ring-amber-300"  },
  rename:    { bg: "bg-teal-100",   text: "text-teal-800",   dot: "bg-teal-500",   ring: "ring-teal-300"   },
};

function FoldBadge({
  badge,
  onClick,
}: {
  badge: FoldBadgeInfo;
  onClick: () => void;
}) {
  const c = BADGE_COLORS[badge.type] ?? {
    bg: "bg-slate-100", text: "text-slate-700", dot: "bg-slate-400", ring: "ring-slate-200",
  };

  return (
    <button
      onClick={(e) => { e.stopPropagation(); onClick(); }}
      title={badge.description}
      className={`
        inline-flex items-center gap-1 px-2 py-0.5 rounded-full
        text-[10px] font-semibold leading-none select-none
        ${c.bg} ${c.text}
        border border-white/60 shadow-sm
        hover:ring-2 hover:ring-offset-1 ${c.ring}
        active:scale-95 transition-all duration-100 cursor-pointer
        whitespace-nowrap
      `}
    >
      <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${c.dot}`} />
      {badge.label}
    </button>
  );
}

export function PrepFlowEdge({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  markerEnd,
  data,
}: EdgeProps) {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const edgeData = data as PrepEdgeData | undefined;
  const folds = edgeData?.foldedChanges ?? [];

  return (
    <>
      <BaseEdge id={id} path={edgePath} style={style} markerEnd={markerEnd} />
      {folds.length > 0 && (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan flex items-center gap-1"
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              pointerEvents: "all",
              zIndex: 10,
            }}
          >
            {folds.map((badge) => (
              <FoldBadge
                key={badge.id}
                badge={badge}
                onClick={() => edgeData?.onFoldClick?.(badge)}
              />
            ))}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}
