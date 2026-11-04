import type { Schemas } from "../api/client";

// Pure data transforms of the analytics screens, kept apart from React for tests.

export type FunnelStep = Schemas["FunnelStep"];

export const stepKinds: Array<{ kind: FunnelStep["kind"]; label: string; hint: string }> = [
  { kind: "command", label: "Команда", hint: "start" },
  { kind: "callback_data", label: "Кнопка", hint: "lab:lab2" },
  { kind: "state", label: "Состояние FSM", hint: "Form:name" },
  { kind: "custom", label: "Событие", hint: "purchase" },
];

export function stepLabel(s: FunnelStep): string {
  switch (s.kind) {
    case "command":
      return "/" + s.value.replace(/^\//, "");
    case "callback_data":
      return `кнопка ${s.value}`;
    case "state":
      return `→ ${s.value}`;
    case "custom":
      return s.value;
  }
}

/** "" is the source of users who came without a deep link label. */
export function sourceLabel(s: string): string {
  return s === "" ? "без метки" : s;
}

/** The empty FSM state is both the entry (no state yet) and the exit (state cleared). */
export function stateLabel(s: string): string {
  return s === "" ? "вне сценария" : s;
}

// --- scenario graph ---

export interface LaidNode {
  state: string;
  x: number;
  y: number;
  layer: number;
  users: number;
}

export interface LaidEdge {
  from: string;
  to: string;
  users: number;
  share: number;
  width: number;
  back: boolean; // goes to an earlier or the same layer: a loop, drawn curved
}

/** Layers by the shortest path from the entry, rows by traffic within a layer. Bots have
 *  loops ("start over"), so this is a directed graph, not a Sankey; edges going back are
 *  marked to be drawn curved. Widths are 1-8 px by users. */
export function layoutGraph(graph: Pick<Schemas["Graph"], "nodes" | "edges">): { nodes: LaidNode[]; edges: LaidEdge[] } {
  const out = new Map<string, Array<{ to: string; users: number }>>();
  for (const e of graph.edges) {
    out.set(e.from, [...(out.get(e.from) ?? []), { to: e.to, users: e.users }]);
  }
  const layer = new Map<string, number>();
  const start = graph.nodes.some((n) => n.state === "") ? "" : graph.nodes[0]?.state;
  if (start !== undefined) {
    layer.set(start, 0);
    const queue = [start];
    while (queue.length) {
      const s = queue.shift()!;
      // heavier edges first: the main path gets the top rows
      for (const e of [...(out.get(s) ?? [])].sort((a, b) => b.users - a.users)) {
        if (!layer.has(e.to)) {
          layer.set(e.to, layer.get(s)! + 1);
          queue.push(e.to);
        }
      }
    }
  }
  // states unreachable from the entry go after the deepest layer
  const deepest = Math.max(0, ...layer.values());
  for (const n of graph.nodes) {
    if (!layer.has(n.state)) layer.set(n.state, deepest + 1);
  }
  const users = new Map(graph.nodes.map((n) => [n.state, Math.max(n.in, n.out)]));
  const byLayer = new Map<number, string[]>();
  for (const [s, l] of layer) byLayer.set(l, [...(byLayer.get(l) ?? []), s]);
  const nodes: LaidNode[] = [];
  for (const [l, states] of byLayer) {
    states
      .sort((a, b) => (users.get(b) ?? 0) - (users.get(a) ?? 0) || a.localeCompare(b))
      .forEach((s, row) => nodes.push({ state: s, layer: l, x: l * 220, y: row * 90, users: users.get(s) ?? 0 }));
  }
  const max = Math.max(1, ...graph.edges.map((e) => e.users));
  const edges = graph.edges.map((e) => ({
    from: e.from,
    to: e.to,
    users: e.users,
    share: e.share,
    width: 1 + Math.round((7 * e.users) / max),
    back: (layer.get(e.to) ?? 0) <= (layer.get(e.from) ?? 0),
  }));
  return { nodes, edges };
}

// --- retention heatmap ---

function rgb(hex: string): [number, number, number] {
  const h = hex.replace("#", "");
  const full = h.length === 3 ? [...h].map((c) => c + c).join("") : h;
  return [0, 2, 4].map((i) => parseInt(full.slice(i, i + 2), 16)) as [number, number, number];
}

function luminance([r, g, b]: [number, number, number]): number {
  const lin = (c: number) => {
    const v = c / 255;
    return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b);
}

/** A cell of the sequential scale: one hue from low to high, and the label ink that
 *  reads on it (white or near-black by the fill's luminance). */
export function heatCell(share: number, low: string, high: string): { background: string; color: string } {
  const t = Math.min(1, Math.max(0, share));
  const a = rgb(low);
  const b = rgb(high);
  const mix = a.map((c, i) => Math.round(c + (b[i]! - c) * t)) as [number, number, number];
  return {
    background: `rgb(${mix.join(",")})`,
    color: luminance(mix) > 0.4 ? "#0b0b0b" : "#ffffff",
  };
}
