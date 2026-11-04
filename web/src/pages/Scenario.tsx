import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useCallback, useMemo } from "react";

import { api, unwrap } from "../api/client";
import { Chart, type Option } from "../components/Chart";
import { useProjectId } from "../components/Layout";
import { ChartCard, ErrorState, Loading, PeriodFilters } from "../components/ui";
import { layoutGraph, stateLabel } from "../lib/analytics";
import { escapeHtml, number, percent } from "../lib/format";
import { periodQuery, usePeriod } from "../lib/period";
import type { ChartTokens } from "../lib/theme";

// direct share labels on the heaviest edges only; the rest are in the tooltip and table
const labelledEdges = 8;

function anchors(laid: ReturnType<typeof layoutGraph>) {
  const xs = laid.nodes.map((n) => n.x);
  const ys = laid.nodes.map((n) => n.y);
  const x = (Math.min(...xs) + Math.max(...xs)) / 2;
  const hidden = { symbolSize: 0, users: 0, label: { show: false }, tooltip: { show: false }, silent: true };
  return [
    { ...hidden, name: "\u0000top", x, y: Math.min(...ys) - 60 },
    { ...hidden, name: "\u0000bottom", x, y: Math.max(...ys) + 160 },
  ];
}

function graphOption(t: ChartTokens, laid: ReturnType<typeof layoutGraph>): Option {
  const heavy = new Set(
    [...laid.edges].sort((a, b) => b.users - a.users).slice(0, labelledEdges).map((e) => e.from + "\u0000" + e.to),
  );
  return {
    animation: false,
    tooltip: {
      backgroundColor: t.surface,
      borderColor: t.grid,
      textStyle: { color: t.text },
      formatter: (p: { dataType: string; data: { name?: string; users: number; source?: string; target?: string; share?: number } }) =>
        p.dataType === "edge"
          ? `<b>${escapeHtml(number(p.data.users))}</b> <span style="color:${t.textSecondary}">пользователей</span><div>${escapeHtml(stateLabel(p.data.source!))} → ${escapeHtml(stateLabel(p.data.target!))}</div><div style="color:${t.textSecondary}">${escapeHtml(percent(p.data.share ?? 0))} ушедших из состояния</div>`
          : `<b>${escapeHtml(number(p.data.users))}</b> <span style="color:${t.textSecondary}">${escapeHtml(stateLabel(p.data.name!))}</span>`,
    },
    series: [
      {
        type: "graph",
        layout: "none",
        roam: true,
        left: 60,
        right: 60,
        edgeSymbol: ["none", "arrow"],
        edgeSymbolSize: 7,
        label: { show: true, position: "bottom", color: t.text, fontSize: 12 },
        edgeLabel: { color: t.textSecondary, fontSize: 11 },
        data: [
          // ECharts fits the nodes' bounding box to the view: two invisible anchors keep
          // a sane aspect for a single row and leave room below for the loops back
          ...anchors(laid),
          ...laid.nodes.map((n) => ({
          name: n.state,
          x: n.x,
          y: n.y,
          users: n.users,
          symbolSize: 14,
          label: { formatter: () => stateLabel(n.state) },
          // the entry/exit is not a step of the scenario: muted
          itemStyle: { color: n.state === "" ? t.muted : t.series[0], borderColor: t.surface, borderWidth: 2 },
          })),
        ],
        links: laid.edges.map((e) => ({
          source: e.from,
          target: e.to,
          users: e.users,
          share: e.share,
          // loops back are context: quieter than the way forward
          lineStyle: { width: e.width, color: e.back ? t.axis : t.muted, opacity: 1, curveness: e.back ? 0.3 : 0.05 },
          label: { show: heavy.has(e.from + "\u0000" + e.to), formatter: () => percent(e.share) },
          emphasis: { lineStyle: { color: t.series[0] } },
        })),
      },
    ],
  };
}

export function ScenarioPage() {
  const projectId = useProjectId();
  const [period, setPeriod] = usePeriod();
  const q = periodQuery(period);
  const graph = useQuery({
    queryKey: ["graph", projectId, q],
    queryFn: () => unwrap(api.GET("/v1/projects/{projectID}/fsm-graph", { params: { path: { projectID: projectId }, query: q } })),
    placeholderData: keepPreviousData,
  });
  const g = graph.data;
  const laid = useMemo(() => (g ? layoutGraph(g) : null), [g]);
  const option = useCallback((t: ChartTokens) => (laid ? graphOption(t, laid) : {}), [laid]);
  const height = laid ? Math.min(720, Math.max(380, (Math.max(...laid.nodes.map((n) => n.y)) || 0) + 300)) : 380;

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Граф сценария</h1>
        <PeriodFilters period={period} onChange={setPeriod} />
      </div>
      {graph.isPending && <Loading />}
      {graph.error && <ErrorState error={graph.error} />}
      {g && g.edges.length === 0 && <div className="card empty">Переходов FSM за период нет: бот не использует состояния или ещё не получал сообщений.</div>}
      {g && laid && g.edges.length > 0 && (
        <>
          <ChartCard
            title="Переходы между состояниями"
            sub="слева направо от входа · толщина — пользователи · доля — из ушедших из состояния · колесо мыши — масштаб"
            chart={<Chart option={option} height={height} stale={graph.isPlaceholderData} label="Граф переходов FSM" />}
            table={
              <table className="data">
                <thead>
                  <tr>
                    <th>Из</th>
                    <th>В</th>
                    <th className="num">Пользователи</th>
                    <th className="num">Переходов</th>
                    <th className="num">Доля</th>
                  </tr>
                </thead>
                <tbody>
                  {g.edges.map((e) => (
                    <tr key={e.from + "→" + e.to}>
                      <td>{stateLabel(e.from)}</td>
                      <td>{stateLabel(e.to)}</td>
                      <td className="num">{number(e.users)}</td>
                      <td className="num">{number(e.transitions)}</td>
                      <td className="num">{percent(e.share)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            }
          />
          <section className="card">
            <h2>Где переспрашивают</h2>
            <div className="card-sub">хендлер не сменил состояние — пользователя спросили то же самое ещё раз (нейтральные хендлеры не считаются)</div>
            {g.retries.length === 0 ? (
              <div className="empty" style={{ padding: "20px 0" }}>
                Повторов нет.
              </div>
            ) : (
              <table className="data" style={{ marginTop: 8 }}>
                <thead>
                  <tr>
                    <th>Состояние</th>
                    <th className="num">Пользователи</th>
                    <th className="num">Повторов</th>
                  </tr>
                </thead>
                <tbody>
                  {g.retries.map((r) => (
                    <tr key={r.state}>
                      <td>{r.state}</td>
                      <td className="num">{number(r.users)}</td>
                      <td className="num">{number(r.retries)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </section>
        </>
      )}
    </div>
  );
}
