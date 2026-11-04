import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import { api, unwrap } from "../api/client";
import { Chart } from "../components/Chart";
import { useProjectId } from "../components/Layout";
import { ChartCard, ErrorState, Loading, PeriodFilters } from "../components/ui";
import { sourceLabel, stepKinds, type FunnelStep } from "../lib/analytics";
import { barChart } from "../lib/charts";
import { number, percent, rubles } from "../lib/format";
import { periodQuery, usePeriod } from "../lib/period";
import type { ChartTokens } from "../lib/theme";

const top = 10;

export function SourcesPage() {
  const projectId = useProjectId();
  const [period, setPeriod] = usePeriod();
  const [goalDraft, setGoalDraft] = useState<FunnelStep>({ kind: "custom", value: "purchase" });
  const [goal, setGoal] = useState<FunnelStep | null>(null);

  const q = periodQuery(period);
  const sources = useQuery({
    queryKey: ["sources", projectId, q, goal],
    queryFn: () =>
      unwrap(
        api.GET("/v1/projects/{projectID}/sources", {
          params: { path: { projectID: projectId }, query: { ...q, ...(goal ? { goal: `${goal.kind}:${goal.value}` } : {}) } },
        }),
      ),
    placeholderData: keepPreviousData,
  });
  const list = sources.data?.sources ?? [];
  const ranked = list.filter((s) => s.users > 0).slice(0, top);

  const chart = useCallback(
    (t: ChartTokens) =>
      barChart(
        t,
        ranked.map((s) => sourceLabel(s.source)),
        ranked.map((s) => s.users),
        (i) => {
          const s = ranked[i];
          return s && goal ? `${number(s.users)} · ${percent(s.conversion)} в цель` : number(s?.users ?? 0);
        },
      ),
    [ranked, goal],
  );

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Источники</h1>
        <PeriodFilters period={period} onChange={setPeriod} />
      </div>
      <form
        className="card filters"
        onSubmit={(e) => {
          e.preventDefault();
          setGoal(goalDraft.value.trim() ? { ...goalDraft, value: goalDraft.value.trim() } : null);
        }}
      >
        <span className="secondary">Цель</span>
        <select aria-label="Цель: тип" value={goalDraft.kind} onChange={(e) => setGoalDraft({ ...goalDraft, kind: e.target.value as FunnelStep["kind"] })}>
          {stepKinds.map((k) => (
            <option key={k.kind} value={k.kind}>
              {k.label}
            </option>
          ))}
        </select>
        <input aria-label="Цель: значение" value={goalDraft.value} onChange={(e) => setGoalDraft({ ...goalDraft, value: e.target.value })} placeholder="purchase" />
        <button type="submit">Посчитать конверсию</button>
        <span className="muted">пользователи — по первому /start с меткой в периоде</span>
      </form>

      {sources.isPending && <Loading />}
      {sources.error && <ErrorState error={sources.error} />}
      {sources.data && (
        <>
          {ranked.length > 0 && (
            <ChartCard
              title={`Пользователи по источникам${list.length > top ? `, первые ${top}` : ""}`}
              chart={<Chart option={chart} height={Math.max(160, ranked.length * 40)} stale={sources.isPlaceholderData} label="Пользователи по источникам" />}
              table={<SourcesTable list={list} goal={!!goal} />}
            />
          )}
          <section className="card">
            <h2>Все источники</h2>
            {list.length === 0 ? <div className="empty">За период никто не пришёл по /start.</div> : <SourcesTable list={list} goal={!!goal} />}
          </section>
        </>
      )}
    </div>
  );
}

function SourcesTable({ list, goal }: { list: Array<import("../api/client").Schemas["Source"]>; goal: boolean }) {
  return (
    <div style={{ overflowX: "auto" }}>
      <table className="data">
        <thead>
          <tr>
            <th>Метка</th>
            <th className="num">Пользователи</th>
            {goal && <th className="num">В цель</th>}
            {goal && <th className="num">Конверсия</th>}
            <th className="num">Заблокировали</th>
            <th className="num">Расходы</th>
            <th className="num">За пользователя</th>
            {goal && <th className="num">За конверсию</th>}
          </tr>
        </thead>
        <tbody>
          {list.map((s) => (
            <tr key={s.source}>
              <td>{sourceLabel(s.source)}</td>
              <td className="num">{number(s.users)}</td>
              {goal && <td className="num">{number(s.converted)}</td>}
              {goal && <td className="num">{percent(s.conversion)}</td>}
              <td className="num">
                {number(s.blocked)} <span className="muted">({percent(s.block_rate)})</span>
              </td>
              <td className="num">{rubles(s.spend_rub)}</td>
              <td className="num">{rubles(s.cost_per_user_rub)}</td>
              {goal && <td className="num">{rubles(s.cost_per_conversion_rub)}</td>}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
