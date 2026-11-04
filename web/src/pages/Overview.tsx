import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useCallback, useMemo } from "react";

import { api, unwrap, type Schemas } from "../api/client";
import { Chart } from "../components/Chart";
import { useProjectId } from "../components/Layout";
import { ChartCard, ErrorState, Loading, PeriodFilters, StatTile } from "../components/ui";
import { columnChart, lineChart, type Mark } from "../lib/charts";
import { compact, day, ms, number, percent } from "../lib/format";
import { periodQuery, today, usePeriod } from "../lib/period";
import type { ChartTokens } from "../lib/theme";

type Overview = Schemas["Overview"];

export function useOverview(projectId: number) {
  const [period] = usePeriod();
  const q = periodQuery(period);
  return useQuery({
    queryKey: ["overview", projectId, q],
    queryFn: () => unwrap(api.GET("/v1/projects/{projectID}/overview", { params: { path: { projectID: projectId }, query: q } })),
    placeholderData: keepPreviousData, // refetch keeps the frame
  });
}

/** Deploys as marks on the day axis: bot starts of one day merge into one mark. */
export function deployMarks(o: Overview, tz: string): Mark[] {
  const index = new Map(o.days.map((d, i) => [d.day, i]));
  const byDay = new Map<number, string[]>();
  for (const d of o.deploys) {
    const i = index.get(today(tz, new Date(d.ts)));
    if (i === undefined) continue;
    const label = d.sdk_version ? `SDK ${d.sdk_version}` : "запуск";
    byDay.set(i, [...(byDay.get(i) ?? []), label]);
  }
  return [...byDay].map(([i, labels]) => ({
    index: i,
    label: labels.length === 1 ? `Запуск бота · ${labels[0]}` : `Запусков бота: ${labels.length}`,
  }));
}

function DaysTable({ o, columns }: { o: Overview; columns: Array<[string, (d: Overview["days"][number]) => number]> }) {
  return (
    <table className="data">
      <thead>
        <tr>
          <th>День</th>
          {columns.map(([title]) => (
            <th key={title} className="num">
              {title}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {[...o.days].reverse().map((d) => (
          <tr key={d.day}>
            <td>{day(d.day)}</td>
            {columns.map(([title, get]) => (
              <td key={title} className="num">
                {number(get(d))}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function OverviewPage() {
  const projectId = useProjectId();
  const [period, setPeriod] = usePeriod();
  const overview = useOverview(projectId);
  const o = overview.data;
  const stale = overview.isPlaceholderData;

  const marks = useMemo(() => (o ? deployMarks(o, period.tz) : []), [o, period.tz]);
  const days = useMemo(() => o?.days.map((d) => d.day) ?? [], [o]);

  const usersChart = useCallback(
    (t: ChartTokens) =>
      lineChart(t, days, [
        { name: "Активные за день", values: o?.days.map((d) => d.dau) ?? [] },
        { name: "Новые", values: o?.days.map((d) => d.new_users) ?? [] },
      ], marks),
    [o, days, marks],
  );
  const errorsChart = useCallback(
    (t: ChartTokens) =>
      columnChart(t, days, [
        { name: "Исключения в хендлерах", values: o?.days.map((d) => d.handler_errors) ?? [] },
        { name: "Ошибки Bot API", values: o?.days.map((d) => d.api_errors) ?? [] },
      ], marks),
    [o, days, marks],
  );
  const blocksChart = useCallback(
    (t: ChartTokens) => columnChart(t, days, [{ name: "Заблокировали бота", values: o?.days.map((d) => d.blocked) ?? [] }]),
    [o, days],
  );

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Обзор</h1>
        <PeriodFilters period={period} onChange={setPeriod} />
      </div>
      {overview.isPending && <Loading />}
      {overview.error && <ErrorState error={overview.error} />}
      {o && (
        <>
          <div className="tiles" style={stale ? { opacity: 0.5 } : undefined}>
            <StatTile label="Пользователи" value={compact(o.users)} note="писали боту за период" />
            <StatTile label="Новые" value={compact(o.new_users)} note={o.users ? `${percent(o.new_users / o.users)} от активных` : undefined} />
            <StatTile label="WAU" value={compact(o.wau)} note={`MAU ${compact(o.mau)}`} />
            <StatTile label="Заблокировали" value={compact(o.blocked)} note={o.users ? `${percent(o.blocked / o.users)} от активных` : undefined} />
            <StatTile label="Ошибки" value={compact(o.handler_errors + o.api_errors)} note={`хендлеры ${compact(o.handler_errors)} · Bot API ${compact(o.api_errors)}`} />
            <StatTile label="Обработка, p95" value={ms(o.handler_ms.p95)} note={`p50 ${ms(o.handler_ms.p50)} · p99 ${ms(o.handler_ms.p99)}`} />
            <StatTile label="Без обработчика" value={o.updates ? percent(o.unhandled / o.updates) : "—"} note={`${compact(o.unhandled)} из ${compact(o.updates)} апдейтов`} />
          </div>

          <ChartCard
            title="Пользователи по дням"
            sub={marks.length ? "вертикальные линии — запуски бота" : undefined}
            chart={<Chart option={usersChart} stale={stale} label="Активные и новые пользователи по дням" />}
            table={<DaysTable o={o} columns={[["Активные", (d) => d.dau], ["Новые", (d) => d.new_users]]} />}
          />
          <div className="grid-2">
            <ChartCard
              title="Ошибки по дням"
              chart={<Chart option={errorsChart} stale={stale} label="Ошибки хендлеров и Bot API по дням" />}
              table={<DaysTable o={o} columns={[["Хендлеры", (d) => d.handler_errors], ["Bot API", (d) => d.api_errors]]} />}
            />
            <ChartCard
              title="Заблокировали бота"
              sub="пользователей в день"
              chart={<Chart option={blocksChart} stale={stale} label="Блокировки бота по дням" />}
              table={<DaysTable o={o} columns={[["Заблокировали", (d) => d.blocked]]} />}
            />
          </div>
        </>
      )}
    </div>
  );
}
