import { useQuery } from "@tanstack/react-query";
import { useCallback, useState } from "react";

import { api, unwrap } from "../api/client";
import { Chart } from "../components/Chart";
import { useProjectId } from "../components/Layout";
import { ChartCard, ErrorState, Loading, StatTile } from "../components/ui";
import { sourceLabel } from "../lib/analytics";
import { barChart } from "../lib/charts";
import { compact, dateTime, number, percent } from "../lib/format";
import type { ChartTokens } from "../lib/theme";

/** The reason codes, most frequent first, in words. */
export function topReasons(reasons: Record<string, number>, texts: Record<string, string>, n = 2): string {
  return Object.entries(reasons)
    .sort((a, b) => b[1] - a[1])
    .slice(0, n)
    .map(([code, count]) => `${texts[code] ?? code} (${number(count)})`)
    .join("; ");
}

export function AntifraudPage() {
  const projectId = useProjectId();
  const [source, setSource] = useState<string | null>(null);
  const summary = useQuery({
    queryKey: ["antifraud", projectId],
    queryFn: () => unwrap(api.GET("/v1/projects/{projectID}/antifraud", { params: { path: { projectID: projectId } } })),
    refetchInterval: 60_000, // scored once a minute
  });
  const users = useQuery({
    queryKey: ["antifraud-users", projectId, source],
    enabled: source !== null,
    queryFn: async () =>
      (await unwrap(api.GET("/v1/projects/{projectID}/antifraud/users", { params: { path: { projectID: projectId }, query: { source: source!, limit: 50 } } }))).users,
  });

  const s = summary.data;
  const flagged = (s?.sources ?? []).filter((x) => x.suspicious > 0).slice(0, 10);
  const chart = useCallback(
    (t: ChartTokens) =>
      barChart(
        t,
        flagged.map((x) => sourceLabel(x.source)),
        flagged.map((x) => x.suspicious),
        (i) => {
          const x = flagged[i];
          return x ? `${number(x.suspicious)} из ${number(x.users)} · ${percent(x.share)}` : "";
        },
      ),
    [flagged],
  );

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Антифрод</h1>
        {s?.updated_at && <span className="muted">оценка за {s.window_hours} ч · обновлено {dateTime(s.updated_at)}</span>}
      </div>
      {summary.isPending && <Loading />}
      {summary.error && <ErrorState error={summary.error} />}
      {s && !s.updated_at && <div className="card empty">Оценки ещё нет: сервис антифрода считает раз в минуту по пользователям последних суток.</div>}
      {s?.updated_at && (
        <>
          <div className="tiles">
            <StatTile label="Пользователи за сутки" value={compact(s.users)} />
            <StatTile label="Похожи на ботоферму" value={compact(s.suspicious)} note={s.users ? `${percent(s.suspicious / s.users)} от всех` : undefined} />
            <StatTile label="Источников с фермами" value={number(s.sources.filter((x) => x.share >= 0.5).length)} note="где подозрительных больше половины" />
          </div>
          {flagged.length > 0 && (
            <ChartCard
              title="Подозрительные пользователи по источникам"
              sub={`пользователь подозрителен от оценки ${s.threshold}; первые ${flagged.length}`}
              chart={<Chart option={chart} height={Math.max(140, flagged.length * 40)} label="Подозрительные пользователи по источникам" />}
              table={<p className="secondary">Полная таблица ниже.</p>}
            />
          )}
          <section className="card">
            <h2>Источники</h2>
            <div className="card-sub" style={{ marginBottom: 8 }}>
              нажмите на источник — покажем подозрительных пользователей
            </div>
            <table className="data">
              <thead>
                <tr>
                  <th>Источник</th>
                  <th className="num">Пользователи</th>
                  <th className="num">Подозрительные</th>
                  <th className="num">Доля</th>
                  <th>Главные признаки</th>
                </tr>
              </thead>
              <tbody>
                {s.sources.map((x) => (
                  <tr key={x.source}>
                    <td>
                      <button className="link" onClick={() => setSource(x.source)} aria-pressed={source === x.source}>
                        {sourceLabel(x.source)}
                      </button>
                    </td>
                    <td className="num">{number(x.users)}</td>
                    <td className="num">{number(x.suspicious)}</td>
                    <td className="num">{percent(x.share)}</td>
                    <td className="secondary">{topReasons(x.reasons, s.reason_texts) || "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
          {source !== null && (
            <section className="card">
              <h2>Подозрительные из «{sourceLabel(source)}»</h2>
              {users.isPending && <Loading />}
              {users.data?.length === 0 && <div className="empty">Подозрительных нет.</div>}
              {users.data && users.data.length > 0 && (
                <table className="data" style={{ marginTop: 8 }}>
                  <thead>
                    <tr>
                      <th>user_id</th>
                      <th>Пришёл</th>
                      <th className="num">Оценка</th>
                      <th>Признаки</th>
                    </tr>
                  </thead>
                  <tbody>
                    {users.data.map((u) => (
                      <tr key={u.user_id}>
                        <td>
                          <a href={`/p/${projectId}/user?id=${u.user_id}`}>{u.user_id}</a>
                        </td>
                        <td>{dateTime(u.joined)}</td>
                        <td className="num">{u.score.toFixed(2)}</td>
                        <td className="secondary">{u.reasons.map((r) => s.reason_texts[r] ?? r).join("; ")}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </section>
          )}
        </>
      )}
    </div>
  );
}
