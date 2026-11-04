import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";

import { api, unwrap } from "../api/client";
import { useProjectId } from "../components/Layout";
import { ErrorState, Loading } from "../components/ui";
import { heatCell } from "../lib/analytics";
import { day, number, percent } from "../lib/format";
import { browserTimeZone, usePeriod } from "../lib/period";
import { chartTokens, useThemeVersion } from "../lib/theme";

const weekOptions = [4, 8, 12, 26];

/** Cohorts as a heatmap table: one hue, darker is more retained. The table is the chart
 *  and its table view at once: every value is printed in its cell. */
export function RetentionPage() {
  const projectId = useProjectId();
  const [period] = usePeriod();
  const tz = period.tz || browserTimeZone();
  const [weeks, setWeeks] = useState(8);
  const theme = useThemeVersion();
  // the scale is re-read when the theme changes
  const seq = useMemo(() => chartTokens().seq, [theme]);

  const retention = useQuery({
    queryKey: ["retention", projectId, weeks, tz],
    queryFn: () => unwrap(api.GET("/v1/projects/{projectID}/retention", { params: { path: { projectID: projectId }, query: { weeks, tz } } })),
    placeholderData: keepPreviousData,
  });
  const r = retention.data;
  const columns = r ? Math.max(0, ...r.cohorts.map((c) => c.active.length)) : 0;

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Удержание</h1>
        <div className="filters">
          <div className="segmented" role="group" aria-label="Когорт">
            {weekOptions.map((w) => (
              <button key={w} aria-pressed={weeks === w} onClick={() => setWeeks(w)}>
                {w} недель
              </button>
            ))}
          </div>
        </div>
      </div>
      {retention.isPending && <Loading />}
      {retention.error && <ErrorState error={retention.error} />}
      {r && (
        <section className="card" style={retention.isPlaceholderData ? { opacity: 0.5 } : undefined}>
          <div className="card-head">
            <div>
              <h2>Недельные когорты</h2>
              <div className="card-sub">доля пользователей когорты, вернувшихся на неделе N после первого апдейта · {r.tz}</div>
            </div>
            <div className="filters muted" aria-hidden="true">
              0 %
              <span style={{ width: 120, height: 10, borderRadius: 2, background: `linear-gradient(90deg, ${seq.low}, ${seq.high})` }} />
              100 %
            </div>
          </div>
          {r.cohorts.length === 0 ? (
            <div className="empty">За эти недели новых пользователей не было.</div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table className="data heat">
                <thead>
                  <tr>
                    <th>Когорта</th>
                    <th className="num">Пользователи</th>
                    {Array.from({ length: columns }, (_, i) => (
                      <th key={i} className="num">
                        {i === 0 ? "Неделя 0" : `+${i}`}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {r.cohorts.map((c) => (
                    <tr key={c.week}>
                      <td>с {day(c.week)}</td>
                      <td className="num">{number(c.users)}</td>
                      {Array.from({ length: columns }, (_, i) => {
                        const share = c.share[i];
                        if (share === undefined) return <td key={i} />; // the week has not begun
                        return (
                          <td key={i} className="num heat-cell" style={heatCell(share, seq.low, seq.high)} title={`${number(c.active[i] ?? 0)} из ${number(c.users)}`}>
                            {percent(share)}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}
    </div>
  );
}
