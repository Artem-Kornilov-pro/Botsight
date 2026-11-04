import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useState } from "react";
import { useOutletContext } from "react-router-dom";

import { api, errorText, unwrap, type Schemas } from "../api/client";
import { Chart } from "../components/Chart";
import { useProjectId } from "../components/Layout";
import { ChartCard, ErrorState, PeriodFilters } from "../components/ui";
import { sourceLabel, stepKinds, stepLabel, type FunnelStep } from "../lib/analytics";
import { barChart } from "../lib/charts";
import { number, percent } from "../lib/format";
import { periodQuery, usePeriod } from "../lib/period";
import type { ChartTokens } from "../lib/theme";

type Definition = Schemas["FunnelDefinition"];
type Project = Schemas["Project"] | undefined;

const windows = [
  { seconds: 3600, label: "1 час" },
  { seconds: 86400, label: "1 день" },
  { seconds: 7 * 86400, label: "7 дней" },
  { seconds: 30 * 86400, label: "30 дней" },
];

const initial: Definition = {
  steps: [
    { kind: "command", value: "start" },
    { kind: "state", value: "" },
  ],
  window_seconds: 86400,
  breakdown: "",
};

function StepsEditor({ steps, onChange }: { steps: FunnelStep[]; onChange: (s: FunnelStep[]) => void }) {
  const set = (i: number, patch: Partial<FunnelStep>) => onChange(steps.map((s, j) => (j === i ? { ...s, ...patch } : s)));
  return (
    <ol className="stack" style={{ gap: 8, paddingLeft: 20, margin: 0 }}>
      {steps.map((s, i) => (
        <li key={i}>
          <div className="filters">
            <select aria-label={`Шаг ${i + 1}: тип`} value={s.kind} onChange={(e) => set(i, { kind: e.target.value as FunnelStep["kind"] })}>
              {stepKinds.map((k) => (
                <option key={k.kind} value={k.kind}>
                  {k.label}
                </option>
              ))}
            </select>
            <input
              aria-label={`Шаг ${i + 1}: значение`}
              placeholder={stepKinds.find((k) => k.kind === s.kind)?.hint}
              value={s.value}
              onChange={(e) => set(i, { value: e.target.value })}
              maxLength={256}
            />
            {steps.length > 2 && (
              <button type="button" aria-label={`Удалить шаг ${i + 1}`} onClick={() => onChange(steps.filter((_, j) => j !== i))}>
                ✕
              </button>
            )}
          </div>
        </li>
      ))}
    </ol>
  );
}

export function FunnelsPage() {
  const projectId = useProjectId();
  const project = useOutletContext<Project>();
  const canEdit = project?.role === "owner" || project?.role === "editor";
  const qc = useQueryClient();
  const [period, setPeriod] = usePeriod();
  const [draft, setDraft] = useState<Definition>(initial);
  const [submitted, setSubmitted] = useState<Definition | null>(null);
  const [name, setName] = useState("");

  const q = periodQuery(period);
  const result = useQuery({
    queryKey: ["funnel", projectId, q, submitted],
    enabled: submitted !== null,
    queryFn: () =>
      unwrap(
        api.POST("/v1/projects/{projectID}/funnels/query", {
          params: { path: { projectID: projectId }, query: q },
          body: { definition: submitted! },
        }),
      ),
    placeholderData: keepPreviousData,
  });

  const saved = useQuery({
    queryKey: ["funnels", projectId],
    queryFn: async () => (await unwrap(api.GET("/v1/projects/{projectID}/funnels", { params: { path: { projectID: projectId } } }))).funnels,
  });
  const save = useMutation({
    mutationFn: () => unwrap(api.POST("/v1/projects/{projectID}/funnels", { params: { path: { projectID: projectId } }, body: { name, definition: draft } })),
    onSuccess: () => {
      setName("");
      void qc.invalidateQueries({ queryKey: ["funnels", projectId] });
    },
  });
  const remove = useMutation({
    mutationFn: (id: number) => unwrap(api.DELETE("/v1/projects/{projectID}/funnels/{funnelID}", { params: { path: { projectID: projectId, funnelID: id } } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["funnels", projectId] }),
  });

  const f = result.data;
  const chart = useCallback(
    (t: ChartTokens) =>
      barChart(
        t,
        f?.steps.map((s, i) => `${i + 1}. ${stepLabel(s.step)}`) ?? [],
        f?.steps.map((s) => s.users) ?? [],
        (i) => {
          const s = f?.steps[i];
          return s ? (i === 0 ? number(s.users) : `${number(s.users)} · ${percent(s.from_prev)} от предыдущего`) : "";
        },
      ),
    [f],
  );
  const ready = draft.steps.every((s) => s.value.trim() !== "");

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Воронки</h1>
        <PeriodFilters period={period} onChange={setPeriod} />
      </div>
      <div className="grid-2" style={{ gridTemplateColumns: "minmax(320px, 2fr) minmax(240px, 1fr)" }}>
        <form
          className="card form"
          onSubmit={(e) => {
            e.preventDefault();
            setSubmitted({ ...draft, steps: draft.steps.map((s) => ({ ...s, value: s.value.trim() })) });
          }}
        >
          <h2>Шаги по порядку</h2>
          <StepsEditor steps={draft.steps} onChange={(steps) => setDraft({ ...draft, steps })} />
          <div className="filters">
            <button type="button" disabled={draft.steps.length >= 20} onClick={() => setDraft({ ...draft, steps: [...draft.steps, { kind: "state", value: "" }] })}>
              + шаг
            </button>
            <label className="filters secondary">
              окно
              <select value={draft.window_seconds} onChange={(e) => setDraft({ ...draft, window_seconds: Number(e.target.value) })}>
                {windows.map((w) => (
                  <option key={w.seconds} value={w.seconds}>
                    {w.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="filters secondary">
              <input type="checkbox" checked={draft.breakdown === "source"} onChange={(e) => setDraft({ ...draft, breakdown: e.target.checked ? "source" : "" })} />
              по источникам
            </label>
            <button className="primary" type="submit" disabled={!ready}>
              Посчитать
            </button>
          </div>
        </form>
        <section className="card">
          <h2>Сохранённые</h2>
          {saved.data?.length === 0 && <p className="muted">Пока нет.</p>}
          <ul className="stack" style={{ listStyle: "none", padding: 0, gap: 6, margin: "10px 0" }}>
            {saved.data?.map((s) => (
              <li key={s.id} className="filters" style={{ justifyContent: "space-between" }}>
                <button className="link" onClick={() => { setDraft(s.definition); setSubmitted(s.definition); }}>
                  {s.name}
                </button>
                {canEdit && (
                  <button aria-label={`Удалить воронку ${s.name}`} onClick={() => remove.mutate(s.id)}>
                    ✕
                  </button>
                )}
              </li>
            ))}
          </ul>
          {canEdit && (
            <form className="filters" onSubmit={(e) => { e.preventDefault(); save.mutate(); }}>
              <input placeholder="Название" value={name} onChange={(e) => setName(e.target.value)} required maxLength={100} aria-label="Название воронки" />
              <button type="submit" disabled={!ready || save.isPending}>
                Сохранить
              </button>
            </form>
          )}
          {save.error && <div className="error">{errorText(save.error)}</div>}
        </section>
      </div>

      {result.error && <ErrorState error={result.error} />}
      {!submitted && <div className="empty">Задайте шаги и нажмите «Посчитать».</div>}
      {f && (
        <>
          <ChartCard
            title="Дошли до шага"
            sub={`шаги по порядку в пределах окна · из первого шага дошли до последнего ${percent(f.steps[f.steps.length - 1]?.from_first ?? 0)}`}
            chart={<Chart option={chart} height={Math.max(160, f.steps.length * 48)} stale={result.isPlaceholderData} label="Воронка: пользователи на каждом шаге" />}
            table={
              <table className="data">
                <thead>
                  <tr>
                    <th>Шаг</th>
                    <th className="num">Пользователи</th>
                    <th className="num">От предыдущего</th>
                    <th className="num">От первого</th>
                  </tr>
                </thead>
                <tbody>
                  {f.steps.map((s, i) => (
                    <tr key={i}>
                      <td>{`${i + 1}. ${stepLabel(s.step)}`}</td>
                      <td className="num">{number(s.users)}</td>
                      <td className="num">{percent(s.from_prev)}</td>
                      <td className="num">{percent(s.from_first)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            }
          />
          {f.breakdown && f.breakdown.length > 0 && (
            // sources are many: a table, conversion to the last step is what matters
            <section className="card">
              <h2>По источникам</h2>
              <div className="card-sub" style={{ marginBottom: 8 }}>
                метка первого /start; пользователи на каждом шаге
              </div>
              <div style={{ overflowX: "auto" }}>
                <table className="data">
                  <thead>
                    <tr>
                      <th>Источник</th>
                      {f.steps.map((s, i) => (
                        <th key={i} className="num">
                          {i + 1}. {stepLabel(s.step)}
                        </th>
                      ))}
                      <th className="num">Конверсия</th>
                    </tr>
                  </thead>
                  <tbody>
                    {f.breakdown.map((b) => (
                      <tr key={b.source}>
                        <td>{sourceLabel(b.source)}</td>
                        {b.steps.map((s, i) => (
                          <td key={i} className="num">
                            {number(s.users)}
                          </td>
                        ))}
                        <td className="num">{percent(b.steps[b.steps.length - 1]?.from_first ?? 0)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}
        </>
      )}
    </div>
  );
}
