import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useOutletContext } from "react-router-dom";

import { api, ApiError, errorText, unwrap, type Schemas } from "../api/client";
import { useProjectId } from "../components/Layout";
import { ErrorState, Loading } from "../components/ui";
import { stepKinds, type FunnelStep } from "../lib/analytics";
import { dateTime, number, percent } from "../lib/format";

type Segment = Schemas["Segment"];
type Broadcast = Schemas["Broadcast"];
type Project = Schemas["Project"] | undefined;

const statusText: Record<Broadcast["status"], string> = { sending: "идёт", done: "завершена", canceled: "отменена" };

// delivery states are statuses: the reserved status colors, always with words beside
export const parts = [
  { key: "sent", label: "доставлено", color: "var(--good)" },
  { key: "blocked", label: "заблокировали", color: "var(--warning)" },
  { key: "failed", label: "ошибка", color: "var(--critical)" },
  { key: "pending", label: "в очереди", color: "var(--grid)" },
] as const;

/** Thin segmented bar: one whole split into delivery states, no text on it. */
export function Progress({ stats, total }: { stats: Broadcast["stats"]; total: number }) {
  return (
    <div style={{ display: "flex", gap: 2, height: 8, borderRadius: 4, overflow: "hidden", minWidth: 160 }} role="img" aria-label={parts.map((p) => `${p.label} ${stats[p.key]}`).join(", ")}>
      {parts.map((p) =>
        stats[p.key] > 0 ? <span key={p.key} style={{ flex: stats[p.key] / Math.max(1, total), background: p.color, borderRadius: 4 }} /> : null,
      )}
    </div>
  );
}

function StepPicker({ label, value, onChange }: { label: string; value?: FunnelStep; onChange: (s?: FunnelStep) => void }) {
  return (
    <label className="filters secondary">
      {label}
      <select aria-label={`${label}: тип`} value={value?.kind ?? ""} onChange={(e) => onChange(e.target.value ? { kind: e.target.value as FunnelStep["kind"], value: value?.value ?? "" } : undefined)}>
        <option value="">—</option>
        {stepKinds.map((k) => (
          <option key={k.kind} value={k.kind}>
            {k.label}
          </option>
        ))}
      </select>
      {value && <input aria-label={`${label}: значение`} value={value.value} onChange={(e) => onChange({ ...value, value: e.target.value })} placeholder={stepKinds.find((k) => k.kind === value.kind)?.hint} />}
    </label>
  );
}

export function BroadcastsPage() {
  const projectId = useProjectId();
  const project = useOutletContext<Project>();
  const canSend = project?.role === "owner" || project?.role === "editor";
  const qc = useQueryClient();
  const [segment, setSegment] = useState<Segment>({ active_days: 30 });
  const [text, setText] = useState("");
  const [parseMode, setParseMode] = useState<"" | "HTML" | "MarkdownV2">("");
  const [open, setOpen] = useState<number | null>(null);

  const clean = (s: Segment): Segment => ({
    ...s,
    source: s.source?.trim() ? s.source.trim() : undefined,
    reached: s.reached?.value.trim() ? s.reached : undefined,
    not_reached: s.not_reached?.value.trim() ? s.not_reached : undefined,
  });

  const list = useQuery({
    queryKey: ["broadcasts", projectId],
    queryFn: async () => (await unwrap(api.GET("/v1/projects/{projectID}/broadcasts", { params: { path: { projectID: projectId } } }))).broadcasts,
    // progress moves while the bots send
    refetchInterval: (q) => (q.state.data?.some((b) => b.status === "sending") ? 5000 : false),
  });
  const preview = useMutation({
    mutationFn: () => unwrap(api.POST("/v1/projects/{projectID}/broadcasts/preview", { params: { path: { projectID: projectId } }, body: { segment: clean(segment) } })),
  });
  const create = useMutation({
    mutationFn: () => unwrap(api.POST("/v1/projects/{projectID}/broadcasts", { params: { path: { projectID: projectId } }, body: { text, parse_mode: parseMode, segment: clean(segment) } })),
    onSuccess: () => {
      setText("");
      preview.reset();
      void qc.invalidateQueries({ queryKey: ["broadcasts", projectId] });
    },
  });
  const cancel = useMutation({
    mutationFn: (id: number) => unwrap(api.POST("/v1/projects/{projectID}/broadcasts/{broadcastID}/cancel", { params: { path: { projectID: projectId, broadcastID: id } } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["broadcasts", projectId] }),
  });
  const detail = useQuery({
    queryKey: ["broadcast", projectId, open],
    enabled: open !== null,
    queryFn: () => unwrap(api.GET("/v1/projects/{projectID}/broadcasts/{broadcastID}", { params: { path: { projectID: projectId, broadcastID: open! } } })),
  });

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Рассылки</h1>
      </div>
      <section className="card stack" style={{ gap: 12 }}>
        <div>
          <h2>Кому</h2>
          <div className="card-sub">писали боту в личном чате; заблокировавшие бота исключаются всегда</div>
        </div>
        <div className="filters">
          <label className="filters secondary">
            активны за, дней
            <input type="number" min={1} max={365} value={segment.active_days} onChange={(e) => setSegment({ ...segment, active_days: Number(e.target.value) })} style={{ width: 80 }} aria-label="Активны за, дней" />
          </label>
          <label className="filters secondary">
            не заходят, дней
            <input type="number" min={0} value={segment.inactive_days ?? 0} onChange={(e) => setSegment({ ...segment, inactive_days: Number(e.target.value) || undefined })} style={{ width: 80 }} aria-label="Не заходят, дней" />
          </label>
          <label className="filters secondary">
            метка
            <input value={segment.source ?? ""} onChange={(e) => setSegment({ ...segment, source: e.target.value })} placeholder="любая" style={{ width: 140 }} aria-label="Метка" />
          </label>
        </div>
        <div className="filters">
          <StepPicker label="сделали" value={segment.reached} onChange={(s) => setSegment({ ...segment, reached: s })} />
          <StepPicker label="не сделали" value={segment.not_reached} onChange={(s) => setSegment({ ...segment, not_reached: s })} />
          <button type="button" onClick={() => preview.mutate()} disabled={preview.isPending}>
            Посчитать получателей
          </button>
          {preview.data && (
            <b role="status">
              {number(preview.data.recipients)} получателей{preview.data.recipients > preview.data.max_recipients ? ` — больше лимита ${number(preview.data.max_recipients)}` : ""}
            </b>
          )}
          {preview.error && <span className="error">{errorText(preview.error)}</span>}
        </div>
        {canSend && (
          <form
            className="form"
            onSubmit={(e) => {
              e.preventDefault();
              if (confirm("Отправить рассылку? Её начнут отправлять боты с broadcasts=True в SDK.")) create.mutate();
            }}
          >
            <h2>Что</h2>
            <textarea rows={4} value={text} onChange={(e) => setText(e.target.value)} maxLength={4096} required placeholder="Текст сообщения, до 4096 символов" aria-label="Текст рассылки" />
            <div className="filters">
              <select value={parseMode} onChange={(e) => setParseMode(e.target.value as typeof parseMode)} aria-label="Разметка">
                <option value="">без разметки</option>
                <option value="HTML">HTML</option>
                <option value="MarkdownV2">MarkdownV2</option>
              </select>
              <button className="primary" type="submit" disabled={create.isPending || !text.trim()}>
                Отправить
              </button>
              <span className="muted">отправляет сам бот: включите broadcasts=True в setup()</span>
            </div>
            {create.error && (
              <div className="error">
                {create.error instanceof ApiError && create.error.code === "empty_segment" ? "В сегменте никого нет" : errorText(create.error)}
              </div>
            )}
          </form>
        )}
      </section>

      <section className="card">
        <div className="card-head">
          <h2>Рассылки</h2>
          <div className="filters" aria-hidden="true">
            {parts.map((p) => (
              <span key={p.key} className="filters secondary" style={{ gap: 4 }}>
                <span className="dot" style={{ background: p.color, borderRadius: 2 }} />
                {p.label}
              </span>
            ))}
          </div>
        </div>
        {list.isPending && <Loading />}
        {list.error && <ErrorState error={list.error} />}
        {list.data?.length === 0 && <div className="empty">Рассылок ещё не было.</div>}
        {list.data && list.data.length > 0 && (
          <table className="data">
            <thead>
              <tr>
                <th>Текст</th>
                <th>Статус</th>
                <th>Прогресс</th>
                <th className="num">Доставлено</th>
                <th className="num">Заблокировали</th>
                <th className="num">Ошибки</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {list.data.map((b) => (
                <tr key={b.id}>
                  <td style={{ maxWidth: 320 }}>
                    <button className="link" onClick={() => setOpen(open === b.id ? null : b.id)} aria-expanded={open === b.id} style={{ textAlign: "left" }}>
                      {b.text.length > 60 ? b.text.slice(0, 60) + "…" : b.text}
                    </button>
                    <div className="muted">
                      {dateTime(b.created_at)} · {number(b.total)} получателей
                    </div>
                  </td>
                  <td>{statusText[b.status]}</td>
                  <td>
                    <Progress stats={b.stats} total={b.total} />
                  </td>
                  <td className="num">
                    {number(b.stats.sent)} <span className="muted">{percent(b.stats.sent / Math.max(1, b.total))}</span>
                  </td>
                  <td className="num">{number(b.stats.blocked)}</td>
                  <td className="num">{number(b.stats.failed)}</td>
                  <td className="num">
                    {canSend && b.status === "sending" && <button onClick={() => cancel.mutate(b.id)}>Отменить</button>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {open !== null && detail.data && (
          <div className="card" style={{ marginTop: 12 }}>
            <h2>Причины ошибок</h2>
            {Object.keys(detail.data.stats.errors ?? {}).length === 0 ? (
              <p className="muted">Ошибок нет.</p>
            ) : (
              <ul>
                {Object.entries(detail.data.stats.errors ?? {}).map(([reason, n]) => (
                  <li key={reason}>
                    {reason}: {number(n)}
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </section>
    </div>
  );
}
