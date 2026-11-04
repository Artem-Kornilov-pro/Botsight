import { useMutation, useQuery } from "@tanstack/react-query";
import { Fragment, useState } from "react";

import { api, ApiError, errorText, unwrap, type Schemas } from "../api/client";
import { useProjectId } from "../components/Layout";
import { ErrorState, Loading } from "../components/ui";
import { dateTime, number, percent } from "../lib/format";

type Topic = Schemas["Topic"];

const aiErrors: Record<string, string> = {
  ai_unavailable: "AI-модуль недоступен",
  llm_disabled: "LLM не настроена — вопросы недоступны, темы называются ключевыми словами",
  invalid_sql: "Не получилось составить безопасный запрос — переформулируйте вопрос",
  query_failed: "Запрос не выполнился — переформулируйте вопрос",
  invalid_question: "Вопрос не похож на вопрос о данных бота",
  rate_limited: "Лимит вопросов на час исчерпан",
};

/** Growth of a topic's share as a signed delta; the arrow and the word carry it, not color alone. */
export function growthText(t: Pick<Topic, "growth" | "messages_prev_7d">): string {
  if (!t.growth || !t.messages_prev_7d) return "новая";
  const change = t.growth - 1;
  if (Math.abs(change) < 0.05) return "без изменений";
  return `${change > 0 ? "▲" : "▼"} ${percent(Math.abs(change))}`;
}

function Examples({ projectId, topic }: { projectId: number; topic: number }) {
  const examples = useQuery({
    queryKey: ["topic-examples", projectId, topic],
    queryFn: async () =>
      (await unwrap(api.GET("/v1/projects/{projectID}/topics/{topicID}/examples", { params: { path: { projectID: projectId, topicID: topic } } }))).messages,
  });
  if (examples.isPending) return <Loading />;
  if (examples.error) return <ErrorState error={examples.error} />;
  return (
    <ul className="feed">
      {examples.data?.map((m) => (
        <li key={m.event_id} style={{ gridTemplateColumns: "120px 1fr" }}>
          <span className="ts">{dateTime(m.ts)}</span>
          <span>{m.text}</span>
        </li>
      ))}
    </ul>
  );
}

export function AIPage() {
  const projectId = useProjectId();
  const [open, setOpen] = useState<number | null>(null);
  const [question, setQuestion] = useState("");
  const topics = useQuery({
    queryKey: ["topics", projectId],
    queryFn: async () => (await unwrap(api.GET("/v1/projects/{projectID}/topics", { params: { path: { projectID: projectId } } }))).topics,
  });
  const alerts = useQuery({
    queryKey: ["alerts", projectId],
    queryFn: async () => (await unwrap(api.GET("/v1/projects/{projectID}/alerts", { params: { path: { projectID: projectId } } }))).alerts,
  });
  const ask = useMutation({
    mutationFn: (q: string) => unwrap(api.POST("/v1/projects/{projectID}/ask", { params: { path: { projectID: projectId } }, body: { question: q } })),
  });
  const a: Schemas["Answer"] | undefined = ask.data;

  return (
    <div className="stack">
      <div className="page-head">
        <h1>AI</h1>
      </div>

      <form
        className="card form"
        onSubmit={(e) => {
          e.preventDefault();
          if (question.trim()) ask.mutate(question.trim());
        }}
      >
        <h2>Спросить</h2>
        <div className="filters">
          <input
            style={{ flex: 1 }}
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            maxLength={500}
            placeholder="Сколько пользователей из vk_ads дошли до оплаты на этой неделе?"
            aria-label="Вопрос"
          />
          <button className="primary" type="submit" disabled={ask.isPending}>
            {ask.isPending ? "Думаю…" : "Спросить"}
          </button>
        </div>
        {ask.error && (
          <div className="error" role="alert">
            {ask.error instanceof ApiError ? (aiErrors[ask.error.code] ?? errorText(ask.error)) : errorText(ask.error)}
          </div>
        )}
        {a && (
          <div className="stack" style={{ gap: 8 }}>
            <h2>{a.title}</h2>
            {a.chart === "number" && a.rows[0] ? (
              <div className="tile-value" style={{ fontSize: 40 }}>
                {String(a.rows[0][0])}
              </div>
            ) : (
              <div style={{ maxHeight: 360, overflow: "auto" }}>
                <table className="data">
                  <thead>
                    <tr>
                      {a.columns.map((c) => (
                        <th key={c}>{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {a.rows.map((r, i) => (
                      <tr key={i}>
                        {r.map((v, j) => (
                          <td key={j} className={typeof v === "number" ? "num" : undefined}>
                            {typeof v === "number" ? number(v) : String(v ?? "")}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
            {a.truncated && <span className="muted">показаны первые 1000 строк</span>}
            {/* the query is shown: the owner can check what was counted */}
            <details>
              <summary className="secondary">SQL</summary>
              <pre className="code">{a.sql}</pre>
            </details>
          </div>
        )}
      </form>

      <section className="card">
        <div className="card-head">
          <h2>Темы сообщений</h2>
          <span className="card-sub">кластеры за 7 дней, раз в час; нужен сбор текстов в настройках</span>
        </div>
        {topics.isPending && <Loading />}
        {topics.error && <ErrorState error={topics.error} />}
        {topics.data?.length === 0 && <div className="empty">Тем пока нет: включите сбор текстов и подождите час.</div>}
        {topics.data && topics.data.length > 0 && (
          <table className="data">
            <thead>
              <tr>
                <th>Тема</th>
                <th className="num">Сообщений за 7 дней</th>
                <th className="num">Доля</th>
                <th className="num">К прошлой неделе</th>
              </tr>
            </thead>
            <tbody>
              {topics.data.map((t) => (
                <Fragment key={t.id}>
                  <tr>
                    <td>
                      <button className="link" onClick={() => setOpen(open === t.id ? null : t.id)} aria-expanded={open === t.id}>
                        {t.name}
                      </button>
                      <div className="muted">{t.description || t.keywords.slice(0, 5).join(", ")}</div>
                    </td>
                    <td className="num">{number(t.messages_7d)}</td>
                    <td className="num">{percent(t.share_7d)}</td>
                    <td className="num">{growthText(t)}</td>
                  </tr>
                  {open === t.id && (
                    <tr>
                      <td colSpan={4}>
                        <Examples projectId={projectId} topic={t.id} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section className="card">
        <h2>Алерты</h2>
        {alerts.data?.length === 0 && <div className="empty">Алертов нет.</div>}
        <ul className="stack" style={{ listStyle: "none", padding: 0, margin: "8px 0 0", gap: 10 }}>
          {alerts.data?.map((al) => (
            <li key={al.id}>
              <div className="filters">
                <span className="badge">{al.kind === "anomaly" ? "аномалия" : al.kind === "fraud" ? "антифрод" : "тема растёт"}</span>
                <b>{al.title}</b>
                <span className="muted">{dateTime(al.created_at)}</span>
              </div>
              <div className="secondary">{al.text}</div>
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
