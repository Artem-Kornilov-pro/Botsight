import { keepPreviousData, useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { Link } from "react-router-dom";

import { api, unwrap } from "../api/client";
import { useProjectId } from "../components/Layout";
import { ErrorState, Loading, PeriodFilters, StatTile } from "../components/ui";
import { compact, dateTime, ms, number, plural } from "../lib/format";
import { periodQuery, usePeriod } from "../lib/period";

function Table(props: { title: string; sub?: string; empty: string; head: string[]; rows: ReactNode[][]; numeric: number }) {
  return (
    <section className="card">
      <h2>{props.title}</h2>
      {props.sub && <div className="card-sub">{props.sub}</div>}
      {props.rows.length === 0 ? (
        <div className="empty" style={{ padding: "20px 0" }}>
          {props.empty}
        </div>
      ) : (
        <div style={{ overflowX: "auto", marginTop: 8 }}>
          <table className="data">
            <thead>
              <tr>
                {props.head.map((h, i) => (
                  <th key={h} className={i >= props.head.length - props.numeric ? "num" : undefined}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {props.rows.map((r, i) => (
                <tr key={i}>
                  {r.map((c, j) => (
                    <td key={j} className={j >= r.length - props.numeric ? "num" : undefined}>
                      {c}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

const apiErrorText: Record<string, string> = {
  bot_blocked: "пользователь заблокировал бота",
  user_deactivated: "аккаунт удалён",
  rate_limited: "упёрлись в лимит Telegram",
  not_modified: "сообщение не изменилось",
  bad_request: "неверный запрос",
  forbidden: "нет доступа",
  migrated: "группа стала супергруппой",
  network: "сеть",
};

export function ErrorsPage() {
  const projectId = useProjectId();
  const [period, setPeriod] = usePeriod();
  const q = periodQuery(period);
  const errors = useQuery({
    queryKey: ["errors", projectId, q],
    queryFn: () => unwrap(api.GET("/v1/projects/{projectID}/errors", { params: { path: { projectID: projectId }, query: q } })),
    placeholderData: keepPreviousData,
  });
  const e = errors.data;
  const handlerTotal = e?.handlers.reduce((n, h) => n + h.count, 0) ?? 0;
  const apiTotal = e?.api.reduce((n, a) => n + a.count, 0) ?? 0;

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Ошибки</h1>
        <PeriodFilters period={period} onChange={setPeriod} />
      </div>
      {errors.isPending && <Loading />}
      {errors.error && <ErrorState error={errors.error} />}
      {e && (
        <div className="stack" style={errors.isPlaceholderData ? { opacity: 0.5 } : undefined}>
          <div className="tiles">
            <StatTile label="Исключения в хендлерах" value={compact(handlerTotal)} note={plural(e.handlers.length, "вид", "вида", "видов")} />
            <StatTile label="Ошибки Bot API" value={compact(apiTotal)} note={plural(e.api.length, "вид", "вида", "видов")} />
            <StatTile label="Упёрлись в лимит, 429" value={compact(e.rate_limited)} note="Telegram просил подождать" />
            <StatTile label="Без обработчика" value={compact(e.unhandled.reduce((n, u) => n + u.count, 0))} note="апдейтов, которые никто не взял" />
          </div>
          <Table
            title="Исключения в хендлерах"
            empty="Хендлеры не падали."
            head={["Хендлер", "Исключение", "Последний раз", "Пользователи", "Раз"]}
            numeric={2}
            rows={e.handlers.map((h) => [<code key="h">{h.handler}</code>, h.error, dateTime(h.last_seen), number(h.users), number(h.count)])}
          />
          <Table
            title="Ошибки Bot API"
            empty="Telegram не возвращал ошибок."
            head={["Метод", "Ошибка", "Последний раз", "Чатов", "Раз"]}
            numeric={2}
            rows={e.api.map((a) => [
              <code key="m">{a.method}</code>,
              `${a.code || ""} ${a.error}${apiErrorText[a.error] ? ` — ${apiErrorText[a.error]}` : ""}`.trim(),
              dateTime(a.last_seen),
              number(a.chats),
              number(a.count),
            ])}
          />
          <div className="grid-2">
            <Table
              title="Медленные хендлеры"
              sub="от 5 вызовов, по p95"
              empty="Медленных нет."
              head={["Хендлер", "Вызовов", "p50", "p95", "Макс."]}
              numeric={4}
              rows={e.slow.map((s) => [<code key="h">{s.handler}</code>, number(s.calls), ms(s.ms.p50), ms(s.ms.p95), ms(s.max_ms)])}
            />
            <Table
              title="Апдейты без обработчика"
              sub="добавьте хендлер или отметьте его как запасной"
              empty="Все апдейты обработаны."
              head={["Тип", "Содержимое", "Пользователи", "Раз"]}
              numeric={2}
              rows={e.unhandled.map((u) => [u.name, u.content_type || "—", number(u.users), number(u.count)])}
            />
          </div>
          <p className="muted">
            Разобрать конкретный случай — в <Link to={`/p/${projectId}/user`}>таймлайне пользователя</Link>.
          </p>
        </div>
      )}
    </div>
  );
}
