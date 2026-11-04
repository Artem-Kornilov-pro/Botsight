import { useState } from "react";

import { useProjectId } from "../components/Layout";
import { time } from "../lib/format";
import { describe, useLive, type LiveStatus } from "../lib/live";

const statusText: Record<LiveStatus, string> = {
  connecting: "подключение…",
  open: "в эфире",
  reconnecting: "переподключение…",
  unavailable: "недоступно",
};

const statusColor: Record<LiveStatus, string> = {
  connecting: "var(--text-muted)",
  open: "var(--good)",
  reconnecting: "var(--warning)",
  unavailable: "var(--critical)",
};

const typeText: Record<string, string> = {
  update: "апдейт",
  step: "шаг",
  outgoing: "Bot API",
  custom: "событие",
  lifecycle: "бот",
};

export function LivePage() {
  const projectId = useProjectId();
  const [paused, setPaused] = useState(false);
  const live = useLive(projectId, { paused });

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Живая лента</h1>
        <div className="filters">
          {/* status: a color with an icon-like dot and words, never color alone */}
          <span className="badge" role="status">
            <span className="dot" style={{ background: paused ? "var(--text-muted)" : statusColor[live.status] }} />
            {paused ? "пауза" : statusText[live.status]}
          </span>
          <button onClick={() => setPaused((p) => !p)}>{paused ? "Продолжить" : "Пауза"}</button>
          <button onClick={live.clear}>Очистить</button>
        </div>
      </div>
      <section className="card">
        <div className="card-head">
          <h2>События по мере поступления</h2>
          <span className="card-sub">
            получено {live.total}
            {live.dropped > 0 && ` · пропущено ${live.dropped} (лента не успевала)`}
          </span>
        </div>
        {live.events.length === 0 ? (
          <div className="empty">Событий пока нет. Напишите боту — они появятся здесь.</div>
        ) : (
          <ul className="feed">
            {live.events.map((e) => (
              <li key={e.event_id}>
                <span className="ts">{time(e.ts)}</span>
                <span className="secondary">
                  {typeText[e.type] ?? e.type}
                  {e.user_id && <span className="muted"> · {e.user_id}</span>}
                </span>
                <span style={e.error ? { color: "var(--critical)" } : undefined}>{describe(e)}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
