import { useEffect, useReducer } from "react";

/** An event of GET /v1/projects/{id}/live (SSE), see internal/live in the backend. */
export interface LiveEvent {
  event_id: string;
  ts: string;
  type: "update" | "step" | "outgoing" | "custom" | "lifecycle";
  name: string;
  user_id?: string;
  chat_id?: number;
  content_type?: string;
  command?: string;
  start_param?: string;
  callback_data?: string;
  handler?: string;
  state_from?: string;
  state_to?: string;
  member_status?: string;
  ok?: boolean;
  duration_ms?: number;
  error_code?: number;
  error?: string;
}

export type LiveStatus = "connecting" | "open" | "reconnecting" | "unavailable";

export interface LiveState {
  status: LiveStatus;
  events: LiveEvent[]; // newest first
  total: number;
  dropped: number;
}

export type LiveAction =
  | { type: "status"; status: LiveStatus }
  | { type: "event"; event: LiveEvent }
  | { type: "dropped"; count: number }
  | { type: "clear" };

export const initialLive: LiveState = { status: "connecting", events: [], total: 0, dropped: 0 };

export function liveReducer(max: number) {
  return (s: LiveState, a: LiveAction): LiveState => {
    switch (a.type) {
      case "status":
        return { ...s, status: a.status };
      case "event":
        // the stream may repeat an event after a reconnect; the id decides
        if (s.events.some((e) => e.event_id === a.event.event_id)) return s;
        return { ...s, events: [a.event, ...s.events].slice(0, max), total: s.total + 1 };
      case "dropped":
        return { ...s, dropped: s.dropped + a.count };
      case "clear":
        return { ...s, events: [], total: 0, dropped: 0 };
    }
  };
}

/** Subscribes to the project's live stream while mounted and not paused. EventSource
 *  reconnects by itself; the session cookie goes with it on the same origin. */
export function useLive(projectId: number, { max = 200, paused = false } = {}) {
  const [state, dispatch] = useReducer(liveReducer(max), initialLive);
  useEffect(() => {
    if (paused || !projectId) return;
    dispatch({ type: "status", status: "connecting" });
    const source = new EventSource(`/v1/projects/${projectId}/live`, { withCredentials: true });
    source.addEventListener("ready", () => dispatch({ type: "status", status: "open" }));
    source.addEventListener("event", (m) => dispatch({ type: "event", event: JSON.parse((m as MessageEvent).data) }));
    source.addEventListener("dropped", (m) =>
      dispatch({ type: "dropped", count: JSON.parse((m as MessageEvent).data).count }),
    );
    source.onerror = () => {
      // CLOSED means the server refused (503 without Kafka, 429): EventSource gives up
      dispatch({ type: "status", status: source.readyState === EventSource.CLOSED ? "unavailable" : "reconnecting" });
    };
    return () => source.close();
  }, [projectId, paused]);
  return { ...state, clear: () => dispatch({ type: "clear" }) };
}

/** One line about an event for the feed, in the words of the dashboard. */
export function describe(e: LiveEvent): string {
  switch (e.type) {
    case "update":
      if (e.command) return `/${e.command}${e.start_param ? " " + e.start_param : ""}`;
      if (e.callback_data) return `кнопка ${e.callback_data}`;
      if (e.member_status) return `статус: ${e.member_status}`;
      return e.content_type ? `${e.name} · ${e.content_type}` : e.name;
    case "step":
      if (e.error) return `${e.handler} упал: ${e.error}`;
      return `${e.handler ?? ""} ${e.state_from || "∅"} → ${e.state_to || "∅"}`;
    case "outgoing":
      return e.ok ? `${e.name} · ${e.duration_ms ?? 0} мс` : `${e.name} · ошибка ${e.error_code ?? ""} ${e.error ?? ""}`.trim();
    case "custom":
      return e.name;
    case "lifecycle":
      return e.name === "bot_started" ? "бот запущен" : "бот остановлен";
  }
}
