import { useInfiniteQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";

import { api, unwrap, type Schemas } from "../api/client";
import { useProjectId } from "../components/Layout";
import { ErrorState, Loading } from "../components/ui";
import { dateTimeSec } from "../lib/format";
import { describe, type LiveEvent } from "../lib/live";

type TimelineEvent = Schemas["TimelineEvent"];

const typeText: Record<string, string> = { update: "апдейт", step: "шаг", outgoing: "Bot API", custom: "событие", lifecycle: "бот" };

/** The events of one user, newest first, for a complaint like "the bot did not answer". */
export function UserPage() {
  const projectId = useProjectId();
  const [params, setParams] = useSearchParams();
  const userId = params.get("id") ?? "";
  const valid = /^[1-9]\d{0,19}$/.test(userId);

  const timeline = useInfiniteQuery({
    queryKey: ["timeline", projectId, userId],
    enabled: valid,
    initialPageParam: "",
    queryFn: ({ pageParam }) =>
      unwrap(
        api.GET("/v1/projects/{projectID}/users/{userID}/timeline", {
          params: { path: { projectID: projectId, userID: userId }, query: { limit: 100, ...(pageParam ? { cursor: pageParam } : {}) } },
        }),
      ),
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const events: TimelineEvent[] = timeline.data?.pages.flatMap((p) => p.events) ?? [];

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Пользователь</h1>
        <form
          className="filters"
          onSubmit={(e) => {
            e.preventDefault();
            const v = new FormData(e.currentTarget).get("id");
            setParams({ id: String(v ?? "").trim() });
          }}
        >
          <input name="id" defaultValue={userId} placeholder="user_id из Telegram" aria-label="user_id" inputMode="numeric" />
          <button className="primary" type="submit">
            Показать
          </button>
        </form>
      </div>
      {!userId && <div className="card empty">Введите user_id — покажем, что пользователь делал и что ему отвечал бот, начиная с последнего.</div>}
      {userId && !valid && <div className="card error">user_id — положительное число.</div>}
      {timeline.isPending && valid && <Loading />}
      {timeline.error && <ErrorState error={timeline.error} />}
      {timeline.data && (
        <section className="card">
          <div className="card-head">
            <h2>События, новые сверху</h2>
            <span className="card-sub">{events.length} показано · Bot API — по личному чату с пользователем</span>
          </div>
          {events.length === 0 ? (
            <div className="empty">За 90 дней событий этого пользователя нет.</div>
          ) : (
            <ul className="feed">
              {events.map((e) => (
                <li key={e.event_id} style={{ gridTemplateColumns: "170px 90px 1fr" }}>
                  <span className="ts">{dateTimeSec(e.ts)}</span>
                  <span className="secondary">{typeText[e.type] ?? e.type}</span>
                  <span style={e.error ? { color: "var(--critical)" } : undefined}>
                    {describe(e as LiveEvent)}
                    {e.state && <span className="muted"> · в состоянии {e.state}</span>}
                  </span>
                </li>
              ))}
            </ul>
          )}
          {timeline.hasNextPage && (
            <button style={{ marginTop: 12 }} disabled={timeline.isFetchingNextPage} onClick={() => void timeline.fetchNextPage()}>
              Показать ещё
            </button>
          )}
        </section>
      )}
    </div>
  );
}
