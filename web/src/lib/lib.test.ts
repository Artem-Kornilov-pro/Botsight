import { api, ApiError, csrfMiddleware, errorText, setCsrfToken, unwrap } from "../api/client";
import { deployMarks } from "../pages/Overview";
import { compact, day, escapeHtml, ms } from "./format";
import { describe as describeEvent, initialLive, liveReducer, type LiveEvent } from "./live";
import { periodQuery, today } from "./period";

describe("format", () => {
  it("compacts only from ten thousand", () => {
    expect(compact(1284)).toMatch(/^1\s284$/); // Intl puts a no-break space
    expect(compact(12_900)).toMatch(/12,9\s?тыс/);
  });
  it("shows a calendar day without a time zone shift", () => {
    expect(day("2026-11-04")).toBe("4 нояб.");
  });
  it("formats durations", () => {
    expect(ms(111)).toBe("111 мс");
    expect(ms(2500)).toBe("2,5 с");
  });
  it("escapes HTML for chart tooltips", () => {
    expect(escapeHtml(`<img src=x onerror="a">`)).not.toContain("<");
  });
});

describe("period", () => {
  it("is whole days in the time zone, today included", () => {
    // 22:30 UTC is already the next day in Moscow
    const now = new Date("2026-11-04T22:30:00Z");
    expect(today("UTC", now)).toBe("2026-11-04");
    expect(today("Europe/Moscow", now)).toBe("2026-11-05");
    expect(periodQuery({ range: "7d", tz: "Europe/Moscow" }, now)).toEqual({
      from: "2026-10-30",
      to: "2026-11-05",
      tz: "Europe/Moscow",
    });
    expect(periodQuery({ range: "30d", tz: "UTC" }, now).from).toBe("2026-10-06");
  });
});

describe("live", () => {
  const ev = (id: string, extra: Partial<LiveEvent> = {}): LiveEvent => ({
    event_id: id,
    ts: "2026-11-04T10:00:00Z",
    type: "update",
    name: "message",
    ...extra,
  });

  it("keeps the newest events, once each", () => {
    const reduce = liveReducer(2);
    let s = reduce(initialLive, { type: "event", event: ev("a") });
    s = reduce(s, { type: "event", event: ev("b") });
    s = reduce(s, { type: "event", event: ev("b") }); // repeated after a reconnect
    s = reduce(s, { type: "event", event: ev("c") });
    expect(s.events.map((e) => e.event_id)).toEqual(["c", "b"]);
    expect(s.total).toBe(3);
    s = reduce(s, { type: "dropped", count: 5 });
    expect(s.dropped).toBe(5);
    expect(reduce(s, { type: "clear" }).events).toEqual([]);
  });

  it("describes events in the dashboard's words", () => {
    expect(describeEvent(ev("1", { command: "start", start_param: "vk" }))).toBe("/start vk");
    expect(describeEvent(ev("2", { type: "outgoing", name: "sendMessage", ok: false, error_code: 403, error: "bot_blocked" }))).toBe(
      "sendMessage · ошибка 403 bot_blocked",
    );
    expect(describeEvent(ev("3", { type: "step", handler: "bot:h", state_from: "", state_to: "Form:a" }))).toBe("bot:h ∅ → Form:a");
  });
});

describe("api client", () => {
  it("sends the CSRF token on state-changing requests only", async () => {
    setCsrfToken("t0k");
    const onRequest = csrfMiddleware.onRequest!;
    const call = async (method: string) => {
      const request = new Request("http://x/v1/projects", { method });
      const out = (await onRequest({ request } as never)) as Request;
      return out.headers.get("X-CSRF-Token");
    };
    expect(await call("POST")).toBe("t0k");
    expect(await call("GET")).toBeNull();
    setCsrfToken("");
  });

  it("turns error answers into ApiError with the stable code", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ error: { code: "invalid_credentials", message: "bad" } }), {
        status: 401,
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const err = await unwrap(api.POST("/v1/auth/login", { body: { email: "a@b.c", password: "x" }, fetch: fetchMock })).catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(401);
    expect(errorText(err)).toBe("Неверная почта или пароль");
    vi.unstubAllGlobals();
  });
});

describe("deploy marks", () => {
  it("puts bot starts on their day in the time zone and merges a day", () => {
    const o = {
      days: [{ day: "2026-11-03" }, { day: "2026-11-04" }],
      deploys: [
        { ts: "2026-11-03T22:30:00Z", sdk_version: "0.1.2" }, // Nov 4 in Moscow
        { ts: "2026-11-04T08:00:00Z", sdk_version: "0.1.3" },
        { ts: "2026-11-01T08:00:00Z", sdk_version: "0.1.0" }, // outside the period
      ],
    } as never;
    expect(deployMarks(o, "Europe/Moscow")).toEqual([{ index: 1, label: "Запусков бота: 2" }]);
    expect(deployMarks(o, "UTC")).toEqual([
      { index: 0, label: "Запуск бота · SDK 0.1.2" },
      { index: 1, label: "Запуск бота · SDK 0.1.3" },
    ]);
  });
});
