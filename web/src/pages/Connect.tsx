import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Link, useLocation } from "react-router-dom";

import { useProjectId } from "../components/Layout";
import { describe, useLive } from "../lib/live";

const ingestURL = import.meta.env.VITE_INGEST_URL ?? (import.meta.env.DEV ? "http://localhost:8081" : "https://ingest.botsight.ru");
const proxyURL = import.meta.env.VITE_PROXY_URL ?? (import.meta.env.DEV ? "http://localhost:8084" : "https://proxy.botsight.ru");

function CopyBlock({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div style={{ position: "relative" }}>
      <pre className="code">{code}</pre>
      <button
        style={{ position: "absolute", top: 8, right: 8 }}
        onClick={() => {
          void navigator.clipboard?.writeText(code);
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        }}
      >
        {copied ? "Скопировано" : "Копировать"}
      </button>
    </div>
  );
}

/** Connecting a bot (Botsight.md, 16.2): the key once, the code, then waiting for the
 *  first event on the live stream. */
export function ConnectPage() {
  const projectId = useProjectId();
  const location = useLocation();
  const qc = useQueryClient();
  const key: string | undefined = (location.state as { key?: string } | null)?.key;
  const live = useLive(projectId, { max: 5 });
  const first = live.events[live.events.length - 1];

  useEffect(() => {
    // the first event fills projects.bot_id: refresh the list badge
    if (first) void qc.invalidateQueries({ queryKey: ["projects"] });
  }, [first, qc]);

  const keyExpr = key ? `"${key}"` : `os.environ["BOTSIGHT_KEY"]`;
  const sdk = `pip install botsight

from botsight import setup

dp.include_router(router)
setup(dp, bot, api_key=${keyExpr}${import.meta.env.DEV ? `,\n      endpoint="${ingestURL}"` : ""})
await dp.start_polling(bot)`;
  const proxy = `# бот без SDK: только адрес Bot API
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer

session = AiohttpSession(api=TelegramAPIServer.from_base("${proxyURL}/k/${key ?? "<api_key>"}"))
bot = Bot(token=TOKEN, session=session)`;

  return (
    <div className="stack">
      <div className="page-head">
        <h1>Подключение бота</h1>
      </div>
      {key && (
        <section className="card" style={{ borderColor: "var(--warning)" }}>
          <h2>Ключ проекта</h2>
          <p className="secondary">Ключ показывается один раз — сохраните его в переменную окружения BOTSIGHT_KEY.</p>
          <CopyBlock code={key} />
        </section>
      )}
      <section className="card">
        <h2>Две строки в коде бота на aiogram 3</h2>
        <p className="secondary">Хендлеры менять не нужно. Тексты сообщений по умолчанию не собираются.</p>
        <CopyBlock code={sdk} />
      </section>
      <section className="card">
        <h2>Или без SDK — через прокси Bot API</h2>
        <CopyBlock code={proxy} />
      </section>
      <section className="card" aria-live="polite">
        {first ? (
          <>
            <h2>Бот подключён</h2>
            <p className="secondary">
              Первое событие: <b>{describe(first)}</b>. Через минуту появятся графики.
            </p>
            <Link to={`/p/${projectId}`}>Открыть обзор →</Link>
          </>
        ) : (
          <>
            <h2>Ждём первое событие…</h2>
            <p className="secondary">
              {live.status === "unavailable"
                ? "Живая лента недоступна — события всё равно сохраняются, проверьте обзор позже."
                : "Запустите бота и отправьте ему /start — событие появится здесь сразу."}
            </p>
          </>
        )}
      </section>
    </div>
  );
}
