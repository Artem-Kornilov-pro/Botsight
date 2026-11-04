import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";

import { api, errorText, unwrap } from "../api/client";
import { useAccount, useSignOut } from "../auth/session";
import { useProjects } from "../components/Layout";
import { ErrorState, Loading } from "../components/ui";
import { dateTime } from "../lib/format";

const roles: Record<string, string> = { owner: "владелец", editor: "редактор", viewer: "наблюдатель" };

export function ProjectsPage() {
  const projects = useProjects();
  const account = useAccount();
  const signOut = useSignOut();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const [name, setName] = useState("");

  const create = useMutation({
    mutationFn: () => unwrap(api.POST("/v1/projects", { body: { name } })),
    onSuccess: (res) => {
      // the project screen must find the new project at once, not after a refetch
      qc.setQueryData<typeof res.project[]>(["projects"], (list) => [...(list ?? []), res.project]);
      void qc.invalidateQueries({ queryKey: ["projects"] });
      // the key is shown once: it travels to the connect screen in memory, not in the URL
      navigate(`/p/${res.project.id}/connect`, { state: { key: res.api_key.key } });
    },
  });

  return (
    <div className="main" style={{ margin: "0 auto", maxWidth: 880 }}>
      <div className="page-head">
        <h1>Проекты</h1>
        <div className="filters secondary">
          {account.data?.email}
          <button className="link" onClick={() => signOut.mutate(undefined, { onSettled: () => navigate("/login") })}>
            Выйти
          </button>
        </div>
      </div>
      <div className="stack">
        <form
          className="card filters"
          onSubmit={(e) => {
            e.preventDefault();
            create.mutate();
          }}
        >
          <input placeholder="Название бота, например labbot" value={name} onChange={(e) => setName(e.target.value)} required maxLength={100} style={{ flex: 1 }} aria-label="Название проекта" />
          <button className="primary" type="submit" disabled={create.isPending}>
            Подключить бота
          </button>
          {create.error && <span className="error">{errorText(create.error)}</span>}
        </form>

        {projects.isPending && <Loading />}
        {projects.error && <ErrorState error={projects.error} />}
        {projects.data?.length === 0 && <div className="empty">Пока нет проектов: подключите первого бота.</div>}
        {projects.data?.map((p) => (
          <Link key={p.id} to={`/p/${p.id}`} className="card" style={{ color: "inherit", textDecoration: "none" }}>
            <div className="card-head" style={{ marginBottom: 0 }}>
              <h2>{p.name}</h2>
              <span className="badge">
                <span className="dot" style={{ background: p.bot_id ? "var(--good)" : "var(--text-muted)" }} />
                {p.bot_id ? `бот ${p.bot_id}` : "ждём первое событие"}
              </span>
            </div>
            <div className="card-sub">
              {roles[p.role] ?? p.role} · создан {dateTime(p.created_at)}
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
