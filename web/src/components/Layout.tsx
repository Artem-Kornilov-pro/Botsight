import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Navigate, NavLink, Outlet, useLocation, useNavigate, useParams } from "react-router-dom";

import { api, ApiError, unwrap } from "../api/client";
import { useAccount, useSignOut } from "../auth/session";
import { applyTheme, savedTheme, type ThemeChoice } from "../lib/theme";
import { Loading } from "./ui";

/** Pages behind sign-in; a 401 anywhere resets the account query and lands here. */
export function RequireAuth() {
  const account = useAccount();
  const location = useLocation();
  if (account.isPending) return <Loading />;
  if (account.error instanceof ApiError && account.error.status === 401) {
    return <Navigate to={`/login?next=${encodeURIComponent(location.pathname + location.search)}`} replace />;
  }
  if (!account.data) return <Navigate to="/login" replace />;
  return <Outlet />;
}

export function useProjects() {
  return useQuery({
    queryKey: ["projects"],
    queryFn: async () => (await unwrap(api.GET("/v1/projects"))).projects,
  });
}

/** The project of the route, numeric. */
export function useProjectId(): number {
  const { projectId } = useParams();
  return Number(projectId);
}

const sections: Array<{ title?: string; links: Array<{ to: string; label: string; end?: boolean }> }> = [
  {
    links: [
      { to: "", label: "Обзор", end: true },
      { to: "live", label: "Живая лента" },
    ],
  },
  {
    title: "Аналитика",
    links: [
      { to: "funnels", label: "Воронки" },
      { to: "sources", label: "Источники" },
      { to: "retention", label: "Удержание" },
      { to: "scenario", label: "Граф сценария" },
      { to: "errors", label: "Ошибки" },
      { to: "user", label: "Пользователь" },
    ],
  },
  { title: "Бот", links: [{ to: "connect", label: "Подключение" }] },
];

export function ProjectLayout() {
  const projectId = useProjectId();
  const projects = useProjects();
  const account = useAccount();
  const signOut = useSignOut();
  const navigate = useNavigate();
  const location = useLocation();
  const [theme, setTheme] = useState<ThemeChoice>(savedTheme());

  const current = projects.data?.find((p) => p.id === projectId);
  // a project that is not in a fresh list is gone or someone else's
  if (projects.data && !current && !projects.isFetching) return <Navigate to="/" replace />;

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">Botsight</div>
        <select
          className="project-switch"
          aria-label="Проект"
          value={projectId}
          onChange={(e) => {
            // keep the screen, switch the project
            const rest = location.pathname.split("/").slice(3).join("/");
            navigate(`/p/${e.target.value}/${rest}${location.search}`);
          }}
        >
          {projects.data?.map((p) => (
            <option key={p.id} value={p.id}>
              {p.name}
            </option>
          ))}
        </select>
        <nav className="nav">
          {sections.map((s, i) => (
            <div key={i}>
              {s.title && <div className="nav-section">{s.title}</div>}
              {s.links.map((l) => (
                <NavLink key={l.to} to={`/p/${projectId}/${l.to}${location.search}`} end={l.end}>
                  {l.label}
                </NavLink>
              ))}
            </div>
          ))}
        </nav>
        <div className="sidebar-footer">
          <a href="/">Все проекты</a>
          <select
            aria-label="Тема"
            value={theme}
            onChange={(e) => {
              const v = e.target.value as ThemeChoice;
              applyTheme(v);
              setTheme(v);
            }}
          >
            <option value="system">Тема: как в системе</option>
            <option value="light">Тема: светлая</option>
            <option value="dark">Тема: тёмная</option>
          </select>
          <span className="muted">{account.data?.email ?? account.data?.name}</span>
          <button className="link" style={{ textAlign: "left" }} onClick={() => signOut.mutate(undefined, { onSettled: () => navigate("/login") })}>
            Выйти
          </button>
        </div>
      </aside>
      <main className="main">
        <Outlet context={current} />
      </main>
    </div>
  );
}
