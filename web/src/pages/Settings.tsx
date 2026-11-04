import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type ReactNode } from "react";
import { useOutletContext } from "react-router-dom";

import { api, errorText, unwrap, type Schemas } from "../api/client";
import { useProjectId } from "../components/Layout";
import { dateTime, rubles } from "../lib/format";

type Project = Schemas["Project"] | undefined;
type Role = Schemas["Role"];

const roleText: Record<Role, string> = { owner: "владелец", editor: "редактор", viewer: "наблюдатель" };

function Section(props: { title: string; sub?: string; children: ReactNode }) {
  return (
    <section className="card stack" style={{ gap: 12 }}>
      <div>
        <h2>{props.title}</h2>
        {props.sub && <div className="card-sub">{props.sub}</div>}
      </div>
      {props.children}
    </section>
  );
}

function Failure({ error }: { error: unknown }) {
  return error ? <div className="error">{errorText(error)}</div> : null;
}

/** Settings of a project (Botsight.md, 16.1). Actions the role cannot do are not shown;
 *  the API checks the role anyway. */
export function SettingsPage() {
  const project = useOutletContext<Project>();
  const role = project?.role ?? "viewer";
  const editor = role === "owner" || role === "editor";
  const owner = role === "owner";
  if (!project) return null;
  return (
    <div className="stack">
      <div className="page-head">
        <h1>Настройки</h1>
        <span className="badge">ваша роль: {roleText[role]}</span>
      </div>
      <ProjectSettings project={project} canEdit={editor} />
      <AdSpend canEdit={editor} />
      {owner && <Keys />}
      <Members canManage={owner} />
    </div>
  );
}

function ProjectSettings({ project, canEdit }: { project: Schemas["Project"]; canEdit: boolean }) {
  const qc = useQueryClient();
  const [name, setName] = useState(project.name);
  const [captureText, setCaptureText] = useState(project.settings.capture_text);
  const [neutral, setNeutral] = useState(project.settings.neutral_handlers.join("\n"));
  const save = useMutation({
    mutationFn: () =>
      unwrap(
        api.PATCH("/v1/projects/{projectID}", {
          params: { path: { projectID: project.id } },
          body: {
            name,
            settings: {
              capture_text: captureText,
              neutral_handlers: neutral.split("\n").map((s) => s.trim()).filter(Boolean),
            },
          },
        }),
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["projects"] }),
  });
  return (
    <Section title="Проект">
      <form
        className="form"
        onSubmit={(e) => {
          e.preventDefault();
          save.mutate();
        }}
      >
        <label className="field">
          Название
          <input value={name} onChange={(e) => setName(e.target.value)} disabled={!canEdit} maxLength={100} required />
        </label>
        <label className="filters secondary">
          <input type="checkbox" checked={captureText} onChange={(e) => setCaptureText(e.target.checked)} disabled={!canEdit} />
          Собирать тексты сообщений для AI-тем (хранятся 30 дней; в SDK нужен capture_text=True)
        </label>
        <label className="field">
          Нейтральные хендлеры — не считаются повтором шага, по одному в строке (module:qualname)
          <textarea rows={3} value={neutral} onChange={(e) => setNeutral(e.target.value)} disabled={!canEdit} placeholder="bot.handlers:help" />
        </label>
        {canEdit && (
          <div className="filters">
            <button className="primary" type="submit" disabled={save.isPending}>
              Сохранить
            </button>
            {save.isSuccess && <span className="secondary">Сохранено</span>}
          </div>
        )}
        <Failure error={save.error} />
      </form>
    </Section>
  );
}

function Keys() {
  const projectId = useProjectId();
  const qc = useQueryClient();
  const [name, setName] = useState("");
  const [shown, setShown] = useState<string | null>(null);
  const keys = useQuery({
    queryKey: ["keys", projectId],
    queryFn: async () => (await unwrap(api.GET("/v1/projects/{projectID}/keys", { params: { path: { projectID: projectId } } }))).keys,
  });
  const create = useMutation({
    mutationFn: () => unwrap(api.POST("/v1/projects/{projectID}/keys", { params: { path: { projectID: projectId } }, body: { name } })),
    onSuccess: (k) => {
      setShown(k.key);
      setName("");
      void qc.invalidateQueries({ queryKey: ["keys", projectId] });
    },
  });
  const revoke = useMutation({
    mutationFn: (id: number) => unwrap(api.DELETE("/v1/projects/{projectID}/keys/{keyID}", { params: { path: { projectID: projectId, keyID: id } } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["keys", projectId] }),
  });
  return (
    <Section title="API-ключи" sub="ключ уходит в SDK бота; отзыв действует на всех серверах приёма сразу">
      {shown && (
        <div className="card" style={{ borderColor: "var(--warning)" }} role="status">
          Новый ключ — показывается один раз:
          <pre className="code" style={{ marginTop: 8 }}>{shown}</pre>
        </div>
      )}
      <table className="data">
        <thead>
          <tr>
            <th>Название</th>
            <th>Префикс</th>
            <th>Создан</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {keys.data?.map((k) => (
            <tr key={k.id}>
              <td>{k.name || "—"}</td>
              <td>
                <code>{k.prefix}</code>
              </td>
              <td>{dateTime(k.created_at)}</td>
              <td className="num">
                {k.revoked_at ? (
                  <span className="muted">отозван {dateTime(k.revoked_at)}</span>
                ) : (
                  <button
                    onClick={() => {
                      if (confirm(`Отозвать ключ ${k.prefix}? Боты с ним перестанут отправлять события.`)) revoke.mutate(k.id);
                    }}
                  >
                    Отозвать
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <form
        className="filters"
        onSubmit={(e) => {
          e.preventDefault();
          create.mutate();
        }}
      >
        <input placeholder="Название, например prod" value={name} onChange={(e) => setName(e.target.value)} maxLength={100} aria-label="Название ключа" />
        <button type="submit" disabled={create.isPending}>
          Создать ключ
        </button>
      </form>
      <Failure error={create.error ?? revoke.error} />
    </Section>
  );
}

function Members({ canManage }: { canManage: boolean }) {
  const projectId = useProjectId();
  const qc = useQueryClient();
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<Role>("viewer");
  const members = useQuery({
    queryKey: ["members", projectId],
    queryFn: async () => (await unwrap(api.GET("/v1/projects/{projectID}/members", { params: { path: { projectID: projectId } } }))).members,
  });
  const refresh = () => qc.invalidateQueries({ queryKey: ["members", projectId] });
  const add = useMutation({
    mutationFn: () => unwrap(api.POST("/v1/projects/{projectID}/members", { params: { path: { projectID: projectId } }, body: { email, role } })),
    onSuccess: () => {
      setEmail("");
      void refresh();
    },
  });
  const change = useMutation({
    mutationFn: (v: { id: number; role: Role }) =>
      unwrap(api.PATCH("/v1/projects/{projectID}/members/{accountID}", { params: { path: { projectID: projectId, accountID: v.id } }, body: { role: v.role } })),
    onSettled: refresh,
  });
  const remove = useMutation({
    mutationFn: (id: number) => unwrap(api.DELETE("/v1/projects/{projectID}/members/{accountID}", { params: { path: { projectID: projectId, accountID: id } } })),
    onSettled: refresh,
  });
  return (
    <Section title="Участники" sub="наблюдатель читает, редактор меняет настройки, воронки, расходы и рассылки, владелец ещё управляет ключами и участниками">
      <table className="data">
        <thead>
          <tr>
            <th>Участник</th>
            <th>Роль</th>
            <th>Добавлен</th>
            {canManage && <th />}
          </tr>
        </thead>
        <tbody>
          {members.data?.map((m) => (
            <tr key={m.account_id}>
              <td>{m.email ?? (m.telegram_username ? "@" + m.telegram_username : m.name)}</td>
              <td>
                {canManage ? (
                  <select aria-label={`Роль ${m.email ?? m.account_id}`} value={m.role} onChange={(e) => change.mutate({ id: m.account_id, role: e.target.value as Role })}>
                    {(Object.keys(roleText) as Role[]).map((r) => (
                      <option key={r} value={r}>
                        {roleText[r]}
                      </option>
                    ))}
                  </select>
                ) : (
                  roleText[m.role]
                )}
              </td>
              <td>{dateTime(m.added_at)}</td>
              {canManage && (
                <td className="num">
                  <button onClick={() => remove.mutate(m.account_id)}>Удалить</button>
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
      {canManage && (
        <form
          className="filters"
          onSubmit={(e) => {
            e.preventDefault();
            add.mutate();
          }}
        >
          <input type="email" placeholder="почта зарегистрированного аккаунта" value={email} onChange={(e) => setEmail(e.target.value)} required aria-label="Почта участника" />
          <select value={role} onChange={(e) => setRole(e.target.value as Role)} aria-label="Роль нового участника">
            {(Object.keys(roleText) as Role[]).map((r) => (
              <option key={r} value={r}>
                {roleText[r]}
              </option>
            ))}
          </select>
          <button type="submit" disabled={add.isPending}>
            Добавить
          </button>
        </form>
      )}
      <Failure error={add.error ?? change.error ?? remove.error} />
    </Section>
  );
}

function AdSpend({ canEdit }: { canEdit: boolean }) {
  const projectId = useProjectId();
  const qc = useQueryClient();
  const [form, setForm] = useState({ source: "", from: "", to: "", amount: "" });
  const spend = useQuery({
    queryKey: ["ad-spend", projectId],
    queryFn: async () => (await unwrap(api.GET("/v1/projects/{projectID}/ad-spend", { params: { path: { projectID: projectId } } }))).ad_spend,
  });
  const add = useMutation({
    mutationFn: () =>
      unwrap(
        api.POST("/v1/projects/{projectID}/ad-spend", {
          params: { path: { projectID: projectId } },
          body: { source: form.source, from: form.from, to: form.to, amount_rub: Number(form.amount.replace(",", ".")) },
        }),
      ),
    onSuccess: () => {
      setForm({ source: "", from: "", to: "", amount: "" });
      void qc.invalidateQueries({ queryKey: ["ad-spend", projectId] });
      void qc.invalidateQueries({ queryKey: ["sources", projectId] });
    },
  });
  const remove = useMutation({
    mutationFn: (id: number) => unwrap(api.DELETE("/v1/projects/{projectID}/ad-spend/{spendID}", { params: { path: { projectID: projectId, spendID: id } } })),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["ad-spend", projectId] }),
  });
  return (
    <Section title="Расходы на рекламу" sub="по метке deep link за дни — из них считается стоимость пользователя на экране «Источники»">
      {spend.data && spend.data.length > 0 && (
        <table className="data">
          <thead>
            <tr>
              <th>Метка</th>
              <th>Период</th>
              <th className="num">Сумма</th>
              {canEdit && <th />}
            </tr>
          </thead>
          <tbody>
            {spend.data.map((s) => (
              <tr key={s.id}>
                <td>{s.source}</td>
                <td>
                  {s.from} — {s.to}
                </td>
                <td className="num">{rubles(s.amount_rub)}</td>
                {canEdit && (
                  <td className="num">
                    <button aria-label={`Удалить расход ${s.source}`} onClick={() => remove.mutate(s.id)}>
                      ✕
                    </button>
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {canEdit && (
        <form
          className="filters"
          onSubmit={(e) => {
            e.preventDefault();
            add.mutate();
          }}
        >
          <input placeholder="метка, например vk_ads" value={form.source} onChange={(e) => setForm({ ...form, source: e.target.value })} required pattern="[A-Za-z0-9_\-]{1,64}" aria-label="Метка" />
          <input type="date" value={form.from} onChange={(e) => setForm({ ...form, from: e.target.value })} required aria-label="С" />
          <input type="date" value={form.to} onChange={(e) => setForm({ ...form, to: e.target.value })} required aria-label="По" />
          <input placeholder="сумма, ₽" value={form.amount} onChange={(e) => setForm({ ...form, amount: e.target.value })} required inputMode="decimal" aria-label="Сумма" style={{ width: 120 }} />
          <button type="submit" disabled={add.isPending}>
            Добавить
          </button>
        </form>
      )}
      <Failure error={add.error ?? remove.error} />
    </Section>
  );
}
