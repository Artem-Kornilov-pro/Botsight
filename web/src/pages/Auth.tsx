import { useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";

import { ApiError, errorText } from "../api/client";
import { useSignIn } from "../auth/session";

export function AuthPage({ mode }: { mode: "login" | "register" }) {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [name, setName] = useState("");
  const signIn = useSignIn(mode);
  const navigate = useNavigate();
  const [params] = useSearchParams();
  // only a path of this site: an open redirect would take a phished user anywhere
  const next = params.get("next")?.startsWith("/") && !params.get("next")?.startsWith("//") ? params.get("next")! : "/";

  const field = signIn.error instanceof ApiError ? signIn.error.field : undefined;

  return (
    <div className="auth">
      <form
        className="card form"
        onSubmit={(e) => {
          e.preventDefault();
          signIn.mutate(
            mode === "login" ? { email, password } : { email, password, name },
            { onSuccess: () => navigate(next, { replace: true }) },
          );
        }}
      >
        <h1>{mode === "login" ? "Вход в Botsight" : "Регистрация"}</h1>
        <p className="secondary" style={{ margin: 0 }}>
          Аналитика Telegram-бота в две строки кода.
        </p>
        {mode === "register" && (
          <label className="field">
            Имя
            <input value={name} onChange={(e) => setName(e.target.value)} autoComplete="name" />
          </label>
        )}
        <label className="field">
          Почта
          <input type="email" required value={email} onChange={(e) => setEmail(e.target.value)} autoComplete="email" aria-invalid={field === "email"} />
        </label>
        <label className="field">
          Пароль
          <input
            type="password"
            required
            minLength={mode === "register" ? 8 : undefined}
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete={mode === "login" ? "current-password" : "new-password"}
            aria-invalid={field === "password"}
          />
        </label>
        {signIn.error && (
          <div className="error" role="alert">
            {signIn.error instanceof ApiError && signIn.error.code === "validation_failed" ? signIn.error.message : errorText(signIn.error)}
          </div>
        )}
        <button className="primary" type="submit" disabled={signIn.isPending}>
          {mode === "login" ? "Войти" : "Зарегистрироваться"}
        </button>
        <div className="secondary">
          {mode === "login" ? (
            <>
              Нет аккаунта? <Link to="/register">Регистрация</Link>
            </>
          ) : (
            <>
              Уже есть аккаунт? <Link to="/login">Войти</Link>
            </>
          )}
        </div>
      </form>
    </div>
  );
}
