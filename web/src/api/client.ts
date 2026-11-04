import createClient, { type Middleware } from "openapi-fetch";

import type { components, paths } from "./schema";

export type Schemas = components["schemas"];

/** An error answer of the API: `{"error": {"code", "message", "field"}}`, codes are stable. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly field?: string,
  ) {
    super(message);
  }
}

// The CSRF token comes with the session (sign-in or GET /v1/account) and goes into every
// state-changing request. It lives in memory only: the cookie itself is HttpOnly.
let csrfToken = "";

export function setCsrfToken(token: string): void {
  csrfToken = token;
}

const unsafe = new Set(["POST", "PUT", "PATCH", "DELETE"]);

export const csrfMiddleware: Middleware = {
  onRequest({ request }) {
    if (csrfToken && unsafe.has(request.method)) {
      request.headers.set("X-CSRF-Token", csrfToken);
    }
    return request;
  },
};

// same origin as the page: in development Vite proxies /v1 to the api
export const api = createClient<paths>({ baseUrl: globalThis.location?.origin ?? "", credentials: "include" });
api.use(csrfMiddleware);

/** Turns an openapi-fetch result into its data or an ApiError. */
export async function unwrap<T>(
  call: Promise<{ data?: T; error?: unknown; response: Response }>,
): Promise<T> {
  const { data, error, response } = await call;
  if (response.ok) {
    return data as T;
  }
  const detail = (error as { error?: { code?: string; message?: string; field?: string } } | undefined)?.error;
  throw new ApiError(
    response.status,
    detail?.code ?? "http_" + response.status,
    detail?.message ?? response.statusText,
    detail?.field,
  );
}

/** Russian texts for the stable error codes; the message of the API is the fallback. */
const texts: Record<string, string> = {
  invalid_credentials: "Неверная почта или пароль",
  email_taken: "Эта почта уже зарегистрирована",
  rate_limited: "Слишком много попыток, попробуйте позже",
  validation_failed: "Проверьте введённые данные",
  analytics_unavailable: "Аналитика недоступна: не настроен ClickHouse",
  live_unavailable: "Живая лента недоступна: не настроена Kafka",
  query_too_heavy: "Запрос слишком тяжёлый — сузьте период",
  forbidden: "Недостаточно прав",
  not_found: "Не найдено",
};

export function errorText(err: unknown): string {
  if (err instanceof ApiError) {
    return texts[err.code] ?? err.message;
  }
  return "Не удалось связаться с сервером";
}
