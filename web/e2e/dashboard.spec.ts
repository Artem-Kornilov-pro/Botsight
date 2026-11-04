import { expect, test } from "@playwright/test";

const collector = process.env.COLLECTOR_URL ?? "http://localhost:8081";

function uuid7(): string {
  const hex = Date.now().toString(16).padStart(12, "0");
  const rnd = () => Math.floor(Math.random() * 0x10000).toString(16).padStart(4, "0");
  return `${hex.slice(0, 8)}-${hex.slice(8)}-7${rnd().slice(1)}-8${rnd().slice(1)}-${rnd()}${rnd()}${rnd()}`;
}

test("a new bot: sign up, create a project, see the first event arrive", async ({ page, request }) => {
  const email = `e2e-${Date.now()}@example.com`;
  await page.goto("/register");
  await page.getByLabel("Почта").fill(email);
  await page.getByLabel("Пароль").fill("password123");
  await page.getByRole("button", { name: "Зарегистрироваться" }).click();
  await expect(page.getByRole("heading", { name: "Проекты" })).toBeVisible();

  await page.getByLabel("Название проекта").fill("e2e bot");
  await page.getByRole("button", { name: "Подключить бота" }).click();
  await expect(page.getByRole("heading", { name: "Ключ проекта" })).toBeVisible();
  const key = (await page.locator("pre.code").first().textContent())!.trim();
  expect(key).toMatch(/^bs_live_/);
  await expect(page.getByText("Ждём первое событие…")).toBeVisible();

  // the bot starts: its SDK sends a /start, the live stream shows it
  const now = new Date().toISOString();
  const event = {
    event_id: uuid7(), type: "update", name: "message", ts: now, bot_id: 7012345678,
    user_id: 42, chat_id: 42, chat_type: "private", command: "start", start_param: "e2e",
    handled: true, duration_ms: 5,
  };
  await expect(async () => {
    const res = await request.post(`${collector}/v1/batch`, {
      headers: { Authorization: `Bearer ${key}` },
      data: { schema_version: 1, batch_id: uuid7(), sent_at: now, sdk: { name: "e2e", version: "1" }, dropped: 0, events: [{ ...event, event_id: uuid7() }] },
    });
    expect(res.status()).toBe(202);
    await expect(page.getByRole("heading", { name: "Бот подключён" })).toBeVisible({ timeout: 3000 });
  }).toPass({ timeout: 30_000 });
  await expect(page.getByText("/start e2e")).toBeVisible();
});

test("overview of a bot with traffic", async ({ page }) => {
  const email = process.env.E2E_EMAIL;
  test.skip(!email, "E2E_EMAIL: the account printed by loadgen");
  await page.goto("/login");
  await page.getByLabel("Почта").fill(email!);
  await page.getByLabel("Пароль").fill("loadgen-password");
  await page.getByRole("button", { name: "Войти" }).click();
  await page.getByRole("link", { name: /loadgen 1/ }).click();

  await expect(page.getByRole("heading", { name: "Обзор" })).toBeVisible();
  const users = page.locator(".tile", { hasText: "Пользователи" }).locator(".tile-value");
  await expect(users).not.toHaveText("0");
  await expect(page.getByRole("img", { name: "Активные и новые пользователи по дням" }).locator("svg")).toBeVisible();

  // every chart has its table: the values without hovering
  const card = page.locator("section.card", { hasText: "Пользователи по дням" });
  await card.getByRole("button", { name: "Таблица" }).click();
  await expect(card.locator("table.data tbody tr").first()).toBeVisible();

  await page.getByRole("button", { name: "7 дней" }).click();
  await expect(page).toHaveURL(/range=7d/);

  await page.getByRole("link", { name: "Живая лента" }).click();
  await expect(page.getByRole("status")).toHaveText(/в эфире/);
});

test("analytics screens on loadgen data", async ({ page }) => {
  const email = process.env.E2E_EMAIL;
  test.skip(!email, "E2E_EMAIL: the account printed by loadgen");
  await page.goto("/login");
  await page.getByLabel("Почта").fill(email!);
  await page.getByLabel("Пароль").fill("loadgen-password");
  await page.getByRole("button", { name: "Войти" }).click();
  await page.getByRole("link", { name: /loadgen 1/ }).click();

  // funnel: /start -> Form:lab, split by source
  await page.getByRole("link", { name: "Воронки" }).click();
  await page.getByLabel("Шаг 2: значение").fill("Form:lab");
  await page.getByLabel("по источникам").check();
  await page.getByRole("button", { name: "Посчитать" }).click();
  const funnel = page.locator("section.card", { hasText: "Дошли до шага" });
  await funnel.getByRole("button", { name: "Таблица" }).click();
  await expect(funnel.locator("tbody tr")).toHaveCount(2);
  await expect(funnel.locator("tbody tr").first().locator("td").nth(1)).not.toHaveText("0");
  await expect(page.getByRole("heading", { name: "По источникам" })).toBeVisible();

  for (const [link, heading] of [
    ["Источники", "Все источники"],
    ["Удержание", "Недельные когорты"],
    ["Граф сценария", "Переходы между состояниями"],
    ["Ошибки", "Исключения в хендлерах"],
  ] as const) {
    await page.getByRole("link", { name: link, exact: true }).click();
    await expect(page.getByRole("heading", { name: heading })).toBeVisible();
    await expect(page.getByRole("alert")).toHaveCount(0);
  }

  await page.getByRole("link", { name: "Пользователь" }).click();
  await page.getByLabel("user_id").fill("100000000");
  await page.getByRole("button", { name: "Показать" }).click();
  await expect(page.getByRole("heading", { name: "События, новые сверху" })).toBeVisible();
});

test("settings, antifraud and a broadcast", async ({ page }) => {
  const email = process.env.E2E_EMAIL;
  test.skip(!email, "E2E_EMAIL: the account printed by loadgen");
  page.on("dialog", (d) => d.accept());
  await page.goto("/login");
  await page.getByLabel("Почта").fill(email!);
  await page.getByLabel("Пароль").fill("loadgen-password");
  await page.getByRole("button", { name: "Войти" }).click();
  await page.getByRole("link", { name: /loadgen 1/ }).click();

  // settings: a new key is shown once, ad spend is listed
  await page.getByRole("link", { name: "Настройки" }).click();
  await page.getByLabel("Название ключа").fill("e2e");
  await page.getByRole("button", { name: "Создать ключ" }).click();
  await expect(page.getByRole("status").locator("pre")).toHaveText(/^bs_live_/);
  await page.getByLabel("Метка").fill("vk_ads");
  await page.getByLabel("С", { exact: true }).fill("2026-01-01");
  await page.getByLabel("По", { exact: true }).fill("2026-01-31");
  await page.getByLabel("Сумма").fill("1500");
  await page.getByRole("button", { name: "Добавить" }).first().click();
  await expect(page.getByRole("cell", { name: "2026-01-01 — 2026-01-31" })).toBeVisible();

  // antifraud: loadgen -fraud injects farms; the latest run lists them
  await page.getByRole("link", { name: "Антифрод" }).click();
  await expect(page.getByRole("heading", { name: "Источники" })).toBeVisible({ timeout: 30_000 });
  await page.getByRole("button", { name: "promo_farm" }).click();
  await expect(page.getByRole("heading", { name: /Подозрительные из/ })).toBeVisible();

  // broadcast: preview the segment, send, see it in the list
  await page.getByRole("link", { name: "Рассылки" }).click();
  await page.getByRole("button", { name: "Посчитать получателей" }).click();
  await expect(page.getByRole("status")).toHaveText(/получателей/);
  await page.getByLabel("Текст рассылки").fill("e2e: привет!");
  await page.getByRole("button", { name: "Отправить" }).click();
  await expect(page.getByRole("button", { name: "e2e: привет!" })).toBeVisible();
});
