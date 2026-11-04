import { defineConfig } from "@playwright/test";

// End-to-end tests of the built dashboard (vite preview) against the real backend of
// docker compose: run after loadgen, see .github/workflows/backend.yml.
export default defineConfig({
  testDir: "e2e",
  timeout: 60_000,
  retries: process.env.CI ? 1 : 0,
  use: {
    baseURL: process.env.DASHBOARD_URL ?? "http://localhost:4173",
    trace: "retain-on-failure",
  },
  reporter: process.env.CI ? [["list"], ["html", { open: "never" }]] : "list",
});
