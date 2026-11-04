import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// In development the dashboard and the API share one origin through the proxy: the
// session cookie is SameSite=Lax and no CORS preflight is needed.
const api = process.env.BOTSIGHT_API ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { "/v1": { target: api, changeOrigin: true } },
  },
  // the built dashboard behind the same proxy: what the end-to-end tests open
  preview: {
    port: 4173,
    proxy: { "/v1": { target: api, changeOrigin: true } },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
