import { QueryCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, Navigate, RouterProvider } from "react-router-dom";

import { ApiError } from "./api/client";
import { ProjectLayout, RequireAuth } from "./components/Layout";
import { applyTheme, savedTheme } from "./lib/theme";
import { AuthPage } from "./pages/Auth";
import { ConnectPage } from "./pages/Connect";
import { LivePage } from "./pages/Live";
import { ProjectsPage } from "./pages/Projects";
import "./styles.css";

applyTheme(savedTheme());

const queryClient: QueryClient = new QueryClient({
  queryCache: new QueryCache({
    // the session expired: forget the account, RequireAuth sends to the sign-in page
    onError: (err, query) => {
      if (err instanceof ApiError && err.status === 401 && query.queryKey[0] !== "account") {
        void queryClient.resetQueries({ queryKey: ["account"] });
      }
    },
  }),
  defaultOptions: {
    queries: {
      staleTime: 30_000, // the API caches analytics for a minute anyway
      refetchOnWindowFocus: false,
      retry: (count, err) => !(err instanceof ApiError && err.status < 500) && count < 2,
    },
  },
});

export const router = createBrowserRouter([
  { path: "/login", element: <AuthPage mode="login" /> },
  { path: "/register", element: <AuthPage mode="register" /> },
  {
    element: <RequireAuth />,
    children: [
      { path: "/", element: <ProjectsPage /> },
      {
        path: "/p/:projectId",
        element: <ProjectLayout />,
        children: [
          // chart pages load ECharts on demand: sign-in and the project list stay light
          { index: true, lazy: () => import("./pages/Overview").then((m) => ({ Component: m.OverviewPage })) },
          { path: "funnels", lazy: () => import("./pages/Funnels").then((m) => ({ Component: m.FunnelsPage })) },
          { path: "sources", lazy: () => import("./pages/Sources").then((m) => ({ Component: m.SourcesPage })) },
          { path: "retention", lazy: () => import("./pages/Retention").then((m) => ({ Component: m.RetentionPage })) },
          { path: "scenario", lazy: () => import("./pages/Scenario").then((m) => ({ Component: m.ScenarioPage })) },
          { path: "errors", lazy: () => import("./pages/Errors").then((m) => ({ Component: m.ErrorsPage })) },
          { path: "user", lazy: () => import("./pages/User").then((m) => ({ Component: m.UserPage })) },
          { path: "live", element: <LivePage /> },
          { path: "connect", element: <ConnectPage /> },
        ],
      },
    ],
  },
  { path: "*", element: <Navigate to="/" replace /> },
]);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
);
