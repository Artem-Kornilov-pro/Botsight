import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api, ApiError, setCsrfToken, unwrap, type Schemas } from "../api/client";

export type Account = Schemas["Account"];

/** The signed-in account, or an ApiError 401. Also restores the CSRF token after a reload. */
export function useAccount() {
  return useQuery({
    queryKey: ["account"],
    queryFn: async () => {
      const s = await unwrap(api.GET("/v1/account"));
      setCsrfToken(s.csrf_token);
      return s.account;
    },
    retry: (count, err) => !(err instanceof ApiError && err.status === 401) && count < 2,
    staleTime: Infinity,
  });
}

type Credentials = { email: string; password: string; name?: string };

export function useSignIn(mode: "login" | "register") {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: async (body: Credentials) => {
      const call =
        mode === "login"
          ? api.POST("/v1/auth/login", { body: { email: body.email, password: body.password } })
          : api.POST("/v1/auth/register", { body });
      return unwrap(call);
    },
    onSuccess: (s) => {
      setCsrfToken(s.csrf_token);
      qc.clear();
      qc.setQueryData(["account"], s.account);
    },
  });
}

export function useSignOut() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => unwrap(api.POST("/v1/auth/logout")),
    onSettled: () => {
      setCsrfToken("");
      qc.clear();
      qc.setQueryData(["account"], null);
    },
  });
}
