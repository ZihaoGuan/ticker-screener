import { createContext, useContext, useEffect, useState, type PropsWithChildren } from "react";
import { fetchJson } from "../lib/api";
import type { AuthMeResponse, CapabilityName, RoleName, UserSummary } from "../lib/types";

type AuthContextValue = {
  isLoading: boolean;
  authenticated: boolean;
  user: UserSummary | null;
  role: RoleName;
  capabilities: CapabilityName[];
  refresh: () => Promise<void>;
  requestMagicLink: (email: string) => Promise<string>;
  requestPremiumAccess: (email: string) => Promise<string>;
  verifyMagicLink: (token: string) => Promise<void>;
  logout: () => Promise<void>;
  hasCapability: (capability: CapabilityName) => boolean;
};

const AuthContext = createContext<AuthContextValue | null>(null);

const DEFAULT_AUTH: AuthMeResponse = {
  authenticated: false,
  user: null,
  role: "visitor",
  capabilities: ["view_results"],
};

export function AuthProvider({ children }: PropsWithChildren) {
  const [payload, setPayload] = useState<AuthMeResponse>(DEFAULT_AUTH);
  const [isLoading, setIsLoading] = useState(true);

  const refresh = async () => {
    const next = await fetchJson<AuthMeResponse>("/api/auth/me");
    setPayload(next);
  };

  useEffect(() => {
    void refresh()
      .catch(() => setPayload(DEFAULT_AUTH))
      .finally(() => setIsLoading(false));
  }, []);

  const requestMagicLink = async (email: string) => {
    const response = await fetchJson<{ ok: boolean; email: string; expires_at: string }>("/api/auth/request-link", {
      method: "POST",
      body: JSON.stringify({ email }),
    });
    return `Magic link sent to ${response.email}.`;
  };

  const requestPremiumAccess = async (email: string) => {
    const response = await fetchJson<{ ok: boolean; email: string; message: string }>("/api/auth/request-premium", {
      method: "POST",
      body: JSON.stringify({ email }),
    });
    return response.message;
  };

  const verifyMagicLink = async (token: string) => {
    await fetchJson("/api/auth/verify-link", {
      method: "POST",
      body: JSON.stringify({ token }),
    });
    await refresh();
  };

  const logout = async () => {
    await fetchJson("/api/auth/logout", { method: "POST" });
    setPayload(DEFAULT_AUTH);
  };

  const value: AuthContextValue = {
    isLoading,
    authenticated: payload.authenticated,
    user: payload.user,
    role: payload.role,
    capabilities: payload.capabilities,
    refresh,
    requestMagicLink,
    requestPremiumAccess,
    verifyMagicLink,
    logout,
    hasCapability: (capability) => payload.capabilities.includes(capability),
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used inside AuthProvider.");
  }
  return context;
}
