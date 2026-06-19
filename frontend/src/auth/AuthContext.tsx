import { createContext, useContext, useEffect, useState, type PropsWithChildren } from "react";
import { ApiError, fetchJson } from "../lib/api";
import type { AuthMeResponse, CapabilityName, RoleName, UserSummary } from "../lib/types";

type AuthContextValue = {
  isLoading: boolean;
  isMaintenance: boolean;
  authenticated: boolean;
  user: UserSummary | null;
  role: RoleName;
  capabilities: CapabilityName[];
  refresh: () => Promise<void>;
  requestPremiumAccess: (email: string) => Promise<string>;
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
  const [isMaintenance, setIsMaintenance] = useState(false);

  const refresh = async () => {
    try {
      const next = await fetchJson<AuthMeResponse>("/api/auth/me");
      setPayload(next);
      setIsMaintenance(false);
    } catch (error) {
      if (error instanceof ApiError && error.status === 502) {
        setIsMaintenance(true);
      }
      throw error;
    }
  };

  useEffect(() => {
    void refresh()
      .catch(() => setPayload(DEFAULT_AUTH))
      .finally(() => setIsLoading(false));
  }, []);

  const requestPremiumAccess = async (email: string) => {
    const response = await fetchJson<{ ok: boolean; email: string; message: string }>("/api/auth/request-premium", {
      method: "POST",
      body: JSON.stringify({ email }),
    });
    return response.message;
  };

  const logout = async () => {
    await fetchJson("/api/auth/logout", { method: "POST" });
    setPayload(DEFAULT_AUTH);
  };

  const value: AuthContextValue = {
    isLoading,
    isMaintenance,
    authenticated: payload.authenticated,
    user: payload.user,
    role: payload.role,
    capabilities: payload.capabilities,
    refresh,
    requestPremiumAccess,
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
