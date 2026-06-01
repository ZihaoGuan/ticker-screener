import { Navigate, useLocation } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { useAuth } from "./AuthContext";
import type { CapabilityName } from "../lib/types";

type ProtectedRouteProps = {
  capability: CapabilityName;
  children: React.ReactNode;
};

export function ProtectedRoute({ capability, children }: ProtectedRouteProps) {
  const location = useLocation();
  const auth = useAuth();

  if (auth.isLoading) {
    return <LoadingBlock label="Checking access…" />;
  }
  if (!auth.authenticated) {
    return <Navigate to={`/login?next=${encodeURIComponent(location.pathname + location.search)}`} replace />;
  }
  if (!auth.hasCapability(capability)) {
    return <Navigate to="/" replace />;
  }
  return <>{children}</>;
}
