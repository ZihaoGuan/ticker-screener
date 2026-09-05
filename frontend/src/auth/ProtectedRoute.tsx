import { Navigate, useLocation } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { useAuth } from "./AuthContext";
import type { CapabilityName, RoleName } from "../lib/types";

type ProtectedRouteProps = {
  capability?: CapabilityName;
  allowedRoles?: RoleName[];
  redirectTo?: string;
  children: React.ReactNode;
};

export function ProtectedRoute({ capability, allowedRoles, redirectTo, children }: ProtectedRouteProps) {
  const location = useLocation();
  const auth = useAuth();

  if (auth.isLoading) {
    return <LoadingBlock label="Checking access…" />;
  }
  if (capability && !auth.authenticated) {
    return <Navigate to={`/login?next=${encodeURIComponent(location.pathname + location.search)}`} replace />;
  }
  if ((capability && !auth.hasCapability(capability)) || (allowedRoles && !allowedRoles.includes(auth.role))) {
    return <Navigate to={redirectTo ?? (allowedRoles ? "/charts" : "/")} replace />;
  }
  return <>{children}</>;
}
