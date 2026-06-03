import { Navigate } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { useAuth } from "./AuthContext";
import type { RoleName } from "../lib/types";

type RoleRouteProps = {
  allowedRoles: RoleName[];
  children: React.ReactNode;
  redirectTo?: string;
};

export function RoleRoute({ allowedRoles, children, redirectTo = "/charts" }: RoleRouteProps) {
  const auth = useAuth();

  if (auth.isLoading) {
    return <LoadingBlock label="Checking access…" />;
  }
  if (!allowedRoles.includes(auth.role)) {
    return <Navigate to={redirectTo} replace />;
  }
  return <>{children}</>;
}
