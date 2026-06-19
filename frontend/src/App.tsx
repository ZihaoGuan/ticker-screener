import { Navigate, Route, Routes, useParams } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { ProtectedRoute } from "./auth/ProtectedRoute";
import { RoleRoute } from "./auth/RoleRoute";
import { useAuth } from "./auth/AuthContext";
import { LoadingBlock } from "./components/LoadingBlock";
import { AdminPage } from "./pages/AdminPage";
import { BacktestsPage } from "./pages/BacktestsPage";
import { DashboardPage } from "./pages/DashboardPage";
import { EarningsPage } from "./pages/EarningsPage";
import { GuidePage } from "./pages/GuidePage";
import { LoginPage } from "./pages/LoginPage";
import { OverlapPage } from "./pages/OverlapPage";
import { PortfolioPage } from "./pages/PortfolioPage";
import { ChartsPage } from "./pages/ChartsPage";
import { RatingsPage } from "./pages/RatingsPage";
import { RrgPage } from "./pages/RrgPage";
import { RunsPage } from "./pages/RunsPage";
import { ScannerBoardPage } from "./pages/ScannerBoardPage";
import { ScannerResultPage } from "./pages/ScannerResultPage";
import { ScannerTopHitsPage } from "./pages/ScannerTopHitsPage";
import { WarmupPage } from "./pages/WarmupPage";
import { WeeklyWatchlistPage } from "./pages/WeeklyWatchlistPage";
import { WatchlistsPage } from "./pages/WatchlistsPage";

export default function App() {
  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={<HomeRoute />} />
        <Route path="/login" element={<LoginPage />} />
        <Route
          path="/guide"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <GuidePage />
            </RoleRoute>
          }
        />
        <Route path="/charts" element={<ChartsPage />} />
        <Route path="/earnings" element={<EarningsPage />} />
        <Route
          path="/scanner"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <ScannerBoardPage />
            </RoleRoute>
          }
        />
        <Route
          path="/scanner/top-hits"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <ScannerTopHitsPage />
            </RoleRoute>
          }
        />
        <Route
          path="/scanner/:scannerId"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <ScannerResultPage />
            </RoleRoute>
          }
        />
        <Route
          path="/ratings"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <RatingsPage />
            </RoleRoute>
          }
        />
        <Route
          path="/screeners"
          element={
            <RoleRoute allowedRoles={["admin"]}>
              <RunsPage mode="screeners" />
            </RoleRoute>
          }
        />
        <Route
          path="/screeners/schedules"
          element={
            <RoleRoute allowedRoles={["admin"]}>
              <RunsPage mode="schedules" />
            </RoleRoute>
          }
        />
        <Route
          path="/warmup"
          element={
            <RoleRoute allowedRoles={["admin"]}>
              <WarmupPage />
            </RoleRoute>
          }
        />
        <Route
          path="/backtests"
          element={
            <RoleRoute allowedRoles={["admin"]}>
              <BacktestsPage />
            </RoleRoute>
          }
        />
        <Route path="/runs" element={<Navigate to="/screeners" replace />} />
        <Route
          path="/watchlists/weekly"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <WeeklyWatchlistPage />
            </RoleRoute>
          }
        />
        <Route
          path="/watchlists"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <WatchlistsPage />
            </RoleRoute>
          }
        />
        <Route
          path="/rotation"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <Navigate to="/rotation/sector" replace />
            </RoleRoute>
          }
        />
        <Route
          path="/rotation/:universe"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <RrgPage />
            </RoleRoute>
          }
        />
        <Route path="/rrg" element={<Navigate to="/rotation/sector" replace />} />
        <Route path="/rrg/:universe" element={<LegacyRrgRedirect />} />
        <Route
          path="/report"
          element={
            <RoleRoute allowedRoles={["premium", "admin"]}>
              <OverlapPage />
            </RoleRoute>
          }
        />
        <Route path="/overlap" element={<Navigate to="/report" replace />} />
        <Route
          path="/portfolio"
          element={
            <RoleRoute allowedRoles={["admin"]}>
              <PortfolioPage />
            </RoleRoute>
          }
        />
        <Route
          path="/admin"
          element={
            <ProtectedRoute capability="manage_exclusions">
              <AdminPage />
            </ProtectedRoute>
          }
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppLayout>
  );
}

function HomeRoute() {
  const auth = useAuth();

  if (auth.isLoading) {
    return <LoadingBlock label="Checking access…" />;
  }
  if (auth.role === "visitor") {
    return <Navigate to="/charts" replace />;
  }
  return <DashboardPage />;
}

function LegacyRrgRedirect() {
  const params = useParams();
  const universe = params.universe ?? "sector";
  return <Navigate to={`/rotation/${universe}`} replace />;
}
