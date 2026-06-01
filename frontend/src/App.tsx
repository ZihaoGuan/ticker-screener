import { Navigate, Route, Routes, useParams } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { ProtectedRoute } from "./auth/ProtectedRoute";
import { AdminPage } from "./pages/AdminPage";
import { BacktestsPage } from "./pages/BacktestsPage";
import { DashboardPage } from "./pages/DashboardPage";
import { GuidePage } from "./pages/GuidePage";
import { LoginPage } from "./pages/LoginPage";
import { OverlapPage } from "./pages/OverlapPage";
import { ChartsPage } from "./pages/ChartsPage";
import { RrgPage } from "./pages/RrgPage";
import { RunsPage } from "./pages/RunsPage";
import { WatchlistsPage } from "./pages/WatchlistsPage";

export default function App() {
  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/login" element={<LoginPage />} />
        <Route path="/guide" element={<GuidePage />} />
        <Route path="/charts" element={<ChartsPage />} />
        <Route
          path="/runs"
          element={
            <ProtectedRoute capability="run_screeners">
              <RunsPage />
            </ProtectedRoute>
          }
        />
        <Route path="/watchlists" element={<WatchlistsPage />} />
        <Route path="/rotation" element={<Navigate to="/rotation/sector" replace />} />
        <Route path="/rotation/:universe" element={<RrgPage />} />
        <Route path="/rrg" element={<Navigate to="/rotation/sector" replace />} />
        <Route path="/rrg/:universe" element={<LegacyRrgRedirect />} />
        <Route path="/overlap" element={<OverlapPage />} />
        <Route path="/backtests" element={<BacktestsPage />} />
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

function LegacyRrgRedirect() {
  const params = useParams();
  const universe = params.universe ?? "sector";
  return <Navigate to={`/rotation/${universe}`} replace />;
}
