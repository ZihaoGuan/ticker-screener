import { Suspense, lazy } from "react";
import { Navigate, Route, Routes, useParams } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { ProtectedRoute } from "./auth/ProtectedRoute";
import { RoleRoute } from "./auth/RoleRoute";
import { useAuth } from "./auth/AuthContext";
import { LoadingBlock } from "./components/LoadingBlock";

const DashboardPage = lazy(() => import("./pages/DashboardPage").then((module) => ({ default: module.DashboardPage })));
const LoginPage = lazy(() => import("./pages/LoginPage").then((module) => ({ default: module.LoginPage })));
const GuidePage = lazy(() => import("./pages/GuidePage").then((module) => ({ default: module.GuidePage })));
const ChartsPage = lazy(() => import("./pages/ChartsPage").then((module) => ({ default: module.ChartsPage })));
const EarningsPage = lazy(() => import("./pages/EarningsPage").then((module) => ({ default: module.EarningsPage })));
const ScannerBoardPage = lazy(() => import("./pages/ScannerBoardPage").then((module) => ({ default: module.ScannerBoardPage })));
const ScannerTopHitsPage = lazy(() => import("./pages/ScannerTopHitsPage").then((module) => ({ default: module.ScannerTopHitsPage })));
const ScannerResultPage = lazy(() => import("./pages/ScannerResultPage").then((module) => ({ default: module.ScannerResultPage })));
const RatingsPage = lazy(() => import("./pages/RatingsPage").then((module) => ({ default: module.RatingsPage })));
const RunsPage = lazy(() => import("./pages/RunsPage").then((module) => ({ default: module.RunsPage })));
const WarmupPage = lazy(() => import("./pages/WarmupPage").then((module) => ({ default: module.WarmupPage })));
const BacktestsPage = lazy(() => import("./pages/BacktestsPage").then((module) => ({ default: module.BacktestsPage })));
const WeeklyWatchlistPage = lazy(() => import("./pages/WeeklyWatchlistPage").then((module) => ({ default: module.WeeklyWatchlistPage })));
const WatchlistsPage = lazy(() => import("./pages/WatchlistsPage").then((module) => ({ default: module.WatchlistsPage })));
const PairTradesPage = lazy(() => import("./pages/PairTradesPage").then((module) => ({ default: module.PairTradesPage })));
const RrgPage = lazy(() => import("./pages/RrgPage").then((module) => ({ default: module.RrgPage })));
const OverlapPage = lazy(() => import("./pages/OverlapPage").then((module) => ({ default: module.OverlapPage })));
const MyPicksPage = lazy(() => import("./pages/MyPicksPage").then((module) => ({ default: module.MyPicksPage })));
const PortfolioPage = lazy(() => import("./pages/PortfolioPage").then((module) => ({ default: module.PortfolioPage })));
const AdminPage = lazy(() => import("./pages/AdminPage").then((module) => ({ default: module.AdminPage })));
const AdminTickerRatingsHealthPage = lazy(() => import("./pages/AdminTickerRatingsHealthPage").then((module) => ({ default: module.AdminTickerRatingsHealthPage })));
const AdminDiscordPage = lazy(() => import("./pages/AdminDiscordPage").then((module) => ({ default: module.AdminDiscordPage })));

export default function App() {
  const auth = useAuth();

  if (auth.isMaintenance) {
    return <MaintenancePage />;
  }

  return (
    <AppLayout>
      <Suspense fallback={<LoadingBlock label="Loading page…" />}>
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
            element={<ScannerBoardPage />}
          />
          <Route
            path="/scanner/top-hits"
            element={<ScannerTopHitsPage />}
          />
          <Route
            path="/scanner/:scannerId"
            element={<ScannerResultPage />}
          />
          <Route
            path="/ratings"
            element={
              <RatingsPage />
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
              <RoleRoute allowedRoles={["admin"]}>
                <WeeklyWatchlistPage />
              </RoleRoute>
            }
          />
          <Route
            path="/watchlists"
            element={
              <RoleRoute allowedRoles={["admin"]}>
                <WatchlistsPage />
              </RoleRoute>
            }
          />
          <Route
            path="/pair-trades"
            element={
              <RoleRoute allowedRoles={["admin"]}>
                <PairTradesPage />
              </RoleRoute>
            }
          />
          <Route
            path="/rotation"
            element={<Navigate to="/rotation/sector" replace />}
          />
          <Route
            path="/rotation/:universe"
            element={<RrgPage />}
          />
          <Route path="/rrg" element={<Navigate to="/rotation/sector" replace />} />
          <Route path="/rrg/:universe" element={<LegacyRrgRedirect />} />
          <Route
            path="/report"
            element={
              <RoleRoute allowedRoles={["admin"]}>
                <OverlapPage />
              </RoleRoute>
            }
          />
          <Route path="/overlap" element={<Navigate to="/report" replace />} />
          <Route
            path="/my-picks"
            element={
              <RoleRoute allowedRoles={["admin"]}>
                <MyPicksPage />
              </RoleRoute>
            }
          />
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
          <Route
            path="/admin/ticker-ratings-health"
            element={
              <ProtectedRoute capability="manage_exclusions">
                <AdminTickerRatingsHealthPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/admin/discord-notifications"
            element={
              <ProtectedRoute capability="manage_exclusions">
                <AdminDiscordPage />
              </ProtectedRoute>
            }
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Suspense>
    </AppLayout>
  );
}

function MaintenancePage() {
  return (
    <div className="maintenance-shell">
      <section className="maintenance-card">
        <span className="eyebrow">Ticker Screener</span>
        <h1>Under maintenance</h1>
        <p className="panel-copy">
          Auth service is temporarily unavailable. The web UI switched to maintenance mode after <code>/api/auth/me</code> returned{" "}
          <code>502</code>.
        </p>
        <p className="file-meta">Please try again after backend service recovers.</p>
      </section>
    </div>
  );
}

function HomeRoute() {
  const auth = useAuth();

  if (auth.isLoading) {
    return <LoadingBlock label="Checking access…" />;
  }
  return <DashboardPage />;
}

function LegacyRrgRedirect() {
  const params = useParams();
  const universe = params.universe ?? "sector";
  return <Navigate to={`/rotation/${universe}`} replace />;
}
