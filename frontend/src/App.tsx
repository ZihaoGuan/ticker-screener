import { Suspense, lazy, type ComponentType } from "react";
import { Navigate, Route, Routes, useParams } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { ProtectedRoute } from "./auth/ProtectedRoute";
import { RoleRoute } from "./auth/RoleRoute";
import { useAuth } from "./auth/AuthContext";
import { LoadingBlock } from "./components/LoadingBlock";

const CHUNK_RELOAD_STORAGE_KEY = "ticker-screener:chunk-reload-attempted";

function shouldRetryDynamicImport(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error ?? "");
  return (
    message.includes("Failed to fetch dynamically imported module") ||
    message.includes("Importing a module script failed") ||
    message.includes("Unable to preload CSS")
  );
}

function lazyPage<TModule extends Record<string, unknown>, TKey extends keyof TModule>(
  loader: () => Promise<TModule>,
  exportName: TKey,
) {
  return lazy(async () => {
    try {
      const module = await loader();
      if (typeof window !== "undefined") {
        window.sessionStorage.removeItem(CHUNK_RELOAD_STORAGE_KEY);
      }
      return { default: module[exportName] as ComponentType<any> };
    } catch (error) {
      if (typeof window !== "undefined" && shouldRetryDynamicImport(error)) {
        const alreadyRetried = window.sessionStorage.getItem(CHUNK_RELOAD_STORAGE_KEY);
        if (!alreadyRetried) {
          window.sessionStorage.setItem(CHUNK_RELOAD_STORAGE_KEY, "1");
          window.location.reload();
          await new Promise<never>(() => {});
        }
        window.sessionStorage.removeItem(CHUNK_RELOAD_STORAGE_KEY);
      }
      throw error;
    }
  });
}

const DashboardPage = lazyPage(() => import("./pages/DashboardPage"), "DashboardPage");
const LoginPage = lazyPage(() => import("./pages/LoginPage"), "LoginPage");
const GuidePage = lazyPage(() => import("./pages/GuidePage"), "GuidePage");
const ChartsPage = lazyPage(() => import("./pages/ChartsPage"), "ChartsPage");
const EarningsPage = lazyPage(() => import("./pages/EarningsPage"), "EarningsPage");
const ScannerBoardPage = lazyPage(() => import("./pages/ScannerBoardPage"), "ScannerBoardPage");
const ScannerTopHitsPage = lazyPage(() => import("./pages/ScannerTopHitsPage"), "ScannerTopHitsPage");
const ScannerResultPage = lazyPage(() => import("./pages/ScannerResultPage"), "ScannerResultPage");
const RatingsPage = lazyPage(() => import("./pages/RatingsPage"), "RatingsPage");
const RunsPage = lazyPage(() => import("./pages/RunsPage"), "RunsPage");
const WarmupPage = lazyPage(() => import("./pages/WarmupPage"), "WarmupPage");
const BacktestsPage = lazyPage(() => import("./pages/BacktestsPage"), "BacktestsPage");
const WeeklyWatchlistPage = lazyPage(() => import("./pages/WeeklyWatchlistPage"), "WeeklyWatchlistPage");
const WatchlistsPage = lazyPage(() => import("./pages/WatchlistsPage"), "WatchlistsPage");
const PairTradesPage = lazyPage(() => import("./pages/PairTradesPage"), "PairTradesPage");
const RrgPage = lazyPage(() => import("./pages/RrgPage"), "RrgPage");
const OverlapPage = lazyPage(() => import("./pages/OverlapPage"), "OverlapPage");
const MyPicksPage = lazyPage(() => import("./pages/MyPicksPage"), "MyPicksPage");
const PortfolioPage = lazyPage(() => import("./pages/PortfolioPage"), "PortfolioPage");
const AdminPage = lazyPage(() => import("./pages/AdminPage"), "AdminPage");
const AdminMissingSectorsPage = lazyPage(() => import("./pages/AdminMissingSectorsPage"), "AdminMissingSectorsPage");
const AdminMissingFinvizTickersPage = lazyPage(() => import("./pages/AdminMissingFinvizTickersPage"), "AdminMissingFinvizTickersPage");
const AdminTickerRatingsHealthPage = lazyPage(() => import("./pages/AdminTickerRatingsHealthPage"), "AdminTickerRatingsHealthPage");
const AdminDiscordPage = lazyPage(() => import("./pages/AdminDiscordPage"), "AdminDiscordPage");

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
            path="/admin/missing-sectors"
            element={
              <ProtectedRoute capability="manage_exclusions">
                <AdminMissingSectorsPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/admin/finviz-missing-tickers"
            element={
              <ProtectedRoute capability="manage_exclusions">
                <AdminMissingFinvizTickersPage />
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
