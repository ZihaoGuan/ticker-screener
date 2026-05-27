import { Navigate, Route, Routes } from "react-router-dom";
import { AppLayout } from "./components/AppLayout";
import { AdminPage } from "./pages/AdminPage";
import { BacktestsPage } from "./pages/BacktestsPage";
import { DashboardPage } from "./pages/DashboardPage";
import { OverlapPage } from "./pages/OverlapPage";
import { RrgPage } from "./pages/RrgPage";
import { RunsPage } from "./pages/RunsPage";
import { WatchlistsPage } from "./pages/WatchlistsPage";

export default function App() {
  return (
    <AppLayout>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/runs" element={<RunsPage />} />
        <Route path="/watchlists" element={<WatchlistsPage />} />
        <Route path="/rrg" element={<RrgPage />} />
        <Route path="/overlap" element={<OverlapPage />} />
        <Route path="/backtests" element={<BacktestsPage />} />
        <Route path="/admin" element={<AdminPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppLayout>
  );
}
