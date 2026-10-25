import { RunsPage } from "./RunsPage";
import { Link } from "react-router-dom";

export function BacktestsPage() {
  return <><div className="page-shell"><p><Link to="/backtests/bar-retest">Martin Bar Retest execution books</Link></p></div><RunsPage mode="backtests" /></>;
}
