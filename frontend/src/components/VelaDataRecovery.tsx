import { useState, type ReactNode } from "react";
import { useAuth } from "../auth/AuthContext";
import { fetchJson } from "../lib/api";
import type { JobsResponse } from "../lib/types";

type VelaDataRecoveryProps = {
  ticker: string;
  error: string;
  fallback: ReactNode;
  onRetry: () => void;
};

const INVALID_OHLCV_PATTERN = /invalid (?:ohlcv|candle) (?:data|range)/i;
const MAX_JOB_POLLS = 180;

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Price-data repair failed.";
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

export function VelaDataRecovery({ ticker, error, fallback, onRetry }: VelaDataRecoveryProps) {
  const auth = useAuth();
  const [isRepairing, setIsRepairing] = useState(false);
  const [notice, setNotice] = useState("");
  const canRepair = INVALID_OHLCV_PATTERN.test(error) && auth.hasCapability("sync_history");

  const repair = async () => {
    setIsRepairing(true);
    setNotice("");
    try {
      const launch = await fetchJson<{ ok: boolean; job_id: string }>("/api/admin/history-sync", {
        method: "POST",
        body: JSON.stringify({
          start_date: "2020-01-01",
          tickers: [ticker],
          chunk_size: 1,
          include_excluded_tickers: true,
          replace_existing_history: true,
        }),
      });
      for (let attempt = 0; attempt < MAX_JOB_POLLS; attempt += 1) {
        await wait(1000);
        const job = await fetchJson<JobsResponse["jobs"][number]>(`/api/jobs/${encodeURIComponent(launch.job_id)}`);
        if (job.status === "success") {
          setNotice("Price data repaired. Retrying Vela…");
          onRetry();
          return;
        }
        if (job.status === "failed" || job.status === "cancelled") {
          throw new Error(job.log_tail || "Price-data repair did not complete.");
        }
      }
      setNotice("Repair is still running. Try Vela again shortly.");
    } catch (repairError) {
      setNotice(errorMessage(repairError));
    } finally {
      setIsRepairing(false);
    }
  };

  return (
    <div className="vela-data-recovery">
      {fallback}
      <p className="panel-copy vela-pine-error">Vela could not use this price data: {error}</p>
      {canRepair ? (
        <div className="button-row">
          <button className="ghost-button" type="button" onClick={() => void repair()} disabled={isRepairing}>
            {isRepairing ? "Refetching price data…" : "Refetch price data"}
          </button>
        </div>
      ) : null}
      {notice ? <p className="panel-copy" role="status">{notice}</p> : null}
    </div>
  );
}
