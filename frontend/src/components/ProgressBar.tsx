import type { JobStatus } from "../lib/types";

type ProgressBarProps = {
  status: JobStatus;
  label?: string;
  compact?: boolean;
};

export function ProgressBar({ status, label, compact = false }: ProgressBarProps) {
  const progress = status === "success" ? 100 : status === "failed" ? 100 : 45;
  const wrapperClass = compact ? "progress-track progress-track-compact" : "progress-track";
  const barClass = compact ? "progress-fill progress-fill-compact" : "progress-fill";

  return (
    <div className="progress-shell" aria-label={label ?? `Job progress ${status}`}>
      <div
        className={wrapperClass}
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={status === "running" ? undefined : progress}
        aria-valuetext={status === "running" ? "In progress" : status === "success" ? "Completed" : "Failed"}
      >
        <div className={`${barClass} progress-${status} ${status === "running" ? "progress-running" : ""}`} style={{ width: `${progress}%` }} />
      </div>
      {label ? <span className="progress-label">{label}</span> : null}
    </div>
  );
}
