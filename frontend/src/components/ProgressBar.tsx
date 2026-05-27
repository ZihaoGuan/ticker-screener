import type { JobStatus } from "../lib/types";

type ProgressBarProps = {
  status: JobStatus;
  label?: string;
  compact?: boolean;
  progress?: number | null;
};

export function ProgressBar({ status, label, compact = false, progress }: ProgressBarProps) {
  const resolvedProgress =
    typeof progress === "number"
      ? Math.max(0, Math.min(100, progress))
      : status === "success"
        ? 100
        : status === "failed"
          ? 100
          : 45;
  const wrapperClass = compact ? "progress-track progress-track-compact" : "progress-track";
  const barClass = compact ? "progress-fill progress-fill-compact" : "progress-fill";

  return (
    <div className="progress-shell" aria-label={label ?? `Job progress ${status}`}>
      <div
        className={wrapperClass}
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={status === "running" && typeof progress !== "number" ? undefined : resolvedProgress}
        aria-valuetext={
          status === "running"
            ? typeof progress === "number"
              ? `${resolvedProgress}%`
              : "In progress"
            : status === "success"
              ? "Completed"
              : "Failed"
        }
      >
        <div
          className={`${barClass} progress-${status} ${status === "running" ? "progress-running" : ""}`}
          style={{ width: `${resolvedProgress}%` }}
        />
      </div>
      {label ? <span className="progress-label">{label}</span> : null}
    </div>
  );
}
