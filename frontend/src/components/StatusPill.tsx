import type { JobStatus } from "../lib/types";

export function StatusPill({ status }: { status: JobStatus }) {
  return <span className={`status-pill status-${status}`}>{status}</span>;
}
