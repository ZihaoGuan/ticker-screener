type LoadingBlockProps = {
  label?: string;
  compact?: boolean;
};

export function LoadingBlock({ label = "Loading data…", compact = false }: LoadingBlockProps) {
  return (
    <div className={`loading-block${compact ? " is-compact" : ""}`} role="status" aria-live="polite">
      <span className="loading-spinner" aria-hidden="true" />
      <span className="panel-copy">{label}</span>
    </div>
  );
}
