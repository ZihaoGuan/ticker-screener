import type { PropsWithChildren, ReactNode } from "react";

type PanelProps = PropsWithChildren<{
  title?: string;
  aside?: ReactNode;
  className?: string;
}>;

export function Panel({ title, aside, className = "", children }: PanelProps) {
  return (
    <section className={`panel ${className}`.trim()}>
      {(title || aside) && (
        <div className="panel-head">
          {title ? <h2>{title}</h2> : <span />}
          {aside}
        </div>
      )}
      {children}
    </section>
  );
}
