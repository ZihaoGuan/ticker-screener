import { useEffect, useState } from "react";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";

export function AdminPage() {
  const [payload, setPayload] = useState<{ excluded_tickers: string[]; excluded_count: number }>({
    excluded_tickers: [],
    excluded_count: 0,
  });

  useEffect(() => {
    void fetchJson<{ excluded_tickers: string[]; excluded_count: number }>("/api/admin/exclusions")
      .then(setPayload)
      .catch(() => setPayload({ excluded_tickers: [], excluded_count: 0 }));
  }, []);

  return (
    <div className="page-grid">
      <Panel title="Exclusions" aside={<span className="eyebrow">{payload.excluded_count} symbols</span>}>
        <div className="pill-list">
          {payload.excluded_tickers.map((item) => (
            <span key={item} className="symbol-pill">
              {item}
            </span>
          ))}
        </div>
      </Panel>
    </div>
  );
}
