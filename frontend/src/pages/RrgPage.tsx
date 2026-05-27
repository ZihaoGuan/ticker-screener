import { useEffect, useState } from "react";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";

type RrgSection = {
  id: string;
  title: string;
  description: string;
  index_url: string;
  image_url: string;
};

type RrgResponse = {
  available: boolean;
  date_label: string;
  report_root: string;
  report_index_url: string;
  sections: RrgSection[];
};

const emptyPayload: RrgResponse = {
  available: false,
  date_label: "",
  report_root: "",
  report_index_url: "",
  sections: [],
};

export function RrgPage() {
  const [payload, setPayload] = useState<RrgResponse>(emptyPayload);

  useEffect(() => {
    void fetchJson<RrgResponse>("/api/rrg/latest").then(setPayload).catch(() => setPayload(emptyPayload));
  }, []);

  if (!payload.available) {
    return (
      <div className="page-grid">
        <Panel title="Daily RRG" aside={<span className="eyebrow">No Render Yet</span>}>
          <p className="panel-copy">
            No sector rotation report is available yet. Run the daily RRG workflow first, then this page will pick up
            the latest rendered sector, industry, and theme charts automatically.
          </p>
        </Panel>
      </div>
    );
  }

  return (
    <div className="page-grid">
      <Panel
        title="Daily RRG"
        aside={
          payload.report_index_url ? (
            <a className="ghost-button" href={payload.report_index_url} target="_blank" rel="noreferrer">
              Open Full Report
            </a>
          ) : (
            <span className="eyebrow">{payload.date_label}</span>
          )
        }
      >
        <p className="panel-copy">
          Latest rendered report: <span className="mono">{payload.date_label}</span>
        </p>
      </Panel>

      <div className="rrg-grid">
        {payload.sections.map((section) => (
          <Panel key={section.id} title={section.title} aside={<a className="eyebrow" href={section.index_url} target="_blank" rel="noreferrer">Open</a>}>
            <p className="panel-copy">{section.description}</p>
            <a className="rrg-image-link" href={section.index_url} target="_blank" rel="noreferrer">
              <img className="rrg-image" src={section.image_url} alt={section.title} loading="lazy" />
            </a>
          </Panel>
        ))}
      </div>
    </div>
  );
}
