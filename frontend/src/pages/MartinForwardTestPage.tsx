import { useEffect, useState } from "react";

type Context = {
  configured: boolean;
  runs?: Array<Record<string, unknown>>;
  candidates?: Array<Record<string, unknown>>;
  positions?: Array<Record<string, unknown>>;
  events?: Array<Record<string, unknown>>;
};

export default function MartinForwardTestPage() {
  const [data, setData] = useState<Context | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    fetch("/api/admin/martin-forward-test", { credentials: "include" })
      .then(async (response) => { if (!response.ok) throw new Error(`Request failed (${response.status})`); return response.json(); })
      .then((payload: Context) => { if (active) setData(payload); })
      .catch((reason: Error) => { if (active) setError(reason.message); });
    return () => { active = false; };
  }, []);
  if (error) return <main className="page-shell"><h1>Martin Focus Forward Test</h1><p>{error}</p></main>;
  if (!data) return <main className="page-shell"><h1>Martin Focus Forward Test</h1><p>Loading…</p></main>;
  const rows = (key: "candidates" | "positions" | "events") => data[key] ?? [];
  const latest = data.runs?.[0];
  const summary = latest?.summary_json && typeof latest.summary_json === "object" ? latest.summary_json as Record<string, unknown> : {};
  return <main className="page-shell">
    <h1>Martin Focus Forward Test</h1>
    <p>Strategy <code>martin_daily_v1</code> · Source: immutable scanner_top_hits_snapshot guru board · simulated, no broker orders</p>
    {latest ? <p>Session {String(latest.session_date ?? "—")} · Source snapshot run {String(summary.source_run_id ?? latest.source_run_id ?? "—")} · Fingerprint {String(summary.source_fingerprint ?? latest.source_fingerprint ?? "—")}</p> : null}
    {!data.configured ? <p>Database persistence is unavailable.</p> : null}
    <p>Latest session: {latest ? "available" : "none"} · Pending orders: {(data.candidates ?? []).filter((item) => item.status === "pending").length} · Open positions: {(data.positions ?? []).length} · Recent events: {(data.events ?? []).length}</p>
    <h2>Runs</h2><p>{data.runs?.length ?? 0} stored sessions</p>
    <h2>Latest snapshot candidates</h2><Table items={rows("candidates")} fields={["ticker", "status", "rank", "reasons_json"]} />
    <h2>Open positions</h2><Table items={rows("positions").filter((item) => item.status === "open")} fields={["ticker", "opened_on", "payload_json"]} />
    <h2>Recent events</h2><Table items={rows("events")} fields={["event_date", "ticker", "event_type", "payload_json"]} />
  </main>;
}

function Table({ items, fields }: { items: Array<Record<string, unknown>>; fields: string[] }) {
  if (!items.length) return <p>No records yet.</p>;
  return <div className="table-wrap"><table><thead><tr>{fields.map((field) => <th key={field}>{field}</th>)}</tr></thead>
    <tbody>{items.map((item, index) => <tr key={String(item.id ?? index)}>{fields.map((field) => <td key={field}>{format(item[field])}</td>)}</tr>)}</tbody></table></div>;
}

function format(value: unknown): string {
  if (value == null) return "—";
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}
