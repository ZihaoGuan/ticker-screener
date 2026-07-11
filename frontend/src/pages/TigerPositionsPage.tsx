import { useEffect, useMemo, useState, type FormEvent } from "react";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatLocalDateTime } from "../lib/format";
import type { TigerPositionsContextResponse } from "../lib/types";

const EMPTY_CONTEXT: TigerPositionsContextResponse = {
  database_configured: false,
  settings: {
    user_id: 0,
    display_name: "",
    tiger_id: "",
    account: "",
    private_key_env_var: "TIGER_PRIVATE_KEY",
    is_enabled: false,
    last_synced_at: null,
    last_sync_error: null,
    has_private_key: false,
  },
  summary: {
    position_count: 0,
    total_market_value: 0,
    total_cost_basis: 0,
    total_unrealized_pl: 0,
    total_unrealized_pl_pct: 0,
    add_count: 0,
    hold_count: 0,
    trim_count: 0,
    last_synced_at: null,
  },
  positions: [],
};

export function TigerPositionsPage() {
  const [context, setContext] = useState<TigerPositionsContextResponse>(EMPTY_CONTEXT);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [isSyncing, setIsSyncing] = useState(false);
  const [notice, setNotice] = useState("");
  const [form, setForm] = useState({
    display_name: "",
    tiger_id: "",
    account: "",
    private_key_env_var: "TIGER_PRIVATE_KEY",
    is_enabled: true,
  });

  const loadContext = () => {
    setIsLoading(true);
    void fetchJson<TigerPositionsContextResponse>("/api/tiger/positions/me")
      .then((payload) => {
        setContext(payload);
        setForm({
          display_name: payload.settings.display_name || "",
          tiger_id: payload.settings.tiger_id || "",
          account: payload.settings.account || "",
          private_key_env_var: payload.settings.private_key_env_var || "TIGER_PRIVATE_KEY",
          is_enabled: payload.settings.is_enabled,
        });
      })
      .catch((error) => {
        setContext(EMPTY_CONTEXT);
        setNotice(error instanceof Error ? error.message : "Failed to load Tiger positions.");
      })
      .finally(() => setIsLoading(false));
  };

  useEffect(() => {
    loadContext();
  }, []);

  const topActionLabel = useMemo(() => {
    if (!context.positions.length) {
      return "No synced positions yet";
    }
    const counts = { add: 0, hold: 0, trim: 0 };
    context.positions.forEach((item) => {
      const action = item.position_action?.action;
      if (action === "add_position") counts.add += 1;
      if (action === "hold_position") counts.hold += 1;
      if (action === "trim_reduce") counts.trim += 1;
    });
    return `Add ${counts.add} • Hold ${counts.hold} • Trim ${counts.trim}`;
  }, [context.positions]);

  const handleSave = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSaving(true);
    setNotice("");
    try {
      const response = await fetchJson<{ ok: boolean }>("/api/tiger/positions/me/settings", {
        method: "POST",
        body: JSON.stringify(form),
      });
      if (response.ok) {
        setNotice("Tiger settings saved.");
        loadContext();
      }
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to save Tiger settings.");
    } finally {
      setIsSaving(false);
    }
  };

  const handleSync = async () => {
    setIsSyncing(true);
    setNotice("");
    try {
      const response = await fetchJson<{ synced_count: number }>("/api/tiger/positions/me/sync", {
        method: "POST",
      });
      setNotice(`Synced ${response.synced_count} Tiger position(s).`);
      loadContext();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to sync Tiger positions.");
    } finally {
      setIsSyncing(false);
    }
  };

  if (isLoading) {
    return <LoadingBlock label="Loading Tiger positions…" />;
  }

  return (
    <div className="page-grid">
      <Panel title="Tiger Positions">
        <div className="eyebrow">Account Bound</div>
        <p className="panel-copy">Bind one Tiger account to the signed-in app user, sync positions daily, and decorate each holding with the repo&apos;s latest position-action guidance.</p>
        {notice ? <div className="panel-copy">{notice}</div> : null}
        <div className="metric-grid">
          <MetricCard label="Positions" value={String(context.summary.position_count)} />
          <MetricCard label="Market Value" value={formatMoney(context.summary.total_market_value)} />
          <MetricCard label="Unrealized P/L" value={formatMoney(context.summary.total_unrealized_pl)} />
          <MetricCard label="Advice Mix" value={topActionLabel} />
        </div>
      </Panel>

      <div className="split-grid">
        <Panel title="Connection">
          <div className="eyebrow">Tiger SDK</div>
          <p className="panel-copy">Secrets stay in env. The page only stores the account binding and which env var should hold the RSA private key.</p>
          <form className="stack-form" onSubmit={handleSave}>
            <label>
              <span>Display name</span>
              <input value={form.display_name} onChange={(event) => setForm((current) => ({ ...current, display_name: event.target.value }))} placeholder="Main Tiger" />
            </label>
            <label>
              <span>Tiger developer ID</span>
              <input value={form.tiger_id} onChange={(event) => setForm((current) => ({ ...current, tiger_id: event.target.value }))} placeholder="your_tiger_id" />
            </label>
            <label>
              <span>Account</span>
              <input value={form.account} onChange={(event) => setForm((current) => ({ ...current, account: event.target.value }))} placeholder="U12345678" />
            </label>
            <label>
              <span>Private key env var</span>
              <input value={form.private_key_env_var} onChange={(event) => setForm((current) => ({ ...current, private_key_env_var: event.target.value }))} placeholder="TIGER_PRIVATE_KEY" />
            </label>
            <label className="checkbox-row">
              <input type="checkbox" checked={form.is_enabled} onChange={(event) => setForm((current) => ({ ...current, is_enabled: event.target.checked }))} />
              <span>Enable daily Tiger sync for this login account</span>
            </label>
            <div className="button-row">
              <button type="submit" disabled={isSaving}>{isSaving ? "Saving…" : "Save Settings"}</button>
              <button type="button" className="secondary-button" onClick={() => void handleSync()} disabled={isSyncing}>
                {isSyncing ? "Syncing…" : "Sync Now"}
              </button>
            </div>
          </form>
          <div className="panel-copy">
            Key present: <strong>{context.settings.has_private_key ? "Yes" : "No"}</strong>
          </div>
          <div className="panel-copy">
            Last sync: <strong>{context.settings.last_synced_at ? formatLocalDateTime(context.settings.last_synced_at) : "Never"}</strong>
          </div>
          {context.settings.last_sync_error ? <div className="panel-copy">Last error: {context.settings.last_sync_error}</div> : null}
        </Panel>

        <Panel title="Positions">
          <div className="eyebrow">Latest Snapshot</div>
          <p className="panel-copy">The page shows the latest synced Tiger batch for this login account only.</p>
          {!context.database_configured ? <LoadingBlock label="Database is not configured for Tiger position storage." compact /> : null}
          {!context.positions.length ? <div className="panel-copy">No Tiger positions synced yet.</div> : null}
          {context.positions.length ? (
            <div className="detail-table-wrapper">
              <table className="detail-table">
                <thead>
                  <tr>
                    <th>Ticker</th>
                    <th>Qty</th>
                    <th>Mkt Value</th>
                    <th>Unrlzd</th>
                    <th>Action</th>
                    <th>Why</th>
                  </tr>
                </thead>
                <tbody>
                  {context.positions.map((position) => (
                    <tr key={position.id}>
                      <td>{position.ticker}</td>
                      <td>{formatNumber(position.quantity)}</td>
                      <td>{formatMoney(position.market_value)}</td>
                      <td>{formatMoney(position.unrealized_pl)}</td>
                      <td>{humanizeAction(position.position_action?.action)}</td>
                      <td>{position.position_action?.reason_summary || "No position-action snapshot yet."}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </Panel>
      </div>
    </div>
  );
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <article className="metric-card">
      <div className="eyebrow">{label}</div>
      <div className="metric-value">{value}</div>
    </article>
  );
}

function formatMoney(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value);
}

function humanizeAction(value: string | null | undefined) {
  if (!value) {
    return "Review";
  }
  return value.replace(/_/g, " ").replace(/\b\w/g, (match: string) => match.toUpperCase());
}
