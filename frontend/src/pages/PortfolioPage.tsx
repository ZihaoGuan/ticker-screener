import { startTransition, useEffect, useMemo, useState, type ChangeEvent, type FormEvent } from "react";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import { formatLocalDate, formatLocalDateTime } from "../lib/format";
import type { PortfolioContextResponse, PortfolioImportResponse, PortfolioPosition, PortfolioRefreshResponse } from "../lib/types";

const EMPTY_PORTFOLIO_CONTEXT: PortfolioContextResponse = {
  database_configured: false,
  summary: {
    position_count: 0,
    total_market_value: 0,
    total_cost_basis: 0,
    total_unrealized_pl: 0,
    total_unrealized_pl_pct: 0,
    stale_advice_count: 0,
    missing_advice_count: 0,
    last_refreshed_at: null,
  },
  positions: [],
  portfolios: [],
  market_regime: {
    title: "Market Regime Placeholder",
    status: "deferred",
    description: "Space reserved for VIX, Fear & Greed, or other macro gauges in a later iteration.",
  },
};

type PositionFormState = {
  ticker: string;
  shares: string;
  entry_price: string;
  opened_at: string;
  notes: string;
  portfolio_name: string;
};

const EMPTY_POSITION_FORM: PositionFormState = {
  ticker: "",
  shares: "",
  entry_price: "",
  opened_at: "",
  notes: "",
  portfolio_name: "Main",
};

export function PortfolioPage() {
  const [context, setContext] = useState<PortfolioContextResponse>(EMPTY_PORTFOLIO_CONTEXT);
  const [isLoading, setIsLoading] = useState(true);
  const [notice, setNotice] = useState("");
  const [selectedPositionId, setSelectedPositionId] = useState<number | null>(null);
  const [positionForm, setPositionForm] = useState<PositionFormState>(EMPTY_POSITION_FORM);
  const [csvText, setCsvText] = useState("");
  const [csvSourceName, setCsvSourceName] = useState("portfolio.csv");
  const [importResult, setImportResult] = useState<PortfolioImportResponse | null>(null);
  const [isSavingPosition, setIsSavingPosition] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [transactionForm, setTransactionForm] = useState({
    side: "buy",
    shares: "",
    price: "",
    trade_date: "",
    fees: "",
    notes: "",
  });

  const loadPortfolio = () => {
    setIsLoading(true);
    void fetchJson<PortfolioContextResponse>("/api/admin/portfolio")
      .then((payload) => {
        setContext(payload);
        const firstPositionId = payload.positions[0]?.id ?? null;
        startTransition(() => {
          setSelectedPositionId((current) => {
            if (current && payload.positions.some((item) => item.id === current)) {
              return current;
            }
            return firstPositionId;
          });
        });
      })
      .catch((error) => {
        setContext(EMPTY_PORTFOLIO_CONTEXT);
        setNotice(error instanceof Error ? error.message : "Failed to load portfolio.");
      })
      .finally(() => setIsLoading(false));
  };

  useEffect(() => {
    loadPortfolio();
  }, []);

  const selectedPosition = useMemo(
    () => context.positions.find((item) => item.id === selectedPositionId) ?? context.positions[0] ?? null,
    [context.positions, selectedPositionId],
  );

  const handleSubmitPosition = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSavingPosition(true);
    setNotice("");
    try {
      const payload = {
        ticker: positionForm.ticker.trim().toUpperCase(),
        shares: positionForm.shares.trim(),
        entry_price: positionForm.entry_price.trim(),
        opened_at: positionForm.opened_at.trim(),
        notes: positionForm.notes.trim(),
        portfolio_name: positionForm.portfolio_name.trim() || "Main",
      };
      await fetchJson<{ ok: boolean }>("/api/admin/portfolio/positions", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setNotice(`Added ${payload.ticker}. Refresh advice when you want updated targets.`);
      setPositionForm(EMPTY_POSITION_FORM);
      loadPortfolio();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to save position.");
    } finally {
      setIsSavingPosition(false);
    }
  };

  const handleDeletePosition = async (position: PortfolioPosition) => {
    if (!window.confirm(`Delete ${position.ticker} from ${position.portfolio_name}?`)) {
      return;
    }
    setIsSavingPosition(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/portfolio/positions/${position.id}/delete`, {
        method: "POST",
      });
      setNotice(`Deleted ${position.ticker}.`);
      if (selectedPositionId === position.id) {
        setSelectedPositionId(null);
      }
      loadPortfolio();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to delete position.");
    } finally {
      setIsSavingPosition(false);
    }
  };

  const handleRefreshAdvice = async (positionId?: number) => {
    setIsRefreshing(true);
    setNotice("");
    try {
      const payload = positionId ? { position_id: positionId } : {};
      const response = await fetchJson<PortfolioRefreshResponse>("/api/admin/portfolio/advice/refresh", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setNotice(`Refreshed advice for ${response.refreshed_count} position(s).`);
      loadPortfolio();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to refresh advice.");
    } finally {
      setIsRefreshing(false);
    }
  };

  const handleImportCsv = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsImporting(true);
    setImportResult(null);
    setNotice("");
    try {
      const response = await fetchJson<PortfolioImportResponse>("/api/admin/portfolio/positions/import", {
        method: "POST",
        body: JSON.stringify({
          csv_text: csvText,
          source_name: csvSourceName.trim() || "portfolio.csv",
          portfolio_name: positionForm.portfolio_name.trim() || "Main",
        }),
      });
      setImportResult(response);
      setNotice(`Imported ${response.accepted_count} row(s). Refresh advice when ready.`);
      if (response.accepted_count > 0) {
        setCsvText("");
        loadPortfolio();
      }
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to import CSV.");
    } finally {
      setIsImporting(false);
    }
  };

  const handleCsvFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    const text = await file.text();
    setCsvText(text);
    setCsvSourceName(file.name || "portfolio.csv");
  };

  const positionsByPortfolio = useMemo(() => {
    const grouped = new Map<string, PortfolioPosition[]>();
    context.positions.forEach((position) => {
      const key = position.portfolio_name || "Main";
      grouped.set(key, [...(grouped.get(key) ?? []), position]);
    });
    return grouped;
  }, [context.positions]);

  const visiblePositions = useMemo(() => context.positions.filter((item) => !item.is_closed), [context.positions]);

  const handleSubmitTransaction = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!selectedPosition) {
      return;
    }
    setIsSavingPosition(true);
    setNotice("");
    try {
      await fetchJson<{ ok: boolean }>(`/api/admin/portfolio/positions/${selectedPosition.id}/transactions`, {
        method: "POST",
        body: JSON.stringify(transactionForm),
      });
      setNotice(`${transactionForm.side === "sell" ? "Sold" : "Added"} ${transactionForm.shares} ${selectedPosition.ticker}.`);
      setTransactionForm({ side: "buy", shares: "", price: "", trade_date: "", fees: "", notes: "" });
      loadPortfolio();
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to record transaction.");
    } finally {
      setIsSavingPosition(false);
    }
  };

  if (isLoading) {
    return <LoadingBlock label="Loading portfolio…" />;
  }

  return (
    <div className="page-grid portfolio-page">
      <div className="portfolio-summary-grid">
        <MetricCard label="Total Market Value" value={formatCurrency(context.summary.total_market_value)} meta={`${context.summary.position_count} positions`} />
        <MetricCard
          label="Unrealized P/L"
          value={formatSignedCurrency(context.summary.total_unrealized_pl)}
          meta={`${formatSignedPercent(context.summary.total_unrealized_pl_pct)} vs cost basis`}
          accent={context.summary.total_unrealized_pl >= 0 ? "up" : "down"}
        />
        <MetricCard
          label="Advice Health"
          value={`${context.summary.stale_advice_count} stale / ${context.summary.missing_advice_count} missing`}
          meta={context.summary.last_refreshed_at ? `Last refresh ${formatLocalDateTime(context.summary.last_refreshed_at)}` : "No refresh yet"}
          accent={context.summary.stale_advice_count === 0 && context.summary.missing_advice_count === 0 ? "up" : "neutral"}
        />
      </div>

      {notice ? <div className="portfolio-notice">{notice}</div> : null}

      <div className="split-grid portfolio-main-grid">
        <div className="page-grid">
          <Panel
            title="Active Positions"
            aside={
              <div className="runs-panel-aside">
                <button className="ghost-button" type="button" disabled={isRefreshing || visiblePositions.length === 0} onClick={() => void handleRefreshAdvice()}>
                  {isRefreshing ? "Refreshing…" : "Refresh All Advice"}
                </button>
              </div>
            }
          >
            {!context.database_configured ? <LoadingBlock label="Database is not configured for portfolio storage." compact /> : null}
            <div className="data-table-responsive">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Ticker</th>
                    <th>Close</th>
                    <th>Avg Cost</th>
                    <th>Shares</th>
                    <th>After TP1 / Avg Up</th>
                    <th>Signal</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {visiblePositions.map((position) => (
                    <tr
                      key={position.id}
                      className={selectedPosition?.id === position.id ? "is-selected-row" : ""}
                      onClick={() => startTransition(() => setSelectedPositionId(position.id))}
                    >
                      <td data-label="Ticker">
                        <div className="ticker-symbol">{position.ticker}</div>
                        <div className="ticker-company-inline">
                          {position.portfolio_name} • opened {formatLocalDate(position.opened_at)}
                        </div>
                      </td>
                      <td data-label="Close">{formatCurrency(position.advice.close_price)}</td>
                      <td data-label="Avg Cost">{formatCurrency(position.entry_price)}</td>
                      <td data-label="Shares">{formatNumber(position.shares)}</td>
                      <td data-label="After TP1 / Avg Up">
                        <div>{formatCurrency(position.advice.net_cost_after_tp1)}</div>
                        <div className="ticker-company-inline">{formatCurrency(position.advice.blended_entry_after_average_up)}</div>
                      </td>
                      <td data-label="Signal">
                        <span className={`portfolio-signal-pill is-${normalizeSignalClass(position.advice.signal_status)}`}>
                          {humanizeSignal(position.advice.signal_status)}
                        </span>
                      </td>
                      <td data-label="Actions">
                        <div className="button-row">
                          <button
                            className="table-action-button"
                            type="button"
                            onClick={(event) => {
                              event.stopPropagation();
                              void handleRefreshAdvice(position.id);
                            }}
                            disabled={isRefreshing}
                          >
                            Refresh
                          </button>
                          <button
                            className="table-action-button"
                            type="button"
                            disabled={isSavingPosition}
                            onClick={(event) => {
                              event.stopPropagation();
                              void handleDeletePosition(position);
                            }}
                          >
                            Delete
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Panel>

          <div className="portfolio-card-grid">
            <Panel title="Add Initial Position">
              <form className="run-toolbar" onSubmit={(event) => void handleSubmitPosition(event)}>
                <div className="run-params-grid">
                  <label className="field">
                    <span>Ticker</span>
                    <input value={positionForm.ticker} onChange={(event) => setPositionForm((current) => ({ ...current, ticker: event.target.value }))} placeholder="AAPL" />
                  </label>
                  <label className="field">
                    <span>Shares</span>
                    <input value={positionForm.shares} onChange={(event) => setPositionForm((current) => ({ ...current, shares: event.target.value }))} placeholder="100" />
                  </label>
                  <label className="field">
                    <span>Entry Price</span>
                    <input value={positionForm.entry_price} onChange={(event) => setPositionForm((current) => ({ ...current, entry_price: event.target.value }))} placeholder="182.55" />
                  </label>
                  <label className="field">
                    <span>Opened At</span>
                    <input type="date" value={positionForm.opened_at} onChange={(event) => setPositionForm((current) => ({ ...current, opened_at: event.target.value }))} />
                  </label>
                  <label className="field">
                    <span>Portfolio Name</span>
                    <input value={positionForm.portfolio_name} onChange={(event) => setPositionForm((current) => ({ ...current, portfolio_name: event.target.value }))} placeholder="Main" />
                  </label>
                  <label className="field" style={{ gridColumn: "1 / -1" }}>
                    <span>Notes</span>
                    <textarea value={positionForm.notes} onChange={(event) => setPositionForm((current) => ({ ...current, notes: event.target.value }))} placeholder="Setup context or plan notes" />
                  </label>
                </div>
                <div className="button-row">
                  <button className="primary-button" type="submit" disabled={isSavingPosition}>
                    {isSavingPosition ? "Saving…" : "Add Position"}
                  </button>
                  <button
                    className="ghost-button"
                    type="button"
                    onClick={() => {
                      setPositionForm(EMPTY_POSITION_FORM);
                    }}
                  >
                    Clear
                  </button>
                </div>
              </form>
            </Panel>

            <Panel title="CSV Import">
              <form className="run-toolbar" onSubmit={(event) => void handleImportCsv(event)}>
                <div className="run-params-grid">
                  <label className="field">
                    <span>Source Name</span>
                    <input value={csvSourceName} onChange={(event) => setCsvSourceName(event.target.value)} placeholder="portfolio.csv" />
                  </label>
                  <label className="field">
                    <span>Load File</span>
                    <input type="file" accept=".csv,text/csv" onChange={(event) => void handleCsvFileChange(event)} />
                  </label>
                  <label className="field" style={{ gridColumn: "1 / -1" }}>
                    <span>CSV Content</span>
                    <textarea
                      value={csvText}
                      onChange={(event) => setCsvText(event.target.value)}
                      placeholder="ticker,shares,entry_price,opened_at,notes,portfolio_name"
                      className="portfolio-csv-input"
                    />
                  </label>
                </div>
                <div className="button-row">
                  <button className="primary-button" type="submit" disabled={isImporting}>
                    {isImporting ? "Importing…" : "Import CSV"}
                  </button>
                </div>
              </form>
              {importResult ? (
                <div className="detail-subsection">
                  <div className="detail-card">
                    <div className="summary-stat">
                      Accepted {importResult.accepted_count} row(s), errors {importResult.error_count}
                    </div>
                    {importResult.accepted.length > 0 ? (
                      <div className="pill-list">
                        {importResult.accepted.map((item) => (
                          <span key={`${item.row}-${item.position.ticker}`} className="symbol-pill">
                            row {item.row}: {item.position.ticker}
                          </span>
                        ))}
                      </div>
                    ) : null}
                    {importResult.errors.length > 0 ? (
                      <div className="range-list">
                        {importResult.errors.map((item) => (
                          <div key={`${item.row}-${item.message}`} className="range-item">
                            <span>row {item.row}</span>
                            <span>{item.message}</span>
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                </div>
              ) : null}
            </Panel>
          </div>

          <Panel title={selectedPosition ? `${selectedPosition.ticker} Advice` : "Advice Detail"}>
            {!selectedPosition ? (
              <LoadingBlock label="Select a position to inspect stop, target, and signal detail." compact />
            ) : (
              <div className="portfolio-advice-layout">
                <div className="portfolio-advice-metrics">
                  <AdviceMetric label="Signal" value={humanizeSignal(selectedPosition.advice.signal_status)} status={selectedPosition.advice.signal_status} />
                  <AdviceMetric
                    label="Position Action"
                    value={humanizePositionAction(selectedPosition.advice.position_action?.action)}
                    status={normalizePositionActionClass(selectedPosition.advice.position_action?.action)}
                  />
                  <AdviceMetric label="Close" value={formatCurrency(selectedPosition.advice.close_price)} />
                  <AdviceMetric label="Current Shares" value={formatNumber(selectedPosition.shares)} />
                  <AdviceMetric label="Average Cost" value={formatCurrency(selectedPosition.entry_price)} />
                  <AdviceMetric label="Stop Loss" value={formatCurrency(selectedPosition.advice.stop_loss_price)} />
                  <AdviceMetric
                    label="Take Profit"
                    value={`${formatCurrency(selectedPosition.advice.tp1_price)} / ${formatCurrency(selectedPosition.advice.tp2_price)}`}
                  />
                  <AdviceMetric label="Net Cost After TP1" value={formatCurrency(selectedPosition.advice.net_cost_after_tp1)} />
                  <AdviceMetric label="Average-Up Trigger" value={formatCurrency(selectedPosition.advice.average_up_price)} />
                  <AdviceMetric label="Blended Entry After Add" value={formatCurrency(selectedPosition.advice.blended_entry_after_average_up)} />
                  <AdviceMetric label="Remaining Cost Basis" value={formatCurrency(selectedPosition.advice.remaining_cost_basis_after_tp1)} />
                  <AdviceMetric label="Realized P/L" value={formatSignedCurrency(selectedPosition.realized_pl)} />
                </div>
                <div className="detail-card">
                  <div className="detail-card-head">
                    <div>
                      <div className="summary-stat">{selectedPosition.ticker} recommendation</div>
                      <p className="panel-copy">{selectedPosition.advice.explanation || "No explanation available yet."}</p>
                      {selectedPosition.advice.position_action?.reason_summary ? (
                        <p className="panel-copy">
                          Position Action: {selectedPosition.advice.position_action.reason_summary}
                        </p>
                      ) : null}
                    </div>
                    <span className={`portfolio-data-badge is-${selectedPosition.advice.market_data_status}`}>
                      {selectedPosition.advice.market_data_status.toUpperCase()}
                    </span>
                  </div>
                  <div className="detail-subsection">
                    <div className="range-item">
                      <span>Advice Date</span>
                      <span>{formatLocalDate(selectedPosition.advice.as_of_date)}</span>
                    </div>
                    <div className="range-item">
                      <span>Latest Trade Date</span>
                      <span>{formatLocalDate(selectedPosition.advice.latest_trade_date)}</span>
                    </div>
                    <div className="range-item">
                      <span>Refresh Timestamp</span>
                      <span>{formatLocalDateTime(selectedPosition.advice.refreshed_at)}</span>
                    </div>
                    <div className="range-item">
                      <span>Current Holding</span>
                      <span>
                        {formatNumber(selectedPosition.shares)} shares @ {formatCurrency(selectedPosition.entry_price)}
                      </span>
                    </div>
                    <div className="range-item">
                      <span>Take Profit Tranches</span>
                      <span>
                        {formatFraction(selectedPosition.advice.tp1_sell_fraction)} @ TP1, {formatFraction(selectedPosition.advice.tp2_sell_fraction)} @ TP2
                      </span>
                    </div>
                    <div className="range-item">
                      <span>Average-Up Scenario</span>
                      <span>
                        Add {formatFraction(selectedPosition.advice.average_up_share_fraction)} near {formatCurrency(selectedPosition.advice.average_up_price)}
                      </span>
                    </div>
                    {selectedPosition.advice.position_action ? (
                      <>
                        <div className="range-item">
                          <span>Position Action Snapshot</span>
                          <span>
                            {humanizePositionAction(selectedPosition.advice.position_action.action)} ({formatScore(selectedPosition.advice.position_action.action_score)})
                          </span>
                        </div>
                        <div className="range-item">
                          <span>Decision Layer</span>
                          <span>
                            Trend {humanizePositionTrend(selectedPosition.advice.position_action.trend_state)}, extension {humanizePositionExtension(selectedPosition.advice.position_action.extension_state)}, danger {selectedPosition.advice.position_action.danger_signal_count}
                          </span>
                        </div>
                        <div className="range-item">
                          <span>Decision As Of</span>
                          <span>{formatLocalDate(selectedPosition.advice.position_action.as_of_date)}</span>
                        </div>
                      </>
                    ) : null}
                    {selectedPosition.notes ? (
                      <div className="range-item">
                        <span>Notes</span>
                        <span>{selectedPosition.notes}</span>
                      </div>
                    ) : null}
                  </div>
                </div>
              </div>
            )}
          </Panel>

          <div className="portfolio-card-grid">
            <Panel title={selectedPosition ? `${selectedPosition.ticker} Buy / Sell` : "Buy / Sell"}>
              {!selectedPosition ? (
                <LoadingBlock label="Select a position to record a buy or sell transaction." compact />
              ) : (
                <form className="run-toolbar" onSubmit={(event) => void handleSubmitTransaction(event)}>
                  <div className="run-params-grid">
                    <label className="field">
                      <span>Action</span>
                      <select value={transactionForm.side} onChange={(event) => setTransactionForm((current) => ({ ...current, side: event.target.value }))}>
                        <option value="buy">Buy More</option>
                        <option value="sell">Sell Shares</option>
                      </select>
                    </label>
                    <label className="field">
                      <span>Shares</span>
                      <input value={transactionForm.shares} onChange={(event) => setTransactionForm((current) => ({ ...current, shares: event.target.value }))} placeholder="25" />
                    </label>
                    <label className="field">
                      <span>Price</span>
                      <input value={transactionForm.price} onChange={(event) => setTransactionForm((current) => ({ ...current, price: event.target.value }))} placeholder="195.40" />
                    </label>
                    <label className="field">
                      <span>Trade Date</span>
                      <input type="date" value={transactionForm.trade_date} onChange={(event) => setTransactionForm((current) => ({ ...current, trade_date: event.target.value }))} />
                    </label>
                    <label className="field">
                      <span>Fees</span>
                      <input value={transactionForm.fees} onChange={(event) => setTransactionForm((current) => ({ ...current, fees: event.target.value }))} placeholder="0" />
                    </label>
                    <label className="field" style={{ gridColumn: "1 / -1" }}>
                      <span>Notes</span>
                      <textarea value={transactionForm.notes} onChange={(event) => setTransactionForm((current) => ({ ...current, notes: event.target.value }))} placeholder="Reason for add or trim" />
                    </label>
                  </div>
                  <div className="button-row">
                    <button className="primary-button" type="submit" disabled={isSavingPosition}>
                      {isSavingPosition ? "Saving…" : transactionForm.side === "sell" ? "Record Sell" : "Record Buy"}
                    </button>
                  </div>
                </form>
              )}
            </Panel>

            <Panel title={selectedPosition ? `${selectedPosition.ticker} Transaction History` : "Transaction History"}>
              {!selectedPosition ? (
                <LoadingBlock label="Select a position to inspect transaction history." compact />
              ) : selectedPosition.transactions.length <= 1 ? (
                <LoadingBlock label="Only the initial buy is recorded so far. Add later buys or sells here." compact />
              ) : (
                <div className="data-table-responsive">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Date</th>
                        <th>Side</th>
                        <th>Shares</th>
                        <th>Price</th>
                        <th>Fees</th>
                        <th>Notes</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedPosition.transactions.map((transaction) => (
                        <tr key={transaction.id}>
                          <td data-label="Date">{formatLocalDate(transaction.trade_date)}</td>
                          <td data-label="Side">
                            <span className={`portfolio-signal-pill is-${transaction.side === "sell" ? "trim" : "hold"}`}>{transaction.side}</span>
                          </td>
                          <td data-label="Shares">{formatNumber(transaction.shares)}</td>
                          <td data-label="Price">{formatCurrency(transaction.price)}</td>
                          <td data-label="Fees">{formatCurrency(transaction.fees)}</td>
                          <td data-label="Notes">{transaction.notes || "-"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Panel>
          </div>
        </div>

        <div className="page-grid portfolio-rail">
          <Panel title="Portfolio Mix" aside={<span className="eyebrow">{positionsByPortfolio.size || 0} buckets</span>}>
            {Array.from(positionsByPortfolio.entries()).length === 0 ? (
              <LoadingBlock label="Positions will group by portfolio here after you add them." compact />
            ) : (
              <div className="portfolio-mix-list">
                {Array.from(positionsByPortfolio.entries()).map(([name, positions]) => (
                  <div key={name} className="range-item">
                    <span>{name}</span>
                    <span>{positions.length} tickers</span>
                  </div>
                ))}
              </div>
            )}
          </Panel>

          <Panel title={context.market_regime.title} aside={<span className="eyebrow">{context.market_regime.status}</span>}>
            <div className="portfolio-placeholder">
              <div className="portfolio-placeholder-chart" />
              <p className="panel-copy">{context.market_regime.description}</p>
            </div>
          </Panel>
        </div>
      </div>
    </div>
  );
}

function MetricCard({
  label,
  value,
  meta,
  accent = "neutral",
}: {
  label: string;
  value: string;
  meta: string;
  accent?: "up" | "down" | "neutral";
}) {
  return (
    <article className={`metric-card portfolio-metric-card accent-${accent === "down" ? "neutral" : accent}`}>
      <div className="metric-card-head">
        <h3>{label}</h3>
        <span className={`accent-mark accent-${accent === "down" ? "neutral" : accent}`} />
      </div>
      <div className="metric-value portfolio-metric-value">{value}</div>
      <p className="card-meta">{meta}</p>
    </article>
  );
}

function AdviceMetric({
  label,
  value,
  status,
}: {
  label: string;
  value: string;
  status?: string;
}) {
  return (
    <div className={`detail-card portfolio-advice-metric${status ? ` is-${normalizeSignalClass(status)}` : ""}`}>
      <div className="eyebrow">{label}</div>
      <div className="summary-stat">{value}</div>
    </div>
  );
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatSignedCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  const amount = formatCurrency(Math.abs(value));
  return `${value >= 0 ? "+" : "-"}${amount}`;
}

function formatSignedPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value);
}

function humanizeSignal(value: string): string {
  switch (value) {
    case "hold":
      return "Hold";
    case "trim":
      return "Trim";
    case "raise_stop":
      return "Raise Stop";
    case "review":
      return "Review";
    default:
      return value.replace(/_/g, " ");
  }
}

function humanizePositionAction(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  if (value === "add_position") {
    return "Add Position";
  }
  if (value === "hold_position") {
    return "Hold";
  }
  if (value === "trim_reduce") {
    return "Trim / Reduce";
  }
  if (value === "avoid_new") {
    return "Avoid New";
  }
  return value.replace(/_/g, " ");
}

function normalizePositionActionClass(value: string | null | undefined): string {
  if (value === "add_position") {
    return "hold";
  }
  if (value === "hold_position") {
    return "raise_stop";
  }
  if (value === "trim_reduce") {
    return "trim";
  }
  return "review";
}

function humanizePositionTrend(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  if (value === "healthy") {
    return "Healthy";
  }
  if (value === "weakening") {
    return "Weakening";
  }
  if (value === "broken") {
    return "Broken";
  }
  return value.replace(/_/g, " ");
}

function humanizePositionExtension(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  if (value === "normal") {
    return "Normal";
  }
  if (value === "stretched") {
    return "Stretched";
  }
  if (value === "extreme") {
    return "Extreme";
  }
  return value.replace(/_/g, " ");
}

function formatScore(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(1);
}

function normalizeSignalClass(value: string): string {
  if (value === "hold" || value === "trim" || value === "raise_stop" || value === "review") {
    return value;
  }
  return "review";
}

function formatFraction(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${Math.round(value * 100)}%`;
}
