import { useMemo, useState } from "react";
import { Panel } from "../components/Panel";
import { PriceChart } from "../components/PriceChart";
import { chartData, watchlistFiles, watchlistTickers } from "../lib/mock-data";

export function WatchlistsPage() {
  const [selectedTicker, setSelectedTicker] = useState(watchlistTickers[0]);
  const positive = selectedTicker.dailyChangePct >= 0;
  const indicatorTone = positive ? "positive" : "negative";
  const smallChartData = useMemo(() => chartData, []);

  return (
    <div className="watchlists-layout">
      <aside className="panel files-pane">
        <div className="panel-head">
          <h2>Files (JSON)</h2>
        </div>
        <div className="file-list">
          {watchlistFiles.map((file, index) => (
            <div key={file.stem} className={`file-row${index === 0 ? " is-selected" : ""}`}>
              <div className="file-name">{file.label}</div>
              <div className="file-meta">{file.dateLabel}</div>
            </div>
          ))}
        </div>

        <div className="panel-head inline-head">
          <h2>Ticker List ({watchlistTickers.length})</h2>
        </div>
        <div className="ticker-list">
          {watchlistTickers.map((item) => (
            <button
              key={item.ticker}
              className={`ticker-row${selectedTicker.ticker === item.ticker ? " is-selected" : ""}`}
              onClick={() => setSelectedTicker(item)}
              type="button"
            >
              <div>
                <div className="ticker-symbol">{item.ticker}</div>
                <div className="ticker-company">{item.company}</div>
                <div className="ticker-tag">
                  {item.scoreLabel}: {item.score}
                </div>
              </div>
              <div className="ticker-side">
                <div className="ticker-price">{item.lastPrice.toFixed(2)}</div>
                <div className={`ticker-change ${item.dailyChangePct >= 0 ? "up" : "down"}`}>
                  {item.dailyChangePct >= 0 ? "+" : ""}
                  {item.dailyChangePct.toFixed(2)}%
                </div>
              </div>
            </button>
          ))}
        </div>
      </aside>

      <div className="watchlists-main">
        <section className="hero-strip">
          <div>
            <div className="hero-symbol-row">
              <h1>{selectedTicker.ticker}</h1>
              <span className="ticker-exchange">NASDAQGS</span>
              <span className="ticker-company-inline">{selectedTicker.company}</span>
            </div>
            <div className="hero-price-row">
              <span className="hero-price">{selectedTicker.lastPrice.toFixed(2)}</span>
              <span className={`hero-change ${indicatorTone}`}>
                {selectedTicker.dailyChangePct >= 0 ? "+" : ""}
                {(selectedTicker.lastPrice * selectedTicker.dailyChangePct / 100).toFixed(2)} ({selectedTicker.dailyChangePct.toFixed(2)}%)
              </span>
            </div>
          </div>
          <div className="hero-stats">
            <div>
              <span className="eyebrow">Industry</span>
              <strong>{selectedTicker.industry}</strong>
            </div>
            <div>
              <span className="eyebrow">Score</span>
              <strong>{selectedTicker.score}</strong>
            </div>
            <div>
              <span className="eyebrow">Mode</span>
              <strong>1D</strong>
            </div>
          </div>
        </section>

        <Panel title="Candles" aside={<div className="legend-row"><span>MA (20)</span><span>MA (50)</span><span>MA (200)</span></div>}>
          <PriceChart ticker={selectedTicker.ticker} candles={smallChartData} />
        </Panel>

        <section className="tab-strip">
          <button className="tab-button is-active">Summary</button>
          <button className="tab-button">Financials</button>
          <button className="tab-button">News</button>
          <button className="tab-button">Analyst Estimates</button>
        </section>

        <div className="summary-grid">
          <Panel title="Growth Trend">
            <div className="summary-stat positive">Accelerating</div>
            <p className="panel-copy">{selectedTicker.summary}</p>
          </Panel>
          <Panel title="RS Rank">
            <div className="big-number">{selectedTicker.score}</div>
          </Panel>
          <Panel title="Group Rank">
            <div className="big-number">1 / 197</div>
          </Panel>
        </div>
      </div>
    </div>
  );
}
