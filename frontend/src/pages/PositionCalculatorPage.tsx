import { useEffect, useMemo, useState } from "react";
import { Panel } from "../components/Panel";
import { calculatePositionSize } from "../lib/positionSizing";
import { calculateTradeManagement, type TradeManagementPreset } from "../lib/tradeManagement";
import { resolveSizingOrigin } from "../lib/tradeReadinessPolicy";
import { useSearchParams } from "react-router-dom";

const STORAGE_KEY = "ticker-screener:position-calculator-defaults";
type Fields = { accountValue: string; riskPercent: string; maxAllocationPercent: string; direction: "long" | "short"; entryPrice: string; stopPrice: string; slippagePerShare: string };
const INITIAL: Fields = { accountValue: "100000", riskPercent: "0.5", maxAllocationPercent: "20", direction: "long", entryPrice: "", stopPrice: "", slippagePerShare: "0" };
const RISK_PRESETS = [0.3, 0.5, 0.7, 1];
const money = (value: number) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
const sharesFormat = (value: number) => new Intl.NumberFormat("en-US", { maximumFractionDigits: 4 }).format(value);
const validPrice = (value: string | null) => value && /^\d+(?:\.\d+)?$/.test(value) && Number.isFinite(Number(value)) && Number(value) > 0 ? value : "";
const validTicker = (value: string | null) => value && /^[A-Z0-9][A-Z0-9.-]{0,9}$/.test(value) ? value : "";
const validDate = (value: string | null) => {
  if (!value || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return "";
  const [year, month, day] = value.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day ? value : "";
};
const formatShares = (value: number | { min: number; max: number } | undefined) => value === undefined ? "" : typeof value === "number" ? `${value} shares` : value.min === value.max ? `${value.min} shares` : `${value.min}–${value.max} shares`;

export function PositionCalculatorPage() {
  const [searchParams] = useSearchParams();
  const context = useMemo(() => {
    const origin = resolveSizingOrigin(searchParams);
    const ticker = validTicker(searchParams.get("ticker")?.toUpperCase() ?? null);
    const entry = validPrice(origin.entry);
    const stop = validPrice(origin.stop);
    const maxEntry = validPrice(origin.maxEntry);
    const signalDate = validDate(origin.signalDate);
    const snapshotDate = validDate(origin.snapshotDate);
    const management = searchParams.get("management");
    return { ...origin, source: origin.source ? "top-hits" : "", ticker, entry, stop, maxEntry, signalDate, snapshotDate, management: management === "balanced_daily" || management === "martin" ? management as TradeManagementPreset : "martin" as TradeManagementPreset };
  }, [searchParams]);
  const [fields, setFields] = useState(() => ({ ...INITIAL, direction: "long" as const, entryPrice: context.entry, stopPrice: context.stop }));
  const [remember, setRemember] = useState(false);
  const [sizeMultiplier, setSizeMultiplier] = useState(1);
  const [wholeShares, setWholeShares] = useState(true);
  const [managementPreset, setManagementPreset] = useState<TradeManagementPreset>(context.management);

  useEffect(() => {
    try {
      const saved = window.localStorage.getItem(STORAGE_KEY);
      if (!saved) return;
      const defaults = JSON.parse(saved) as Partial<Pick<Fields, "accountValue" | "riskPercent" | "maxAllocationPercent">>;
      if ([defaults.accountValue, defaults.riskPercent, defaults.maxAllocationPercent].every((value) => typeof value === "string")) {
        setFields((current) => ({ ...current, accountValue: defaults.accountValue!, riskPercent: defaults.riskPercent!, maxAllocationPercent: defaults.maxAllocationPercent! }));
        setRemember(true);
      }
    } catch {
      window.localStorage.removeItem(STORAGE_KEY);
    }
  }, []);

  const result = useMemo(() => {
    try {
      if (!fields.entryPrice || !fields.stopPrice) return null;
      return calculatePositionSize({
        accountValue: Number(fields.accountValue), riskPercent: Number(fields.riskPercent), direction: fields.direction,
        entryPrice: Number(fields.entryPrice), stopPrice: Number(fields.stopPrice), maxAllocationPercent: Number(fields.maxAllocationPercent),
        slippagePerShare: Number(fields.slippagePerShare), sizeMultiplier, wholeShares,
      });
    } catch {
      return null;
    }
  }, [fields, sizeMultiplier, wholeShares]);

  const inputError = useMemo(() => {
    if (!fields.entryPrice || !fields.stopPrice) return "Enter an entry and stop to calculate size.";
    try {
      calculatePositionSize({
        accountValue: Number(fields.accountValue), riskPercent: Number(fields.riskPercent), direction: fields.direction,
        entryPrice: Number(fields.entryPrice), stopPrice: Number(fields.stopPrice), maxAllocationPercent: Number(fields.maxAllocationPercent),
        slippagePerShare: Number(fields.slippagePerShare), sizeMultiplier, wholeShares,
      });
      return "";
    } catch (error) { return error instanceof Error ? error.message : "Check the entered values."; }
  }, [fields, sizeMultiplier, wholeShares]);

  const overMaxEntry = Boolean(context.maxEntry && Number(fields.entryPrice) > Number(context.maxEntry));
  const adjustedSourceLevels = Boolean(context.source && context.verifiedPolicy && (fields.entryPrice !== context.entry || fields.stopPrice !== context.stop || fields.direction !== "long"));
  const management = result ? calculateTradeManagement(Number(fields.entryPrice), Number(fields.stopPrice), managementPreset, wholeShares ? result.shares : undefined, fields.direction) : null;

  const update = (key: keyof Fields, value: string) => {
    setFields((current) => ({ ...current, [key]: value }));
    if (remember && ["accountValue", "riskPercent", "maxAllocationPercent"].includes(key)) {
      const next = { ...fields, [key]: value };
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ accountValue: next.accountValue, riskPercent: next.riskPercent, maxAllocationPercent: next.maxAllocationPercent }));
    }
  };

  const toggleRemember = (checked: boolean) => {
    setRemember(checked);
    if (checked) window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ accountValue: fields.accountValue, riskPercent: fields.riskPercent, maxAllocationPercent: fields.maxAllocationPercent }));
    else window.localStorage.removeItem(STORAGE_KEY);
  };

  const field = (label: string, key: keyof Fields, options: { min?: string; max?: string; step?: string; suffix?: string } = {}) => (
    <label className="risk-field" key={key}>
      <span>{label}</span>
      <span className="risk-input-wrap"><input type="number" inputMode="decimal" min={options.min ?? "0"} max={options.max} step={options.step ?? "any"} value={fields[key]} onChange={(event) => update(key, event.target.value)} aria-label={label} />{options.suffix ? <span className="risk-input-suffix">{options.suffix}</span> : null}</span>
    </label>
  );

  return <div className="position-calculator-page">
    <header className="risk-page-heading"><h1>Position calculator</h1><p>Size a trade from your account risk, stop distance, and allocation limit.</p></header>
    {context.source ? <section className="risk-context" aria-label="Top Hits trade plan"><strong>{context.ticker || "Top Hits"} · Planned trade context</strong><span>Origin policy: {context.policyLabel}{context.snapshotDate ? ` · Snapshot ${context.snapshotDate}` : ""}</span>{!context.verifiedPolicy ? <span role="alert">Origin policy is unrecognized. Imported levels were not applied; enter and verify your own levels.</span> : null}<span>Trigger {context.entry ? money(Number(context.entry)) : "Unavailable"} · Stop {context.stop ? money(Number(context.stop)) : "Unavailable"}</span><span>Max entry {context.maxEntry ? money(Number(context.maxEntry)) : "Unavailable"}{context.signalDate ? ` · Signal ${context.signalDate}` : ""} · {context.management === "martin" ? "Martin-like" : "Balanced Daily"} management</span>{!context.entry && !context.stop && context.verifiedPolicy ? <small>No saved levels were imported. Any values you enter are manual planning inputs, not a validated snapshot plan.</small> : adjustedSourceLevels ? <small>Entry, stop, or direction changed from the imported plan; sizing is adjusted.</small> : <small>These are editable planning values. Recalculate and check against your actual intended fill.</small>}</section> : null}
    <div className="risk-layout">
      <Panel title="Trade inputs" className="risk-input-panel">
        <div className="risk-input-grid">
          {field("Account value", "accountValue", { min: "0.01", step: "any", suffix: "USD" })}
          <div className="risk-field"><span id="risk-presets-label">Risk per trade</span><div className="risk-preset-row" role="group" aria-labelledby="risk-presets-label">
            {RISK_PRESETS.map((risk) => <button type="button" key={risk} className={`risk-preset${Number(fields.riskPercent) === risk ? " is-selected" : ""}`} aria-pressed={Number(fields.riskPercent) === risk} onClick={() => update("riskPercent", String(risk))}>{risk.toFixed(1)}%</button>)}
            <label className="risk-custom-input"><span className="visually-hidden">Custom risk percent</span><input type="number" min="0.01" max="100" step="any" value={fields.riskPercent} onChange={(event) => update("riskPercent", event.target.value)} /><span>%</span></label>
          </div></div>
          {field("Max allocation", "maxAllocationPercent", { min: "0.01", max: "100", step: "any", suffix: "%" })}
          <label className="risk-field"><span>Direction</span><select value={fields.direction} onChange={(event) => update("direction", event.target.value as Fields["direction"])}><option value="long">Long</option><option value="short">Short</option></select></label>
          {field("Entry price", "entryPrice", { min: "0.000001", suffix: "USD" })}
          {field("Stop price", "stopPrice", { min: "0.000001", suffix: "USD" })}
          {field("Exit slippage per share", "slippagePerShare", { min: "0", suffix: "USD" })}
          <div className="risk-field"><span id="risk-size-label">Size mode</span><div className="risk-segment" role="group" aria-labelledby="risk-size-label">
            {[{ value: 0.5, label: "Half" }, { value: 2 / 3, label: "⅔" }, { value: 1, label: "Full" }].map((mode) => <button type="button" key={mode.label} className={sizeMultiplier === mode.value ? "is-selected" : ""} aria-pressed={sizeMultiplier === mode.value} onClick={() => setSizeMultiplier(mode.value)}>{mode.label}</button>)}
          </div></div>
          <label className="risk-checkbox-field"><input type="checkbox" checked={wholeShares} onChange={(event) => setWholeShares(event.target.checked)} />Whole shares</label>
        </div>
        <label className="risk-remember"><input type="checkbox" checked={remember} onChange={(event) => toggleRemember(event.target.checked)} /><span><strong>Remember on this device</strong><small>Saves account value, risk, and allocation defaults only.</small></span></label>
      </Panel>
      <Panel title="Planned position" className="risk-result-panel">
        {result ? <>
          {overMaxEntry ? <p className="risk-max-entry-warning" role="alert"><strong>WAIT — ABOVE MAX ENTRY</strong><span>Position math is still shown for review, but this fill is above the original Top Hits plan and that readiness plan is invalid. Do not use this as a buy recommendation.</span></p> : null}
          <div className="risk-result-lead"><span>{overMaxEntry ? "Calculated size · plan invalid" : "Buy up to"}</span><strong>{sharesFormat(result.shares)} shares</strong><small>Position value {money(result.positionValue)} at {money(Number(fields.entryPrice))}</small></div>
          {result.allocationCapHit ? <p className="risk-cap-note" role="status">Allocation cap applied. The position is limited to {fields.maxAllocationPercent}% of account value.</p> : null}
          <dl className="risk-result-list">
            <div><dt>Planned cash loss at stop (includes slippage)</dt><dd>{money(result.plannedLoss)}</dd></div>
            <div><dt>Account risk actually used</dt><dd>{money(result.plannedLoss)} <span>({((result.plannedLoss / Number(fields.accountValue)) * 100).toFixed(2)}% · {money(result.riskBudget)} budget)</span></dd></div>
            <div><dt>Limiting factor</dt><dd>{result.allocationCapHit ? "Allocation limit" : "Risk budget"}</dd></div>
            <div><dt>Unused risk after rounding</dt><dd>{money(result.unusedRisk)}</dd></div>
            <div><dt>Theoretical position</dt><dd>{money(result.theoreticalPositionValue)} <span>({sharesFormat(result.theoreticalShares)} shares)</span></dd></div>
            <div><dt>Stop distance</dt><dd>{money(result.stopDistance)} <span>({result.stopDistancePercent.toFixed(2)}%)</span></dd></div>
            <div><dt>Account allocation</dt><dd>{result.allocationPercent.toFixed(2)}%</dd></div>
          </dl>
          <div className="risk-targets"><div><span>1R target price</span><strong>{money(result.oneRTarget)}</strong></div><div><span>2R target price</span><strong>{money(result.target2R)}</strong></div><div><span>3R target price</span><strong>{money(result.target3R)}</strong></div></div>
          {management ? <section className="risk-management"><strong>{management.preset === "martin" ? "Martin-like" : "Balanced Daily"} management planning</strong>{management.targets.map((target) => <div key={target.label}><span>{target.label} · {target.rMultiple}R · {target.sellPercent === 33 ? "sell 33%" : "trim 10–15%"}</span><b>{money(target.price)}{wholeShares ? ` · ${formatShares(target.shares)}` : ""}</b></div>)}<div><span>Runner · daily EMA9</span><b>{wholeShares ? formatShares(management.runnerShares) : "remainder"}</b></div>{management.trimUnavailable ? <p>A {sharesFormat(result.shares)}-share position is too small for a meaningful 10–15% whole-share trim; no trim shares are suggested.</p> : null}<small>Planning guidance only; no orders are placed. Recalculate targets from your actual fill.</small></section> : null}
        </> : <p className="risk-empty" role="status">{inputError}</p>}
        <p className="risk-disclaimer">This is planned risk based on the entered stop and slippage estimate. Gaps and execution slippage can exceed it.</p>
      </Panel>
    </div>
  </div>;
}
