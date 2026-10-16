import { useEffect, useMemo, useState } from "react";
import { Panel } from "../components/Panel";
import { calculatePositionSize } from "../lib/positionSizing";

const STORAGE_KEY = "ticker-screener:position-calculator-defaults";
type Fields = { accountValue: string; riskPercent: string; maxAllocationPercent: string; direction: "long" | "short"; entryPrice: string; stopPrice: string; slippagePerShare: string };
const INITIAL: Fields = { accountValue: "100000", riskPercent: "0.5", maxAllocationPercent: "20", direction: "long", entryPrice: "", stopPrice: "", slippagePerShare: "0" };
const RISK_PRESETS = [0.3, 0.5, 0.7, 1];
const money = (value: number) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
const sharesFormat = (value: number) => new Intl.NumberFormat("en-US", { maximumFractionDigits: 4 }).format(value);

export function PositionCalculatorPage() {
  const [fields, setFields] = useState(INITIAL);
  const [remember, setRemember] = useState(false);
  const [sizeMultiplier, setSizeMultiplier] = useState(1);
  const [wholeShares, setWholeShares] = useState(true);

  useEffect(() => {
    try {
      const saved = window.localStorage.getItem(STORAGE_KEY);
      if (!saved) return;
      const defaults = JSON.parse(saved) as Pick<Fields, "accountValue" | "riskPercent" | "maxAllocationPercent">;
      if ([defaults.accountValue, defaults.riskPercent, defaults.maxAllocationPercent].every((value) => typeof value === "string")) {
        setFields((current) => ({ ...current, ...defaults }));
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
          <div className="risk-result-lead"><span>Position value</span><strong>{money(result.positionValue)}</strong><small>{sharesFormat(result.shares)} shares at {money(Number(fields.entryPrice))}</small></div>
          {result.allocationCapHit ? <p className="risk-cap-note" role="status">Allocation cap applied. The position is limited to {fields.maxAllocationPercent}% of account value.</p> : null}
          <dl className="risk-result-list">
            <div><dt>Planned loss at stop (1R)</dt><dd>{money(result.plannedLoss)}</dd></div>
            <div><dt>Risk budget</dt><dd>{money(result.riskBudget)}</dd></div>
            <div><dt>Unused risk after rounding</dt><dd>{money(result.unusedRisk)}</dd></div>
            <div><dt>Theoretical position</dt><dd>{money(result.theoreticalPositionValue)} <span>({sharesFormat(result.theoreticalShares)} shares)</span></dd></div>
            <div><dt>Stop distance</dt><dd>{money(result.stopDistance)} <span>({result.stopDistancePercent.toFixed(2)}%)</span></dd></div>
            <div><dt>Account allocation</dt><dd>{result.allocationPercent.toFixed(2)}%</dd></div>
          </dl>
          <div className="risk-targets"><div><span>2R target</span><strong>{money(result.target2R)}</strong></div><div><span>3R target</span><strong>{money(result.target3R)}</strong></div></div>
        </> : <p className="risk-empty" role="status">{inputError}</p>}
        <p className="risk-disclaimer">This is planned risk based on the entered stop and slippage estimate. Gaps and execution slippage can exceed it.</p>
      </Panel>
    </div>
  </div>;
}
