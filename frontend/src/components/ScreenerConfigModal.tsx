import { useEffect, useState } from "react";
import type { RunAction } from "../lib/types";
import type { RunPrecheckResponse } from "../lib/types";
import { fetchJson } from "../lib/api";
import "./ScreenerConfigModal.css";

interface ScreenerConfigModalProps {
  action: RunAction | null;
  isOpen: boolean;
  onClose: () => void;
  onSubmit: (params: Record<string, string | string[]>) => Promise<void>;
  isLoading: boolean;
}

export function ScreenerConfigModal({
  action,
  isOpen,
  onClose,
  onSubmit,
  isLoading,
}: ScreenerConfigModalProps) {
  const [fieldValues, setFieldValues] = useState<Record<string, string | string[]>>({});
  const [precheck, setPrecheck] = useState<RunPrecheckResponse | null>(null);
  const [isLoadingPrecheck, setIsLoadingPrecheck] = useState(false);

  useEffect(() => {
    if (!isOpen || !action) {
      setIsLoadingPrecheck(false);
      return;
    }
    const timer = window.setTimeout(() => {
      setIsLoadingPrecheck(true);
      void fetchJson<RunPrecheckResponse>(`/api/runs/${action.id}/precheck`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(fieldValues),
      })
        .then((result) => setPrecheck(result))
        .catch(() =>
          setPrecheck({
            applicable: false,
            configured: false,
            action_id: action.id,
            market_data_source: String(fieldValues.market_data_source || "internet"),
            message: "Unable to load DB coverage precheck.",
          }),
        )
        .finally(() => setIsLoadingPrecheck(false));
    }, 300);
    return () => window.clearTimeout(timer);
  }, [action, fieldValues, isOpen]);

  if (!isOpen || !action) {
    return null;
  }

  const handleFieldChange = (fieldId: string, value: string | string[]) => {
    setFieldValues((current) => ({
      ...current,
      [fieldId]: value,
    }));
  };

  const handleSubmit = async () => {
    await onSubmit(fieldValues);
    setFieldValues({});
    onClose();
  };

  const handleCancel = () => {
    setFieldValues({});
    setPrecheck(null);
    onClose();
  };

  return (
    <div className="modal-overlay" onClick={handleCancel}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>{action.label} Configuration</h2>
          <button className="modal-close" onClick={handleCancel} type="button">
            ✕
          </button>
        </div>

        <div className="modal-body">
          <p className="modal-description">Define parameters for {action.label.toLowerCase()} detection.</p>

          <div className="detail-card" style={{ marginBottom: 18 }}>
            <div className="detail-card-head">
              <div>
                <div className="ticker-symbol">DB Precheck</div>
                <div className="file-meta">Estimate DB-ready vs fallback-needed tickers before launch.</div>
              </div>
              <span className="eyebrow">{isLoadingPrecheck ? "checking" : precheck?.applicable ? "ready" : "n/a"}</span>
            </div>
            {isLoadingPrecheck ? (
              <p className="panel-copy">Checking DB coverage…</p>
            ) : precheck?.applicable ? (
              <>
                <div className="detail-grid">
                  <div>
                    <div className="eyebrow">DB Ready</div>
                    <div className="panel-copy">
                      {precheck.db_ready_tickers ?? 0} / {precheck.total_tickers ?? 0} ({precheck.db_ready_pct ?? 0}%)
                    </div>
                  </div>
                  <div>
                    <div className="eyebrow">Fallback Needed</div>
                    <div className="panel-copy">{precheck.fallback_tickers ?? 0}</div>
                  </div>
                </div>
                <div className="detail-grid">
                  <div>
                    <div className="eyebrow">As Of Date</div>
                    <div className="panel-copy">{precheck.as_of_date || "-"}</div>
                  </div>
                  <div>
                    <div className="eyebrow">Lookback Days</div>
                    <div className="panel-copy">{precheck.lookback_trading_days ?? "-"}</div>
                  </div>
                </div>
                {precheck.benchmark?.required ? (
                  <div className="detail-subsection">
                    <div className="eyebrow">Benchmark</div>
                    <div className="panel-copy">
                      {precheck.benchmark.ticker}: {precheck.benchmark.db_ready ? "DB ready" : "needs fallback"}
                    </div>
                  </div>
                ) : null}
                {(precheck.sample_fallback_tickers ?? []).length > 0 ? (
                  <div className="detail-subsection">
                    <div className="eyebrow">Sample Fallback Tickers</div>
                    <div className="panel-copy">{(precheck.sample_fallback_tickers ?? []).join(", ")}</div>
                  </div>
                ) : null}
              </>
            ) : (
              <p className="panel-copy">{precheck?.message || "DB precheck is not applicable for the current settings."}</p>
            )}
          </div>

          <div className="modal-fields">
            {action.fields.map((field: typeof action.fields[0]) => (
              <label className="modal-field" key={field.id}>
                <div className="field-header">
                  <span className="field-label">{field.label}</span>
                  {field.help_text && <span className="field-help-text">{field.help_text}</span>}
                </div>

                {field.type === "select" ? (
                  <select
                    value={typeof fieldValues[field.id] === "string" ? fieldValues[field.id] : ""}
                    onChange={(e) => handleFieldChange(field.id, e.target.value)}
                    className="modal-select"
                  >
                    <option value="">-- Select {field.label.toLowerCase()} --</option>
                    {field.options.map((option: { value: string; label: string }) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                ) : field.type === "multiselect" ? (
                  <select
                    multiple
                    value={Array.isArray(fieldValues[field.id]) ? fieldValues[field.id] : []}
                    onChange={(e) =>
                      handleFieldChange(
                        field.id,
                        Array.from(e.target.selectedOptions).map((opt) => opt.value),
                      )
                    }
                    className="modal-select modal-multiselect"
                  >
                    {field.options.map((option: { value: string; label: string }) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type={field.type}
                    placeholder={field.placeholder ?? undefined}
                    value={typeof fieldValues[field.id] === "string" ? fieldValues[field.id] : ""}
                    onChange={(e) => handleFieldChange(field.id, e.target.value)}
                    className="modal-input"
                  />
                )}
              </label>
            ))}
          </div>
        </div>

        <div className="modal-footer">
          <button className="modal-button modal-button-cancel" onClick={handleCancel} type="button" disabled={isLoading}>
            CANCEL
          </button>
          <button
            className="modal-button modal-button-primary"
            onClick={() => void handleSubmit()}
            type="button"
            disabled={isLoading}
          >
            {isLoading ? "RUNNING..." : "RUN SCREENER"}
          </button>
        </div>
      </div>
    </div>
  );
}
