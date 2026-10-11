import { useEffect, useRef, useState, type FormEvent } from "react";
import type { RunAction } from "../lib/types";
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
  const [executionMode, setExecutionMode] = useState<RunAction["default_execution_mode"]>("local");
  const [targetWorker, setTargetWorker] = useState("");
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    if (!isOpen || !action) return;
    const previousFocus = document.activeElement as HTMLElement | null;
    const dialog = dialogRef.current;
    setExecutionMode(action.default_execution_mode);
    setTargetWorker("");
    if (dialog && !dialog.open) dialog.showModal();
    return () => {
      if (dialog?.open) dialog.close();
      previousFocus?.focus();
    };
  }, [action, isOpen]);

  if (!isOpen || !action) {
    return null;
  }

  const handleFieldChange = (fieldId: string, value: string | string[]) => {
    setFieldValues((current) => ({
      ...current,
      [fieldId]: value,
    }));
  };

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    await onSubmit({
      ...fieldValues,
      execution_mode: executionMode,
      ...(executionMode === "remote" && targetWorker.trim() ? { target_worker: targetWorker.trim() } : {}),
    });
    setFieldValues({});
    onClose();
  };

  const handleCancel = () => {
    setFieldValues({});
    onClose();
  };

  return (
    <dialog
      ref={dialogRef}
      className="modal-overlay"
      aria-labelledby="screener-config-title"
      onCancel={(event) => {
        event.preventDefault();
        handleCancel();
      }}
      onClick={(event) => {
        if (event.target === event.currentTarget) handleCancel();
      }}
    >
      <form className="modal-content" onSubmit={(event) => void handleSubmit(event)}>
        <div className="modal-header">
          <h2 id="screener-config-title">{action.label} Configuration</h2>
          <button className="modal-close" onClick={handleCancel} type="button" aria-label="Close screener configuration">
            ✕
          </button>
        </div>

        <div className="modal-body">
          <p className="modal-description">Define parameters for {action.label.toLowerCase()} detection.</p>

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
                  <>
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
                    <span className="field-help-text">Hold Cmd or Ctrl to select multiple.</span>
                  </>
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
            <label className="modal-field">
              <div className="field-header">
                <span className="field-label">Run Mode</span>
                <span className="field-help-text">Remote runs are resilient to web deployments. GitHub Actions is available only for supported screeners.</span>
              </div>
              <select value={executionMode} onChange={(event) => setExecutionMode(event.target.value as RunAction["default_execution_mode"])} className="modal-select">
                {action.execution_modes.map((mode) => (
                  <option key={mode} value={mode}>
                    {mode === "local" ? "Local Web Container" : mode === "remote" ? "Remote Worker Queue" : "GitHub Actions"}
                  </option>
                ))}
              </select>
            </label>
            {executionMode === "remote" ? (
              <label className="modal-field">
                <div className="field-header">
                  <span className="field-label">Target Worker</span>
                  <span className="field-help-text">Optional. Leave blank to let any available worker claim the job.</span>
                </div>
                <input
                  type="text"
                  placeholder="worker-a"
                  value={targetWorker}
                  onChange={(event) => setTargetWorker(event.target.value)}
                  className="modal-input"
                />
              </label>
            ) : null}
          </div>
        </div>

        <div className="modal-footer">
          <button className="modal-button modal-button-cancel" onClick={handleCancel} type="button" disabled={isLoading}>
            CANCEL
          </button>
          <button
            className="modal-button modal-button-primary"
            type="submit"
            disabled={isLoading}
          >
            {isLoading ? "RUNNING…" : "RUN SCREENER"}
          </button>
        </div>
      </form>
    </dialog>
  );
}
