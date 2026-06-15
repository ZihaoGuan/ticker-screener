import { useState } from "react";
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
