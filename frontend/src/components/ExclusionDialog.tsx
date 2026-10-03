import { useEffect, useRef, useState, type FormEvent } from "react";

type ExclusionDialogProps = {
  isOpen: boolean;
  mode: "add" | "remove";
  ticker: string;
  title: string;
  confirmLabel: string;
  helperText: string;
  reasonOptions?: string[];
  submitting: boolean;
  onClose: () => void;
  onSubmit: (reason: string) => Promise<void>;
};

export function ExclusionDialog({
  isOpen,
  mode,
  ticker,
  title,
  confirmLabel,
  helperText,
  reasonOptions = [],
  submitting,
  onClose,
  onSubmit,
}: ExclusionDialogProps) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const returnFocusRef = useRef<HTMLElement | null>(null);
  const [selectedReason, setSelectedReason] = useState("");
  const [customReason, setCustomReason] = useState("");

  useEffect(() => {
    if (!isOpen) {
      setSelectedReason("");
      setCustomReason("");
    }
  }, [isOpen]);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) {
      return;
    }
    if (isOpen) {
      if (!dialog.open) {
        returnFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
        dialog.showModal();
      }
      closeButtonRef.current?.focus();
      return;
    }
    if (dialog.open) {
      dialog.close();
    }
    const returnFocus = returnFocusRef.current;
    if (returnFocus?.isConnected) {
      returnFocus.focus();
    }
  }, [isOpen]);

  const reason = selectedReason === "__custom__" ? customReason.trim() : selectedReason.trim() || customReason.trim();

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    await onSubmit(reason);
  };

  return (
    <dialog
      ref={dialogRef}
      className="modal-backdrop"
      aria-labelledby="exclusion-dialog-title"
      onCancel={(event) => {
        event.preventDefault();
        if (!submitting) {
          onClose();
        }
      }}
      onClick={(event) => {
        if (event.target === event.currentTarget && !submitting) {
          onClose();
        }
      }}
    >
      <div
        className="modal-shell"
      >
        <form className="modal-content" onSubmit={(event) => void handleSubmit(event)}>
          <div className="modal-header">
            <div>
              <div className="eyebrow">{mode === "add" ? "Add exclusion" : "Remove exclusion"}</div>
              <h2 id="exclusion-dialog-title">{title}</h2>
            </div>
            <button ref={closeButtonRef} className="ghost-button" type="button" onClick={onClose} disabled={submitting}>
              Close
            </button>
          </div>

          <div className="modal-body">
            <div className="detail-card">
              <div className="eyebrow">Ticker</div>
              <div className="ticker-symbol">{ticker}</div>
              <p className="panel-copy">{helperText}</p>
            </div>

            <label className="field">
              <span>Reason</span>
              {mode === "add" && reasonOptions.length > 0 ? (
                <div className="button-row" style={{ marginBottom: 12, flexWrap: "wrap" }}>
                  {reasonOptions.map((option) => (
                    <button
                      key={option}
                      type="button"
                      className={selectedReason === option ? "primary-button" : "ghost-button"}
                      onClick={() => setSelectedReason(option)}
                      disabled={submitting}
                    >
                      {option}
                    </button>
                  ))}
                  <button
                    type="button"
                    className={selectedReason === "__custom__" ? "primary-button" : "ghost-button"}
                    onClick={() => setSelectedReason("__custom__")}
                    disabled={submitting}
                  >
                    Custom
                  </button>
                </div>
              ) : null}
              {mode !== "add" || selectedReason === "__custom__" || reasonOptions.length === 0 ? (
                <textarea
                  value={customReason}
                  onChange={(event) => setCustomReason(event.target.value)}
                  rows={4}
                  placeholder="Example: Too small-cap, structured product, bad data quality, no longer want in scans"
                  required={mode !== "add" || selectedReason === "__custom__" || reasonOptions.length === 0}
                />
              ) : null}
            </label>
          </div>

          <div className="modal-footer">
            <button className="secondary-button" type="button" onClick={onClose} disabled={submitting}>
              Cancel
            </button>
            <button className="primary-button" type="submit" disabled={submitting || !reason}>
              {submitting ? "Saving..." : confirmLabel}
            </button>
          </div>
        </form>
      </div>
    </dialog>
  );
}
