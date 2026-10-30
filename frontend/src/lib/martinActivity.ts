export const martinActivityActions: Record<string, { label: string; direction: "buy" | "sell" }> = {
  order_filled: { label: "Buy", direction: "buy" }, trim_3r: { label: "Trim", direction: "sell" },
  earnings_half_trim: { label: "Earnings trim", direction: "sell" },
  earnings_exit: { label: "Earnings exit", direction: "sell" },
  earnings_exit_scheduled: { label: "Earnings exit scheduled", direction: "sell" },
  earnings_half_trim_scheduled: { label: "Earnings trim scheduled", direction: "sell" },
  stop: { label: "Stop exit", direction: "sell" }, gap_stop: { label: "Gap stop exit", direction: "sell" },
  ema9_exit: { label: "EMA9 exit", direction: "sell" }, rotation_exit: { label: "Rotation exit", direction: "sell" },
};

type ActivityRow = Record<string, unknown>;

export function groupMartinActivity(positions: ActivityRow[], events: ActivityRow[]): ActivityRow[][] {
  return positions.map((position) => {
    const ticker = String(position.ticker ?? "—").toUpperCase();
    const payload = position.payload_json && typeof position.payload_json === "object" ? position.payload_json as ActivityRow : {};
    const row = { ...position, ...payload };
    return events.filter((event) => {
      if (!martinActivityActions[String(event.event_type ?? "")]) return false;
      if (event.position_id != null && position.id != null) return String(event.position_id) === String(position.id);
      if (event.position_id != null || String(event.ticker ?? "").toUpperCase() !== ticker) return false;
      if (positions.filter((candidate) => String(candidate.ticker ?? "").toUpperCase() === ticker).length !== 1) return false;
      const opened = String(row.opened_on ?? ""), closed = String(row.closed_on ?? ""), date = String(event.event_date ?? "");
      return Boolean(opened && date >= opened && (!closed || date <= closed));
    }).sort((a, b) => String(a.event_date ?? "").localeCompare(String(b.event_date ?? "")));
  });
}
