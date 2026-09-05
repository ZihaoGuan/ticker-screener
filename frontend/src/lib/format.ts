export function formatLocalDateTime(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

export function formatLocalDate(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  const date = new Date(`${value}T12:00:00`);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
  }).format(date);
}

export function formatCount(value: number): string {
  return new Intl.NumberFormat().format(value);
}

export function humanizePositionAction(value: string | null | undefined): string {
  switch (String(value || "").trim().toLowerCase()) {
    case "add_position": return "Add";
    case "hold_position": return "Hold";
    case "trim_reduce": return "Trim";
    case "avoid_new": return "Avoid";
    default: return "--";
  }
}

export function humanizePositionTrend(value: string | null | undefined): string {
  switch (String(value || "").trim().toLowerCase()) {
    case "healthy": return "Healthy";
    case "weakening": return "Weakening";
    case "broken": return "Broken";
    default: return "--";
  }
}

export function humanizePositionExtension(value: string | null | undefined): string {
  switch (String(value || "").trim().toLowerCase()) {
    case "normal": return "Normal";
    case "stretched": return "Stretched";
    case "extreme": return "Extreme";
    default: return "--";
  }
}

export function toneForPositionAction(value: string | null | undefined): string {
  switch (String(value || "").trim().toLowerCase()) {
    case "add_position": return "is-strong";
    case "hold_position": return "is-neutral";
    case "trim_reduce": return "is-warning";
    case "avoid_new": return "is-weak";
    default: return "";
  }
}
