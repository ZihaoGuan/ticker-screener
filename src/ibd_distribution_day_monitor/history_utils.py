"""Effective history rebasing for as_of evaluation.

Given a most-recent-first history and an optional as_of date, return a
slice such that effective_history[0] is the evaluation session. All
downstream modules consume effective_history without needing to know
as_of_index.
"""

from __future__ import annotations


def prepare_effective_history(
    history: list[dict],
    as_of: str | None,
    required_min_sessions: int,
) -> tuple[list[dict], dict]:
    """Rebase history so index 0 is the evaluation session.

    Args:
        history: Most-recent-first list of OHLCV dicts (history[0] = latest).
        as_of: ISO date string or None. If None, evaluation session = history[0].
            If given and not present, snap back to the latest loaded session
            on or before `as_of`.
        required_min_sessions: If the resulting effective_history has fewer rows,
            an "insufficient_lookback" audit flag is appended.

    Returns:
        (effective_history, audit) where audit has:
            - as_of_resolved: str
            - sessions_available: int
            - audit_flags: list[str]

    Raises:
        ValueError: If history is empty or no loaded session is on/before as_of.
    """
    if not history:
        raise ValueError("history is empty; cannot prepare effective_history")

    audit: dict = {"as_of_resolved": None, "sessions_available": 0, "audit_flags": []}

    if as_of is None:
        effective = history
        audit["as_of_resolved"] = history[0]["date"]
    else:
        idx = next((i for i, row in enumerate(history) if row.get("date") == as_of), None)
        if idx is None:
            idx = next(
                (i for i, row in enumerate(history) if str(row.get("date") or "") <= as_of),
                None,
            )
            if idx is None:
                raise ValueError(f"as_of {as_of} is earlier than all loaded history")
            audit["audit_flags"].append("as_of_snapped_to_latest_available_session")
        effective = history[idx:]
        audit["as_of_resolved"] = effective[0]["date"]

    audit["sessions_available"] = len(effective)
    if len(effective) < required_min_sessions:
        audit["audit_flags"].append("insufficient_lookback")
    return effective, audit
