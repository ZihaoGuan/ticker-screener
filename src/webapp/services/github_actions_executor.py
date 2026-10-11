from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PILOT_ACTIONS = frozenset({"finviz_analyst_recom_strongbuy"})


@dataclass(frozen=True)
class GitHubActionsSettings:
    token: str
    repository: str
    workflow: str
    ref: str

    @property
    def configured(self) -> bool:
        return bool(self.token and self.repository and self.workflow and self.ref)


class GitHubActionsExecutor:
    """Small adapter around GitHub's workflow-dispatch and run endpoints."""

    API_BASE = "https://api.github.com"

    def __init__(self, settings: GitHubActionsSettings) -> None:
        self.settings = settings

    def dispatch(self, *, job_run_id: int, action_id: str, code_version: str, options: dict[str, Any]) -> dict[str, Any]:
        if action_id not in PILOT_ACTIONS:
            raise ValueError(f"GitHub execution is not enabled for {action_id}.")
        if not self.settings.configured:
            raise ValueError("GitHub Actions executor is not configured.")
        payload = {
            "ref": self.settings.ref,
            "return_run_details": True,
            "inputs": {
                "job_run_id": str(job_run_id),
                "attempt": "1",
                "action_id": action_id,
                "code_version": code_version,
                "options_json": json.dumps(options, separators=(",", ":"), sort_keys=True),
            },
        }
        return self._request(
            method="POST",
            path=f"/repos/{self.settings.repository}/actions/workflows/{self.settings.workflow}/dispatches",
            payload=payload,
        )

    def get_status(self, run_id: int) -> dict[str, Any]:
        return self._request(method="GET", path=f"/repos/{self.settings.repository}/actions/runs/{run_id}")

    def cancel(self, run_id: int) -> None:
        self._request(method="POST", path=f"/repos/{self.settings.repository}/actions/runs/{run_id}/cancel")

    def _request(self, *, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(
            f"{self.API_BASE}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.settings.token}",
                "X-GitHub-Api-Version": "2026-03-10",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=20) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raise RuntimeError(f"GitHub Actions request failed ({exc.code}).") from exc
        except URLError as exc:
            raise RuntimeError("GitHub Actions request could not be reached.") from exc
        if not raw:
            return {}
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("GitHub Actions returned an invalid response.") from exc
        return result if isinstance(result, dict) else {}
