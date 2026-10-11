from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from src.webapp.services.external_job_result_service import ExternalJobResultService


class ExternalJobResultServiceTests(unittest.TestCase):
    def test_completion_persists_only_matching_github_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw = root / "raw.json"
            watchlist = root / "watchlist.json"
            summary = root / "summary.json"
            raw.write_text(json.dumps({"hits": [{"ticker": "NVDA"}]}), encoding="utf-8")
            watchlist.write_text("[]", encoding="utf-8")
            summary.write_text(json.dumps({"strategy_id": "finviz_analyst_recom_strongbuy", "date_label": "2026-10-11"}), encoding="utf-8")

            class _Repository:
                def __init__(self) -> None:
                    self.patches: list[dict[str, object]] = []

                def get_job_run(self, _: int) -> dict[str, object]:
                    return {"id": 44, "status": "running", "request_payload": {"action_id": "finviz_analyst_recom_strongbuy", "execution_mode": "github", "options": {}}, "result_payload": {"executor": "github_actions", "github_run_id": 77}}

                def patch_job_run_result(self, _: int, **kwargs: object) -> None:
                    self.patches.append(kwargs)

            class _History:
                def persist_screen_run(self, **kwargs: object) -> int:
                    self.kwargs = kwargs
                    return 9

            repository = _Repository()
            history = _History()
            service = ExternalJobResultService(repository=repository, history=history, artifacts_dir=root / "artifacts", artifact_base_url="https://artifacts.example")  # type: ignore[arg-type]
            service._read_json_url = lambda _: {"job_run_id": 44, "github_run_id": 77, "action_id": "finviz_analyst_recom_strongbuy", "files": {"raw": {}, "watchlist": {}, "summary": {}}}  # type: ignore[method-assign]
            files = iter([raw, watchlist, summary])
            service._download_file = lambda **_: next(files)  # type: ignore[method-assign]

            result = service.complete(job_run_id=44, payload={"github_run_id": 77, "status": "success", "manifest_url": "https://artifacts.example/manifest.json"})

        self.assertEqual(result, {"status": "success", "screen_run_id": 9})
        self.assertEqual(history.kwargs["job_run_id"], 44)
        self.assertEqual(repository.patches[-1]["status"], "success")
