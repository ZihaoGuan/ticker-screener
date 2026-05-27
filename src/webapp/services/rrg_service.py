from __future__ import annotations

from pathlib import Path
from typing import Any


class RrgService:
    def __init__(self, output_dir: Path, reports_fqdn: str = "") -> None:
        self.output_dir = output_dir
        self.reports_fqdn = reports_fqdn.strip()

    def get_latest_report(self) -> dict[str, Any]:
        report_dir = self._resolve_latest_report_dir()
        if report_dir is None:
            return {
                "available": False,
                "date_label": "",
                "report_root": "",
                "report_index_url": "",
                "sections": [],
            }

        relative_root = report_dir.relative_to(self.output_dir).as_posix()
        return {
            "available": True,
            "date_label": relative_root.removeprefix("sector_rotation_rrg_"),
            "report_root": relative_root,
            "report_index_url": self._reports_url(f"{relative_root}/index.html"),
            "sections": [
                {
                    "id": "sector",
                    "title": "Sector Rotation",
                    "description": "Official 11 sector ETFs rendered as the main daily-refreshed weekly RRG map.",
                    "index_url": self._reports_url(f"{relative_root}/sector/index.html"),
                    "image_url": self._reports_url(f"{relative_root}/sector/sector_rrg.svg"),
                },
                {
                    "id": "industry",
                    "title": "Industry Rotation",
                    "description": "Focused industry ETF basket for tactical leadership and rotation checks.",
                    "index_url": self._reports_url(f"{relative_root}/industry/index.html"),
                    "image_url": self._reports_url(f"{relative_root}/industry/sector_rrg.svg"),
                },
                {
                    "id": "theme",
                    "title": "Theme Rotation",
                    "description": "Theme ETF batches split into smaller RRG maps for readability.",
                    "index_url": self._reports_url(f"{relative_root}/theme/index.html"),
                    "image_url": self._reports_url(f"{relative_root}/theme/theme_batch_01/sector_rrg.svg"),
                },
            ],
        }

    def _resolve_latest_report_dir(self) -> Path | None:
        if not self.output_dir.exists():
            return None
        candidates = sorted(
            (path for path in self.output_dir.glob("sector_rotation_rrg_*") if path.is_dir()),
            reverse=True,
        )
        return candidates[0] if candidates else None

    def _reports_url(self, relative_path: str) -> str:
        normalized = relative_path.lstrip("/")
        if self.reports_fqdn:
            return f"https://{self.reports_fqdn}/{normalized}"
        return f"/{normalized}"
