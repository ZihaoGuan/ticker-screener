from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from src.webapp.services.discord_notification_service import DiscordNotificationService


class DiscordNotificationServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.project_root = Path(self.temp_dir.name)

    def test_build_completion_message_links_matching_scanner_route(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="https://ticker.example.com")

        message = service.build_completion_message(
            action_id="weekly_rs",
            job_label="Weekly RS",
            status="success",
            success_count=11,
            trigger_source="manual",
            watchlist_file="/tmp/weekly_rs_new_high_2026-06-20.json",
            app_base_url="https://ticker.example.com",
        )

        assert message is not None
        self.assertIn("Hits: 11", message)
        self.assertIn("https://ticker.example.com/scanner/weekly_rs", message)

    def test_build_completion_message_falls_back_to_screeners_when_no_scanner_route(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="https://ticker.example.com")

        message = service.build_completion_message(
            action_id="high_tight_flag",
            job_label="High Tight Flag",
            status="success",
            success_count=3,
            trigger_source="scheduler",
            watchlist_file="/tmp/high_tight_flag_2026-06-20.json",
            app_base_url="https://ticker.example.com",
        )

        assert message is not None
        self.assertIn("https://ticker.example.com/screeners", message)

    def test_build_completion_message_links_gamma_squeeze_route(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="https://ticker.example.com")

        message = service.build_completion_message(
            action_id="gamma_squeeze",
            job_label="Gamma Squeeze",
            status="success",
            success_count=4,
            trigger_source="scheduler",
            watchlist_file="/tmp/gamma_squeeze_2026-06-24.json",
            app_base_url="https://ticker.example.com",
        )

        assert message is not None
        self.assertIn("https://ticker.example.com/scanner/gamma_squeeze", message)

    def test_notify_job_completion_skips_when_settings_incomplete(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        service._post_webhook = MagicMock()

        notified = service.notify_job_completion(
            action_id="weekly_rs",
            job_label="Weekly RS",
            status="success",
            success_count=5,
            trigger_source="manual",
            watchlist_file="/tmp/weekly_rs_new_high_2026-06-20.json",
        )

        self.assertFalse(notified)
        service._post_webhook.assert_not_called()


if __name__ == "__main__":
    unittest.main()
