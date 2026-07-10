from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import MagicMock, patch

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

    def test_send_message_posts_when_webhook_is_configured(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        service.update_settings(webhook_url="https://discord.example/webhook", app_base_url="https://ticker.example.com")
        service._post_webhook = MagicMock()

        notified = service.send_message("Hello scanner")

        self.assertTrue(notified)
        service._post_webhook.assert_called_once_with(
            webhook_url="https://discord.example/webhook",
            message="Hello scanner",
        )

    def test_send_message_returns_false_when_webhook_post_fails(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        service.update_settings(webhook_url="https://discord.example/webhook", app_base_url="https://ticker.example.com")
        service._post_webhook = MagicMock(side_effect=RuntimeError("boom"))

        notified = service.send_message("Hello scanner")

        self.assertFalse(notified)
        self.assertEqual(service.get_last_error_message(), "boom")

    def test_send_message_records_http_error_detail(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        service.update_settings(webhook_url="https://discord.example/webhook", app_base_url="https://ticker.example.com")
        service._last_transport = "urllib"
        service._post_webhook = MagicMock(side_effect=HTTPError("https://discord.example/webhook", 404, "Not Found", hdrs=None, fp=None))

        notified = service.send_message("Hello scanner")

        self.assertFalse(notified)
        self.assertEqual(service.get_last_error_message(), "urllib http 404 Not Found")

    def test_send_message_splits_large_payload_using_workflow_limit(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        service.update_settings(webhook_url="https://discord.example/webhook", app_base_url="https://ticker.example.com")
        service._post_webhook = MagicMock()
        lines = [f"`TK{i:03d}` gap 5.0% vol 1.50x" for i in range(120)]

        notified = service.send_message("\n".join(lines))

        self.assertTrue(notified)
        self.assertGreater(service._post_webhook.call_count, 1)
        for hook_call in service._post_webhook.call_args_list:
            message = hook_call.kwargs["message"]
            self.assertLessEqual(len(message), 1800)

    def test_send_message_continues_after_chunk_failure(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        service.update_settings(webhook_url="https://discord.example/webhook", app_base_url="https://ticker.example.com")
        posted_messages: list[str] = []

        def fake_post(*, webhook_url: str, message: str) -> None:
            posted_messages.append(message)
            if len(posted_messages) == 1:
                raise RuntimeError("boom")

        service._post_webhook = MagicMock(side_effect=fake_post)
        lines = [f"`TK{i:03d}` gap 5.0% vol 1.50x" for i in range(120)]

        notified = service.send_message("\n".join(lines))

        self.assertFalse(notified)
        self.assertGreater(len(posted_messages), 1)

    def test_post_webhook_uses_same_curl_shape_as_github_actions(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")

        with patch("src.webapp.services.discord_notification_service.shutil.which", return_value="/usr/bin/curl"), patch(
            "src.webapp.services.discord_notification_service.subprocess.run"
        ) as run_mock:
            service._post_webhook(
                webhook_url="https://discord.example/webhook",
                message="Hello scanner",
            )

        run_mock.assert_called_once()
        command = run_mock.call_args.args[0]
        self.assertEqual(command[:6], ["curl", "-sS", "-X", "POST", "-H", "Content-Type: application/json"])
        self.assertIn("--data", command)
        data_index = command.index("--data")
        self.assertTrue(command[data_index + 1].startswith("@"))
        self.assertEqual(command[-1], "https://discord.example/webhook")

    def test_post_webhook_falls_back_to_urllib_when_curl_is_unavailable(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        response = MagicMock()
        response.__enter__.return_value = response
        response.__exit__.return_value = None

        with patch("src.webapp.services.discord_notification_service.shutil.which", return_value=None), patch(
            "src.webapp.services.discord_notification_service.request.urlopen",
            return_value=response,
        ) as urlopen_mock:
            service._post_webhook(
                webhook_url="https://discord.example/webhook",
                message="Hello scanner",
            )

        urlopen_mock.assert_called_once()
        req = urlopen_mock.call_args.args[0]
        self.assertEqual(req.full_url, "https://discord.example/webhook")
        self.assertEqual(req.get_method(), "POST")
        self.assertEqual(req.headers["Content-type"], "application/json")
        self.assertIn(b'"content": "Hello scanner"', req.data)

    def test_send_message_records_curl_error_detail(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        service.update_settings(webhook_url="https://discord.example/webhook", app_base_url="https://ticker.example.com")
        service._last_transport = "curl"
        service._post_webhook = MagicMock(
            side_effect=subprocess.CalledProcessError(
                22,
                ["curl"],
                stderr="curl: (22) The requested URL returned error: 403",
            )
        )

        notified = service.send_message("Hello scanner")

        self.assertFalse(notified)
        self.assertEqual(
            service.get_last_error_message(),
            "curl exit 22: curl: (22) The requested URL returned error: 403",
        )

    def test_notify_job_completion_returns_false_when_webhook_post_fails(self) -> None:
        service = DiscordNotificationService(project_root=self.project_root, app_base_url="")
        service.update_settings(webhook_url="https://discord.example/webhook", app_base_url="https://ticker.example.com")
        service._post_webhook = MagicMock(side_effect=RuntimeError("boom"))

        notified = service.notify_job_completion(
            action_id="weekly_rs",
            job_label="Weekly RS",
            status="success",
            success_count=5,
            trigger_source="manual",
            watchlist_file="/tmp/weekly_rs_new_high_2026-06-20.json",
        )

        self.assertFalse(notified)


if __name__ == "__main__":
    unittest.main()
