import { useEffect, useState, type FormEvent } from "react";
import { NavLink } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { fetchJson } from "../lib/api";
import type { DiscordNotificationSettingsResponse } from "../lib/types";
import "./RunsPage.css";

const EMPTY_SETTINGS: DiscordNotificationSettingsResponse = {
  webhook_url: "",
  app_base_url: "",
  effective_app_base_url: "",
  enabled: false,
};

export function AdminDiscordPage() {
  const [settings, setSettings] = useState<DiscordNotificationSettingsResponse>(EMPTY_SETTINGS);
  const [webhookUrl, setWebhookUrl] = useState("");
  const [appBaseUrl, setAppBaseUrl] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [notice, setNotice] = useState("");

  const loadSettings = () => {
    setIsLoading(true);
    void fetchJson<DiscordNotificationSettingsResponse>("/api/admin/discord-notifications")
      .then((result) => {
        setSettings(result);
        setWebhookUrl(result.webhook_url);
        setAppBaseUrl(result.app_base_url);
      })
      .catch((error) => {
        setNotice(error instanceof Error ? error.message : "Failed to load Discord notification settings.");
        setSettings(EMPTY_SETTINGS);
      })
      .finally(() => setIsLoading(false));
  };

  useEffect(() => {
    loadSettings();
  }, []);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSaving(true);
    setNotice("");
    try {
      const response = await fetchJson<{ ok: boolean; settings: DiscordNotificationSettingsResponse }>("/api/admin/discord-notifications", {
        method: "POST",
        body: JSON.stringify({
          webhook_url: webhookUrl,
          app_base_url: appBaseUrl,
        }),
      });
      setSettings(response.settings);
      setWebhookUrl(response.settings.webhook_url);
      setAppBaseUrl(response.settings.app_base_url);
      setNotice(response.settings.enabled ? "Discord notifications saved and enabled." : "Discord notifications saved. Notifications stay off until both fields are filled.");
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Failed to save Discord notification settings.");
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <div className="page-grid">
      <section className="panel screeners-subnav-panel">
        <div className="screeners-subnav-copy">
          <span className="eyebrow">Admin</span>
          <h1>Discord Notifications</h1>
          <p className="panel-copy">Control scanner completion alerts for ad hoc and scheduled jobs. Empty fields keep notifications off.</p>
        </div>
        <div className="screeners-subnav-links" role="tablist" aria-label="Admin sections">
          <NavLink to="/admin" end className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
            Admin Overview
          </NavLink>
          <NavLink to="/admin/discord-notifications" className={({ isActive }) => `screeners-subnav-link${isActive ? " is-active" : ""}`}>
            Discord Alerts
          </NavLink>
        </div>
      </section>

      <Panel title="Discord Webhook">
        {isLoading ? <LoadingBlock label="Loading Discord notification settings…" /> : null}
        {!isLoading ? (
          <form className="form-grid" onSubmit={handleSubmit}>
            <label>
              <span>Discord Webhook URL</span>
              <input
                type="password"
                value={webhookUrl}
                onChange={(event) => setWebhookUrl(event.target.value)}
                placeholder="https://discord.com/api/webhooks/..."
              />
            </label>
            <label>
              <span>Public App Base URL</span>
              <input
                type="url"
                value={appBaseUrl}
                onChange={(event) => setAppBaseUrl(event.target.value)}
                placeholder="https://ticker.example.com"
              />
            </label>
            <div className="panel-copy">
              Effective base URL: <code>{settings.effective_app_base_url || "Not configured"}</code>
            </div>
            <div className="panel-copy">
              Status: <strong>{settings.enabled ? "Enabled" : "Disabled"}</strong>
            </div>
            <div className="actions-row">
              <button type="submit" className="primary-button" disabled={isSaving}>
                {isSaving ? "Saving…" : "Save Settings"}
              </button>
            </div>
            {notice ? <div className="panel-copy">{notice}</div> : null}
          </form>
        ) : null}
      </Panel>
    </div>
  );
}
