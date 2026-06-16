import { useState, type FormEvent } from "react";
import { useSearchParams } from "react-router-dom";
import { Panel } from "../components/Panel";
import { useAuth } from "../auth/AuthContext";

export function LoginPage() {
  const auth = useAuth();
  const [searchParams] = useSearchParams();
  const nextPath = searchParams.get("next") ?? "/";
  const loginError = searchParams.get("error") ?? "";
  const [premiumEmail, setPremiumEmail] = useState("");
  const [premiumMessage, setPremiumMessage] = useState("");
  const [isSubmittingPremium, setIsSubmittingPremium] = useState(false);

  const handlePremiumRequest = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSubmittingPremium(true);
    setPremiumMessage("");
    try {
      const responseMessage = await auth.requestPremiumAccess(premiumEmail);
      setPremiumMessage(responseMessage);
      setPremiumEmail("");
    } catch (error) {
      setPremiumMessage(error instanceof Error ? error.message : "Unable to request premium access.");
    } finally {
      setIsSubmittingPremium(false);
    }
  };

  return (
    <div className="page-grid">
      <Panel title="Sign In" aside={<span className="eyebrow">Google OAuth</span>}>
        <p className="panel-copy">Use approved Google account to sign in. Visitors can still browse results without logging in.</p>
        <div className="button-row">
          <a className="primary-button" href={`/api/auth/google/start?next=${encodeURIComponent(nextPath)}`}>
            Sign In With Google
          </a>
        </div>
        {loginError ? <p className="panel-copy">{loginError}</p> : null}
        <p className="panel-copy">Admin setup: add your Google email to `WEBAPP_AUTH_BOOTSTRAP_ADMIN_EMAILS` or create active user record in Admin, then sign in with same Google email.</p>
      </Panel>
      <Panel title="Request Premium Access" aside={<span className="eyebrow">Visitor</span>}>
        <p className="panel-copy">Visitors can browse results. Request premium access here if you want screener run permissions, then sign in with same Google email after approval.</p>
        <form className="run-toolbar" onSubmit={(event) => void handlePremiumRequest(event)}>
          <div className="run-params-grid">
            <label className="field">
              <span>Email</span>
              <input
                type="email"
                value={premiumEmail}
                onChange={(event) => setPremiumEmail(event.target.value)}
                placeholder="you@example.com"
                autoComplete="email"
              />
            </label>
          </div>
          <div className="button-row">
            <button className="primary-button" type="submit" disabled={isSubmittingPremium}>
              {isSubmittingPremium ? "Submitting..." : "Request Premium"}
            </button>
          </div>
        </form>
        {premiumMessage ? <p className="panel-copy">{premiumMessage}</p> : null}
      </Panel>
    </div>
  );
}
