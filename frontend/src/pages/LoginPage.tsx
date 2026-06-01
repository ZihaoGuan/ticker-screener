import { useEffect, useState, type FormEvent } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { LoadingBlock } from "../components/LoadingBlock";
import { Panel } from "../components/Panel";
import { useAuth } from "../auth/AuthContext";

export function LoginPage() {
  const auth = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const token = searchParams.get("token") ?? "";
  const nextPath = searchParams.get("next") ?? "/";
  const [email, setEmail] = useState("");
  const [premiumEmail, setPremiumEmail] = useState("");
  const [message, setMessage] = useState("");
  const [premiumMessage, setPremiumMessage] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isSubmittingPremium, setIsSubmittingPremium] = useState(false);

  useEffect(() => {
    if (!token) {
      return;
    }
    setIsSubmitting(true);
    void auth
      .verifyMagicLink(token)
      .then(() => {
        navigate(nextPath, { replace: true });
      })
      .catch((error) => {
        setMessage(error instanceof Error ? error.message : "Unable to verify sign-in link.");
      })
      .finally(() => setIsSubmitting(false));
  }, [auth, navigate, nextPath, token]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSubmitting(true);
    setMessage("");
    try {
      const responseMessage = await auth.requestMagicLink(email);
      setMessage(responseMessage);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Unable to request sign-in link.");
    } finally {
      setIsSubmitting(false);
    }
  };

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
      <Panel title="Sign In" aside={<span className="eyebrow">Magic Link</span>}>
        {token && isSubmitting ? <LoadingBlock label="Verifying sign-in link…" /> : null}
        <p className="panel-copy">Enter your email to receive a sign-in link. Visitors can still browse results without logging in.</p>
        <form className="run-toolbar" onSubmit={(event) => void handleSubmit(event)}>
          <div className="run-params-grid">
            <label className="field">
              <span>Email</span>
              <input
                type="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                placeholder="you@example.com"
                autoComplete="email"
              />
            </label>
          </div>
          <div className="button-row">
            <button className="primary-button" type="submit" disabled={isSubmitting || token.length > 0}>
              {isSubmitting && !token ? "Sending..." : "Send Sign-In Link"}
            </button>
          </div>
        </form>
        {message ? <p className="panel-copy">{message}</p> : null}
      </Panel>
      <Panel title="Request Premium Access" aside={<span className="eyebrow">Visitor</span>}>
        <p className="panel-copy">Visitors can browse results. Request premium access here if you want screener run permissions.</p>
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
            <button className="primary-button" type="submit" disabled={isSubmittingPremium || token.length > 0}>
              {isSubmittingPremium ? "Submitting..." : "Request Premium"}
            </button>
          </div>
        </form>
        {premiumMessage ? <p className="panel-copy">{premiumMessage}</p> : null}
      </Panel>
    </div>
  );
}
