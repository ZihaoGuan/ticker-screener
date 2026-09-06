const trialHref = "/api/auth/google/start?next=%2F";

export function LandingPage() {
  return (
    <div className="landing-page">
      <header className="landing-nav">
        <a className="landing-brand" href="#top" aria-label="Ticker Screener home">
          Ticker Screener
        </a>
        <nav aria-label="Public navigation">
          <a href="#how-it-works">How It Works</a>
          <a href="/login">Sign In</a>
          <a className="primary-button" href={trialHref}>Start Free</a>
        </nav>
      </header>

      <main id="top">
        <section className="landing-hero">
          <div className="landing-hero-copy">
            <span className="landing-kicker">Daily stock intelligence, ranked</span>
            <h1>Find market leaders before they become obvious.</h1>
            <p>
              Cut through thousands of stocks with momentum, fundamentals, market regime, and proven setup scanners in one focused workflow.
            </p>
            <div className="landing-actions">
              <a className="primary-button landing-primary-cta" href={trialHref}>Start 14-Day Free Trial</a>
              <a className="ghost-button" href="#sample">See an Example</a>
            </div>
            <span className="landing-reassurance">No credit card required · Continue with Google</span>
          </div>

          <div id="sample" className="landing-product-preview" aria-label="Illustrative Top Hits scanner result">
            <div className="landing-preview-head">
              <div>
                <span>Top Hits</span>
                <strong>Stocks confirmed by multiple signals</strong>
              </div>
              <span className="landing-live-pill">Example</span>
            </div>
            <div className="landing-preview-table" role="table" aria-label="Example ranked stocks">
              <div className="landing-preview-row landing-preview-labels" role="row">
                <span role="columnheader">Ticker</span>
                <span role="columnheader">Signals</span>
                <span role="columnheader">RS</span>
                <span role="columnheader">Trend</span>
              </div>
              <PreviewRow ticker="NVDA" signals="5" rs="98" trend="Leading" />
              <PreviewRow ticker="VST" signals="4" rs="95" trend="Leading" />
              <PreviewRow ticker="PLTR" signals="4" rs="93" trend="Accelerating" />
            </div>
            <p>Illustrative product preview. Trial data comes from the latest available market snapshot.</p>
          </div>
        </section>

        <section id="how-it-works" className="landing-benefits" aria-labelledby="benefits-title">
          <div className="landing-section-copy">
            <span className="landing-kicker">One decision-ready workflow</span>
            <h2 id="benefits-title">Stop searching. Start ranking.</h2>
          </div>
          <div className="landing-benefit-grid">
            <article>
              <span>01</span>
              <h3>See the strongest names first</h3>
              <p>Top Hits ranks stocks that survive multiple independent scanners.</p>
            </article>
            <article>
              <span>02</span>
              <h3>Validate the full setup</h3>
              <p>Review price action, relative strength, fundamentals, and sector leadership together.</p>
            </article>
            <article>
              <span>03</span>
              <h3>Match risk to the tape</h3>
              <p>Use market regime and breadth signals before committing capital.</p>
            </article>
          </div>
        </section>

        <section className="landing-final-cta">
          <span className="landing-kicker">Your next leader is already in the data</span>
          <h2>Build tomorrow’s watchlist today.</h2>
          <a className="primary-button landing-primary-cta" href={trialHref}>Start 14-Day Free Trial</a>
          <span className="landing-reassurance">No credit card required</span>
        </section>
      </main>
    </div>
  );
}

function PreviewRow({ ticker, signals, rs, trend }: { ticker: string; signals: string; rs: string; trend: string }) {
  return (
    <div className="landing-preview-row" role="row">
      <strong role="cell">{ticker}</strong>
      <span role="cell">{signals}</span>
      <span role="cell">{rs}</span>
      <span className="landing-trend" role="cell">{trend}</span>
    </div>
  );
}
