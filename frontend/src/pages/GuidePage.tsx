import { Panel } from "../components/Panel";

type PrincipleCard = {
  id: string;
  title: string;
  body: string[];
};

type SignalCard = {
  id: string;
  title: string;
  setup: string;
  why: string;
  trap: string;
};

type ProcessStep = {
  id: string;
  title: string;
  copy: string[];
};

const principleCards: PrincipleCard[] = [
  {
    id: "leadership",
    title: "Start with leadership, not with bargains",
    body: [
      "I do not begin with what looks cheap. I begin with what institutions are still willing to defend. That is why this app keeps coming back to RS, RS new highs before price, and names that refuse to crack even when the tape gets messy.",
      "A weak stock can always get weaker. A true leader usually tells on itself early. The job is not to predict every turn. The job is to notice who is acting better than the market before the crowd sees the breakout.",
    ],
  },
  {
    id: "buy-reset",
    title: "Buy the reset, not the emotional extension",
    body: [
      "Most A+ entries do not happen when the stock is already far away from support. They happen after expansion, then a controlled pause, then one clean pivot where risk is obvious.",
      "That is why so many guides in the posts talk about the 8 EMA, 21 EMA, weekly 8 EMA, gap support, and tight flags. I want the stock to prove demand first, then offer a calm place to enter.",
    ],
  },
  {
    id: "risk-first",
    title: "Risk comes before upside",
    body: [
      "A setup is only as good as its invalidation. If I cannot point to the exact low, line, or structure that proves me wrong, then the trade is not ready.",
      "This app is built to help with that framing. The signal is useful, but the structure around the signal matters more: support, distance from highs, whether the pullback is controlled, and whether the stock is still acting like a leader.",
    ],
  },
];

const signalCards: SignalCard[] = [
  {
    id: "rs-line",
    title: "RS line and RS rating",
    setup:
      "RS line is the stock divided by the benchmark. RS rating is a quick score from 0 to 99 that summarizes how strongly the stock has been outperforming over multiple lookback windows.",
    why:
      "I use RS to answer one question first: who is being accumulated while the average chart is wobbling? If the RS line is climbing while price is just resting, that is usually the footprint of real demand.",
    trap:
      "A high RS value by itself is not a buy signal. If price is wildly extended, if volume is climactic, or if the structure underneath is sloppy, elite RS can still lead to a bad entry.",
  },
  {
    id: "rs-before-price",
    title: "RS new high before price",
    setup:
      "This is one of the cleanest clues in the app. The RS line pushes to a new high before the stock price does the same.",
    why:
      "That usually means sponsorship is already showing up under the surface. Price has not fully broken out yet, but relative demand is already stronger than the benchmark.",
    trap:
      "Treat it like a promotion into focus, not an automatic buy button. I still want a base, a pivot, or some controlled weakness into support before I act.",
  },
  {
    id: "buy-rs-on-weakness",
    title: "Buy RS on weakness",
    setup:
      "This is the pattern behind many of the posts. The market shakes, the stock pulls back or goes sideways, but RS does not really give up.",
    why:
      "That is where the next leaders often hide. When weak tape cannot force a stock to break down, it tells you who institutions still want on their books.",
    trap:
      "Not every dip is constructive weakness. I want tight action, lighter selling pressure, respect for support, and no sign that the stock is turning into a laggard.",
  },
  {
    id: "ema-support",
    title: "8 EMA, 21 EMA, and weekly 8 EMA",
    setup:
      "These are support references, not magic numbers. The 8 EMA shows very tight trend support. The 21 EMA gives more room. The weekly 8 EMA helps me judge the higher-timeframe character of the move.",
    why:
      "Strong names usually do not need to destroy every support level before moving again. The line they hold tells you how urgent buyers are.",
    trap:
      "Do not buy just because price touched an average. I want context too: leader status, prior expansion, volume dry-up, and some sign that selling pressure is actually exhausting.",
  },
  {
    id: "gap-support",
    title: "Gap support and earnings flags",
    setup:
      "Big earnings gaps and strong continuation gaps often leave a price zone that becomes memory on the chart. When a winner comes back into that zone and firms up, the risk can be cleaner than chasing the original move.",
    why:
      "A lot of powerful continuation trades are really second-chance trades. The market already showed you urgency. Now you wait for the stock to reset into a spot where buyers can defend visibly.",
    trap:
      "A gap fill is not bullish by default. If the stock undercuts the zone easily, gives up RS, or cannot reclaim the level with force, the support may be gone.",
  },
  {
    id: "htf",
    title: "HTF, weekly pullbacks, and the reset trade",
    setup:
      "HTF here is about names that already proved they can move with violence, then calm down without really breaking character. Weekly RS leadership plus a controlled reset is often where the cleaner re-entry lives.",
    why:
      "The biggest winners often move in waves. Expansion. Pause. Re-acceleration. I am not trying to buy every first breakout if the stock later offers a tighter weekly setup.",
    trap:
      "If the reset gets too deep, too loose, or too obvious, the character changes. A proper reset should feel controlled, not damaged.",
  },
  {
    id: "fearzone",
    title: "Fearzone and contrarian pressure",
    setup:
      "Fearzone-style signals are not saying every selloff is a buy. They are trying to find moments where emotion stretches farther than structure probably should.",
    why:
      "Used well, this helps spot names that are panicking into support while the longer trend is still alive. That can be a useful watchlist signal, especially when the market is washing people out.",
    trap:
      "A fear signal without trend support, relative strength, or a clear reclaim can just be a weak stock getting weaker. Contrarian only works when there is something real underneath it.",
  },
];

const processSteps: ProcessStep[] = [
  {
    id: "context",
    title: "1. Read context first",
    copy: [
      "I want to know what kind of market I am in before I get romantic about any single ticker. Use the RRG pages, sector context, and the broad tape to see whether money is rotating into growth, hiding in defense, or just chopping around.",
      "If the market is under pressure, I care even more about names that hold RS, respect weekly support, and refuse to get damaged.",
    ],
  },
  {
    id: "selection",
    title: "2. Promote leaders into focus",
    copy: [
      "The watchlist side of the app is there to narrow attention fast. RS new high before price, weekly RS leadership, HVE, earnings continuation behavior, and constructive pullback logic all help answer the same question: which names deserve my screen time right now?",
      "This is where the app saves time. It does not replace judgment. It helps cut the pile down to names with a real reason to matter.",
    ],
  },
  {
    id: "entry",
    title: "3. Wait for a clean trigger",
    copy: [
      "A watchlist name is not yet a trade. I still want a pivot, an inside day, a reclaim, a trendline break, or some other clean trigger that lets me define risk tightly.",
      "The best setups usually feel calm right before they move. Tightness is not boring. Tightness is what gives you asymmetric risk.",
    ],
  },
  {
    id: "risk",
    title: "4. Respect the stop",
    copy: [
      "If the setup fails through the level that made it attractive, I am out. Under the inside-day low, under the squat low, under the key gap zone, under the weekly support line. Whatever the structure was, honor it.",
      "Small losses are not proof the setup was bad. They are the price of staying in the game long enough for the real leaders to pay for many failed tries.",
    ],
  },
  {
    id: "management",
    title: "5. Manage winners like a human, not a robot",
    copy: [
      "The posts around trimming, taking profits, and handling new positions all point to the same thing: trade management is contextual. Sometimes the right move is to press. Sometimes the right move is to de-risk into strength.",
      "I do not want this page to pretend there is one formula. If a stock is extended from the 8 EMA, hitting ADR stretch, or running into obvious supply, I get more defensive. If it is just beginning a fresh expansion from a tight pivot, I can give it more room.",
    ],
  },
];

export function GuidePage() {
  return (
    <div className="page-grid">
      <Panel title="Signal Guide" aside={<span className="eyebrow">How this app thinks</span>}>
        <div className="guide-topic">
          <p className="panel-copy">
            This page is not meant to be a dictionary of fancy chart words. It is meant to explain the trading
            judgment behind the signals so the app feels like a teammate, not a black box.
          </p>
          <p className="panel-copy">
            The posts in this project keep repeating the same rhythm: find true leaders, let them reset, define risk
            clearly, and do not confuse motion with quality. That same rhythm sits underneath the screeners, charts,
            and weekly watchlists here.
          </p>
        </div>
      </Panel>

      <div className="guide-grid">
        {principleCards.map((card) => (
          <Panel key={card.id} title={card.title} aside={<span className="eyebrow">Core idea</span>}>
            <div className="guide-topic">
              {card.body.map((paragraph) => (
                <p key={paragraph} className="panel-copy">
                  {paragraph}
                </p>
              ))}
            </div>
          </Panel>
        ))}
      </div>

      <Panel title="Signal Language" aside={<span className="eyebrow">What the labels really mean</span>}>
        <div className="guide-grid">
          {signalCards.map((topic) => (
            <div key={topic.id} className="guide-topic">
              <div>
                <div className="eyebrow">{topic.title}</div>
                <p className="panel-copy">{topic.setup}</p>
              </div>
              <div>
                <div className="eyebrow">Why I care</div>
                <p className="panel-copy">{topic.why}</p>
              </div>
              <div>
                <div className="eyebrow">Easy mistake</div>
                <p className="panel-copy">{topic.trap}</p>
              </div>
            </div>
          ))}
        </div>
      </Panel>

      <Panel title="Practical Workflow" aside={<span className="eyebrow">How to use the route</span>}>
        <div className="guide-grid">
          {processSteps.map((step) => (
            <div key={step.id} className="guide-topic">
              <div className="eyebrow">{step.title}</div>
              {step.copy.map((paragraph) => (
                <p key={paragraph} className="panel-copy">
                  {paragraph}
                </p>
              ))}
            </div>
          ))}
        </div>
      </Panel>
    </div>
  );
}
