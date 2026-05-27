import { Panel } from "../components/Panel";

type GuideTopic = {
  id: string;
  title: string;
  what: string;
  why: string;
  how: string;
};

const topics: GuideTopic[] = [
  {
    id: "rs-line",
    title: "RS Line",
    what: "RS line is the stock price divided by the benchmark price. In this app the benchmark is usually SPY, so a rising RS line means the stock is outperforming the market even before the chart fully breaks out.",
    why: "The philosophy is leadership first, confirmation second. A strong stock often tells on itself through relative strength before price becomes obvious to everyone.",
    how: "Use it to separate true leaders from ordinary bounces. If price is pulling back but the RS line is holding up or rising, that usually deserves more attention than a stock weakening together with the tape.",
  },
  {
    id: "rs-new-high-before-price",
    title: "RS New High Before Price",
    what: "This means the RS line makes a new lookback high before price itself makes a matching high.",
    why: "The logic is that institutions can accumulate quietly before the obvious breakout. Relative strength leading price is often early evidence that demand is already there.",
    how: "In the chart we mark these moments separately from ordinary RS new highs. Treat them as a watchlist promotion signal, not an automatic buy button by themselves.",
  },
  {
    id: "buy-rs-on-weakness",
    title: "Buy RS On Weakness",
    what: "This is not one indicator. It is a behavior pattern: the stock pulls back or consolidates, but it still acts better than the market or its peers.",
    why: "The philosophy is to buy quality under pressure instead of chasing extension. Weak tape often reveals which names institutions still want to own.",
    how: "When the market is messy, prioritize names whose RS stays near highs, whose pullbacks are controlled, and whose support tests happen on lighter pressure.",
  },
  {
    id: "ema-support",
    title: "EMA 8, EMA 21, Weekly 8 EMA",
    what: "These moving averages are used here as dynamic support layers, not magic lines. EMA 8 is fast support, EMA 21 is a more forgiving swing support, and Weekly 8 EMA is a higher-timeframe support reference.",
    why: "The philosophy is trend integrity. Strong leaders usually do not need to violate every support level before they resume. Which average holds tells you how urgent buyers are.",
    how: "If a stock holds EMA 8, the trend is very tight. If it needs EMA 21 or weekly 8 EMA, the setup may still be valid but the character is looser and the timing often matters more.",
  },
  {
    id: "ipo-vwap",
    title: "IPO VWAP",
    what: "IPO VWAP tracks the volume-weighted average price from the start of the available life of the chart.",
    why: "The philosophy is cost basis memory. For newer leaders, VWAP often acts like a moving line of institutional profitability and can matter more than older static levels.",
    how: "Use it as one more structure clue. When price reclaims and holds IPO VWAP together with RS strength, it often suggests the stock is regaining sponsorship rather than just bouncing.",
  },
  {
    id: "gap-zone",
    title: "Gap Zone",
    what: "A gap zone is the price area left open between two sessions when price jumps and leaves untraded space behind.",
    why: "The philosophy is unfinished business and memory. Gaps often become support or resistance because they mark places where supply-demand shifted abruptly.",
    how: "In this app gap zones are drawn as boxes rather than noisy horizontal lines. If a strong stock pulls back into earnings-gap support and holds, that is often a much better risk-reward entry than chasing the breakout later.",
  },
  {
    id: "htf",
    title: "HTF and Weekly Pullback Logic",
    what: "HTF here is the high-tight-flag family or higher-timeframe leader behavior depending on context. The weekly pullback workflow looks for names that already proved leadership, then reset in an orderly way.",
    why: "The philosophy is that the biggest winners often move in stages: expansion, controlled reset, then re-acceleration. Buying the reset is usually cleaner than buying emotional extension.",
    how: "Use weekly RS leadership, higher-timeframe support, and constructive pullback behavior together. The point is not 'buy any dip'; it is 'buy the reset in names that already showed real demand.'",
  },
  {
    id: "rrg",
    title: "RRG",
    what: "RRG plots relative trend on one axis and relative momentum on the other. It is a map of rotation, not a single-stock entry trigger.",
    why: "The philosophy is top-down context. You want to know where strength is rotating before you choose which individual charts deserve focus.",
    how: "Use Sector, Industry, and Theme RRG pages to ask which groups are leading, improving, weakening, or lagging. Then marry that context with the single-name setups on the watchlist side.",
  },
];

export function GuidePage() {
  return (
    <div className="page-grid">
      <Panel title="Signal Guide" aside={<span className="eyebrow">Logic and philosophy</span>}>
        <p className="panel-copy">
          This page explains how the app thinks about key indicators and setup language. The goal is not to worship
          lines on a chart, but to make the decision process more legible: leadership, support, timing, and context.
        </p>
      </Panel>

      <div className="guide-grid">
        {topics.map((topic) => (
          <Panel key={topic.id} title={topic.title}>
            <div className="guide-topic">
              <div>
                <div className="eyebrow">What it is</div>
                <p className="panel-copy">{topic.what}</p>
              </div>
              <div>
                <div className="eyebrow">Why it matters</div>
                <p className="panel-copy">{topic.why}</p>
              </div>
              <div>
                <div className="eyebrow">How to read it here</div>
                <p className="panel-copy">{topic.how}</p>
              </div>
            </div>
          </Panel>
        ))}
      </div>
    </div>
  );
}
