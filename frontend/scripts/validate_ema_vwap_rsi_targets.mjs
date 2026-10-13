import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { PineEngine } from "@luxalgo/vela-pinets";

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const pineSource = await readFile(resolve(scriptDirectory, "../../scripts/pine/ema_vwap_rsi_targets.pine"), "utf8");
const bars = Array.from({ length: 700 }, (_, index) => {
  const close = 100 + index * 0.05 + Math.sin(index / 8) * 9;
  const open = close - Math.cos(index / 5) * 1.5;
  return { time: 1_704_067_200 + index * 86_400, open, high: Math.max(open, close) + 2, low: Math.min(open, close) - 2, close, volume: 1_000_000 + index * 1_000 };
});

const engine = new PineEngine();
const prepared = await engine.prepare(pineSource, "ema-vwap-rsi-targets-validation");
let model;
await new Promise((resolve, reject) => {
  engine.execute(
    { prepared, market: { symbol: "TEST", timeframe: "1D" }, bars, mode: "static" },
    { onModel: (value) => { model = value; }, onError: reject, onDone: resolve },
  );
});
if (!model) throw new Error("Vela Pine engine produced no model.");
const summary = { series: model.series.length, fills: model.fills.length, lines: model.lines.length, labels: model.labels.length, barColors: model.barColors.length };

if (summary.series < 5 || summary.fills < 1 || summary.lines < 1 || summary.labels < 2 || summary.barColors < 1) {
  throw new Error(`Unexpected Vela Pine output: ${JSON.stringify(summary)}`);
}

console.log(`Validated 9/20 + VWAP + RSI targets: ${JSON.stringify(summary)}`);

const rsSourceTemplate = await readFile(resolve(scriptDirectory, "../../scripts/pine/ticker_screener_relative_strength.pine"), "utf8");
const rsSource = rsSourceTemplate
  .replace("__TICKER_SCREENER_BENCHMARK__", "SPY")
  .replace("__TICKER_SCREENER_BENCHMARK_ANCHOR__", "500")
  .replace("__TICKER_SCREENER_RS_RATING__", "97");
const benchmarkBars = bars.map((bar, index) => {
  const close = 500 + index * 0.03 + Math.sin(index / 15) * 3;
  return { ...bar, open: close - 1, high: close + 2, low: close - 2, close };
});
const rsPrepared = await engine.prepare(rsSource, "rs-rating-overlay-validation");
let rsModel;
await new Promise((resolve, reject) => {
  engine.execute(
    {
      prepared: rsPrepared,
      market: { symbol: "TEST", timeframe: "1D" },
      bars,
      mode: "static",
      fetchSeries: async () => benchmarkBars,
    },
    { onModel: (value) => { rsModel = value; }, onError: reject, onDone: resolve },
  );
});
const hasRatingLabel = rsModel?.labels.some((label) => label.text === "RS Rating\n97") ?? false;
if (!rsModel || rsModel.series.length < 1 || !hasRatingLabel) {
  throw new Error(`Unexpected RS Rating overlay output: ${JSON.stringify(rsModel ? { series: rsModel.series.length, labels: rsModel.labels.length, hasRatingLabel } : null)}`);
}

console.log(`Validated RS Rating overlay: ${JSON.stringify({ series: rsModel.series.length, labels: rsModel.labels.length })}`);
