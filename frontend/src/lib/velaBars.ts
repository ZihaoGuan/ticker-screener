import type { OHLCV } from "@luxalgo/vela";
import type { CandlePoint } from "./types";

/**
 * Converts the app's daily candle contract into Vela's offline-bar contract.
 *
 * Vela treats bar time as epoch milliseconds. Dates are deliberately anchored
 * at UTC midnight so a user's browser timezone cannot move a daily candle.
 */
export function toVelaBars(candles: CandlePoint[]): OHLCV[] {
  let previousTime = Number.NEGATIVE_INFINITY;

  return candles.map((candle) => {
    const time = Date.parse(`${candle.time}T00:00:00.000Z`);
    const values = [time, candle.open, candle.high, candle.low, candle.close, candle.volume];
    if (!values.every(Number.isFinite)) {
      throw new Error(`Invalid candle data for ${candle.time}.`);
    }
    if (time <= previousTime) {
      throw new Error("Chart candles must be sorted with unique dates.");
    }
    if (candle.high < Math.max(candle.open, candle.close) || candle.low > Math.min(candle.open, candle.close) || candle.volume < 0) {
      throw new Error(`Invalid OHLCV range for ${candle.time}.`);
    }
    previousTime = time;

    return {
      time,
      open: candle.open,
      high: candle.high,
      low: candle.low,
      close: candle.close,
      volume: candle.volume,
    };
  });
}
