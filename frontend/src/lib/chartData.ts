import type { CandlePoint, WatchlistChartResponse } from "./types";

export function buildChartCandles(payload: WatchlistChartResponse | null | undefined): CandlePoint[] {
  if (!payload) {
    return [];
  }
  const volumeByTime = new Map((payload.volume ?? []).map((item) => [item.time, item.value]));
  return (payload.candles ?? []).map((item) => ({
    ...item,
    volume: volumeByTime.get(item.time) ?? 0,
  }));
}

export function aggregateCandlesWeekly(candles: CandlePoint[]): CandlePoint[] {
  const weekly: CandlePoint[] = [];
  let activeWeek = "";
  for (const candle of candles) {
    const date = new Date(`${candle.time}T00:00:00Z`);
    const day = date.getUTCDay() || 7;
    date.setUTCDate(date.getUTCDate() - day + 1);
    const week = date.toISOString().slice(0, 10);
    const current = weekly[weekly.length - 1];
    if (!current || week !== activeWeek) {
      weekly.push({ ...candle });
      activeWeek = week;
      continue;
    }
    current.time = candle.time;
    current.high = Math.max(current.high, candle.high);
    current.low = Math.min(current.low, candle.low);
    current.close = candle.close;
    current.volume += candle.volume;
  }
  return weekly;
}

export function buildExponentialMovingAverage(candles: CandlePoint[], length: number): Array<{ time: string; value: number }> {
  if (candles.length === 0 || length <= 0) {
    return [];
  }
  const alpha = 2 / (length + 1);
  let ema = candles[0].close;
  const points = [{ time: candles[0].time, value: Number(ema.toFixed(2)) }];
  for (let index = 1; index < candles.length; index += 1) {
    ema = candles[index].close * alpha + ema * (1 - alpha);
    points.push({ time: candles[index].time, value: Number(ema.toFixed(2)) });
  }
  return points;
}

export function buildSimpleMovingAverage(candles: CandlePoint[], length: number): Array<{ time: string; value: number }> {
  if (candles.length === 0 || length <= 0) {
    return [];
  }
  let rollingTotal = 0;
  return candles.map((candle, index) => {
    rollingTotal += candle.close;
    if (index >= length) {
      rollingTotal -= candles[index - length].close;
    }
    return {
      time: candle.time,
      value: Number((rollingTotal / Math.min(index + 1, length)).toFixed(2)),
    };
  });
}
