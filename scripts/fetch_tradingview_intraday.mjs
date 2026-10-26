// Bounded read-only intraday request for one ticker and one US session day.
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { exactUsStocks } from './tradingview_symbol_resolution.mjs';
const [clientDir, symbol, day, timeframe='15', symbolMode='legacy'] = process.argv.slice(2);
if (!clientDir || !/^[A-Z0-9._-]+:[A-Z0-9._-]+$/.test(symbol ?? '') ||
    !/^\d{4}-\d{2}-\d{2}$/.test(day ?? '') || !['15','1'].includes(timeframe) || !['legacy','resolve'].includes(symbolMode)) {
  process.stderr.write('usage: node fetch_tradingview_intraday.mjs CLIENT_DIR EXCHANGE:SYMBOL YYYY-MM-DD [15|1] [legacy|resolve]\n');
  process.exit(64);
}
const load = file => import(pathToFileURL(path.join(clientDir, 'dist', file)).href);
const { TradingViewClient } = await load('client/client.js');
const { runOperation } = await load('data/operation.js');
const { planHistory, openMarket, loadHistory, selectCandles } = await load('data/history.js');
const credentials = process.env.TRADINGVIEW_SESSION && process.env.TRADINGVIEW_SIGNATURE
  ? {session:process.env.TRADINGVIEW_SESSION, signature:process.env.TRADINGVIEW_SIGNATURE} : undefined;
const from = new Date(`${day}T00:00:00Z`), to = new Date(`${day}T23:59:59Z`);
const client = new TradingViewClient({credentials});
const codeOf = error => error?.code ?? error?.error_code ?? 'PROVIDER_ERROR';
async function request(symbol) {
  const query = {...baseQuery, symbol};
  const plan = planHistory(query);
  return runOperation({...query, client}, 'history', ({client,resolve,reject}) => {
    const chart = client.createChart(); let metadata;
    chart.on('error', reject);
    chart.on('symbolLoaded', info => { metadata = info; });
    loadHistory(chart, plan, true, completion => resolve({metadata,completion,candles:selectCandles(chart.candles,plan)}));
    openMarket(chart,plan);
    return () => chart.delete();
  });
}
const baseQuery = {timeframe, from, to, maxCount:timeframe === '1' ? 500 : 100,
  session:'regular', adjustment:'splits', timezone:'America/New_York', timeoutMs:45000};
let resolvedSymbol = symbol, initialErrorCode = null;
try {
  let result;
  try { result = await request(symbol); }
  catch (error) {
    initialErrorCode = codeOf(error);
    if (initialErrorCode !== 'SYMBOL_ERROR' || symbolMode !== 'resolve') throw error;
    const ticker = symbol.slice(symbol.indexOf(':') + 1);
    const { searchMarkets } = await load('index.js');
    const matches = exactUsStocks(await searchMarkets(ticker, {type:'stock', country:'US'}), ticker);
    if (matches.length !== 1 || matches[0] === symbol) throw error;
    resolvedSymbol = matches[0];
    result = await request(resolvedSymbol);
  }
  const query = {...baseQuery, symbol:resolvedSymbol, from:from.toISOString(), to:to.toISOString()};
  process.stdout.write(JSON.stringify({query,requested_symbol:symbol,resolved_symbol:resolvedSymbol,
    symbol_resolution:{requested_symbol:symbol,resolved_symbol:resolvedSymbol,initial_error_code:initialErrorCode,
      retry_count:initialErrorCode ? 1 : 0},retrieved_at:new Date().toISOString(),...result})+'\n');
} catch (error) {
  process.stdout.write(JSON.stringify({error_code:codeOf(error),requested_symbol:symbol,
    resolved_symbol:resolvedSymbol,symbol_resolution:{requested_symbol:symbol,resolved_symbol,
      initial_error_code:initialErrorCode ?? codeOf(error),retry_count:initialErrorCode ? 1 : 0}})+'\n');
  process.exitCode = 2;
} finally { await client.close(); }
