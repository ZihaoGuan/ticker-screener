const allowedExchanges = new Set(['NYSE', 'NASDAQ', 'NYSEARCA', 'AMEX', 'NYSEAMERICAN', 'ARCA']);

export function exactUsStocks(results, ticker) {
  const rows = Array.isArray(results) ? results : results?.results ?? results?.symbols ?? [];
  return [...new Set(rows.filter(row => {
    const symbol = row?.symbol ?? row?.ticker;
    const exchange = row?.exchange ?? row?.exchange_id;
    const type = String(row?.type ?? row?.kind ?? '').toLowerCase();
    return symbol === ticker && allowedExchanges.has(String(exchange ?? '').toUpperCase()) &&
      (type === 'stock' || type === 'equity');
  }).map(row => `${String(row.exchange ?? row.exchange_id).toUpperCase()}:${ticker}`))];
}
