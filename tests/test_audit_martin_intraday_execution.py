import datetime as dt
import importlib.util
import sys
import subprocess
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'audit_martin_intraday_execution.py'
spec = importlib.util.spec_from_file_location('intraday_audit', SCRIPT)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class IntradayAuditTests(unittest.TestCase):
    day = dt.date(2026, 10, 23)
    symbol = 'NASDAQ:TEST'
    metadata = {'full_name': symbol}

    def test_script_adds_repo_root_to_import_path_when_run_outside_repo(self):
        script = str(SCRIPT)
        root = str(SCRIPT.parents[1])
        probe = (
            'import runpy, sys; '
            f'ns = runpy.run_path({script!r}); '
            f'assert {root!r} in sys.path'
        )
        with tempfile.TemporaryDirectory() as temp:
            result = subprocess.run([sys.executable, '-c', probe], cwd=temp,
                                    text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_session_starts_uses_calendar_without_empty_date_range(self):
        class Calendar:
            def is_session(self, day):
                return True
            def date_to_session(self, day, direction):
                self.asserted_direction = direction
                return day
            def session_open(self, session):
                return Timestamp(dt.datetime(2026, 10, 23, 13, 30, tzinfo=dt.timezone.utc))
            def session_close(self, session):
                return Timestamp(dt.datetime(2026, 10, 23, 20, 0, tzinfo=dt.timezone.utc))
        class Timestamp:
            def __init__(self, value): self.value = value
            def to_pydatetime(self): return self.value
        calendar = Calendar()
        fake_xcals = types.SimpleNamespace(get_calendar=lambda name: calendar)
        with patch.dict(sys.modules, {'exchange_calendars': fake_xcals}):
            starts = audit.session_starts(self.day)
        self.assertEqual(calendar.asserted_direction, 'none')
        self.assertEqual(len(starts), 26)

    def bars(self, lows, *, opens=None):
        starts = list(range(1000, 1026))
        rows = []
        for i, timestamp in enumerate(starts):
            low = lows[i] if i < len(lows) else 11
            op = opens[i] if opens and i < len(opens) else 12
            rows.append({'time': timestamp, 'open': op, 'high': 13, 'low': low, 'close': 12, 'volume': 100})
        return rows

    def classify(self, bars, **kw):
        original = audit.session_starts
        audit.session_starts = lambda _day, interval=15: list(range(1000, 1026))
        daily = kw.pop('daily', {'open': bars[0]['open'], 'high': 13, 'low': min(x['low'] for x in bars),
                 'close': bars[-1]['close'], 'volume': 100*len(bars)})
        try:
            return audit.classify(candles=bars, metadata=kw.pop('metadata', self.metadata),
                requested_symbol=self.symbol, day=self.day, entry=kw.pop('entry', 11.5), stop=10.5,
                daily=daily, **kw)['classification']
        finally:
            audit.session_starts = original

    def test_entry_touch_before_stop(self):
        bars = self.bars([11.4, 10.4] + [11]*24)
        self.assertEqual(self.classify(bars), 'stop_after_entry')

    def test_entry_touch_without_later_stop_is_a_fill(self):
        bars = self.bars([11.4] + [11]*25)
        self.assertEqual(self.classify(bars), 'entry_before_stop')

    def test_entry_and_stop_in_same_bar_is_ambiguous(self):
        bars = self.bars([10.4] + [11]*25)
        self.assertEqual(self.classify(bars), 'same_bar_ambiguous')

    def test_open_below_stop_is_stop_before_entry(self):
        bars = self.bars([10.4] + [11]*25, opens=[10.2])
        self.assertEqual(self.classify(bars), 'stop_before_entry')

    def test_missing_session_coverage(self):
        bars = self.bars([11.4])[:-1]
        self.assertEqual(self.classify(bars), 'missing_bars')

    def test_substitute_feed_is_not_certified(self):
        bars = self.bars([11.4] + [11]*25)
        daily = {'open': 12, 'high': 13, 'low': 12, 'close': 12, 'volume': 2600}
        self.assertEqual(self.classify(bars, metadata={'full_name':'BATS:TEST'}, daily=daily), 'feed_mismatch')

    def test_bats_feed_is_accepted_when_covered_and_range_and_open_fill_agree(self):
        bars = self.bars([11.4] + [11]*25)
        daily = {'open': 12, 'high': 13, 'low': 11, 'close': 12, 'volume': 2600}
        original = audit.session_starts
        audit.session_starts = lambda _day, interval=15: list(range(1000, 1026))
        try:
            result = audit.classify(candles=bars, metadata={'full_name':'BATS:TEST'},
                requested_symbol=self.symbol, day=self.day, entry=11.5, stop=10.5, daily=daily)
        finally:
            audit.session_starts = original
        self.assertEqual(result['classification'], 'entry_before_stop')
        self.assertFalse(result['feed_mismatch'])

    def test_missing_daily_bar_never_certifies_sequence(self):
        result = audit.classify(candles=self.bars([11.4] + [11]*25), metadata=self.metadata,
            requested_symbol=self.symbol, day=self.day, entry=11.5, stop=10.5, daily=None)
        self.assertEqual(result['classification'], 'missing_bars')
        self.assertEqual(result['reason'], 'stored_daily_bar_missing')

    def test_load_fills_query_selects_only_closed_same_day_limit_stop_fills(self):
        class Cursor:
            query = ''
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def execute(self, query, params): self.query = query
            def fetchall(self): return []
        class Connection:
            cursor_obj = Cursor()
            def cursor(self): return self.cursor_obj
        conn = Connection()
        audit.load_fills(conn, 'martin_range_bar_retest_test', self.day, self.day)
        sql = ' '.join(conn.cursor_obj.query.split())
        self.assertIn("p.status='closed'", sql)
        self.assertIn('p.opened_on=e.event_date AND p.closed_on=e.event_date', sql)
        self.assertIn("e.payload_json->>'same_day_stop'='true'", sql)
        self.assertIn("(e.payload_json->>'price')::numeric", sql)
        self.assertIn('d.open > COALESCE', sql)

    def test_output_dir_refuses_to_overwrite_existing_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'audit'
            output.mkdir()
            (output / 'audit.json').write_text('{"prior":"evidence"}')
            with self.assertRaisesRegex(RuntimeError, 'output_directory_not_empty'):
                audit.prepare_output_dir(output)
            self.assertEqual((output / 'audit.json').read_text(), '{"prior":"evidence"}')

    def test_provider_failure_is_sanitized_and_does_not_escape(self):
        with patch.object(audit, 'fetch', side_effect=RuntimeError('secret session token')):
            result = audit.fetch_safely('/client', self.symbol, self.day.isoformat(), '15')
        self.assertIsNone(result)

    def test_cli_requires_execution_book_prefix(self):
        with self.assertRaises(SystemExit) as raised:
            audit.main(['--book-id','wrong','--from-date','2026-10-23','--to-date','2026-10-23',
                        '--client-dir','/tmp/client','--output-dir','/tmp/audit'])
        self.assertEqual(raised.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
