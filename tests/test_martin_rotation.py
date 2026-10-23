from __future__ import annotations

import copy
import datetime as dt
import unittest
from contextlib import contextmanager
from unittest.mock import patch
from types import SimpleNamespace

import pandas as pd

from src.trade_plan_policies import STRUCTURAL_STOP_POLICY, structural_plan_payload
from src.webapp.services.martin_forward_test_service import MartinForwardTestService, RS_EXPANDED_STRATEGY_ID
from src.webapp.services.martin_rotation import (ROTATION_STRATEGY_ID, account_budget, metrics, replacement_metrics,
                                                rotation_decision, size_order, valid_history)
from tests.test_martin_forward_test_service import _ForwardRepo


class RotationRepo(_ForwardRepo):
    def __init__(self):
        super().__init__()
        self.summaries = {}

    @contextmanager
    def atomic_session(self, strategy_id):
        before = copy.deepcopy(self.__dict__)
        try:
            yield
        except Exception:
            self.__dict__ = before
            raise

    def latest_session_date(self, strategy_id):
        return max((key[1] for key in self.runs if key[0] == strategy_id), default=None)

    def account_state(self, strategy_id, exclude_order_ticker=None):
        positions = [row for row in self.positions if row['strategy_id'] == strategy_id]
        realized = sum(float(row['payload_json'].get('realized_pnl') or 0) for row in positions)
        return account_budget([row for row in positions if row['status'] == 'open'],
                              [row for row in self.pending_orders(strategy_id=strategy_id) if row['ticker'] != exclude_order_ticker], realized)

    def last_rotation_exit(self, strategy_id):
        return max((key[2] for key in self.events if key[1] == 'rotation_exit'), default=None)

    def set_run_metadata(self, run_id, metadata):
        self.summaries.setdefault(run_id, {}).update(metadata)

    def update_run_summary(self, **kwargs):
        self.set_run_metadata(kwargs['run_id'], self.account_state(kwargs['strategy_id']))

    def save_session(self, **kwargs):
        run_id = super().save_session(**kwargs)
        self.summaries.setdefault(run_id, {}).update(kwargs['summary'])
        return run_id


class RotationTests(unittest.TestCase):
    def setUp(self):
        self.day = dt.date(2026, 10, 12)
        self.index = pd.bdate_range(end='2026-10-14', periods=82)
        def frame(values):
            close = pd.Series(values, index=self.index, dtype=float)
            return pd.DataFrame({'Open': close - .05, 'High': close + .2, 'Low': close - .2, 'Close': close, 'Volume': 1_000_000.}, index=self.index)
        self.frames = {'OLD': frame([100.] * 82), 'NEW': frame([50 + .7 * i for i in range(82)]),
                       'SPY': frame([90 + .7 * i for i in range(82)]), 'QQQ': frame([95 + .7 * i for i in range(82)])}
        self.dates = self.dates_for(self.day)
        self.repo = RotationRepo()
        self.service = MartinForwardTestService(self.repo, bar_loader=self.load)

    def dates_for(self, day):
        return [index.date() for index in self.index if index.date() <= day][-35:]

    def load(self, tickers, day, days, **kwargs):
        return {ticker: self.frames[ticker].loc[self.frames[ticker].index.date <= day].tail(days) for ticker in tickers if ticker in self.frames}

    def position(self, ticker='OLD', *, trimmed=False, shares=100, entry=100., stop=95.):
        p = {'ticker': ticker, 'entry_price': entry, 'stop': stop, 'original_stop': stop, 'initial_r': entry-stop,
             'initial_r_per_share': entry-stop, 'initial_shares': shares, 'shares': shares, 'remaining_shares': shares,
             'max_favorable_r': 0., 'holding_sessions': 11, 'trimmed': trimmed, 'realized_pnl': 0., 'realized_r': 0.}
        ident = self.repo.reserve_position(ticker=ticker, opened_on=self.index[-12].date(), payload=p, strategy_id=ROTATION_STRATEGY_ID)
        return next(row for row in self.repo.positions if row['id'] == ident)

    def full_book(self):
        self.position()
        for i in range(7):
            ticker = f'KEEP{i}'
            self.frames[ticker] = self.frames['OLD'].copy()
            self.position(ticker, trimmed=True, shares=10)

    def row(self, day=None, ticker='NEW', rs=95, stop_distance=1.):
        day = day or self.day
        close = float(self.frames[ticker].loc[pd.Timestamp(day), 'Close'])
        row = {'ticker': ticker, 'scanner_ids': ['rs'], 'daily_rs_rating': rs, 'market_cap': 2e9,
               'close': close, 'adr14_pct': 4., 'trade_readiness': 'defined_risk', 'pullback_quality': {'state': 'neutral'},
               'earnings_date': '2026-11-10', 'trade_plan': {'source_strategy_id': 'ema21_pullback_buy', 'state': 'active',
                   'signal_date': str(day), 'trigger_price': close, 'stop_price': close-stop_distance, 'max_entry_price': close+1}}
        row['trade_plans'] = {STRUCTURAL_STOP_POLICY: structural_plan_payload(row, day, self.frames[ticker].loc[:str(day)])}
        return row

    def snapshot(self, day=None, rows=None, fingerprint='fp'):
        day = day or self.day
        return {'target_trading_date': str(day), '_snapshot_run_id': 10, 'source_fingerprint': fingerprint,
                'guru_board': {'rows': [self.row(day)] if rows is None else rows}}

    def run_day(self, day=None, snapshot=None):
        day = day or self.day
        with patch('src.webapp.services.martin_forward_test_service.session_dates', side_effect=self.dates_for), \
             patch('src.webapp.services.martin_forward_test_service.require_completed_session'):
            return self.service.run_session(as_of_date=day, snapshot=snapshot or self.snapshot(day), strategy_id=ROTATION_STRATEGY_ID)

    def classified(self):
        from src.webapp.services.martin_forward_test_service import classify_rs_expanded_candidate
        row = self.row()
        candidate = classify_rs_expanded_candidate(row, 1, self.day, self.frames)
        candidate['status'] = 'waiting_capacity'
        return row, candidate

    def decide(self, **overrides):
        row, candidate = self.classified()
        params = dict(day=self.day, positions=self.repo.open_positions(strategy_id=ROTATION_STRATEGY_ID), candidates=[candidate],
                      rows=[row], frames=self.frames, last_rotation=None, dates=self.dates)
        params.update(overrides)
        return rotation_decision(**params)

    def test_policy_schedules_and_guards_each_outgoing_condition(self):
        position = self.position()
        self.assertEqual(self.decide()['status'], 'scheduled')
        for key, value in [('trimmed', True), ('exit_next_open', True), ('max_favorable_r', 1), ('entry_price', 96)]:
            original = copy.deepcopy(position['payload_json'])
            position['payload_json'][key] = value
            self.assertEqual(self.decide()['reason'], 'rotation_no_stalled_holding', key)
            position['payload_json'] = original
        position['opened_on'] = self.index[-5].date()
        position['payload_json']['holding_sessions'] = 2
        self.assertEqual(self.decide()['reason'], 'rotation_no_stalled_holding')

    def test_incoming_gates_market_cooldown_and_missing_evidence(self):
        self.position()
        row, candidate = self.classified()
        for changes in ({'earnings_date': None}, {'earnings_date': str(self.day+dt.timedelta(days=7))}):
            self.assertIsNone(replacement_metrics(candidate, {**row, **changes}, self.frames['NEW'], self.day, self.dates))
        for changes in ({'rs_tier': 'emerging'}, {'signal_date': '2026-10-09'}, {'limit_price': 1}):
            self.assertIsNone(replacement_metrics({**candidate, **changes}, row, self.frames['NEW'], self.day, self.dates))
        low_volume = self.frames['NEW'].assign(Volume=1)
        self.assertIsNone(replacement_metrics(candidate, row, low_volume, self.day, self.dates))
        self.assertEqual(self.decide(last_rotation=self.dates[-5])['reason'], 'rotation_cooldown')
        self.assertEqual(self.decide(last_rotation=self.dates[-5])['cooldown_sessions_remaining'], 1)
        self.assertEqual(self.decide(last_rotation=self.dates[-6])['status'], 'scheduled')
        self.assertEqual(self.decide(last_rotation=self.dates[-6])['cooldown_sessions_remaining'], 0)
        self.assertEqual(self.decide(frames={})['reason'], 'rotation_benchmark_data_missing')
        bad_market = {**self.frames, 'QQQ': self.frames['OLD'].copy()}
        self.assertEqual(self.decide(frames=bad_market)['reason'], 'rotation_market_regime')

    def test_full_schedule_fill_then_manage_following_session(self):
        self.full_book()
        result = self.run_day()
        self.assertEqual(result['rotation_scheduled'], 1)
        self.assertEqual(len(self.repo.open_positions(strategy_id=ROTATION_STRATEGY_ID)), 8)
        next_day = dt.date(2026, 10, 13)
        result = self.run_day(next_day)
        self.assertEqual(result['rotation_exited'], 1)
        self.assertEqual(result['filled'], 1)
        incoming = next(row for row in self.repo.positions if row['ticker'] == 'NEW')
        self.assertNotIn('rotation_intent', incoming['payload_json'])
        self.assertIn('rotation_entry_intent', incoming['payload_json'])
        result = self.run_day(dt.date(2026, 10, 14))
        self.assertFalse(result['partial'])
        self.assertEqual(incoming['status'], 'open')
        self.assertEqual(len(self.repo.open_positions(strategy_id=ROTATION_STRATEGY_ID)), 8)

    def test_same_day_new_fingerprint_does_not_repeat_and_missing_data_rolls_back(self):
        self.full_book()
        self.run_day()
        before = copy.deepcopy(self.repo.__dict__)
        result = self.run_day(snapshot=self.snapshot(fingerprint='changed'))
        self.assertTrue(result['skipped'])
        self.assertEqual(self.repo.__dict__, before)
        self.frames['OLD'] = self.frames['OLD'].drop(pd.Timestamp('2026-10-13'))
        with self.assertRaisesRegex(ValueError, 'execution history'):
            self.run_day(dt.date(2026, 10, 13))
        self.assertEqual(self.repo.__dict__, before)

    def test_gap_above_max_expires_after_exit_and_cooldown_starts(self):
        self.full_book()
        self.run_day()
        day = dt.date(2026, 10, 13)
        self.frames['NEW'].loc[str(day), ['Open', 'High', 'Low', 'Close']] = [120, 121, 119, 120]
        result = self.run_day(day)
        self.assertEqual(result['rotation_exited'], 1)
        self.assertEqual(result['filled'], 0)
        self.assertEqual(result['expired'], 1)
        self.assertEqual(self.repo.last_rotation_exit(ROTATION_STRATEGY_ID), day)
        self.assertEqual(len(self.repo.open_positions(strategy_id=ROTATION_STRATEGY_ID)), 7)

    def test_protective_open_exit_funds_pair_without_rotation_relabel(self):
        self.full_book()
        self.run_day()
        day = dt.date(2026, 10, 13)
        self.frames['OLD'].loc[str(day), ['Open', 'High', 'Low', 'Close']] = [94, 94.5, 93, 94]
        result = self.run_day(day)
        self.assertEqual(result['rotation_exited'], 0)
        self.assertEqual(result['filled'], 1)
        self.assertIsNone(self.repo.last_rotation_exit(ROTATION_STRATEGY_ID))
        self.assertTrue(any(key[1] == 'gap_stop' for key in self.repo.events))

    def test_eight_slot_limit_and_unused_cash_does_not_force_rotation(self):
        self.full_book()
        self.repo.positions[0]['payload_json']['trimmed'] = True
        result = self.run_day()
        self.assertEqual(result['rotation_scheduled'], 0)
        self.assertEqual(self.repo.candidates[0]['status'], 'waiting_capacity')
        self.assertEqual(self.repo.summaries[result['run_id']]['rotation']['reason'], 'rotation_no_stalled_holding')

    def test_equal_rs_weaker_pending_is_replaced_before_a_holding(self):
        self.full_book()
        self.repo.positions.pop()
        self.frames['SLOW'] = self.frames['OLD'].copy()
        result = self.run_day(snapshot=self.snapshot(rows=[self.row(ticker='SLOW'), self.row()]))
        states = {row['ticker']: row['status'] for row in self.repo.candidates}
        self.assertEqual(states, {'SLOW': 'cancelled', 'NEW': 'pending'})
        self.assertEqual(result['rotation_scheduled'], 0)
        self.assertNotIn('rotation_intent', self.repo.positions[0]['payload_json'])
        self.assertEqual(self.repo.summaries[result['run_id']]['rotation']['status'], 'pending_replaced')

    def test_replacement_gap_below_stop_keeps_inherited_cancellation(self):
        self.full_book()
        self.run_day()
        day = dt.date(2026, 10, 13)
        self.frames['NEW'].loc[str(day), ['Open', 'High', 'Low', 'Close']] = [90, 91, 89, 90]
        result = self.run_day(day)
        self.assertEqual(result['cancelled'], 1)
        self.assertEqual(result['filled'], 0)
        self.assertEqual(self.repo.candidates[0]['status'], 'cancelled')

    def test_intraday_entry_stop_cannot_release_capacity_before_other_entry_sizing(self):
        self.frames['A'] = self.frames['NEW'].copy()
        self.run_day(snapshot=self.snapshot(rows=[self.row(ticker='A'), self.row()]))
        day = dt.date(2026, 10, 13)
        self.frames['A'].loc[str(day), ['Low']] = 90
        observed = []
        original = self.service._rotation_execution_order
        def track(order, *args):
            if order['ticker'] == 'NEW':
                observed.extend(row['status'] for row in self.repo.positions if row['ticker'] == 'A')
            return original(order, *args)
        with patch.object(self.service, '_rotation_execution_order', side_effect=track):
            self.run_day(day)
        self.assertEqual(observed, ['open'])
        self.assertEqual(next(row['status'] for row in self.repo.positions if row['ticker'] == 'A'), 'closed')

    def test_budget_risk_allocation_cash_and_contingent_reservations(self):
        candidate = {'limit_price': 100, 'stop': 95, 'rs_tier': 'preferred'}
        self.assertEqual(size_order(candidate, {'available_cash': 100_000, 'available_risk': 2_500}), 100)
        self.assertEqual(size_order({**candidate, 'stop': 99}, {'available_cash': 100_000, 'available_risk': 2_500}), 200)
        self.assertEqual(size_order(candidate, {'available_cash': 250, 'available_risk': 2_500}), 2)
        self.assertEqual(size_order(candidate, {'available_cash': 100_000, 'available_risk': 10}), 2)
        p = self.position()
        normal = {'ticker': 'A', 'payload_json': {'shares': 10, 'limit_price': 100, 'stop': 95}}
        contingent = {'ticker': 'B', 'payload_json': {**normal['payload_json'], 'rotation_intent': {'outgoing': p['id']}}}
        budget = account_budget([p], [normal, contingent], -500)
        self.assertEqual(budget['available_cash'], 88_500)
        self.assertEqual(budget['committed_stop_risk'], 550)

    def test_newbook_aggregate_budget_binds_before_eight_fresh_risk_positions(self):
        rows = []
        for i in range(8):
            ticker = f'N{i}'
            self.frames[ticker] = self.frames['NEW'].copy()
            rows.append(self.row(ticker=ticker, stop_distance=5))
        self.run_day(snapshot=self.snapshot(rows=rows))
        orders = self.repo.pending_orders(strategy_id=ROTATION_STRATEGY_ID)
        self.assertLessEqual(len(orders), 8)
        self.assertLessEqual(self.repo.account_state(ROTATION_STRATEGY_ID)['committed_stop_risk'], 2500)
        self.assertTrue(any(row['status'] == 'waiting_capacity' for row in self.repo.candidates))

    def test_bad_candidate_is_individual_block_and_other_books_do_not_rotate(self):
        self.frames['NEW'] = self.frames['NEW'].drop(pd.Timestamp(self.dates[-3]))
        self.run_day()
        self.assertEqual(self.repo.candidates[0]['status'], 'blocked_data')
        baseline = RotationRepo()
        service = MartinForwardTestService(baseline, bar_loader=self.load)
        service.run_session(as_of_date=self.day, snapshot=self.snapshot(), strategy_id=RS_EXPANDED_STRATEGY_ID)
        self.assertFalse(any('rotation' in key[1] for key in baseline.events))

    def test_mismatched_snapshot_and_missed_session_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'exact session'):
            self.run_day(snapshot=self.snapshot(day=dt.date(2026, 10, 9)))
        self.run_day()
        before = copy.deepcopy(self.repo.__dict__)
        with self.assertRaisesRegex(ValueError, 'missed intermediate'):
            self.run_day(dt.date(2026, 10, 14))
        self.assertEqual(self.repo.__dict__, before)

    def test_history_missing_zero_volume_and_short_metrics_fail_closed(self):
        self.assertFalse(valid_history(self.frames['NEW'].assign(Volume=0), self.dates))
        self.assertIsNone(metrics(self.frames['NEW'], self.day, self.dates[-5:]))

    def test_repository_atomic_wrapper_holds_lock_and_never_inner_commits(self):
        from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
        class Connection:
            def __init__(self):
                self.enters, self.committed, self.rolled_back, self.sql, self.lock = 0, False, False, [], True
            def __enter__(self): self.enters += 1; return self
            def __exit__(self, kind, *_args):
                self.committed, self.rolled_back = kind is None, kind is not None
                return False
            def cursor(self): return Cursor(self)
        class Cursor:
            def __init__(self, connection): self.connection = connection
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, sql, params=()): self.connection.sql.append((sql, params))
            def fetchone(self): return (self.connection.lock,)
        for failure in (False, True):
            connection = Connection()
            repository = MartinForwardTestRepository(database_url='postgresql://unused')
            repository._schema_ready = True
            with patch.dict('sys.modules', {'psycopg': SimpleNamespace(connect=lambda *_args: connection)}):
                def run():
                    with repository.atomic_session(ROTATION_STRATEGY_ID):
                        with repository._connect() as borrowed:
                            borrowed.cursor().execute('SELECT 1')
                        self.assertFalse(connection.committed)
                        if failure: raise ValueError('abort')
                if failure:
                    with self.assertRaisesRegex(ValueError, 'abort'): run()
                else:
                    run()
            self.assertEqual(connection.enters, 1)
            self.assertEqual(connection.rolled_back, failure)
            self.assertEqual(connection.committed, not failure)
            self.assertIsNone(repository._session_connection)
            self.assertIn('pg_try_advisory_xact_lock', connection.sql[0][0])
            self.assertEqual(connection.sql[0][1], (f'martin-forward:{ROTATION_STRATEGY_ID}',))


if __name__ == '__main__':
    unittest.main()
