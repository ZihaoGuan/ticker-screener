import datetime as dt
from contextlib import nullcontext

import pandas as pd

from src.martin_bar_retest import evaluate
from src.webapp.services import martin_bar_retest_service as service_module
from src.webapp.services.martin_bar_retest_service import MartinBarRetestService
from scripts.run_martin_bar_retest import _load_source


def _bars(count=130):
    dates = pd.bdate_range("2025-01-02", periods=count)
    close = [20 + i * .03 for i in range(count)]
    return pd.DataFrame({"Open": close, "High": [x + .2 for x in close], "Low": [x - .2 for x in close],
                         "Close": close, "Volume": [2_000_000] * count}, index=dates)


def test_source_loader_unwraps_persisted_screen_hit_payload():
    class History:
        def list_runs(self, **_kwargs): return [{"id": 7, "strategy_id": "martin_bar_watchlist_v1", "created_at": "2025-06-01T20:00:00+00:00"}]
        def get_run(self, _id, *, include_hits=False, **_kwargs):
            if include_hits:
                return {"hits": [{"ticker": "ABC", "rank": 1,
                    "metrics_json": {"adr14_pct": 4.2},
                    "hit_payload_json": {"discovery_categories": ["leader_30_session"], "rules_version": "martin_bar_watchlist_v1"}}]}
            return {"id": 7, "strategy_id": "martin_bar_watchlist_v1", "run_date": "2025-06-01",
                    "created_at": "2025-06-01T20:00:00+00:00", "config_hash": "cfg", "scope_hash": "scope",
                    "hit_count": 1, "result_summary_json": {"date_label": "2025-06-01", "total_tickers": 20,
                    "passed_tickers": 1, "failed_tickers": 19}}
    source = _load_source(History(), dt.date(2025, 6, 1))
    assert source["rules_version"] == "martin_bar_watchlist_v1"
    assert source["hits"][0]["discovery_categories"] == ["leader_30_session"]
    assert source["hits"][0]["adr14_pct"] == 4.2
    assert source["source_fingerprint"]


def test_evaluator_uses_only_asof_bars_and_blocks_missing_or_invalid_history():
    frame = _bars()
    day = frame.index[-1].date()
    sessions = [x.date() for x in frame.index]
    event = {"ticker": "ABC", "event_date": sessions[-2].isoformat()}
    future = frame.iloc[[-1]].copy()
    future.index = [frame.index[-1] + pd.Timedelta(days=1)]
    future.loc[:, ["Open", "High", "Low", "Close", "Volume"]] = [1000, 2000, 500, 1500, 9_000_000]
    result = evaluate(pd.concat([frame, future]), event, day, sessions)
    without_future = evaluate(frame, event, day, sessions)
    assert result["status"] in {"watch", "support_zone", "confirmed", "waiting_risk", "retired"}
    assert result == without_future
    assert evaluate(frame.iloc[:-1], event, day, sessions)["status"] == "blocked_data"
    bad = frame.copy(); bad.iloc[-1, bad.columns.get_loc("Low")] = 0
    assert evaluate(bad, event, day, sessions)["reasons"] == ["invalid_or_duplicate_ohlcv"]
    bad.iloc[-1, bad.columns.get_loc("Low")] = None
    assert evaluate(bad, event, day, sessions)["reasons"] == ["invalid_or_duplicate_ohlcv"]
    zero_volume = frame.copy(); zero_volume.iloc[-3, zero_volume.columns.get_loc("Volume")] = 0
    assert evaluate(zero_volume, event, day, sessions)["status"] != "blocked_data"
    extra = frame.iloc[[0]].copy()
    extra.index = [frame.index[0] + pd.Timedelta(days=2)]
    assert evaluate(pd.concat([frame, extra]), event, day, sessions)["status"] == "blocked_data"
    gap = evaluate(frame, {**event, "gap_low": 30}, day, sessions)
    assert gap["status"] == "retired" and gap["reasons"] == ["gap_low_close_failed"]


def test_first_episode_reclaims_chosen_fast_ema_and_five_percent_boundary():
    frame = _bars(130)
    sessions = [x.date() for x in frame.index]
    frame[["Open", "High", "Low", "Close"]] *= 1.25
    # Broad prior ranges make the configured quarter-ADR support band measurable.
    frame.iloc[-15:, frame.columns.get_loc("High")] += 8
    close = float(frame.iloc[-2]["Close"] + .2)
    frame.iloc[-2, frame.columns.get_loc("Low")] = close - .8
    frame.iloc[-1, frame.columns.get_loc("Open")] = close - .3
    frame.iloc[-1, frame.columns.get_loc("Low")] = close * .95
    frame.iloc[-1, frame.columns.get_loc("Close")] = close
    frame.iloc[-1, frame.columns.get_loc("High")] = close + 1
    event = {"ticker": "ABC", "event_date": sessions[-5].isoformat()}
    exact = evaluate(frame, event, sessions[-1], sessions)
    assert exact["status"] == "confirmed"
    assert exact["selected_ema_period"] == 9
    assert abs(exact["reference_risk_pct"] - 5) < 1e-9
    frame.iloc[-1, frame.columns.get_loc("Low")] = close * .949
    too_wide = evaluate(frame, event, sessions[-1], sessions)
    assert too_wide["status"] == "waiting_risk"


def test_first_episode_expires_after_five_sessions():
    frame = _bars(130)
    sessions = [x.date() for x in frame.index]
    frame.iloc[-14:, frame.columns.get_loc("High")] += 2
    frame.iloc[-7, frame.columns.get_loc("Low")] = float(frame.iloc[-7]["Close"] - .5)
    event = {"ticker": "ABC", "event_date": sessions[-9].isoformat()}
    result = evaluate(frame, event, sessions[-1], sessions)
    assert result["status"] == "retired"
    assert result["reasons"] == ["first_support_episode_expired"]


def test_missing_pre_event_session_in_required_calculation_window_blocks():
    frame = _bars(210)
    sessions = [x.date() for x in frame.index]
    event_date = sessions[-30]
    missing_position = len(sessions) - 125  # Outside latest 120, but inside event warmup.
    incomplete = frame.drop(frame.index[missing_position])
    result = evaluate(incomplete, {"ticker": "ABC", "event_date": event_date.isoformat()}, sessions[-1], sessions)
    assert result["status"] == "blocked_data"
    assert result["event_date"] == event_date.isoformat()
    assert result["event_identity"] == {}


def test_older_optional_bars_do_not_change_indicator_seed_or_decision():
    frame = _bars(210)
    sessions = [x.date() for x in frame.index]
    event_date = sessions[-30]
    event_position = sessions.index(event_date)
    required_start = event_position - 119
    event = {"ticker": "ABC", "event_date": event_date.isoformat()}
    baseline = evaluate(frame, event, sessions[-1], sessions)
    changed_older = frame.copy()
    changed_older.iloc[:required_start, changed_older.columns.get_loc("Close")] += 50
    changed_older.iloc[:required_start, changed_older.columns.get_loc("Open")] += 50
    changed_older.iloc[:required_start, changed_older.columns.get_loc("High")] += 50
    changed_older.iloc[:required_start, changed_older.columns.get_loc("Low")] += 50
    assert evaluate(changed_older, event, sessions[-1], sessions) == baseline


class Repo:
    def __init__(self): self.state = {}; self.saved = []
    def latest_gap_watch_states(self, _): return self.state
    def atomic_session(self, _): return nullcontext()
    def session_run_id(self, *_): return None
    def latest_session_date(self, *_): return None
    def save_session(self, **kwargs): self.saved.append(kwargs)


def test_repeated_leader_and_zero_hit_day_keep_original_event(monkeypatch):
    frame = _bars(130)
    dates = [x.date() for x in frame.index[-10:]]
    monkeypatch.setattr(service_module, "session_dates", lambda *_: dates)
    monkeypatch.setattr(service_module, "require_completed_session", lambda *_: None)
    repo = Repo()
    svc = MartinBarRetestService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
    day = dates[-1]
    old = {"ticker": "ABC", "event_date": "2025-01-02", "event_identity": {"event_date": "2025-01-02"},
           "source_run_id": 1, "category": "potent_mover"}
    repo.state = {"ABC": {**old, "status": "watch"}}
    leader = {"target_trading_date": day.isoformat(), "rules_version": "martin_bar_watchlist_v1", "source_run_id": 2,
              "hits": [{"ticker": "ABC", "discovery_categories": ["leader_30_session"]}]}
    result = svc._run(day, leader, dates, True)
    assert result["candidates"][0]["event_identity"] == old["event_identity"]
    empty = {"target_trading_date": day.isoformat(), "rules_version": "martin_bar_watchlist_v1", "hits": []}
    result = svc._run(day, empty, dates, True)
    assert result["active_event_count"] == 1
    repo.state = {"ABC": {**old, "status": "retired"}}
    result = svc._run(day, leader, dates, True)
    assert result["active_event_count"] == 0


def test_fresh_gap_replaces_event_and_apply_persists_zero_hit_session(monkeypatch):
    frame = _bars(130)
    dates = [x.date() for x in frame.index[-10:]]
    monkeypatch.setattr(service_module, "session_dates", lambda *_: dates)
    monkeypatch.setattr(service_module, "require_completed_session", lambda *_: None)
    repo = Repo()
    svc = MartinBarRetestService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
    day = dates[-1]
    source = {"target_trading_date": day.isoformat(), "rules_version": "martin_bar_watchlist_v1", "source_run_id": 9,
              "hits": [{"ticker": "ABC", "discovery_categories": ["gap_mover"], "low": 18.7}]}
    result = svc._run(day, source, dates, True)
    assert result["candidates"][0]["invalidation"] == {"basis": "original_gap_low", "price": 18.7}
    applied = svc._run(day, {"target_trading_date": day.isoformat(), "rules_version": "martin_bar_watchlist_v1", "hits": []}, dates, False)
    assert applied["zero_hit_day"] and len(repo.saved) == 1


def test_blocked_data_watch_survives_until_bars_recover():
    frame = _bars(130)
    dates = [x.date() for x in frame.index[-10:]]
    repo = Repo()
    identity = {"ticker": "ABC", "event_date": dates[-4].isoformat(), "category": "leader_30_session"}
    repo.state = {"ABC": {"ticker": "ABC", "event_date": dates[-4].isoformat(), "event_identity": identity,
                          "source_run_id": 1, "category": "leader_30_session", "status": "watch"}}
    available = {"bars": False}
    window = frame.loc[frame.index.date >= dates[0]]
    svc = MartinBarRetestService(repo, bar_loader=lambda *_a, **_k: {"ABC": window} if available["bars"] else {})
    blocked = svc._run(dates[-2], None, dates, True)["candidates"][0]
    assert blocked["status"] == "blocked_data"
    assert blocked["event_identity"] == identity
    repo.state = {"ABC": blocked}
    available["bars"] = True
    recovered = svc._run(dates[-1], None, dates, True)["candidates"][0]
    assert recovered["event_identity"] == identity
    assert recovered["status"] != "blocked_data"


def test_first_leader_hit_starts_watch_without_a_trade():
    frame = _bars(130)
    dates = [x.date() for x in frame.index]
    repo = Repo()
    service = MartinBarRetestService(repo, bar_loader=lambda *_a, **_k: {"ABC": frame})
    day = dates[-1]
    source = {"target_trading_date": day.isoformat(), "rules_version": "martin_bar_watchlist_v1",
              "source_run_id": 10, "hits": [{"ticker": "ABC", "discovery_categories": ["leader_30_session"]}]}
    result = service._run(day, source, dates, False)
    assert result["new_event_count"] == 1
    assert result["candidates"][0]["event_category"] == "leader_30_session"
    assert result["candidates"][0]["status"] == "watch"
    assert len(repo.saved) == 1


def test_apply_does_not_duplicate_an_already_processed_session(monkeypatch):
    day = dt.date(2026, 10, 23)
    monkeypatch.setattr(service_module, "session_dates", lambda *_: [dt.date(2026, 10, 22), day])
    monkeypatch.setattr(service_module, "require_completed_session", lambda *_: None)

    class ExistingRepo(Repo):
        def session_run_id(self, *_): return 42

    repo = ExistingRepo()
    source = {"target_trading_date": day.isoformat(), "rules_version": "martin_bar_watchlist_v1",
              "source_run_id": 10, "hits": []}
    result = MartinBarRetestService(repo).run_session(as_of_date=day, source_record=source, dry_run=False)
    assert result["reason"] == "session_already_processed"
    assert repo.saved == []
