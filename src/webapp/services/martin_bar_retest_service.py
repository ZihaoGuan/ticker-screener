from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
from typing import Any

import pandas as pd

from src.market_data_access import load_many_ticker_windows
from src.martin_bar_retest import STRATEGY_ID, evaluate, fingerprint
from src.webapp.repositories.martin_forward_test_repository import MartinForwardTestRepository
from src.webapp.services.martin_rotation import require_completed_session, session_dates


class MartinBarRetestService:
    def __init__(self, repository: MartinForwardTestRepository | None = None, *, database_url: str = "", bar_loader=None):
        self.repository = repository or MartinForwardTestRepository(database_url=database_url)
        self.database_url = database_url
        self.bar_loader = bar_loader or load_many_ticker_windows

    def run_session(self, *, as_of_date: dt.date, source_record: dict[str, Any] | None, dry_run: bool = True,
                    policy_id: str = STRATEGY_ID, book_id: str | None = None) -> dict[str, Any]:
        if policy_id != STRATEGY_ID:
            raise ValueError(f"Unsupported Martin Bar Retest policy identity: {policy_id}")
        book = book_id or policy_id
        sessions = session_dates(as_of_date, 320)
        if not sessions or sessions[-1] != as_of_date:
            raise ValueError("Martin Bar Retest requires an XNYS session")
        require_completed_session(as_of_date)
        if source_record is None:
            return {"strategy_id": policy_id, "session_date": as_of_date.isoformat(),
                    "status": "missing_source", "reason": "persisted_watchlist_run_missing", "dry_run": dry_run}
        if (str(source_record.get("target_trading_date") or source_record.get("run_date"))[:10] != as_of_date.isoformat()
                or source_record.get("rules_version") != "martin_bar_watchlist_v1"):
            return {"strategy_id": policy_id, "session_date": as_of_date.isoformat(),
                    "status": "blocked_source", "reason": "source_date_or_rules_version_mismatch", "dry_run": dry_run}
        if not dry_run:
            with self.repository.atomic_session(book):
                if self.repository.session_run_id(book, as_of_date) is not None:
                    return {"strategy_id": policy_id, "book_id": book, "session_date": as_of_date.isoformat(), "skipped": True, "reason": "session_already_processed"}
                latest = self.repository.latest_session_date(book)
                if latest is not None and latest != sessions[-2]:
                    raise ValueError("Martin Bar Retest requires sequential XNYS sessions, including zero-hit days")
                return self._run(as_of_date, source_record, sessions, False, policy_id=policy_id, book_id=book)
        return self._run(as_of_date, source_record, sessions, True, policy_id=policy_id, book_id=book)

    def _run(self, day: dt.date, source: dict[str, Any] | None, sessions: list[dt.date], dry_run: bool,
             *, policy_id: str = STRATEGY_ID, book_id: str | None = None) -> dict[str, Any]:
        book = book_id or policy_id
        prior = self.repository.latest_gap_watch_states(book) if hasattr(self.repository, "latest_gap_watch_states") else {}
        previously_seen = set(prior)
        active: dict[str, dict[str, Any]] = {}
        for ticker, payload in prior.items():
            if payload.get("status") != "retired" and payload.get("event_identity"):
                invalidation = payload.get("invalidation") or {}
                active[ticker] = {**payload, "ticker": ticker, "event_identity": payload["event_identity"],
                    "gap_low": payload.get("gap_low", invalidation.get("price") if invalidation.get("basis") == "original_gap_low" else None),
                    "category": payload.get("category") or payload.get("event_category"),
                    "source_published_at": payload.get("source_published_at"),
                    "discovery_categories": payload.get("discovery_categories", [])}
        source_valid = bool(source and str(source.get("target_trading_date") or source.get("run_date"))[:10] == day.isoformat()
                            and source.get("rules_version") == "martin_bar_watchlist_v1")
        new_events = []
        if source_valid:
            for hit in source.get("hits", []):
                ticker = str(hit.get("ticker") or "").upper()
                categories = set(hit.get("discovery_categories") or [])
                fresh = categories & {"potent_mover", "gap_mover"}
                if not ticker or (not fresh and (not categories.intersection({"leader_30_session"}) or ticker in previously_seen)):
                    continue
                # Strongest fresh category establishes the event and its original gap low.
                category = ("gap_mover" if "gap_mover" in fresh else "potent_mover") if fresh else "leader_30_session"
                identity = {"ticker": ticker, "event_date": day.isoformat(), "source_run_id": source.get("source_run_id"), "category": category}
                event = {"ticker": ticker, "event_date": day.isoformat(), "event_identity": identity,
                         "source_run_id": source.get("source_run_id"), "category": category,
                         "discovery_categories": sorted(categories), "source_published_at": source.get("published_at"),
                         "gap_low": (hit.get("low") or (hit.get("candle") or {}).get("low")) if category == "gap_mover" else None}
                active[ticker] = event
                new_events.append(event)
        publication = (source or {}).get("published_at")
        retrospective: bool | None = None
        if publication is not None:
            import exchange_calendars as xcals
            calendar = xcals.get_calendar("XNYS", start=day - dt.timedelta(days=5), end=day + dt.timedelta(days=20))
            following = next((label.date() for label in calendar.sessions_in_range(day.isoformat(), (day + dt.timedelta(days=20)).isoformat()) if label.date() > day), None)
            if following is not None:
                published = pd.Timestamp(publication)
                if published.tzinfo is None:
                    published = published.tz_localize("UTC")
                retrospective = published >= calendar.session_open(following.isoformat())
        tickers = sorted(active)
        try:
            frames = self.bar_loader(tickers, day, 320, database_url=self.database_url, start_date=sessions[0]) if tickers else {}
        except Exception:
            frames = {}
        candidates = []
        for ticker, event in active.items():
            if event.get("category") == "gap_mover" and event.get("gap_low") is None and ticker in frames:
                event_day = dt.date.fromisoformat(str(event["event_date"])[:10])
                matching = frames[ticker].loc[frames[ticker].index.date == event_day]
                if not matching.empty:
                    try:
                        low = float(matching.iloc[-1]["Low"])
                        if math.isfinite(low) and low > 0:
                            event["gap_low"] = low
                    except (TypeError, ValueError, OverflowError):
                        pass
            result = evaluate(frames.get(ticker), event, day, sessions)
            result.update(source_published_at=event.get("source_published_at"), source_run_id=event.get("source_run_id"),
                          event_category=event.get("category"), discovery_categories=event.get("discovery_categories", []),
                          source_mode=("persisted_screen_run_retrospective" if retrospective is True else
                                       "persisted_screen_run_preopen" if retrospective is False else
                                       "persisted_screen_run_publication_unknown"),
                          retrospective_source_scan=retrospective, bar_fingerprint=fingerprint(frames.get(ticker), day))
            candidates.append(result)
        candidates.sort(key=lambda x: (-(float(x.get("adr14_pct") or 0)), x["ticker"]))
        for rank, item in enumerate(candidates, 1): item["rank"] = rank
        source_id = int((source or {}).get("source_run_id") or 0)
        source_fp = str((source or {}).get("source_fingerprint") or "no-source")
        digest = hashlib.sha256(json.dumps({"day": day.isoformat(), "source": source_fp,
            "bars": [(x["ticker"], x.get("bar_fingerprint")) for x in candidates], "rules": policy_id}, sort_keys=True).encode()).hexdigest()
        summary = {"strategy_id": policy_id, "book_id": book, "rule_version": policy_id, "session_date": day.isoformat(),
                   "source_run_id": source_id or None, "source_fingerprint": source_fp,
                   "source_hit_count": len((source or {}).get("hits", [])) if source_valid else 0,
                   "active_event_count": len(candidates), "new_event_count": len(new_events),
                   "zero_hit_day": source_valid and not (source or {}).get("hits"), "retrospective_source_scan": retrospective,
                   "execution_mode": "dry_run" if dry_run else "applied"}
        if source and source.get("replay_provenance"):
            summary["replay_provenance"] = source["replay_provenance"]
        if not dry_run:
            self.repository.save_session(session_date=day, source_run_id=source_id, fingerprint=digest,
                candidates=candidates, summary=summary, strategy_id=book)
        return {**summary, "candidates": candidates, "dry_run": dry_run}
