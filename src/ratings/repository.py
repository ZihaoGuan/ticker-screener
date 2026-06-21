from __future__ import annotations

import datetime as dt
import json
from typing import Any, Iterable

from src.market_data_access import resolve_database_url

from .constants import RATING_STATUS_SCRAPE_FAILED
from .models import FundamentalsSnapshot, RatingSnapshot, SectorMetricBaseline, TechnicalIndicatorRatingSnapshot, TechnicalRatingSnapshot


class RatingsRepository:
    def __init__(self, database_url: str = "") -> None:
        self.database_url = resolve_database_url(database_url)

    def _connect(self):
        if not self.database_url:
            return None
        try:
            import psycopg
        except ImportError:
            return None
        return psycopg.connect(self.database_url)

    def ensure_ticker_metadata_stub(
        self,
        ticker: str,
        *,
        sector: str | None = None,
        industry: str | None = None,
        source: str = "finviz-fundamentals",
    ) -> None:
        connection = self._connect()
        if connection is None:
            return
        sql = """
            INSERT INTO ticker_metadata (ticker, sector, industry, source)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
              sector = COALESCE(ticker_metadata.sector, EXCLUDED.sector),
              industry = COALESCE(ticker_metadata.industry, EXCLUDED.industry),
              updated_at = NOW()
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (ticker.upper(), sector, industry, source))
            connection.commit()

    def list_active_tickers(
        self,
        *,
        tickers: Iterable[str] | None = None,
        sectors: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[str]:
        connection = self._connect()
        if connection is None:
            return []
        normalized_tickers = [str(item).strip().upper() for item in (tickers or []) if str(item).strip()]
        normalized_sectors = _normalize_text_values(sectors)
        sql = """
            SELECT ticker
            FROM ticker_metadata
            WHERE is_active = TRUE
        """
        params: list[Any] = []
        if normalized_tickers:
            sql += """
              AND ticker = ANY(%s)
            """
            params.append(normalized_tickers)
        if normalized_sectors:
            sql += """
              AND LOWER(COALESCE(sector, '')) = ANY(%s)
            """
            params.append(normalized_sectors)
        sql += """
            ORDER BY ticker ASC
        """
        if limit is not None and int(limit) > 0:
            sql += " LIMIT %s"
            params.append(int(limit))
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
        return [str(ticker).upper() for (ticker,) in rows]

    def upsert_fundamentals_snapshots(self, snapshots: Iterable[FundamentalsSnapshot]) -> int:
        rows = list(snapshots)
        if not rows:
            return 0
        connection = self._connect()
        if connection is None:
            return 0
        sql = """
            INSERT INTO ticker_fundamentals_snapshots (
              ticker, as_of_date, sector, industry, market_cap, enterprise_value, forward_pe,
              peg_ratio_5y, price_to_sales, price_to_book, price_to_fcf, profit_margin_pct,
              operating_margin_pct, gross_margin_pct, roa_pct, roe_pct, eps_this_y_pct,
              eps_next_y_pct, eps_next_5y_pct, sales_qq_pct, eps_qq_pct, perf_month_pct,
              perf_quarter_pct, perf_half_pct, perf_year_pct, perf_ytd_pct, volatility_week_pct,
              volatility_month_pct, source, source_url, parse_status, parse_error, scraped_at, updated_at
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()), NOW()
            )
            ON CONFLICT (ticker, as_of_date) DO UPDATE SET
              sector = EXCLUDED.sector,
              industry = EXCLUDED.industry,
              market_cap = EXCLUDED.market_cap,
              enterprise_value = EXCLUDED.enterprise_value,
              forward_pe = EXCLUDED.forward_pe,
              peg_ratio_5y = EXCLUDED.peg_ratio_5y,
              price_to_sales = EXCLUDED.price_to_sales,
              price_to_book = EXCLUDED.price_to_book,
              price_to_fcf = EXCLUDED.price_to_fcf,
              profit_margin_pct = EXCLUDED.profit_margin_pct,
              operating_margin_pct = EXCLUDED.operating_margin_pct,
              gross_margin_pct = EXCLUDED.gross_margin_pct,
              roa_pct = EXCLUDED.roa_pct,
              roe_pct = EXCLUDED.roe_pct,
              eps_this_y_pct = EXCLUDED.eps_this_y_pct,
              eps_next_y_pct = EXCLUDED.eps_next_y_pct,
              eps_next_5y_pct = EXCLUDED.eps_next_5y_pct,
              sales_qq_pct = EXCLUDED.sales_qq_pct,
              eps_qq_pct = EXCLUDED.eps_qq_pct,
              perf_month_pct = EXCLUDED.perf_month_pct,
              perf_quarter_pct = EXCLUDED.perf_quarter_pct,
              perf_half_pct = EXCLUDED.perf_half_pct,
              perf_year_pct = EXCLUDED.perf_year_pct,
              perf_ytd_pct = EXCLUDED.perf_ytd_pct,
              volatility_week_pct = EXCLUDED.volatility_week_pct,
              volatility_month_pct = EXCLUDED.volatility_month_pct,
              source = EXCLUDED.source,
              source_url = EXCLUDED.source_url,
              parse_status = EXCLUDED.parse_status,
              parse_error = EXCLUDED.parse_error,
              scraped_at = COALESCE(EXCLUDED.scraped_at, ticker_fundamentals_snapshots.scraped_at),
              updated_at = NOW()
        """
        payload = [
            (
                item.ticker.upper(),
                item.as_of_date,
                item.sector,
                item.industry,
                item.market_cap,
                item.enterprise_value,
                item.forward_pe,
                item.peg_ratio_5y,
                item.price_to_sales,
                item.price_to_book,
                item.price_to_fcf,
                item.profit_margin_pct,
                item.operating_margin_pct,
                item.gross_margin_pct,
                item.roa_pct,
                item.roe_pct,
                item.eps_this_y_pct,
                item.eps_next_y_pct,
                item.eps_next_5y_pct,
                item.sales_qq_pct,
                item.eps_qq_pct,
                item.perf_month_pct,
                item.perf_quarter_pct,
                item.perf_half_pct,
                item.perf_year_pct,
                item.perf_ytd_pct,
                item.volatility_week_pct,
                item.volatility_month_pct,
                item.source,
                item.source_url,
                item.parse_status,
                item.parse_error,
                item.scraped_at,
            )
            for item in rows
        ]
        with connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, payload)
            connection.commit()
        return len(rows)

    def upsert_chart_fundamentals_cache_entry(
        self,
        *,
        ticker: str,
        as_of_date: dt.date,
        earnings_eps_history: list[dict[str, Any]] | None = None,
        holders_float_held_by_institutions_pct: float | None = None,
        revenue_yoy_pct: float | None = None,
        earnings_yoy_pct: float | None = None,
        implied_move: dict[str, Any] | None = None,
        source_summary: dict[str, Any] | None = None,
        scraped_at: dt.datetime | None = None,
    ) -> None:
        connection = self._connect()
        if connection is None:
            return
        sql = """
            INSERT INTO ticker_chart_fundamentals_cache (
              ticker,
              as_of_date,
              earnings_eps_history_json,
              holders_float_held_by_institutions_pct,
              revenue_yoy_pct,
              earnings_yoy_pct,
              implied_move_json,
              source_summary_json,
              scraped_at,
              updated_at
            ) VALUES (
              %s,
              %s,
              %s::jsonb,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb,
              COALESCE(%s, NOW()),
              NOW()
            )
            ON CONFLICT (ticker, as_of_date) DO UPDATE SET
              earnings_eps_history_json = EXCLUDED.earnings_eps_history_json,
              holders_float_held_by_institutions_pct = EXCLUDED.holders_float_held_by_institutions_pct,
              revenue_yoy_pct = EXCLUDED.revenue_yoy_pct,
              earnings_yoy_pct = EXCLUDED.earnings_yoy_pct,
              implied_move_json = EXCLUDED.implied_move_json,
              source_summary_json = EXCLUDED.source_summary_json,
              scraped_at = COALESCE(EXCLUDED.scraped_at, ticker_chart_fundamentals_cache.scraped_at),
              updated_at = NOW()
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (
                        str(ticker or "").strip().upper(),
                        as_of_date,
                        json.dumps(earnings_eps_history or []),
                        holders_float_held_by_institutions_pct,
                        revenue_yoy_pct,
                        earnings_yoy_pct,
                        json.dumps(implied_move) if implied_move is not None else None,
                        json.dumps(source_summary or {}),
                        scraped_at,
                    ),
                )
            connection.commit()

    def load_latest_chart_fundamentals_cache_entry(self, ticker: str) -> dict[str, Any] | None:
        normalized_ticker = str(ticker or "").strip().upper()
        if not normalized_ticker:
            return None
        connection = self._connect()
        if connection is None:
            return None
        sql = """
            SELECT
              ticker,
              as_of_date,
              earnings_eps_history_json,
              holders_float_held_by_institutions_pct,
              revenue_yoy_pct,
              earnings_yoy_pct,
              implied_move_json,
              source_summary_json,
              scraped_at,
              updated_at
            FROM ticker_chart_fundamentals_cache
            WHERE ticker = %s
            ORDER BY as_of_date DESC, updated_at DESC
            LIMIT 1
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized_ticker,))
                row = cursor.fetchone()
        if not row:
            return None
        (
            cached_ticker,
            as_of_date,
            earnings_eps_history_json,
            holders_pct,
            revenue_yoy_pct,
            earnings_yoy_pct,
            implied_move_json,
            source_summary_json,
            scraped_at,
            updated_at,
        ) = row
        earnings_eps_history = _coerce_json_payload(earnings_eps_history_json, default=[])
        implied_move = _coerce_json_payload(implied_move_json, default=None)
        source_summary = _coerce_json_payload(source_summary_json, default={})
        return {
            "ticker": str(cached_ticker or "").upper(),
            "as_of_date": as_of_date.isoformat() if isinstance(as_of_date, dt.date) else str(as_of_date or ""),
            "earnings_eps_history": earnings_eps_history if isinstance(earnings_eps_history, list) else [],
            "holders_float_held_by_institutions_pct": float(holders_pct) if holders_pct is not None else None,
            "revenue_yoy_pct": float(revenue_yoy_pct) if revenue_yoy_pct is not None else None,
            "earnings_yoy_pct": float(earnings_yoy_pct) if earnings_yoy_pct is not None else None,
            "implied_move": implied_move if isinstance(implied_move, dict) else None,
            "source_summary": source_summary if isinstance(source_summary, dict) else {},
            "scraped_at": scraped_at.isoformat() if hasattr(scraped_at, "isoformat") else None,
            "updated_at": updated_at.isoformat() if hasattr(updated_at, "isoformat") else None,
        }

    def load_latest_chart_fundamentals_dates(self, tickers: Iterable[str]) -> dict[str, dt.date]:
        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        sql = """
            SELECT ticker, MAX(as_of_date)
            FROM ticker_chart_fundamentals_cache
            WHERE ticker = ANY(%s)
            GROUP BY ticker
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized,))
                rows = cursor.fetchall()
        result: dict[str, dt.date] = {}
        for ticker, as_of_date in rows:
            if isinstance(as_of_date, dt.date):
                result[str(ticker).upper()] = as_of_date
        return result

    def replace_sector_metric_baselines(
        self,
        as_of_date: dt.date,
        baselines: Iterable[SectorMetricBaseline],
        *,
        sectors: Iterable[str] | None = None,
    ) -> int:
        rows = list(baselines)
        connection = self._connect()
        if connection is None:
            return 0
        normalized_sectors = _normalize_text_values(sectors)
        with connection:
            with connection.cursor() as cursor:
                if normalized_sectors:
                    cursor.execute(
                        """
                        DELETE FROM sector_metric_baselines
                        WHERE as_of_date = %s
                          AND LOWER(COALESCE(sector, '')) = ANY(%s)
                        """,
                        (as_of_date, normalized_sectors),
                    )
                else:
                    cursor.execute("DELETE FROM sector_metric_baselines WHERE as_of_date = %s", (as_of_date,))
                if rows:
                    cursor.executemany(
                        """
                        INSERT INTO sector_metric_baselines (
                          as_of_date, sector, metric_name, sample_size, filtered_sample_size,
                          median_value, pct10_value, pct90_value, std_value, std_step_value
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                item.as_of_date,
                                item.sector,
                                item.metric_name,
                                item.sample_size,
                                item.filtered_sample_size,
                                item.median_value,
                                item.pct10_value,
                                item.pct90_value,
                                item.std_value,
                                item.std_step_value,
                            )
                            for item in rows
                        ],
                    )
            connection.commit()
        return len(rows)

    def replace_rating_snapshots(
        self,
        as_of_date: dt.date,
        ratings: Iterable[RatingSnapshot],
        *,
        tickers: Iterable[str] | None = None,
    ) -> int:
        rows = list(ratings)
        connection = self._connect()
        if connection is None:
            return 0
        normalized_tickers = [str(item).strip().upper() for item in (tickers or []) if str(item).strip()]
        with connection:
            with connection.cursor() as cursor:
                if normalized_tickers:
                    cursor.execute(
                        """
                        DELETE FROM ticker_rating_snapshots
                        WHERE as_of_date = %s
                          AND ticker = ANY(%s)
                        """,
                        (as_of_date, normalized_tickers),
                    )
                else:
                    cursor.execute("DELETE FROM ticker_rating_snapshots WHERE as_of_date = %s", (as_of_date,))
                if rows:
                    cursor.executemany(
                        """
                        INSERT INTO ticker_rating_snapshots (
                          ticker, as_of_date, sector, valuation_score, profitability_score, growth_score,
                          performance_score, overall_rating, valuation_grade, profitability_grade, growth_grade,
                          performance_grade, rating_status, rating_status_reason, missing_metric_names,
                          insufficient_baseline_metrics
                        ) VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb
                        )
                        """,
                        [
                            (
                                item.ticker.upper(),
                                item.as_of_date,
                                item.sector,
                                item.valuation_score,
                                item.profitability_score,
                                item.growth_score,
                                item.performance_score,
                                item.overall_rating,
                                item.valuation_grade,
                                item.profitability_grade,
                                item.growth_grade,
                                item.performance_grade,
                                item.rating_status,
                                item.rating_status_reason,
                                json.dumps(item.missing_metric_names),
                                json.dumps(item.insufficient_baseline_metrics),
                            )
                            for item in rows
                        ],
                    )
            connection.commit()
        return len(rows)

    def replace_technical_rating_snapshots(
        self,
        as_of_date: dt.date,
        ratings: Iterable[TechnicalRatingSnapshot],
        *,
        tickers: Iterable[str] | None = None,
    ) -> int:
        rows = list(ratings)
        connection = self._connect()
        if connection is None:
            return 0
        normalized_tickers = [str(item).strip().upper() for item in (tickers or []) if str(item).strip()]
        with connection:
            with connection.cursor() as cursor:
                if normalized_tickers:
                    cursor.execute(
                        """
                        DELETE FROM ticker_technical_rating_snapshots
                        WHERE as_of_date = %s
                          AND ticker = ANY(%s)
                        """,
                        (as_of_date, normalized_tickers),
                    )
                else:
                    cursor.execute("DELETE FROM ticker_technical_rating_snapshots WHERE as_of_date = %s", (as_of_date,))
                if rows:
                    cursor.executemany(
                        """
                        INSERT INTO ticker_technical_rating_snapshots (
                          ticker, as_of_date, trend_regime_score, dma_speed_score, divergence_health_score,
                          leadership_score, structure_volume_score, overall_rating, rating_band, technical_status,
                          technical_status_reason, flags, missing_metric_names
                        ) VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb
                        )
                        """,
                        [
                            (
                                item.ticker.upper(),
                                item.as_of_date,
                                item.trend_regime_score,
                                item.dma_speed_score,
                                item.divergence_health_score,
                                item.leadership_score,
                                item.structure_volume_score,
                                item.overall_rating,
                                item.rating_band,
                                item.technical_status,
                                item.technical_status_reason,
                                json.dumps(item.flags),
                                json.dumps(item.missing_metric_names),
                            )
                            for item in rows
                        ],
                    )
            connection.commit()
        return len(rows)

    def replace_technical_indicator_rating_snapshots(
        self,
        as_of_date: dt.date,
        ratings: Iterable[TechnicalIndicatorRatingSnapshot],
        *,
        tickers: Iterable[str] | None = None,
    ) -> int:
        rows = list(ratings)
        connection = self._connect()
        if connection is None:
            return 0
        normalized_tickers = [str(item).strip().upper() for item in (tickers or []) if str(item).strip()]
        with connection:
            with connection.cursor() as cursor:
                if normalized_tickers:
                    cursor.execute(
                        """
                        DELETE FROM ticker_technical_indicator_rating_snapshots
                        WHERE as_of_date = %s
                          AND ticker = ANY(%s)
                        """,
                        (as_of_date, normalized_tickers),
                    )
                else:
                    cursor.execute("DELETE FROM ticker_technical_indicator_rating_snapshots WHERE as_of_date = %s", (as_of_date,))
                if rows:
                    cursor.executemany(
                        """
                        INSERT INTO ticker_technical_indicator_rating_snapshots (
                          ticker, as_of_date, timeframe, moving_average_score, oscillator_score, overall_score,
                          rating_label, technical_status, technical_status_reason, missing_metric_names
                        ) VALUES (
                          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                        )
                        """,
                        [
                            (
                                item.ticker.upper(),
                                item.as_of_date,
                                item.timeframe,
                                item.moving_average_score,
                                item.oscillator_score,
                                item.overall_score,
                                item.rating_label,
                                item.technical_status,
                                item.technical_status_reason,
                                json.dumps(item.missing_metric_names),
                            )
                            for item in rows
                        ],
                    )
            connection.commit()
        return len(rows)

    def load_fundamentals_for_date(
        self,
        as_of_date: dt.date,
        *,
        sectors: Iterable[str] | None = None,
    ) -> list[FundamentalsSnapshot]:
        connection = self._connect()
        if connection is None:
            return []
        normalized_sectors = _normalize_text_values(sectors)
        sql = """
            SELECT fs.ticker, fs.as_of_date, COALESCE(fs.sector, tm.sector) AS sector,
                   COALESCE(fs.industry, tm.industry) AS industry, fs.source, fs.source_url, fs.parse_status, fs.parse_error,
                   fs.scraped_at, fs.updated_at, fs.market_cap, fs.enterprise_value, fs.forward_pe, fs.peg_ratio_5y,
                   fs.price_to_sales, fs.price_to_book, fs.price_to_fcf, fs.profit_margin_pct, fs.operating_margin_pct,
                   fs.gross_margin_pct, fs.roa_pct, fs.roe_pct, fs.eps_this_y_pct, fs.eps_next_y_pct, fs.eps_next_5y_pct,
                   fs.sales_qq_pct, fs.eps_qq_pct, fs.perf_month_pct, fs.perf_quarter_pct, fs.perf_half_pct, fs.perf_year_pct,
                   fs.perf_ytd_pct, fs.volatility_week_pct, fs.volatility_month_pct
            FROM ticker_fundamentals_snapshots fs
            LEFT JOIN ticker_metadata tm ON tm.ticker = fs.ticker
            WHERE fs.as_of_date = %s
        """
        params: list[Any] = [as_of_date]
        if normalized_sectors:
            sql += """
              AND LOWER(COALESCE(fs.sector, tm.sector, '')) = ANY(%s)
            """
            params.append(normalized_sectors)
        sql += """
            ORDER BY fs.ticker ASC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
        snapshots: list[FundamentalsSnapshot] = []
        for row in rows:
            snapshots.append(FundamentalsSnapshot(*row))
        return snapshots

    def load_sector_baselines_for_date(
        self,
        as_of_date: dt.date,
        *,
        sectors: Iterable[str] | None = None,
    ) -> dict[str, dict[str, SectorMetricBaseline]]:
        connection = self._connect()
        if connection is None:
            return {}
        normalized_sectors = _normalize_text_values(sectors)
        sql = """
            SELECT as_of_date, sector, metric_name, sample_size, filtered_sample_size,
                   median_value, pct10_value, pct90_value, std_value, std_step_value
            FROM sector_metric_baselines
            WHERE as_of_date = %s
        """
        params: list[Any] = [as_of_date]
        if normalized_sectors:
            sql += """
              AND LOWER(COALESCE(sector, '')) = ANY(%s)
            """
            params.append(normalized_sectors)
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
        grouped: dict[str, dict[str, SectorMetricBaseline]] = {}
        for row in rows:
            item = SectorMetricBaseline(*row)
            grouped.setdefault(item.sector, {})[item.metric_name] = item
        return grouped

    def load_latest_ticker_rating_bundle(self, ticker: str) -> dict[str, Any] | None:
        connection = self._connect()
        if connection is None:
            return None
        fundamentals_sql = """
            SELECT
              f.as_of_date,
              f.ticker,
              f.sector,
              f.industry,
              f.market_cap,
              f.enterprise_value,
              f.forward_pe,
              f.peg_ratio_5y,
              f.price_to_sales,
              f.price_to_book,
              f.price_to_fcf,
              f.profit_margin_pct,
              f.operating_margin_pct,
              f.gross_margin_pct,
              f.roa_pct,
              f.roe_pct,
              f.eps_this_y_pct,
              f.eps_next_y_pct,
              f.eps_next_5y_pct,
              f.sales_qq_pct,
              f.eps_qq_pct,
              f.perf_month_pct,
              f.perf_quarter_pct,
              f.perf_half_pct,
              f.perf_year_pct,
              f.perf_ytd_pct,
              f.volatility_week_pct,
              f.volatility_month_pct,
              f.source,
              f.parse_status,
              f.parse_error
            FROM ticker_fundamentals_snapshots f
            WHERE f.ticker = %s
            ORDER BY f.as_of_date DESC
            LIMIT 1
        """
        rating_sql = """
            SELECT
              r.as_of_date,
              r.ticker,
              COALESCE(r.sector, f.sector, tm.sector) AS sector,
              COALESCE(f.industry, tm.industry) AS industry,
              r.valuation_score,
              r.profitability_score,
              r.growth_score,
              r.performance_score,
              r.overall_rating,
              r.valuation_grade,
              r.profitability_grade,
              r.growth_grade,
              r.performance_grade,
              r.rating_status,
              r.rating_status_reason,
              r.missing_metric_names,
              r.insufficient_baseline_metrics
            FROM ticker_rating_snapshots r
            LEFT JOIN ticker_fundamentals_snapshots f
              ON f.ticker = r.ticker AND f.as_of_date = r.as_of_date
            LEFT JOIN ticker_metadata tm
              ON tm.ticker = r.ticker
            WHERE r.ticker = %s
            ORDER BY r.as_of_date DESC, r.updated_at DESC
            LIMIT 1
        """
        rank_row = None
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(fundamentals_sql, (ticker.upper(),))
                fundamentals_row = cursor.fetchone()
                cursor.execute(rating_sql, (ticker.upper(),))
                rating_row = cursor.fetchone()
                if rating_row:
                    rating_as_of_value = rating_row[0]
                    if isinstance(rating_as_of_value, dt.date):
                        rank_sql = """
                            WITH ranked AS (
                                SELECT
                                  ticker,
                                  ROW_NUMBER() OVER (ORDER BY overall_rating DESC NULLS LAST, ticker ASC) AS current_rank
                                FROM ticker_rating_snapshots
                                WHERE as_of_date = %s
                                  AND rating_status = 'ok'
                            )
                            SELECT current_rank
                            FROM ranked
                            WHERE ticker = %s
                              AND current_rank <= 200
                        """
                        cursor.execute(rank_sql, (rating_as_of_value, ticker.upper()))
                        rank_row = cursor.fetchone()
        if not fundamentals_row and not rating_row:
            return None

        fundamentals_snapshot: dict[str, Any] | None = None
        if fundamentals_row:
            (
                fundamentals_as_of_date,
                fundamentals_ticker,
                fundamentals_sector,
                fundamentals_industry,
                market_cap,
                enterprise_value,
                forward_pe,
                peg_ratio_5y,
                price_to_sales,
                price_to_book,
                price_to_fcf,
                profit_margin_pct,
                operating_margin_pct,
                gross_margin_pct,
                roa_pct,
                roe_pct,
                eps_this_y_pct,
                eps_next_y_pct,
                eps_next_5y_pct,
                sales_qq_pct,
                eps_qq_pct,
                perf_month_pct,
                perf_quarter_pct,
                perf_half_pct,
                perf_year_pct,
                perf_ytd_pct,
                volatility_week_pct,
                volatility_month_pct,
                source,
                parse_status,
                parse_error,
            ) = fundamentals_row
            fundamentals_snapshot = {
                "as_of_date": fundamentals_as_of_date.isoformat() if isinstance(fundamentals_as_of_date, dt.date) else str(fundamentals_as_of_date),
                "ticker": fundamentals_ticker,
                "sector": fundamentals_sector,
                "industry": fundamentals_industry,
                "market_cap": float(market_cap) if market_cap is not None else None,
                "enterprise_value": float(enterprise_value) if enterprise_value is not None else None,
                "forward_pe": float(forward_pe) if forward_pe is not None else None,
                "peg_ratio_5y": float(peg_ratio_5y) if peg_ratio_5y is not None else None,
                "price_to_sales": float(price_to_sales) if price_to_sales is not None else None,
                "price_to_book": float(price_to_book) if price_to_book is not None else None,
                "price_to_fcf": float(price_to_fcf) if price_to_fcf is not None else None,
                "profit_margin_pct": float(profit_margin_pct) if profit_margin_pct is not None else None,
                "operating_margin_pct": float(operating_margin_pct) if operating_margin_pct is not None else None,
                "gross_margin_pct": float(gross_margin_pct) if gross_margin_pct is not None else None,
                "roa_pct": float(roa_pct) if roa_pct is not None else None,
                "roe_pct": float(roe_pct) if roe_pct is not None else None,
                "eps_this_y_pct": float(eps_this_y_pct) if eps_this_y_pct is not None else None,
                "eps_next_y_pct": float(eps_next_y_pct) if eps_next_y_pct is not None else None,
                "eps_next_5y_pct": float(eps_next_5y_pct) if eps_next_5y_pct is not None else None,
                "sales_qq_pct": float(sales_qq_pct) if sales_qq_pct is not None else None,
                "eps_qq_pct": float(eps_qq_pct) if eps_qq_pct is not None else None,
                "perf_month_pct": float(perf_month_pct) if perf_month_pct is not None else None,
                "perf_quarter_pct": float(perf_quarter_pct) if perf_quarter_pct is not None else None,
                "perf_half_pct": float(perf_half_pct) if perf_half_pct is not None else None,
                "perf_year_pct": float(perf_year_pct) if perf_year_pct is not None else None,
                "perf_ytd_pct": float(perf_ytd_pct) if perf_ytd_pct is not None else None,
                "volatility_week_pct": float(volatility_week_pct) if volatility_week_pct is not None else None,
                "volatility_month_pct": float(volatility_month_pct) if volatility_month_pct is not None else None,
                "source": source,
                "parse_status": parse_status,
                "parse_error": parse_error,
            }

        rating_snapshot: dict[str, Any] | None = None
        rating_diagnostics = {
            "missing_metric_names": [],
            "insufficient_baseline_metrics": [],
        }
        rating_as_of_date: dt.date | None = None
        if rating_row:
            (
                rating_as_of_date,
                normalized_ticker,
                sector,
                industry,
                valuation_score,
                profitability_score,
                growth_score,
                performance_score,
                overall_rating,
                valuation_grade,
                profitability_grade,
                growth_grade,
                performance_grade,
                rating_status,
                rating_status_reason,
                missing_metric_names,
                insufficient_baseline_metrics,
            ) = rating_row
            rating_snapshot = {
                "as_of_date": rating_as_of_date.isoformat() if isinstance(rating_as_of_date, dt.date) else str(rating_as_of_date),
                "valuation_score": float(valuation_score) if valuation_score is not None else None,
                "profitability_score": float(profitability_score) if profitability_score is not None else None,
                "growth_score": float(growth_score) if growth_score is not None else None,
                "performance_score": float(performance_score) if performance_score is not None else None,
                "overall_rating": float(overall_rating) if overall_rating is not None else None,
                "valuation_grade": valuation_grade,
                "profitability_grade": profitability_grade,
                "growth_grade": growth_grade,
                "performance_grade": performance_grade,
                "rating_status": rating_status,
                "rating_status_reason": rating_status_reason,
            }
            rating_diagnostics = {
                "missing_metric_names": list(missing_metric_names or []),
                "insufficient_baseline_metrics": list(insufficient_baseline_metrics or []),
            }

        fundamental_rank: int | None = None
        if rating_as_of_date is not None:
            if rank_row and rank_row[0] is not None:
                fundamental_rank = int(rank_row[0])
        return {
            "fundamentals_snapshot": fundamentals_snapshot,
            "rating_snapshot": rating_snapshot,
            "fundamental_rank": {
                "as_of_date": rating_as_of_date.isoformat() if isinstance(rating_as_of_date, dt.date) else str(rating_as_of_date),
                "current_rank": fundamental_rank,
                "list_limit": 200,
            },
            "rating_diagnostics": rating_diagnostics,
        }

    def load_latest_fundamentals_dates(self, tickers: Iterable[str]) -> dict[str, dt.date]:
        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        sql = """
            SELECT ticker, MAX(as_of_date)
            FROM ticker_fundamentals_snapshots
            WHERE ticker = ANY(%s)
            GROUP BY ticker
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized,))
                rows = cursor.fetchall()
        result: dict[str, dt.date] = {}
        for ticker, as_of_date in rows:
            if isinstance(as_of_date, dt.date):
                result[str(ticker).upper()] = as_of_date
        return result

    def load_latest_fundamentals_statuses(self, tickers: Iterable[str]) -> dict[str, dict[str, Any]]:
        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        sql = """
            SELECT DISTINCT ON (ticker) ticker, as_of_date, parse_status
            FROM ticker_fundamentals_snapshots
            WHERE ticker = ANY(%s)
            ORDER BY ticker, as_of_date DESC, updated_at DESC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized,))
                rows = cursor.fetchall()
        result: dict[str, dict[str, Any]] = {}
        for ticker, as_of_date, parse_status in rows:
            ticker_key = str(ticker).upper()
            if not isinstance(as_of_date, dt.date):
                continue
            status_value = str(parse_status or "").strip() or RATING_STATUS_SCRAPE_FAILED
            result[ticker_key] = {
                "as_of_date": as_of_date,
                "parse_status": status_value,
            }
        return result

    def load_latest_rating_snapshots_for_tickers(self, tickers: Iterable[str]) -> dict[str, dict[str, Any]]:
        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        sql = """
            SELECT DISTINCT ON (r.ticker)
              r.ticker,
              r.as_of_date,
              COALESCE(r.sector, f.sector, tm.sector) AS sector,
              COALESCE(f.industry, tm.industry) AS industry,
              f.perf_year_pct,
              f.perf_ytd_pct,
              r.overall_rating,
              r.valuation_grade,
              r.profitability_grade,
              r.growth_grade,
              r.performance_grade,
              r.rating_status,
              r.rating_status_reason
            FROM ticker_rating_snapshots r
            LEFT JOIN ticker_fundamentals_snapshots f
              ON f.ticker = r.ticker AND f.as_of_date = r.as_of_date
            LEFT JOIN ticker_metadata tm
              ON tm.ticker = r.ticker
            WHERE r.ticker = ANY(%s)
            ORDER BY r.ticker, r.as_of_date DESC, r.updated_at DESC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized,))
                rows = cursor.fetchall()
        result: dict[str, dict[str, Any]] = {}
        for (
            ticker,
            as_of_date,
            sector,
            industry,
            perf_year_pct,
            perf_ytd_pct,
            overall_rating,
            valuation_grade,
            profitability_grade,
            growth_grade,
            performance_grade,
            rating_status,
            rating_status_reason,
        ) in rows:
            result[str(ticker).upper()] = {
                "as_of_date": as_of_date.isoformat() if isinstance(as_of_date, dt.date) else str(as_of_date or ""),
                "sector": sector,
                "industry": industry,
                "perf_year_pct": float(perf_year_pct) if perf_year_pct is not None else None,
                "perf_ytd_pct": float(perf_ytd_pct) if perf_ytd_pct is not None else None,
                "overall_rating": float(overall_rating) if overall_rating is not None else None,
                "valuation_grade": valuation_grade,
                "profitability_grade": profitability_grade,
                "growth_grade": growth_grade,
                "performance_grade": performance_grade,
                "rating_status": rating_status,
                "rating_status_reason": rating_status_reason,
            }
        return result

    def load_latest_technical_rating_snapshots_for_tickers(self, tickers: Iterable[str]) -> dict[str, dict[str, Any]]:
        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        sql = """
            SELECT DISTINCT ON (r.ticker)
              r.ticker,
              r.as_of_date,
              tm.sector,
              tm.industry,
              r.overall_rating,
              r.leadership_score,
              r.rating_band,
              r.technical_status,
              r.technical_status_reason,
              r.flags
            FROM ticker_technical_rating_snapshots r
            LEFT JOIN ticker_metadata tm
              ON tm.ticker = r.ticker
            WHERE r.ticker = ANY(%s)
            ORDER BY r.ticker, r.as_of_date DESC, r.updated_at DESC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized,))
                rows = cursor.fetchall()
        result: dict[str, dict[str, Any]] = {}
        for (
            ticker,
            as_of_date,
            sector,
            industry,
            overall_rating,
            leadership_score,
            rating_band,
            technical_status,
            technical_status_reason,
            flags,
        ) in rows:
            result[str(ticker).upper()] = {
                "as_of_date": as_of_date.isoformat() if isinstance(as_of_date, dt.date) else str(as_of_date or ""),
                "sector": sector,
                "industry": industry,
                "overall_rating": float(overall_rating) if overall_rating is not None else None,
                "leadership_score": float(leadership_score) if leadership_score is not None else None,
                "rating_band": rating_band,
                "technical_status": technical_status,
                "technical_status_reason": technical_status_reason,
                "flags": list(flags or []),
            }
        return result

    def load_latest_technical_indicator_ratings_for_tickers(
        self,
        tickers: Iterable[str],
        *,
        as_of_date: dt.date | None = None,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        normalized = sorted({str(item).strip().upper() for item in tickers if str(item).strip()})
        if not normalized:
            return {}
        connection = self._connect()
        if connection is None:
            return {}
        sql = """
            SELECT DISTINCT ON (r.ticker, r.timeframe)
              r.ticker,
              r.timeframe,
              r.as_of_date,
              tm.sector,
              tm.industry,
              r.moving_average_score,
              r.oscillator_score,
              r.overall_score,
              r.rating_label,
              r.technical_status,
              r.technical_status_reason,
              r.missing_metric_names
            FROM ticker_technical_indicator_rating_snapshots r
            LEFT JOIN ticker_metadata tm
              ON tm.ticker = r.ticker
            WHERE r.ticker = ANY(%s)
              AND (%s::date IS NULL OR r.as_of_date = %s)
            ORDER BY r.ticker, r.timeframe, r.as_of_date DESC, r.updated_at DESC
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized, as_of_date, as_of_date))
                rows = cursor.fetchall()
        result: dict[str, dict[str, dict[str, Any]]] = {}
        for (
            ticker,
            timeframe,
            as_of_date,
            sector,
            industry,
            moving_average_score,
            oscillator_score,
            overall_score,
            rating_label,
            technical_status,
            technical_status_reason,
            missing_metric_names,
        ) in rows:
            ticker_key = str(ticker).upper()
            result.setdefault(ticker_key, {})[str(timeframe)] = {
                "ticker": ticker_key,
                "timeframe": str(timeframe),
                "as_of_date": as_of_date.isoformat() if isinstance(as_of_date, dt.date) else str(as_of_date or ""),
                "sector": sector,
                "industry": industry,
                "moving_average_score": float(moving_average_score) if moving_average_score is not None else None,
                "oscillator_score": float(oscillator_score) if oscillator_score is not None else None,
                "overall_score": float(overall_score) if overall_score is not None else None,
                "rating_label": rating_label,
                "technical_status": technical_status,
                "technical_status_reason": technical_status_reason,
                "missing_metric_names": list(missing_metric_names or []),
            }
        return result

    def list_top_rating_snapshots(
        self,
        *,
        as_of_date: dt.date | None = None,
        limit: int = 100,
        rating_status: str = "ok",
        sector: str = "",
    ) -> dict[str, Any]:
        connection = self._connect()
        if connection is None:
            return {"as_of_date": None, "previous_as_of_date": None, "rows": [], "status_counts": {}}
        normalized_limit = max(1, min(int(limit), 500))
        normalized_status = str(rating_status or "").strip().lower()
        normalized_sector = str(sector or "").strip().lower()
        date_sql = """
            SELECT COALESCE(%s::date, (SELECT MAX(as_of_date) FROM ticker_rating_snapshots))
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(date_sql, (as_of_date,))
                date_row = cursor.fetchone()
                target_date = date_row[0] if date_row else None
                if not isinstance(target_date, dt.date):
                    return {"as_of_date": None, "previous_as_of_date": None, "rows": [], "status_counts": {}}
                cursor.execute(
                    """
                    SELECT DISTINCT COALESCE(r.sector, f.sector) AS sector
                    FROM ticker_rating_snapshots r
                    LEFT JOIN ticker_fundamentals_snapshots f
                      ON f.ticker = r.ticker AND f.as_of_date = r.as_of_date
                    WHERE r.as_of_date = %s
                      AND COALESCE(r.sector, f.sector, '') <> ''
                    ORDER BY sector ASC
                    """,
                    (target_date,),
                )
                sector_options = [str(sector) for (sector,) in cursor.fetchall() if sector]
                cursor.execute(
                    """
                    SELECT MAX(as_of_date)
                    FROM ticker_rating_snapshots
                    WHERE as_of_date < %s
                    """,
                    (target_date,),
                )
                previous_date_row = cursor.fetchone()
                previous_date = previous_date_row[0] if previous_date_row else None
                cursor.execute(
                    """
                    SELECT r.rating_status, COUNT(*)
                    FROM ticker_rating_snapshots r
                    LEFT JOIN ticker_fundamentals_snapshots f
                      ON f.ticker = r.ticker AND f.as_of_date = r.as_of_date
                    WHERE r.as_of_date = %s
                      AND (%s = '' OR LOWER(COALESCE(r.sector, f.sector, '')) = %s)
                    GROUP BY rating_status
                    """,
                    (target_date, normalized_sector, normalized_sector),
                )
                status_counts = {
                    str(status or "unknown"): int(count or 0)
                    for status, count in cursor.fetchall()
                }
                cursor.execute(
                    """
                    WITH filtered AS (
                      SELECT
                        r.ticker,
                        r.as_of_date,
                        COALESCE(r.sector, f.sector) AS sector,
                        f.industry,
                        f.perf_year_pct,
                        f.perf_ytd_pct,
                        r.overall_rating,
                        r.valuation_score,
                        r.profitability_score,
                        r.growth_score,
                        r.performance_score,
                        r.valuation_grade,
                        r.profitability_grade,
                        r.growth_grade,
                        r.performance_grade,
                        r.rating_status,
                        r.rating_status_reason
                      FROM ticker_rating_snapshots r
                      LEFT JOIN ticker_fundamentals_snapshots f
                        ON f.ticker = r.ticker AND f.as_of_date = r.as_of_date
                      WHERE r.as_of_date = %s
                        AND (%s = '' OR LOWER(COALESCE(r.rating_status, '')) = %s)
                        AND (%s = '' OR LOWER(COALESCE(r.sector, f.sector, '')) = %s)
                    ),
                    ranked AS (
                      SELECT
                        *,
                        ROW_NUMBER() OVER (ORDER BY overall_rating DESC NULLS LAST, ticker ASC) AS current_rank
                      FROM filtered
                    )
                    SELECT
                      ticker,
                      as_of_date,
                      sector,
                      industry,
                      perf_year_pct,
                      perf_ytd_pct,
                      overall_rating,
                      valuation_score,
                      profitability_score,
                      growth_score,
                      performance_score,
                      valuation_grade,
                      profitability_grade,
                      growth_grade,
                      performance_grade,
                      rating_status,
                      rating_status_reason,
                      current_rank
                    FROM ranked
                    ORDER BY current_rank ASC
                    LIMIT %s
                    """,
                    (target_date, normalized_status, normalized_status, normalized_sector, normalized_sector, normalized_limit),
                )
                rows = cursor.fetchall()
                previous_ranks: dict[str, int] = {}
                if isinstance(previous_date, dt.date):
                    cursor.execute(
                        """
                        WITH filtered AS (
                          SELECT
                            r.ticker,
                            r.overall_rating
                          FROM ticker_rating_snapshots r
                          LEFT JOIN ticker_fundamentals_snapshots f
                            ON f.ticker = r.ticker AND f.as_of_date = r.as_of_date
                          WHERE r.as_of_date = %s
                            AND (%s = '' OR LOWER(COALESCE(r.rating_status, '')) = %s)
                            AND (%s = '' OR LOWER(COALESCE(r.sector, f.sector, '')) = %s)
                        ),
                        ranked AS (
                          SELECT
                            ticker,
                            ROW_NUMBER() OVER (ORDER BY overall_rating DESC NULLS LAST, ticker ASC) AS previous_rank
                          FROM filtered
                        )
                        SELECT ticker, previous_rank
                        FROM ranked
                        """,
                        (previous_date, normalized_status, normalized_status, normalized_sector, normalized_sector),
                    )
                    previous_ranks = {
                        str(ticker or "").upper(): int(previous_rank)
                        for ticker, previous_rank in cursor.fetchall()
                    }
        ranked_rows = [
            _attach_rank_change(
                {
                    "ticker": str(ticker or "").upper(),
                    "as_of_date": snapshot_date.isoformat() if isinstance(snapshot_date, dt.date) else str(snapshot_date),
                    "sector": sector,
                    "industry": industry,
                    "perf_year_pct": float(perf_year_pct) if perf_year_pct is not None else None,
                    "perf_ytd_pct": float(perf_ytd_pct) if perf_ytd_pct is not None else None,
                    "current_rank": int(current_rank),
                    "overall_rating": float(overall_rating) if overall_rating is not None else None,
                    "valuation_score": float(valuation_score) if valuation_score is not None else None,
                    "profitability_score": float(profitability_score) if profitability_score is not None else None,
                    "growth_score": float(growth_score) if growth_score is not None else None,
                    "performance_score": float(performance_score) if performance_score is not None else None,
                    "valuation_grade": valuation_grade,
                    "profitability_grade": profitability_grade,
                    "growth_grade": growth_grade,
                    "performance_grade": performance_grade,
                    "rating_status": rating_status_value,
                    "rating_status_reason": rating_status_reason,
                },
                previous_ranks,
            )
            for (
                ticker,
                snapshot_date,
                sector,
                industry,
                perf_year_pct,
                perf_ytd_pct,
                overall_rating,
                valuation_score,
                profitability_score,
                growth_score,
                performance_score,
                valuation_grade,
                profitability_grade,
                growth_grade,
                performance_grade,
                rating_status_value,
                rating_status_reason,
                current_rank,
            ) in rows
        ]
        return {
            "as_of_date": target_date.isoformat(),
            "previous_as_of_date": previous_date.isoformat() if isinstance(previous_date, dt.date) else None,
            "rows": ranked_rows,
            "status_counts": dict(sorted(status_counts.items())),
            "sector_options": sector_options,
        }

    def list_top_technical_rating_snapshots(
        self,
        *,
        as_of_date: dt.date | None = None,
        limit: int = 100,
        technical_status: str = "ok",
        sector: str = "",
    ) -> dict[str, Any]:
        connection = self._connect()
        if connection is None:
            return {"as_of_date": None, "previous_as_of_date": None, "rows": [], "status_counts": {}}
        normalized_limit = max(1, min(int(limit), 500))
        normalized_status = str(technical_status or "").strip().lower()
        normalized_sector = str(sector or "").strip().lower()
        date_sql = """
            SELECT COALESCE(%s::date, (SELECT MAX(as_of_date) FROM ticker_technical_rating_snapshots))
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(date_sql, (as_of_date,))
                date_row = cursor.fetchone()
                target_date = date_row[0] if date_row else None
                if not isinstance(target_date, dt.date):
                    return {"as_of_date": None, "previous_as_of_date": None, "rows": [], "status_counts": {}}
                cursor.execute(
                    """
                    SELECT DISTINCT tm.sector
                    FROM ticker_technical_rating_snapshots r
                    LEFT JOIN ticker_metadata tm
                      ON tm.ticker = r.ticker
                    WHERE r.as_of_date = %s
                      AND COALESCE(tm.sector, '') <> ''
                    ORDER BY tm.sector ASC
                    """,
                    (target_date,),
                )
                sector_options = [str(sector) for (sector,) in cursor.fetchall() if sector]
                cursor.execute(
                    """
                    SELECT MAX(as_of_date)
                    FROM ticker_technical_rating_snapshots
                    WHERE as_of_date < %s
                    """,
                    (target_date,),
                )
                previous_date_row = cursor.fetchone()
                previous_date = previous_date_row[0] if previous_date_row else None
                cursor.execute(
                    """
                    SELECT r.technical_status, COUNT(*)
                    FROM ticker_technical_rating_snapshots r
                    LEFT JOIN ticker_metadata tm
                      ON tm.ticker = r.ticker
                    WHERE r.as_of_date = %s
                      AND (%s = '' OR LOWER(COALESCE(tm.sector, '')) = %s)
                    GROUP BY technical_status
                    """,
                    (target_date, normalized_sector, normalized_sector),
                )
                status_counts = {
                    str(status or "unknown"): int(count or 0)
                    for status, count in cursor.fetchall()
                }
                cursor.execute(
                    """
                    WITH filtered AS (
                      SELECT
                        r.ticker,
                        r.as_of_date,
                        tm.sector,
                        tm.industry,
                        r.overall_rating,
                        r.trend_regime_score,
                        r.dma_speed_score,
                        r.divergence_health_score,
                        r.leadership_score,
                        r.structure_volume_score,
                        r.rating_band,
                        r.technical_status,
                        r.technical_status_reason,
                        r.flags
                      FROM ticker_technical_rating_snapshots r
                      LEFT JOIN ticker_metadata tm
                        ON tm.ticker = r.ticker
                      WHERE r.as_of_date = %s
                        AND (%s = '' OR LOWER(COALESCE(r.technical_status, '')) = %s)
                        AND (%s = '' OR LOWER(COALESCE(tm.sector, '')) = %s)
                    ),
                    ranked AS (
                      SELECT
                        *,
                        ROW_NUMBER() OVER (ORDER BY overall_rating DESC NULLS LAST, ticker ASC) AS current_rank
                      FROM filtered
                    )
                    SELECT
                      ticker,
                      as_of_date,
                      sector,
                      industry,
                      overall_rating,
                      trend_regime_score,
                      dma_speed_score,
                      divergence_health_score,
                      leadership_score,
                      structure_volume_score,
                      rating_band,
                      technical_status,
                      technical_status_reason,
                      flags,
                      current_rank
                    FROM ranked
                    ORDER BY current_rank ASC
                    LIMIT %s
                    """,
                    (target_date, normalized_status, normalized_status, normalized_sector, normalized_sector, normalized_limit),
                )
                rows = cursor.fetchall()
                previous_ranks: dict[str, int] = {}
                if isinstance(previous_date, dt.date):
                    cursor.execute(
                        """
                        WITH filtered AS (
                          SELECT
                            r.ticker,
                            r.overall_rating
                          FROM ticker_technical_rating_snapshots r
                          LEFT JOIN ticker_metadata tm
                            ON tm.ticker = r.ticker
                          WHERE r.as_of_date = %s
                            AND (%s = '' OR LOWER(COALESCE(r.technical_status, '')) = %s)
                            AND (%s = '' OR LOWER(COALESCE(tm.sector, '')) = %s)
                        ),
                        ranked AS (
                          SELECT
                            ticker,
                            ROW_NUMBER() OVER (ORDER BY overall_rating DESC NULLS LAST, ticker ASC) AS previous_rank
                          FROM filtered
                        )
                        SELECT ticker, previous_rank
                        FROM ranked
                        """,
                        (previous_date, normalized_status, normalized_status, normalized_sector, normalized_sector),
                    )
                    previous_ranks = {
                        str(ticker or "").upper(): int(previous_rank)
                        for ticker, previous_rank in cursor.fetchall()
                    }
        ranked_rows = [
            _attach_rank_change(
                {
                    "ticker": str(ticker or "").upper(),
                    "as_of_date": snapshot_date.isoformat() if isinstance(snapshot_date, dt.date) else str(snapshot_date),
                    "sector": sector,
                    "industry": industry,
                    "current_rank": int(current_rank),
                    "overall_rating": float(overall_rating) if overall_rating is not None else None,
                    "trend_regime_score": float(trend_regime_score) if trend_regime_score is not None else None,
                    "dma_speed_score": float(dma_speed_score) if dma_speed_score is not None else None,
                    "divergence_health_score": float(divergence_health_score) if divergence_health_score is not None else None,
                    "leadership_score": float(leadership_score) if leadership_score is not None else None,
                    "structure_volume_score": float(structure_volume_score) if structure_volume_score is not None else None,
                    "rating_band": rating_band,
                    "technical_status": technical_status_value,
                    "technical_status_reason": technical_status_reason,
                    "flags": list(flags or []),
                },
                previous_ranks,
            )
            for (
                ticker,
                snapshot_date,
                sector,
                industry,
                overall_rating,
                trend_regime_score,
                dma_speed_score,
                divergence_health_score,
                leadership_score,
                structure_volume_score,
                rating_band,
                technical_status_value,
                technical_status_reason,
                flags,
                current_rank,
            ) in rows
        ]
        return {
            "as_of_date": target_date.isoformat(),
            "previous_as_of_date": previous_date.isoformat() if isinstance(previous_date, dt.date) else None,
            "rows": ranked_rows,
            "status_counts": dict(sorted(status_counts.items())),
            "sector_options": sector_options,
        }

    def list_top_technical_indicator_rating_snapshots(
        self,
        *,
        as_of_date: dt.date | None = None,
        limit: int = 100,
        technical_status: str = "ok",
        sector: str = "",
    ) -> dict[str, Any]:
        connection = self._connect()
        if connection is None:
            return {"as_of_date": None, "previous_as_of_date": None, "rows": [], "status_counts": {}}
        normalized_limit = max(1, min(int(limit), 500))
        normalized_status = str(technical_status or "").strip().lower()
        normalized_sector = str(sector or "").strip().lower()
        date_sql = """
            SELECT COALESCE(%s::date, (SELECT MAX(as_of_date) FROM ticker_technical_indicator_rating_snapshots))
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(date_sql, (as_of_date,))
                date_row = cursor.fetchone()
                target_date = date_row[0] if date_row else None
                if not isinstance(target_date, dt.date):
                    return {"as_of_date": None, "previous_as_of_date": None, "rows": [], "status_counts": {}}
                cursor.execute(
                    """
                    SELECT DISTINCT tm.sector
                    FROM ticker_technical_indicator_rating_snapshots r
                    LEFT JOIN ticker_metadata tm
                      ON tm.ticker = r.ticker
                    WHERE r.as_of_date = %s
                      AND COALESCE(tm.sector, '') <> ''
                    ORDER BY tm.sector ASC
                    """,
                    (target_date,),
                )
                sector_options = [str(value) for (value,) in cursor.fetchall() if value]
                cursor.execute(
                    """
                    SELECT MAX(as_of_date)
                    FROM ticker_technical_indicator_rating_snapshots
                    WHERE as_of_date < %s
                    """,
                    (target_date,),
                )
                previous_date_row = cursor.fetchone()
                previous_date = previous_date_row[0] if previous_date_row else None
                cursor.execute(
                    """
                    WITH pivoted AS (
                      SELECT
                        r.ticker,
                        MAX(CASE WHEN r.timeframe = '1d' THEN LOWER(COALESCE(r.technical_status, '')) END) AS daily_status,
                        MAX(CASE WHEN r.timeframe = '1w' THEN LOWER(COALESCE(r.technical_status, '')) END) AS weekly_status,
                        MAX(CASE WHEN r.timeframe = '1m' THEN LOWER(COALESCE(r.technical_status, '')) END) AS monthly_status
                      FROM ticker_technical_indicator_rating_snapshots r
                      LEFT JOIN ticker_metadata tm
                        ON tm.ticker = r.ticker
                      WHERE r.as_of_date = %s
                        AND (%s = '' OR LOWER(COALESCE(tm.sector, '')) = %s)
                      GROUP BY r.ticker
                    )
                    SELECT
                      CASE
                        WHEN COALESCE(daily_status, '') = 'ok'
                         AND COALESCE(weekly_status, '') = 'ok'
                         AND COALESCE(monthly_status, '') = 'ok' THEN 'ok'
                        ELSE 'missing_metrics'
                      END AS combined_status,
                      COUNT(*)
                    FROM pivoted
                    GROUP BY combined_status
                    """,
                    (target_date, normalized_sector, normalized_sector),
                )
                status_counts = {str(status or "unknown"): int(count or 0) for status, count in cursor.fetchall()}
                rows = self._fetch_top_technical_indicator_rows(
                    cursor,
                    target_date=target_date,
                    normalized_status=normalized_status,
                    normalized_sector=normalized_sector,
                    limit=normalized_limit,
                )
                previous_ranks: dict[str, int] = {}
                if isinstance(previous_date, dt.date):
                    previous_rows = self._fetch_top_technical_indicator_rows(
                        cursor,
                        target_date=previous_date,
                        normalized_status=normalized_status,
                        normalized_sector=normalized_sector,
                        limit=5000,
                    )
                    previous_ranks = {
                        str(row[0] or "").upper(): index
                        for index, row in enumerate(previous_rows, start=1)
                    }
        ranked_rows = [
            _attach_rank_change(
                {
                    "ticker": str(ticker or "").upper(),
                    "as_of_date": snapshot_date.isoformat() if isinstance(snapshot_date, dt.date) else str(snapshot_date or ""),
                    "sector": sector_value,
                    "industry": industry_value,
                    "current_rank": index,
                    "combined_status": combined_status,
                    "daily": _technical_indicator_cell(
                        "1d",
                        snapshot_date,
                        daily_ma,
                        daily_osc,
                        daily_overall,
                        daily_label,
                        daily_status,
                        daily_reason,
                    ),
                    "weekly": _technical_indicator_cell(
                        "1w",
                        snapshot_date,
                        weekly_ma,
                        weekly_osc,
                        weekly_overall,
                        weekly_label,
                        weekly_status,
                        weekly_reason,
                    ),
                    "monthly": _technical_indicator_cell(
                        "1m",
                        snapshot_date,
                        monthly_ma,
                        monthly_osc,
                        monthly_overall,
                        monthly_label,
                        monthly_status,
                        monthly_reason,
                    ),
                },
                previous_ranks,
            )
            for index, (
                ticker,
                snapshot_date,
                sector_value,
                industry_value,
                combined_status,
                daily_ma,
                daily_osc,
                daily_overall,
                daily_label,
                daily_status,
                daily_reason,
                weekly_ma,
                weekly_osc,
                weekly_overall,
                weekly_label,
                weekly_status,
                weekly_reason,
                monthly_ma,
                monthly_osc,
                monthly_overall,
                monthly_label,
                monthly_status,
                monthly_reason,
            ) in enumerate(rows, start=1)
        ]
        return {
            "as_of_date": target_date.isoformat(),
            "previous_as_of_date": previous_date.isoformat() if isinstance(previous_date, dt.date) else None,
            "rows": ranked_rows,
            "status_counts": dict(sorted(status_counts.items())),
            "sector_options": sector_options,
        }

    def _fetch_top_technical_indicator_rows(
        self,
        cursor: Any,
        *,
        target_date: dt.date,
        normalized_status: str,
        normalized_sector: str,
        limit: int,
    ) -> list[tuple[Any, ...]]:
        cursor.execute(
            """
            WITH pivoted AS (
              SELECT
                r.ticker,
                r.as_of_date,
                tm.sector,
                tm.industry,
                MAX(CASE WHEN r.timeframe = '1d' THEN r.moving_average_score END) AS daily_ma,
                MAX(CASE WHEN r.timeframe = '1d' THEN r.oscillator_score END) AS daily_osc,
                MAX(CASE WHEN r.timeframe = '1d' THEN r.overall_score END) AS daily_overall,
                MAX(CASE WHEN r.timeframe = '1d' THEN r.rating_label END) AS daily_label,
                MAX(CASE WHEN r.timeframe = '1d' THEN LOWER(COALESCE(r.technical_status, '')) END) AS daily_status,
                MAX(CASE WHEN r.timeframe = '1d' THEN r.technical_status_reason END) AS daily_reason,
                MAX(CASE WHEN r.timeframe = '1w' THEN r.moving_average_score END) AS weekly_ma,
                MAX(CASE WHEN r.timeframe = '1w' THEN r.oscillator_score END) AS weekly_osc,
                MAX(CASE WHEN r.timeframe = '1w' THEN r.overall_score END) AS weekly_overall,
                MAX(CASE WHEN r.timeframe = '1w' THEN r.rating_label END) AS weekly_label,
                MAX(CASE WHEN r.timeframe = '1w' THEN LOWER(COALESCE(r.technical_status, '')) END) AS weekly_status,
                MAX(CASE WHEN r.timeframe = '1w' THEN r.technical_status_reason END) AS weekly_reason,
                MAX(CASE WHEN r.timeframe = '1m' THEN r.moving_average_score END) AS monthly_ma,
                MAX(CASE WHEN r.timeframe = '1m' THEN r.oscillator_score END) AS monthly_osc,
                MAX(CASE WHEN r.timeframe = '1m' THEN r.overall_score END) AS monthly_overall,
                MAX(CASE WHEN r.timeframe = '1m' THEN r.rating_label END) AS monthly_label,
                MAX(CASE WHEN r.timeframe = '1m' THEN LOWER(COALESCE(r.technical_status, '')) END) AS monthly_status,
                MAX(CASE WHEN r.timeframe = '1m' THEN r.technical_status_reason END) AS monthly_reason
              FROM ticker_technical_indicator_rating_snapshots r
              LEFT JOIN ticker_metadata tm
                ON tm.ticker = r.ticker
              WHERE r.as_of_date = %s
                AND (%s = '' OR LOWER(COALESCE(tm.sector, '')) = %s)
              GROUP BY r.ticker, r.as_of_date, tm.sector, tm.industry
            ),
            filtered AS (
              SELECT
                *,
                CASE
                  WHEN COALESCE(daily_status, '') = 'ok'
                   AND COALESCE(weekly_status, '') = 'ok'
                   AND COALESCE(monthly_status, '') = 'ok' THEN 'ok'
                  ELSE 'missing_metrics'
                END AS combined_status
              FROM pivoted
            )
            SELECT
              ticker,
              as_of_date,
              sector,
              industry,
              combined_status,
              daily_ma,
              daily_osc,
              daily_overall,
              daily_label,
              daily_status,
              daily_reason,
              weekly_ma,
              weekly_osc,
              weekly_overall,
              weekly_label,
              weekly_status,
              weekly_reason,
              monthly_ma,
              monthly_osc,
              monthly_overall,
              monthly_label,
              monthly_status,
              monthly_reason
            FROM filtered
            WHERE (%s = '' OR combined_status = %s)
            ORDER BY daily_overall DESC NULLS LAST, weekly_overall DESC NULLS LAST, monthly_overall DESC NULLS LAST, ticker ASC
            LIMIT %s
            """,
            (target_date, normalized_sector, normalized_sector, normalized_status, normalized_status, max(1, int(limit))),
        )
        return list(cursor.fetchall())


def _normalize_text_values(values: Iterable[str] | None) -> list[str]:
    if not values:
        return []
    return [normalized for normalized in (str(item).strip().lower() for item in values) if normalized]


def _attach_rank_change(row: dict[str, Any], previous_ranks: dict[str, int]) -> dict[str, Any]:
    current_rank = int(row["current_rank"])
    previous_rank = previous_ranks.get(str(row.get("ticker") or "").upper())
    if previous_rank is None:
        row["previous_rank"] = None
        row["rank_change"] = "new"
        row["rank_delta"] = None
        return row
    rank_delta = previous_rank - current_rank
    if rank_delta > 0:
        rank_change = "up"
    elif rank_delta < 0:
        rank_change = "down"
    else:
        rank_change = "same"
    row["previous_rank"] = previous_rank
    row["rank_change"] = rank_change
    row["rank_delta"] = rank_delta
    return row


def _technical_indicator_cell(
    timeframe: str,
    as_of_date: dt.date | str | None,
    moving_average_score: Any,
    oscillator_score: Any,
    overall_score: Any,
    rating_label: Any,
    technical_status: Any,
    technical_status_reason: Any,
) -> dict[str, Any]:
    return {
        "timeframe": timeframe,
        "as_of_date": as_of_date.isoformat() if isinstance(as_of_date, dt.date) else str(as_of_date or ""),
        "moving_average_score": float(moving_average_score) if moving_average_score is not None else None,
        "oscillator_score": float(oscillator_score) if oscillator_score is not None else None,
        "overall_score": float(overall_score) if overall_score is not None else None,
        "rating_label": rating_label,
        "technical_status": technical_status,
        "technical_status_reason": technical_status_reason,
    }


def _coerce_json_payload(value: Any, *, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return default
