from __future__ import annotations

import datetime as dt
import json
from typing import Any, Iterable

from src.market_data_access import resolve_database_url

from .models import FundamentalsSnapshot, RatingSnapshot, SectorMetricBaseline


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
        normalized_tickers = tuple(str(item).strip().upper() for item in (tickers or []) if str(item).strip())
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
            SELECT ticker, as_of_date, sector, industry, source, source_url, parse_status, parse_error,
                   scraped_at, updated_at, market_cap, enterprise_value, forward_pe, peg_ratio_5y,
                   price_to_sales, price_to_book, price_to_fcf, profit_margin_pct, operating_margin_pct,
                   gross_margin_pct, roa_pct, roe_pct, eps_this_y_pct, eps_next_y_pct, eps_next_5y_pct,
                   sales_qq_pct, eps_qq_pct, perf_month_pct, perf_quarter_pct, perf_half_pct, perf_year_pct,
                   perf_ytd_pct, volatility_week_pct, volatility_month_pct
            FROM ticker_fundamentals_snapshots fs
            LEFT JOIN ticker_metadata tm ON tm.ticker = fs.ticker
            WHERE as_of_date = %s
        """
        params: list[Any] = [as_of_date]
        if normalized_sectors:
            sql += """
              AND LOWER(COALESCE(fs.sector, tm.sector, '')) = ANY(%s)
            """
            params.append(normalized_sectors)
        sql += """
            ORDER BY ticker ASC
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
        sql = """
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
              f.parse_error,
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
            FROM ticker_fundamentals_snapshots f
            LEFT JOIN ticker_rating_snapshots r
              ON r.ticker = f.ticker AND r.as_of_date = f.as_of_date
            WHERE f.ticker = %s
            ORDER BY f.as_of_date DESC
            LIMIT 1
        """
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (ticker.upper(),))
                row = cursor.fetchone()
        if not row:
            return None
        (
            as_of_date,
            normalized_ticker,
            sector,
            industry,
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
        ) = row
        return {
            "fundamentals_snapshot": {
                "as_of_date": as_of_date.isoformat() if isinstance(as_of_date, dt.date) else str(as_of_date),
                "ticker": normalized_ticker,
                "sector": sector,
                "industry": industry,
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
            },
            "rating_snapshot": {
                "as_of_date": as_of_date.isoformat() if isinstance(as_of_date, dt.date) else str(as_of_date),
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
            },
            "rating_diagnostics": {
                "missing_metric_names": list(missing_metric_names or []),
                "insufficient_baseline_metrics": list(insufficient_baseline_metrics or []),
            },
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


def _normalize_text_values(values: Iterable[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    return tuple(normalized for normalized in (str(item).strip().lower() for item in values) if normalized)
