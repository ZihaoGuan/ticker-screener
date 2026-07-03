from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any


DATE_LABEL_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

_STRATEGY_SPECS: dict[str, dict[str, Any]] = {
    "rs": {
        "stem_template": "rs_new_high_before_price_{date_label}",
        "legacy_watchlist_templates": ("rs_new_high_before_price_{date_label}.json",),
        "legacy_raw_templates": ("rs_new_high_before_price_{date_label}.json",),
        "legacy_summary_templates": ("run_summary_{date_label}.json",),
    },
    "daily_rs_new_high": {
        "stem_template": "daily_rs_new_high_{date_label}",
        "legacy_watchlist_templates": ("daily_rs_new_high_{date_label}.json",),
        "legacy_raw_templates": ("daily_rs_new_high_{date_label}.json",),
        "legacy_summary_templates": ("daily_rs_new_high_run_summary_{date_label}.json",),
    },
    "weekly_rs": {
        "stem_template": "weekly_rs_new_high_{date_label}",
        "legacy_watchlist_templates": ("weekly_rs_new_high_{date_label}.json",),
        "legacy_raw_templates": ("weekly_rs_new_high_{date_label}.json",),
        "legacy_summary_templates": ("weekly_rs_run_summary_{date_label}.json",),
    },
    "weekly_rs_new_high": {
        "stem_template": "weekly_rs_new_high_all_{date_label}",
        "legacy_watchlist_templates": ("weekly_rs_new_high_all_{date_label}.json",),
        "legacy_raw_templates": ("weekly_rs_new_high_all_{date_label}.json",),
        "legacy_summary_templates": ("weekly_rs_new_high_all_run_summary_{date_label}.json",),
    },
    "weekly_htf_pullback": {
        "stem_template": "weekly_htf_pullback_{date_label}",
        "legacy_watchlist_templates": ("weekly_htf_pullback_{date_label}.json",),
        "legacy_raw_templates": ("weekly_htf_pullback_{date_label}.json",),
        "legacy_summary_templates": ("weekly_htf_pullback_run_summary_{date_label}.json",),
    },
    "vcp": {
        "stem_template": "vcp_{date_label}",
        "legacy_watchlist_templates": ("vcp_{date_label}.json",),
        "legacy_raw_templates": ("vcp_{date_label}.json",),
        "legacy_summary_templates": ("vcp_run_summary_{date_label}.json",),
    },
    "vcp_scored": {
        "stem_template": "vcp_scored_{date_label}",
        "legacy_watchlist_templates": ("vcp_scored_{date_label}.json",),
        "legacy_raw_templates": ("vcp_scored_{date_label}.json",),
        "legacy_summary_templates": ("vcp_scored_run_summary_{date_label}.json",),
    },
    "vcp_v3": {
        "stem_template": "vcp_v3_{date_label}",
        "legacy_watchlist_templates": ("vcp_v3_{date_label}.json",),
        "legacy_raw_templates": ("vcp_v3_{date_label}.json",),
        "legacy_summary_templates": ("vcp_v3_run_summary_{date_label}.json",),
    },
    "cup_handle": {
        "stem_template": "cup_handle_{date_label}",
        "legacy_watchlist_templates": ("cup_handle_{date_label}.json",),
        "legacy_raw_templates": ("cup_handle_{date_label}.json",),
        "legacy_summary_templates": ("cup_handle_run_summary_{date_label}.json",),
    },
    "gap_fill": {
        "stem_template": "gap_fill_{date_label}",
        "legacy_watchlist_templates": ("gap_fill_{date_label}.json",),
        "legacy_raw_templates": ("gap_fill_{date_label}.json",),
        "legacy_summary_templates": ("gap_fill_run_summary_{date_label}.json",),
    },
    "leif_high_tight_flag": {
        "stem_template": "leif_high_tight_flag_{date_label}",
        "legacy_watchlist_templates": ("leif_high_tight_flag_{date_label}.json",),
        "legacy_raw_templates": ("leif_high_tight_flag_{date_label}.json",),
        "legacy_summary_templates": ("leif_high_tight_flag_run_summary_{date_label}.json",),
    },
    "high_tight_flag_setup": {
        "stem_template": "high_tight_flag_setup_{date_label}",
        "legacy_watchlist_templates": ("high_tight_flag_setup_{date_label}.json",),
        "legacy_raw_templates": ("high_tight_flag_setup_{date_label}.json",),
        "legacy_summary_templates": ("high_tight_flag_setup_run_summary_{date_label}.json",),
    },
    "hve": {
        "stem_template": "hve_{date_label}",
        "legacy_watchlist_templates": ("hve_{date_label}.json",),
        "legacy_raw_templates": ("hve_{date_label}.json",),
        "legacy_summary_templates": ("hve_run_summary_{date_label}.json",),
    },
    "elite_rs_hv1": {
        "stem_template": "elite_rs_hv1_{date_label}",
        "legacy_watchlist_templates": ("elite_rs_hv1_{date_label}.json",),
        "legacy_raw_templates": ("elite_rs_hv1_{date_label}.json",),
        "legacy_summary_templates": ("elite_rs_hv1_run_summary_{date_label}.json",),
    },
    "elite_rs_recent_peg": {
        "stem_template": "elite_rs_recent_peg_{date_label}",
        "legacy_watchlist_templates": ("elite_rs_recent_peg_{date_label}.json",),
        "legacy_raw_templates": ("elite_rs_recent_peg_{date_label}.json",),
        "legacy_summary_templates": ("elite_rs_recent_peg_run_summary_{date_label}.json",),
    },
    "pine_peg": {
        "stem_template": "pine_peg_{date_label}",
        "legacy_watchlist_templates": ("pine_peg_{date_label}.json",),
        "legacy_raw_templates": ("pine_peg_{date_label}.json",),
        "legacy_summary_templates": ("pine_peg_run_summary_{date_label}.json",),
    },
    "monster_gap": {
        "stem_template": "monster_gap_{date_label}",
        "legacy_watchlist_templates": ("monster_gap_{date_label}.json",),
        "legacy_raw_templates": ("monster_gap_{date_label}.json",),
        "legacy_summary_templates": ("monster_gap_run_summary_{date_label}.json",),
    },
    "monster_peg": {
        "stem_template": "monster_peg_{date_label}",
        "legacy_watchlist_templates": ("monster_peg_{date_label}.json",),
        "legacy_raw_templates": ("monster_peg_{date_label}.json",),
        "legacy_summary_templates": ("monster_peg_run_summary_{date_label}.json",),
    },
    "bb_squeeze": {
        "stem_template": "bb_squeeze_{date_label}",
        "legacy_watchlist_templates": ("bb_squeeze_{date_label}.json",),
        "legacy_raw_templates": ("bb_squeeze_{date_label}.json",),
        "legacy_summary_templates": ("bb_squeeze_run_summary_{date_label}.json",),
    },
    "ema21_pullback_buy": {
        "stem_template": "ema21_pullback_buy_{date_label}",
        "legacy_watchlist_templates": ("ema21_pullback_buy_{date_label}.json",),
        "legacy_raw_templates": ("ema21_pullback_buy_{date_label}.json",),
        "legacy_summary_templates": ("ema21_pullback_buy_run_summary_{date_label}.json",),
    },
    "sma200_pullback_buy": {
        "stem_template": "sma200_pullback_buy_{date_label}",
        "legacy_watchlist_templates": ("sma200_pullback_buy_{date_label}.json",),
        "legacy_raw_templates": ("sma200_pullback_buy_{date_label}.json",),
        "legacy_summary_templates": ("sma200_pullback_buy_run_summary_{date_label}.json",),
    },
    "sepa_vcp": {
        "stem_template": "sepa_vcp_{date_label}",
        "legacy_watchlist_templates": ("sepa_vcp_{date_label}.json",),
        "legacy_raw_templates": ("sepa_vcp_{date_label}.json",),
        "legacy_summary_templates": ("sepa_vcp_run_summary_{date_label}.json",),
    },
    "rti": {
        "stem_template": "rti_{date_label}",
        "legacy_watchlist_templates": ("rti_{date_label}.json",),
        "legacy_raw_templates": ("rti_{date_label}.json",),
        "legacy_summary_templates": ("rti_run_summary_{date_label}.json",),
    },
    "sean_breakout": {
        "stem_template": "sean_breakout_{date_label}",
        "legacy_watchlist_templates": ("sean_breakout_{date_label}.json",),
        "legacy_raw_templates": ("sean_breakout_{date_label}.json",),
        "legacy_summary_templates": ("sean_breakout_run_summary_{date_label}.json",),
    },
    "vcs_setup_stage": {
        "stem_template": "vcs_setup_stage_{date_label}",
        "legacy_watchlist_templates": ("vcs_setup_stage_{date_label}.json",),
        "legacy_raw_templates": ("vcs_setup_stage_{date_label}.json",),
        "legacy_summary_templates": ("vcs_setup_stage_run_summary_{date_label}.json",),
    },
    "vcs_critical_tightness": {
        "stem_template": "vcs_critical_tightness_{date_label}",
        "legacy_watchlist_templates": ("vcs_critical_tightness_{date_label}.json",),
        "legacy_raw_templates": ("vcs_critical_tightness_{date_label}.json",),
        "legacy_summary_templates": ("vcs_critical_tightness_run_summary_{date_label}.json",),
    },
    "inside_dryup": {
        "stem_template": "inside_dryup_{date_label}",
        "legacy_watchlist_templates": ("inside_dryup_{date_label}.json",),
        "legacy_raw_templates": ("inside_dryup_{date_label}.json",),
        "legacy_summary_templates": ("inside_dryup_run_summary_{date_label}.json",),
    },
    "inside_dryup_v2": {
        "stem_template": "inside_dryup_v2_{date_label}",
        "legacy_watchlist_templates": ("inside_dryup_v2_{date_label}.json",),
        "legacy_raw_templates": ("inside_dryup_v2_{date_label}.json",),
        "legacy_summary_templates": ("inside_dryup_v2_run_summary_{date_label}.json",),
    },
    "wyckoff_buy_signal": {
        "stem_template": "wyckoff_buy_signal_{date_label}",
        "legacy_watchlist_templates": ("wyckoff_buy_signal_{date_label}.json",),
        "legacy_raw_templates": ("wyckoff_buy_signal_{date_label}.json",),
        "legacy_summary_templates": ("wyckoff_buy_signal_run_summary_{date_label}.json",),
    },
    "wyckoff_sell_signal": {
        "stem_template": "wyckoff_sell_signal_{date_label}",
        "legacy_watchlist_templates": ("wyckoff_sell_signal_{date_label}.json",),
        "legacy_raw_templates": ("wyckoff_sell_signal_{date_label}.json",),
        "legacy_summary_templates": ("wyckoff_sell_signal_run_summary_{date_label}.json",),
    },
    "ftd_sweep": {
        "stem_template": "ftd_sweep_{date_label}",
        "legacy_watchlist_templates": ("ftd_sweep_{date_label}.json",),
        "legacy_raw_templates": ("ftd_sweep_{date_label}.json",),
        "legacy_summary_templates": ("ftd_sweep_run_summary_{date_label}.json",),
    },
    "fearzone": {
        "stem_template": "fearzone_{date_label}",
        "legacy_watchlist_templates": ("fearzone_{date_label}.json",),
        "legacy_raw_templates": ("fearzone_{date_label}.json",),
        "legacy_summary_templates": ("fearzone_run_summary_{date_label}.json",),
    },
    "fearzone_zeiierman": {
        "stem_template": "fearzone_zeiierman_{date_label}",
        "legacy_watchlist_templates": ("fearzone_zeiierman_{date_label}.json",),
        "legacy_raw_templates": ("fearzone_zeiierman_{date_label}.json",),
        "legacy_summary_templates": ("fearzone_zeiierman_run_summary_{date_label}.json",),
    },
    "td9_bullish": {
        "stem_template": "td9_bullish_{date_label}",
        "legacy_watchlist_templates": ("td9_bullish_{date_label}.json",),
        "legacy_raw_templates": ("td9_bullish_{date_label}.json",),
        "legacy_summary_templates": ("td9_bullish_run_summary_{date_label}.json",),
    },
    "td9_bearish": {
        "stem_template": "td9_bearish_{date_label}",
        "legacy_watchlist_templates": ("td9_bearish_{date_label}.json",),
        "legacy_raw_templates": ("td9_bearish_{date_label}.json",),
        "legacy_summary_templates": ("td9_bearish_run_summary_{date_label}.json",),
    },
    "macd_golden_cross": {
        "stem_template": "macd_golden_cross_{date_label}",
        "legacy_watchlist_templates": ("macd_golden_cross_{date_label}.json",),
        "legacy_raw_templates": ("macd_golden_cross_{date_label}.json",),
        "legacy_summary_templates": ("macd_golden_cross_run_summary_{date_label}.json",),
    },
    "macd_dead_cross": {
        "stem_template": "macd_dead_cross_{date_label}",
        "legacy_watchlist_templates": ("macd_dead_cross_{date_label}.json",),
        "legacy_raw_templates": ("macd_dead_cross_{date_label}.json",),
        "legacy_summary_templates": ("macd_dead_cross_run_summary_{date_label}.json",),
    },
    "rsi_ma_bb_bullish": {
        "stem_template": "rsi_ma_bb_bullish_{date_label}",
        "legacy_watchlist_templates": ("rsi_ma_bb_bullish_{date_label}.json",),
        "legacy_raw_templates": ("rsi_ma_bb_bullish_{date_label}.json",),
        "legacy_summary_templates": ("rsi_ma_bb_bullish_run_summary_{date_label}.json",),
    },
    "rsi_ma_bb_bearish": {
        "stem_template": "rsi_ma_bb_bearish_{date_label}",
        "legacy_watchlist_templates": ("rsi_ma_bb_bearish_{date_label}.json",),
        "legacy_raw_templates": ("rsi_ma_bb_bearish_{date_label}.json",),
        "legacy_summary_templates": ("rsi_ma_bb_bearish_run_summary_{date_label}.json",),
    },
    "base_detection": {
        "stem_template": "base_detection_{date_label}",
        "legacy_watchlist_templates": ("base_detection_{date_label}.json",),
        "legacy_raw_templates": ("base_detection_{date_label}.json",),
        "legacy_summary_templates": ("base_detection_run_summary_{date_label}.json",),
    },
    "cup_detection": {
        "stem_template": "cup_detection_{date_label}",
        "legacy_watchlist_templates": ("cup_detection_{date_label}.json",),
        "legacy_raw_templates": ("cup_detection_{date_label}.json",),
        "legacy_summary_templates": ("cup_detection_run_summary_{date_label}.json",),
    },
    "double_bottom_detection": {
        "stem_template": "double_bottom_detection_{date_label}",
        "legacy_watchlist_templates": ("double_bottom_detection_{date_label}.json",),
        "legacy_raw_templates": ("double_bottom_detection_{date_label}.json",),
        "legacy_summary_templates": ("double_bottom_detection_run_summary_{date_label}.json",),
    },
    "weekly_tight_close": {
        "stem_template": "weekly_tight_close_{date_label}",
        "legacy_watchlist_templates": ("weekly_tight_close_{date_label}.json",),
        "legacy_raw_templates": ("weekly_tight_close_{date_label}.json",),
        "legacy_summary_templates": ("weekly_tight_close_run_summary_{date_label}.json",),
    },
    "weinstein_stage2_early": {
        "stem_template": "weinstein_stage2_early_{date_label}",
        "legacy_watchlist_templates": ("weinstein_stage2_early_{date_label}.json",),
        "legacy_raw_templates": ("weinstein_stage2_early_{date_label}.json",),
        "legacy_summary_templates": ("weinstein_stage2_early_run_summary_{date_label}.json",),
    },
    "weekly_tight_close_breakout": {
        "stem_template": "weekly_tight_close_breakout_{date_label}",
        "legacy_watchlist_templates": ("weekly_tight_close_breakout_{date_label}.json",),
        "legacy_raw_templates": ("weekly_tight_close_breakout_{date_label}.json",),
        "legacy_summary_templates": ("weekly_tight_close_breakout_run_summary_{date_label}.json",),
    },
    "three_weeks_tight": {
        "stem_template": "three_weeks_tight_{date_label}",
        "legacy_watchlist_templates": ("three_weeks_tight_{date_label}.json",),
        "legacy_raw_templates": ("three_weeks_tight_{date_label}.json",),
        "legacy_summary_templates": ("three_weeks_tight_run_summary_{date_label}.json",),
    },
    "near_200ma": {
        "stem_template": "near_200ma_{date_label}",
        "legacy_watchlist_templates": ("near_200ma_{date_label}.json",),
        "legacy_raw_templates": ("near_200ma_{date_label}.json",),
        "legacy_summary_templates": ("near_200ma_run_summary_{date_label}.json",),
    },
    "lost_21ema": {
        "stem_template": "lost_21ema_{date_label}",
        "legacy_watchlist_templates": ("lost_21ema_{date_label}.json",),
        "legacy_raw_templates": ("lost_21ema_{date_label}.json",),
        "legacy_summary_templates": ("lost_21ema_run_summary_{date_label}.json",),
    },
    "trend_template": {
        "stem_template": "trend_template_{date_label}",
        "legacy_watchlist_templates": ("trend_template_{date_label}.json",),
        "legacy_raw_templates": ("trend_template_{date_label}.json",),
        "legacy_summary_templates": ("trend_template_run_summary_{date_label}.json",),
    },
    "market_correction_resilience": {
        "stem_template": "market_correction_resilience_{date_label}",
        "legacy_watchlist_templates": ("market_correction_resilience_{date_label}.json",),
        "legacy_raw_templates": ("market_correction_resilience_{date_label}.json",),
        "legacy_summary_templates": ("market_correction_resilience_run_summary_{date_label}.json",),
    },
    "stockbee_momentum_burst": {
        "stem_template": "stockbee_momentum_burst_{date_label}",
        "legacy_watchlist_templates": ("stockbee_momentum_burst_{date_label}.json",),
        "legacy_raw_templates": ("stockbee_momentum_burst_{date_label}.json",),
        "legacy_summary_templates": ("stockbee_momentum_burst_run_summary_{date_label}.json",),
    },
    "vcp_spec": {
        "stem_template": "vcp_spec_{date_label}",
        "legacy_watchlist_templates": ("vcp_spec_{date_label}.json",),
        "legacy_raw_templates": ("vcp_spec_{date_label}.json",),
        "legacy_summary_templates": ("vcp_spec_run_summary_{date_label}.json",),
    },
    "eight_week_100_runup": {
        "stem_template": "eight_week_100_runup_{date_label}",
        "legacy_watchlist_templates": (
            "eight_week_100_runup_{date_label}.json",
            "htf_8w_runup_{date_label}.json",
        ),
        "legacy_raw_templates": (
            "eight_week_100_runup_{date_label}.json",
            "htf_8w_runup_{date_label}.json",
        ),
        "legacy_summary_templates": (
            "eight_week_100_runup_run_summary_{date_label}.json",
            "htf_8w_runup_run_summary_{date_label}.json",
        ),
    },
    "pre_earnings_ma_stack": {
        "stem_template": "pre_earnings_ma_stack_{date_label}",
        "legacy_watchlist_templates": ("pre_earnings_ma_stack_{date_label}.json",),
        "legacy_raw_templates": ("pre_earnings_ma_stack_{date_label}.json",),
        "legacy_summary_templates": ("pre_earnings_ma_stack_run_summary_{date_label}.json",),
    },
    "pre_earnings_focus": {
        "stem_template": "pre_earnings_focus_{date_label}",
        "legacy_watchlist_templates": ("pre_earnings_focus_{date_label}.json",),
        "legacy_raw_templates": ("pre_earnings_focus_{date_label}.json",),
        "legacy_summary_templates": ("pre_earnings_run_summary_{date_label}.json",),
    },
    "earnings_growth": {
        "stem_template": "earnings_growth_{date_label}",
        "legacy_watchlist_templates": ("earnings_growth_{date_label}.json",),
        "legacy_raw_templates": ("earnings_growth_{date_label}.json",),
        "legacy_summary_templates": ("earnings_growth_run_summary_{date_label}.json",),
    },
    "earnings_trade_analyzer": {
        "stem_template": "earnings_trade_analyzer_{date_label}",
        "legacy_watchlist_templates": ("earnings_trade_analyzer_{date_label}.json",),
        "legacy_raw_templates": ("earnings_trade_analyzer_{date_label}.json",),
        "legacy_summary_templates": ("earnings_trade_analyzer_run_summary_{date_label}.json",),
    },
    "pead_screener": {
        "stem_template": "pead_screener_{date_label}",
        "legacy_watchlist_templates": ("pead_screener_{date_label}.json",),
        "legacy_raw_templates": ("pead_screener_{date_label}.json",),
        "legacy_summary_templates": ("pead_screener_run_summary_{date_label}.json",),
    },
    "canslim": {
        "stem_template": "canslim_{date_label}",
        "legacy_watchlist_templates": ("canslim_{date_label}.json",),
        "legacy_raw_templates": ("canslim_{date_label}.json",),
        "legacy_summary_templates": ("canslim_run_summary_{date_label}.json",),
    },
    "canslim_v2": {
        "stem_template": "canslim_v2_{date_label}",
        "legacy_watchlist_templates": ("canslim_v2_{date_label}.json",),
        "legacy_raw_templates": ("canslim_v2_{date_label}.json",),
        "legacy_summary_templates": ("canslim_v2_run_summary_{date_label}.json",),
    },
    "venu_scanner": {
        "stem_template": "venu_scanner_{date_label}",
        "legacy_watchlist_templates": ("venu_scanner_{date_label}.json",),
        "legacy_raw_templates": ("venu_scanner_{date_label}.json",),
        "legacy_summary_templates": ("venu_scanner_run_summary_{date_label}.json",),
    },
    "earnings_weekly_criteria": {
        "stem_template": "earnings_weekly_criteria_{date_label}",
        "legacy_watchlist_templates": ("earnings_weekly_criteria_{date_label}.json",),
        "legacy_raw_templates": ("earnings_weekly_criteria_{date_label}.json",),
        "legacy_summary_templates": ("earnings_weekly_criteria_run_summary_{date_label}.json",),
    },
    "flashalpha_gex_close": {
        "stem_template": "flashalpha_gex_close_{date_label}",
        "legacy_watchlist_templates": ("flashalpha_gex_close_{date_label}.json",),
        "legacy_raw_templates": ("flashalpha_gex_close_{date_label}.json",),
        "legacy_summary_templates": ("flashalpha_gex_close_run_summary_{date_label}.json",),
    },
    "gamma_squeeze": {
        "stem_template": "gamma_squeeze_{date_label}",
        "legacy_watchlist_templates": ("gamma_squeeze_{date_label}.json",),
        "legacy_raw_templates": ("gamma_squeeze_{date_label}.json",),
        "legacy_summary_templates": ("gamma_squeeze_run_summary_{date_label}.json",),
    },
    "legacy_peg": {
        "stem_template": "legacy_peg_earnings_gap_{date_label}",
        "legacy_watchlist_templates": ("legacy_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_raw_templates": ("legacy_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_summary_templates": ("legacy_peg_run_summary_{date_label}.json", "peg_run_summary_{date_label}.json"),
    },
    "sean_peg": {
        "stem_template": "sean_peg_earnings_gap_{date_label}",
        "legacy_watchlist_templates": ("sean_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_raw_templates": ("sean_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_summary_templates": ("sean_peg_run_summary_{date_label}.json", "peg_run_summary_{date_label}.json"),
    },
    "sean_gap_up": {
        "stem_template": "sean_peg_earnings_gap_{date_label}",
        "legacy_watchlist_templates": ("sean_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_raw_templates": ("sean_peg_earnings_gap_{date_label}.json", "peg_earnings_gap_{date_label}.json"),
        "legacy_summary_templates": ("sean_peg_run_summary_{date_label}.json", "peg_run_summary_{date_label}.json"),
    },
}

_LEGACY_PREFIX_TO_STRATEGY: tuple[tuple[str, str], ...] = (
    ("weekly_htf_pullback", "weekly_htf_pullback"),
    ("weekly_rs_new_high_all", "weekly_rs_new_high"),
    ("weekly_rs_new_high", "weekly_rs"),
    ("daily_rs_new_high", "daily_rs_new_high"),
    ("rs_new_high_before_price", "rs"),
    ("legacy_peg_earnings_gap", "legacy_peg"),
    ("sean_gap_up_earnings_gap", "sean_gap_up"),
    ("sean_peg_earnings_gap", "sean_peg"),
    ("peg_earnings_gap", "legacy_peg"),
    ("cup_handle", "cup_handle"),
    ("gap_fill", "gap_fill"),
    ("ftd_sweep", "ftd_sweep"),
    ("fearzone_zeiierman", "fearzone_zeiierman"),
    ("td9_bullish", "td9_bullish"),
    ("td9_bearish", "td9_bearish"),
    ("macd_golden_cross", "macd_golden_cross"),
    ("macd_dead_cross", "macd_dead_cross"),
    ("rsi_ma_bb_bullish", "rsi_ma_bb_bullish"),
    ("rsi_ma_bb_bearish", "rsi_ma_bb_bearish"),
    ("base_detection", "base_detection"),
    ("cup_detection", "cup_detection"),
    ("double_bottom_detection", "double_bottom_detection"),
    ("weinstein_stage2_early", "weinstein_stage2_early"),
    ("weekly_tight_close", "weekly_tight_close"),
    ("weekly_tight_close_breakout", "weekly_tight_close_breakout"),
    ("three_weeks_tight", "three_weeks_tight"),
    ("fearzone", "fearzone"),
    ("near_200ma", "near_200ma"),
    ("lost_21ema", "lost_21ema"),
    ("trend_template", "trend_template"),
    ("market_correction_resilience", "market_correction_resilience"),
    ("stockbee_momentum_burst", "stockbee_momentum_burst"),
    ("vcp_spec", "vcp_spec"),
    ("pre_earnings_ma_stack", "pre_earnings_ma_stack"),
    ("pre_earnings_focus", "pre_earnings_focus"),
    ("earnings_weekly_criteria", "earnings_weekly_criteria"),
    ("earnings_trade_analyzer", "earnings_trade_analyzer"),
    ("pead_screener", "pead_screener"),
    ("flashalpha_gex_close", "flashalpha_gex_close"),
    ("gamma_squeeze", "gamma_squeeze"),
    ("earnings_growth", "earnings_growth"),
    ("canslim_v2", "canslim_v2"),
    ("canslim", "canslim"),
    ("fundamental_quality", "fundamental_quality"),
    ("minervini_growth_acceleration", "minervini_growth_acceleration"),
    ("industry_group_rs_rank", "industry_group_rs_rank"),
    ("venu_scanner", "venu_scanner"),
    ("inside_dryup_v2", "inside_dryup_v2"),
    ("wyckoff_buy_signal", "wyckoff_buy_signal"),
    ("wyckoff_sell_signal", "wyckoff_sell_signal"),
    ("inside_dryup", "inside_dryup"),
    ("eight_week_100_runup", "eight_week_100_runup"),
    ("htf_8w_runup", "eight_week_100_runup"),
    ("elite_rs_hv1", "elite_rs_hv1"),
    ("elite_rs_recent_peg", "elite_rs_recent_peg"),
    ("pine_peg", "pine_peg"),
    ("monster_gap", "monster_gap"),
    ("monster_peg", "monster_peg"),
    ("hve", "hve"),
    ("bb_squeeze", "bb_squeeze"),
    ("ema21_pullback_buy", "ema21_pullback_buy"),
    ("sma200_pullback_buy", "sma200_pullback_buy"),
    ("sepa_vcp", "sepa_vcp"),
    ("rti", "rti"),
    ("sean_breakout", "sean_breakout"),
    ("vcs_setup_stage", "vcs_setup_stage"),
    ("vcs_critical_tightness", "vcs_critical_tightness"),
    ("vcp_scored", "vcp_scored"),
    ("vcp_v3", "vcp_v3"),
    ("vcp", "vcp"),
)


@dataclass(frozen=True)
class ScreenerArtifactPaths:
    strategy_id: str
    date_label: str
    date_folder: str
    logical_stem: str
    root_dir: Path
    raw_results_path: Path
    watchlist_path: Path
    summary_path: Path


def logical_stem_for_strategy(strategy_id: str, date_label: str) -> str:
    spec = _STRATEGY_SPECS.get(strategy_id)
    if spec is None:
        return f"{strategy_id}_{date_label}"
    return str(spec["stem_template"]).format(date_label=date_label)


def resolve_artifact_date_folder(date_label: str) -> str:
    normalized = str(date_label or "").strip()
    match = DATE_LABEL_RE.search(normalized)
    if match:
        return match.group(0)
    return normalized or "unknown-date"


def build_screener_artifact_paths(artifacts_dir: Path, *, strategy_id: str, date_label: str) -> ScreenerArtifactPaths:
    logical_stem = logical_stem_for_strategy(strategy_id, date_label)
    date_folder = resolve_artifact_date_folder(date_label)
    root_dir = artifacts_dir / "screeners" / date_folder / strategy_id
    return ScreenerArtifactPaths(
        strategy_id=strategy_id,
        date_label=date_label,
        date_folder=date_folder,
        logical_stem=logical_stem,
        root_dir=root_dir,
        raw_results_path=root_dir / "raw_results.json",
        watchlist_path=root_dir / "watchlist.json",
        summary_path=root_dir / "run_summary.json",
    )


def strategy_spec(strategy_id: str) -> dict[str, Any] | None:
    return _STRATEGY_SPECS.get(strategy_id)


def strategy_id_from_legacy_stem(stem: str) -> str:
    lower = str(stem or "").strip().lower()
    for prefix, strategy_id in _LEGACY_PREFIX_TO_STRATEGY:
        if lower.startswith(prefix):
            return strategy_id
    return ""


def date_label_from_text(value: str) -> str:
    match = DATE_LABEL_RE.search(str(value or ""))
    return match.group(0) if match else ""


def watchlist_stem_from_path(path_value: str | Path) -> str:
    path = Path(path_value)
    if path.suffix.lower() != ".json":
        return ""
    if path.name == "watchlist.json":
        parts = path.parts
        try:
            index = parts.index("screeners")
        except ValueError:
            return ""
        if len(parts) <= index + 3:
            return ""
        date_folder = parts[index + 1]
        strategy_id = parts[index + 2]
        summary_path = path.parent / "run_summary.json"
        date_label = date_folder
        if summary_path.exists():
            try:
                import json

                payload = json.loads(summary_path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            summary_label = str(payload.get("date_label") or "").strip()
            if summary_label:
                date_label = summary_label
            profile = str(payload.get("strategy_profile") or "").strip().lower()
            if strategy_id == "legacy_peg" and profile == "sean-peg":
                strategy_id = "sean_peg"
        return logical_stem_for_strategy(strategy_id, date_label)
    return path.stem


def resolve_legacy_paths(artifacts_dir: Path, *, strategy_id: str, date_label: str) -> dict[str, list[Path]]:
    spec = strategy_spec(strategy_id)
    if spec is None:
        return {"watchlist": [], "raw": [], "summary": []}
    return {
        "watchlist": [(artifacts_dir / "watchlists" / template.format(date_label=date_label)) for template in spec["legacy_watchlist_templates"]],
        "raw": [(artifacts_dir / "raw" / template.format(date_label=date_label)) for template in spec["legacy_raw_templates"]],
        "summary": [(artifacts_dir / "raw" / template.format(date_label=date_label)) for template in spec["legacy_summary_templates"]],
    }
