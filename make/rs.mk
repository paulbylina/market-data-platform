.PHONY: rs-build rs-view rs-test rs-smoke rs-data-stock rs-data-benchmark rs-data rs-gap-candidates rs-gap-entry rs-gap-exits rs-gap-wide-stops rs-gap-relative-sweep rs-gap-spy-confirmation

RS_CONFIG ?= configs/scanners/rs_scanner.json
RS_GAP_OUTPUT_DIR ?= data/research/intraday_gap_up

# GET DATA
rs-data-stock:
	uv run python -m scripts.rs.get_rs_data stock --config $(RS_CONFIG)

rs-data-benchmark:
	uv run python -m scripts.rs.get_rs_data benchmark --config $(RS_CONFIG)

rs-data:
	uv run python -m scripts.rs.get_rs_data all --config $(RS_CONFIG)

# VALIDATE DATA
rs-validate-data:
	uv run python scripts/rs/validate_rs_data_coverage.py

# VALIDATE SERVING
rs-validate-serving:
	uv run python scripts/rs/validate_rs_serving_coverage.py

# BUILD
rs-build:
	uv run python -m scripts.rs.build_rs_serving --config $(RS_CONFIG)

# REFRESH
rs-refresh: rs-data rs-build

# BACKTEST - ROBUSTNESS
rs-backtest-grid:
	uv run python scripts/rs/backtests/backtest_rs_parameter_grid.py

# BACKTEST - EXPOSURE
rs-backtest-exposure:
	uv run python scripts/rs/backtests/backtest_rs_exposure_caps.py

# BACKTEST - REGIME
rs-backtest-regime:
	uv run python scripts/rs/backtests/backtest_rs_regime_grid.py

# BACKTEST - REGIME EXPOSURE
rs-backtest-regime-exposure:
	uv run python scripts/rs/backtests/backtest_rs_regime_exposure_caps.py

# BACTEST - SECTOR REGIME
rs-backtest-sector-regime:
	uv run python scripts/rs/backtests/backtest_rs_sector_regime_grid.py

# BACTEST - SECTOR RELATIVE REGIME
rs-backtest-sector-relative-regime:
	uv run python scripts/rs/backtests/backtest_rs_sector_relative_regime.py

# BACKTEST - SECTOR PEER RANK
rs-backtest-sector-peer-rank:
	uv run python scripts/rs/backtests/backtest_rs_sector_peer_rank.py

# BACTEST - FOCUSED LEADERS
rs-backtest-focused-leaders:
	uv run python scripts/rs/backtests/backtest_rs_focused_leader_confirmation.py

# VIEW
rs-view:
	uv run python -m scripts.rs.view_rs_serving --config $(RS_CONFIG)

# TEST
rs-test:
	uv run pytest tests/unit/test_relative_strength_scanner.py -v

rs-smoke:
	uv run pytest tests/smoke/test_rs_scanner_massive_smoke.py -m smoke -s -v

# PULLBACK DAILY SIGNALS
rs-daily-signals:
	uv run python scripts/rs/signals/generate_daily_pullback_rvol_signals.py

# REFRESH DAILY SIGNALS
rs-refresh-signals: rs-data rs-build rs-daily-signals

# PULLBACK RENDER APP
rs-signal-app:
	uv run uvicorn src.apps.daily_signal_api.main:app --reload



# INTRADAY GAP-UP RESEARCH
rs-gap-candidates:
	uv run python -m scripts.rs.intraday.build_gap_up_candidate_trades --config $(RS_CONFIG) --output-dir $(RS_GAP_OUTPUT_DIR)

rs-gap-entry:
	uv run python -m scripts.rs.intraday.backtest_gap_up_15m_entry --output-dir $(RS_GAP_OUTPUT_DIR)

rs-gap-exits:
	uv run python -m scripts.rs.intraday.backtest_gap_up_15m_exits --output-dir $(RS_GAP_OUTPUT_DIR)

rs-gap-wide-stops:
	uv run python -m scripts.rs.intraday.backtest_gap_up_15m_wide_stops --output-dir $(RS_GAP_OUTPUT_DIR)

rs-gap-relative-sweep:
	uv run python -m scripts.rs.intraday.backtest_gap_up_15m_relative_gap_sweep --config $(RS_CONFIG) --output-dir $(RS_GAP_OUTPUT_DIR)

rs-gap-spy-confirmation:
	uv run python -m scripts.rs.intraday.backtest_gap_up_15m_spy_confirmation --config $(RS_CONFIG) --output-dir $(RS_GAP_OUTPUT_DIR)