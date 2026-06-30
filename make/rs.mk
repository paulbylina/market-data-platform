.PHONY: rs-build rs-view rs-test rs-smoke rs-data-stock rs-data-benchmark rs-data


# GET DATA
rs-data-stock:
	uv run python -m scripts.rs.get_rs_data stock --config configs/scanners/rs_scanner.json

rs-data-benchmark:
	uv run python -m scripts.rs.get_rs_data benchmark --config configs/scanners/rs_scanner.json

rs-data:
	uv run python -m scripts.rs.get_rs_data all --config configs/scanners/rs_scanner.json

# VALIDATE DATA
rs-validate-data:
	uv run python scripts/rs/validate_rs_data_coverage.py

# VALIDATE SERVING
rs-validate-serving:
	uv run python scripts/rs/validate_rs_serving_coverage.py

# BUILD
rs-build:
	uv run python -m scripts.rs.build_rs_serving --config configs/scanners/rs_scanner.json

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
	uv run python -m scripts.rs.view_rs_serving --config configs/scanners/rs_scanner.json

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