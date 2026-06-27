.PHONY: rs-run rs-test rs-smoke rs-data-stock rs-data-benchmark rs-data

# GET DATA
rs-data-stock:
	uv run python -m scripts.run_daily_eod_from_rs_config stock --config configs/scanners/rs_scanner.json

rs-data-benchmark:
	uv run python -m scripts.run_daily_eod_from_rs_config benchmark --config configs/scanners/rs_scanner.json

rs-data:
	uv run python -m scripts.run_daily_eod_from_rs_config all --config configs/scanners/rs_scanner.json

# RUN
rs-run:
	uv run python -m scripts.run_rs_scanner_from_curated --config configs/scanners/rs_scanner.json

# TEST
rs-test:
	uv run pytest tests/unit/test_relative_strength_scanner.py -v

rs-smoke:
	uv run pytest tests/smoke/test_rs_scanner_massive_smoke.py -m smoke -s -v