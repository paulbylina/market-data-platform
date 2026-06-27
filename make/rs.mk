.PHONY: rs-build rs-view rs-test rs-smoke rs-data-stock rs-data-benchmark rs-data


# GET DATA
rs-data-stock:
	uv run python -m scripts.rs.get_rs_data stock --config configs/scanners/rs_scanner.json

rs-data-benchmark:
	uv run python -m scripts.rs.get_rs_data benchmark --config configs/scanners/rs_scanner.json

rs-data:
	uv run python -m scripts.rs.get_rs_data all --config configs/scanners/rs_scanner.json


# BUILD
rs-build:
	uv run python -m scripts.rs.build_rs_serving --config configs/scanners/rs_scanner.json


# VIEW
rs-view:
	uv run python -m scripts.rs.view_rs_serving --config configs/scanners/rs_scanner.json


# TEST
rs-test:
	uv run pytest tests/unit/test_relative_strength_scanner.py -v

rs-smoke:
	uv run pytest tests/smoke/test_rs_scanner_massive_smoke.py -m smoke -s -v