PYTHON := uv run python
PYTEST := uv run python -m pytest

SYMBOL ?= AAPL
START ?= 2024-01-02
END ?= 2024-01-03
SOURCE_TF ?= 1m
TARGET_TF ?= 5m
SYMBOLS_FILE ?= config/symbols_test.txt

.PHONY: help sync test test-integration test-batch \
	run-daily run-minute run-derived run-batch run-universe \
	size size-nondaily clean-1m clean-generated

sync:
	uv sync

test:
	$(PYTEST) -q

test-integration:
	$(PYTEST) tests/integration -q -vv

test-batch:
	$(PYTEST) tests/integration/test_batch_market_timeframe_refresh.py -q -vv

run-daily:
	$(PYTHON) -c "from src.pipelines.market.daily_eod_pipeline import run_daily_eod_pipeline; run_daily_eod_pipeline('$(SYMBOL)', '$(START)', '$(END)')"

run-minute:
	$(PYTHON) -c "from src.pipelines.market.minute_bars_pipeline import run_minute_bars_pipeline; run_minute_bars_pipeline('$(SYMBOL)', '$(START)', '$(END)')"

run-derived:
	$(PYTHON) -c "from src.pipelines.market.run_derived_bars_pipeline import run_derived_bars_pipeline; run_derived_bars_pipeline('$(SYMBOL)', '$(START)', '$(END)', source_timeframe='$(SOURCE_TF)', target_timeframe='$(TARGET_TF)')"

run-batch:
	$(PYTHON) -c "from pathlib import Path; from src.pipelines.market.batch_market_timeframe_refresh import run_batch_market_timeframe_refresh; summary = run_batch_market_timeframe_refresh(symbols_file=Path('$(SYMBOLS_FILE)'), start_date='$(START)', end_date='$(END)', source_timeframes=('1d','1m'), derived_timeframes=('5m','15m','60m','1w','1mo')); print('successes:', summary['success_count']); print('failures:', summary['failure_count']); print('skipped:', summary['skipped_count'])"


# =============== REFRESH DATA PIPELINES ===============

run-universe-intraday-eligible:
	$(PYTHON) -c "from pathlib import Path; from src.pipelines.market.refresh_market_universe import refresh_market_universe; \
	summary = refresh_market_universe(symbols_file=Path('config/symbols_intraday_eligible.txt')); \
	print('total_successes:', summary['total_success_count']); \
	print('total_failures:', summary['total_failure_count']); \
	print('total_skipped:', summary['total_skipped_count'])"

run-universe-10:
	$(PYTHON) -c "from pathlib import Path; from src.pipelines.market.refresh_market_universe import refresh_market_universe; \
	summary = refresh_market_universe(symbols_file=Path('config/symbols_10.txt')); \
	print('total_successes:', summary['total_success_count']); \
	print('total_failures:', summary['total_failure_count']); \
	print('total_skipped:', summary['total_skipped_count'])"

run-universe-30:
	$(PYTHON) -c "from pathlib import Path; from src.pipelines.market.refresh_market_universe import refresh_market_universe; \
	summary = refresh_market_universe(symbols_file=Path('config/symbols_DOW_30.txt')); \
	print('total_successes:', summary['total_success_count']); \
	print('total_failures:', summary['total_failure_count']); \
	print('total_skipped:', summary['total_skipped_count'])"

run-universe-500:
	$(PYTHON) -c "from pathlib import Path; from src.pipelines.market.refresh_market_universe import refresh_market_universe; \
	summary = refresh_market_universe(symbols_file=Path('config/symbols_SP_500.txt')); \
	print('total_successes:', summary['total_success_count']); \
	print('total_failures:', summary['total_failure_count']); \
	print('total_skipped:', summary['total_skipped_count'])"

run-universe-full:
	$(PYTHON) -c "from pathlib import Path; from src.pipelines.market.refresh_market_universe import refresh_market_universe; \
	summary = refresh_market_universe(symbols_file=Path('config/symbols.txt')); \
	print('total_successes:', summary['total_success_count']); \
	print('total_failures:', summary['total_failure_count']); \
	print('total_skipped:', summary['total_skipped_count'])"

# =================== GET DATA SIZES ===================

size:
	du -ch data/raw data/staging data/curated data/quality data/serving | tail -n 1

size-intraday-details:
	@du -sh \
		data/raw/massive/1m \
		data/staging/market/1m \
		data/curated/market/1m \
		data/staging/market/5m \
		data/curated/market/5m \
		data/staging/market/15m \
		data/curated/market/15m \
		data/staging/market/60m \
		data/curated/market/60m

size-intraday-total:
	du -ch \
		data/raw/massive/1m \
		data/staging/market/1m \
		data/curated/market/1m \
		data/staging/market/5m \
		data/curated/market/5m \
		data/staging/market/15m \
		data/curated/market/15m \
		data/staging/market/60m \
		data/curated/market/60m | tail -n 1

# =================== CLEANUP ===================

clean-1m:
	rm -rf data/raw/massive/1m data/staging/market/1m data/curated/market/1m data/quality/market/1m

delete-data:
	@rm -rf data/raw data/staging data/curated data/quality data/serving
	@mkdir -p data/raw data/staging data/curated data/quality data/serving

# =================== DATA STATISTICS ===================

# raw-stats-1m:
# 	@$(PYTHON) -c "import json; from pathlib import Path; from statistics import mean, median; raw_dir=Path('data/raw/massive/1m'); files=sorted(raw_dir.glob('*.json')); rows=[]; [rows.append((p.name, (lambda payload: payload.get('resultsCount', len(payload.get('results', []))))(json.load(open(p, 'r', encoding='utf-8'))))) for p in files]; counts=[count for _, count in rows]; print(f'raw_dir: {raw_dir}'); print(f'file_count: {len(files)}'); print(f'total_datapoints: {sum(counts)}' if counts else 'total_datapoints: 0'); print(f'min_datapoints: {min(rows, key=lambda x: x[1])[1]} ({min(rows, key=lambda x: x[1])[0]})' if rows else 'min_datapoints: n/a'); print(f'max_datapoints: {max(rows, key=lambda x: x[1])[1]} ({max(rows, key=lambda x: x[1])[0]})' if rows else 'max_datapoints: n/a'); print(f'average_datapoints: {mean(counts):.2f}' if counts else 'average_datapoints: n/a'); print(f'median_datapoints: {median(counts):.2f}' if counts else 'median_datapoints: n/a')"


raw-stats-1m:
	@$(PYTHON) -c "import json; from pathlib import Path; \
	from statistics import mean, median; raw_dir=Path('data/raw/massive/1m'); \
	files=sorted(raw_dir.glob('*.json')); rows=[]; \
	[rows.append((p.name, (lambda payload: payload.get('resultsCount', len(payload.get('results', []))))(json.load(open(p, 'r', encoding='utf-8'))))) for p in files]; \
	counts=[count for _, count in rows]; \
	print(f'\nraw_dir: {raw_dir}'); \
	print(f'file_count: {len(files)}'); print(f'total_datapoints: {sum(counts)}' if counts else 'total_datapoints: 0'); \
	print(f'min_datapoints: {min(rows, key=lambda x: x[1])[1]} ({min(rows, key=lambda x: x[1])[0]})' if rows else 'min_datapoints: n/a'); \
	print(f'max_datapoints: {max(rows, key=lambda x: x[1])[1]} ({max(rows, key=lambda x: x[1])[0]})' if rows else 'max_datapoints: n/a'); \
	print(f'average_datapoints: {mean(counts):.2f}' if counts else 'average_datapoints: n/a'); \
	print(f'median_datapoints: {median(counts):.2f}' if counts else 'median_datapoints: n/a')"

derived-stats-60m:
	@$(PYTHON) -c "from pathlib import Path; import pandas as pd; from statistics import mean, median; \
	curated_dir=Path('data/curated/market/60m'); files=sorted(curated_dir.glob('*.parquet')); rows=[]; \
	[rows.append((p.name, len(pd.read_parquet(p)))) for p in files]; counts=[count for _, count in rows]; \
	print(f'\ncurated_dir: {curated_dir}'); print(f'file_count: {len(files)}'); \
	print(f'total_derived_bars: {sum(counts)}' if counts else 'total_derived_bars: 0'); \
	print(f'min_derived_bars: {min(rows, key=lambda x: x[1])[1]} ({min(rows, key=lambda x: x[1])[0]})' if rows else 'min_derived_bars: n/a'); \
	print(f'max_derived_bars: {max(rows, key=lambda x: x[1])[1]} ({max(rows, key=lambda x: x[1])[0]})' if rows else 'max_derived_bars: n/a'); \
	print(f'average_derived_bars: {mean(counts):.2f}' if counts else 'average_derived_bars: n/a'); \
	print(f'median_derived_bars: {median(counts):.2f}' if counts else 'median_derived_bars: n/a'); \
	print(f'symbols_below_60_bars: {sum(1 for c in counts if c < 60)}' if counts else 'symbols_below_60_bars: 0')"

derived-stats-1mo:
	@$(PYTHON) -c "from pathlib import Path; import pandas as pd; from statistics import mean, median; \
	curated_dir=Path('data/curated/market/1mo'); files=sorted(curated_dir.glob('*.parquet')); rows=[]; \
	[rows.append((p.name, len(pd.read_parquet(p)))) for p in files]; counts=[count for _, count in rows]; \
	print(f'\ncurated_dir: {curated_dir}'); print(f'file_count: {len(files)}'); \
	print(f'total_derived_bars: {sum(counts)}' if counts else 'total_derived_bars: 0'); \
	print(f'min_derived_bars: {min(rows, key=lambda x: x[1])[1]} ({min(rows, key=lambda x: x[1])[0]})' if rows else 'min_derived_bars: n/a'); \
	print(f'max_derived_bars: {max(rows, key=lambda x: x[1])[1]} ({max(rows, key=lambda x: x[1])[0]})' if rows else 'max_derived_bars: n/a'); \
	print(f'average_derived_bars: {mean(counts):.2f}' if counts else 'average_derived_bars: n/a'); \
	print(f'median_derived_bars: {median(counts):.2f}' if counts else 'median_derived_bars: n/a'); \
	print(f'symbols_below_60_bars: {sum(1 for c in counts if c < 60)}' if counts else 'symbols_below_60_bars: 0')"


derived-filter-60m:
	@$(PYTHON) -c "from pathlib import Path; import pandas as pd; threshold=60; curated_dir=Path('data/curated/market/60m'); \
	files=sorted(curated_dir.glob('*.parquet')); rows=[(p.name, len(pd.read_parquet(p))) for p in files]; \
	failing=[row for row in rows if row[1] < threshold]; \
	print(f'\nthreshold: {threshold}'); print(f'fail_count: {len(failing)}'); [print(f'{name}: {count}') for name, count in failing]"


# =================== CREATE ELIGABLE SYMBOL LIST =================

build-intraday-eligible-60m:
	@$(PYTHON) -c "from pathlib import Path; import pandas as pd; threshold=60; curated_dir=Path('data/curated/market/60m'); output_path=Path('config/symbols_intraday_eligible.txt'); files=sorted(curated_dir.glob('*.parquet')); eligible=[]; [eligible.append((p.name.split('_')[0], len(pd.read_parquet(p)))) for p in files if len(pd.read_parquet(p)) >= threshold]; output_path.write_text('\n'.join(symbol for symbol, _ in sorted(eligible)) + ('\n' if eligible else ''), encoding='utf-8'); print(f'threshold: {threshold}'); print(f'eligible_count: {len(eligible)}'); print(f'output_file: {output_path}')"


# =================== AWS ================== 
aws-get-objects-1d:
	aws s3 ls s3://krakow-trading-group-market-data/market-data-platform/data/raw/massive/1d --recursive | wc -l


# =================== HELP =================

help:
	@echo "Available targets:"
	@echo "  make sync"
	@echo "  make test"
	@echo "  make test-integration"
	@echo "  make test-batch"
	@echo "  make run-daily SYMBOL=AAPL START=2024-01-02 END=2024-01-10"
	@echo "  make run-minute SYMBOL=AAPL START=2024-01-02 END=2024-01-03"
	@echo "  make run-derived SYMBOL=AAPL START=2024-01-02 END=2024-01-03 SOURCE_TF=1m TARGET_TF=5m"
	@echo "  make run-batch SYMBOLS_FILE=config/symbols_test.txt START=2024-01-02 END=2024-01-03"
	@echo "  make run-universe-[10, 30, 500, intraday-eligable]"
	@echo "  make size"
	@echo "  make size-nondaily"
	@echo "  make clean-1m"
	@echo "  make clean-generated"
	@echo "  build-intraday-eligible-60m"
	@echo "  derived-stats-60m"
	@echo "  aws-get-objects"