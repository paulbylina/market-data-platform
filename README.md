# market-data-platform

Production-style end-of-day stock data pipeline using Massive (Polygon), with ETL, validation, z-score feature generation, GitHub Actions scheduling, and S3 cloud storage.

## Overview

This project is a modular Python pipeline for downloading, standardizing, validating, transforming, and storing end-of-day stock market data.

The current version focuses on:

- pulling daily OHLCV bars from the Massive aggregates API
- storing raw vendor responses
- standardizing data into a consistent staging schema
- validating daily bar integrity
- generating rolling z-score features
- writing analytics-ready curated outputs
- running on a schedule with GitHub Actions
- syncing pipeline outputs to Amazon S3

## Current Features

- Massive API client
- Daily bar ingestion
- Standardization layer
- Validation layer
- Feature generation
- Raw, staging, curated, quality, and serving outputs
- Unit test foundation
- End-to-end pipeline runner
- Daily GitHub Actions workflow
- S3 cloud storage sync

## Project Structure

```text
market-data-platform/
├── .github/
│   └── workflows/
├── config/
├── data/
│   ├── raw/
│   ├── staging/
│   ├── curated/
│   ├── quality/
│   └── serving/
├── docs/
├── logs/
├── notebooks/
├── orchestration/
├── src/
│   ├── features/
│   ├── ingestion/
│   ├── pipelines/
│   ├── standardization/
│   ├── storage/
│   ├── utils/
│   └── validation/
├── tests/
│   ├── data_quality/
│   ├── integration/
│   └── unit/
├── pyproject.toml
└── uv.lock
```

## Pipeline Flow

The current pipeline flow is:

1. fetch raw daily bars from Massive
2. standardize the raw response into a tabular schema
3. validate required fields and OHLCV integrity
4. generate rolling feature columns
5. write outputs to raw, staging, curated, quality, and serving layers
6. sync pipeline outputs to Amazon S3

## Output Layers
**Raw** - Untouched API response data

**Staging** - Cleaned and standardized daily bars.

**Curated** - Validated and feature-enriched daily bars.

**Quality** - Validation failures, warnings, and summary outputs.

**Serving** - Downstream-ready output artifacts for screening and relative-volume style use cases.

## Current Derived Features
- volume_mean_30d
- volume_std_30d
- volume_zscore_30d
- close_mean_30d
- close_std_30d
- close_price_zscore_30d

## Tech Stack
-  Python
-  uv
-  httpx
-  pandas
-  numpy
-  pandera
-  pyarrow
-  duckdb
-  pydantic
-  pytest
-  ruff
-  GitHub Actions
-  Amazon S3

## Local Setup
**1. Clone the repo**
```bash
git clone git@github.com:paulbylina/market-data-platform.git
```
**2. Install dependencies**
```bash
uv sync
```
**3. Create a .env file**
```text
MASSIVE_API_KEY=your_api_key_here
```
or

```bash
cp .env.example .env
```

## Running the Pipeline
Example:
```bash
uv run python -c "from src.pipelines.daily_eod_pipeline import run_daily_eod_pipeline; run_daily_eod_pipeline('AAPL', '2023-10-01', '2024-01-31')"
```


## Automated Daily Runs
This repository includes a GitHub Actions workflow that runs the market pipeline on a weekday schedule and can also be triggered manually from the GitHub Actions tab.

Current workflow behavior:

-  runs on a GitHub Actions schedule
-  uses repository secrets for Massive and AWS credentials
-  executes the pipeline in GitHub-hosted runners
-  syncs the ```data/``` output directory to Amazon S3
-  uses ```aws s3 sync ... --delete``` so cloud output stays aligned with the - latest pipeline state


## Required GitHub Repository Secrets
The scheduled workflow expects these repository secrets:

- ```MASSIVE_API_KEY```
- ```AWS_ACCESS_KEY_ID```
- ```AWS_SECRET_ACCESS_KEY```
- ```AWS_REGION```
- ```S3_BUCKET```


## Cloud Storage
Pipeline outputs are synced to Amazon S3 so data is preserved outside the local development environment and can be accessed by future downstream jobs or applications.

Example S3 layout:

```s3://<your-bucket>/market-data-platform/data/```


## Running Tests
```bash
uv run pytest
```

## Documentation
- [NinjaTrader Tick Data Recording Flow](docs/workflows/ninjatrader-data-flow.md)