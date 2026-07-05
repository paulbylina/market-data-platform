# Data Source Map

## Canonical 10-year daily source

Path:

data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv

Status:

Trusted for 10-year live-safe research.

Coverage:

- rows: 8,309,576
- tickers: 4,883
- date range: 2016-07-05 to 2026-07-02
- join coverage for $5-$50 gap-down feature sample: 100%

Use for:

- true previous-day volume joins
- 2016-2026 validation
- live-safe volume grade filters

Required join:

features.ticker + features.prev_trade_date
to
daily.ticker + daily.trade_date

Metric:

prev_day_volume_metric = previous_day_dollar_volume / avg_dollar_volume_20d_prior

## Do not use for 10-year validation

Path:

data/research/full_market_scanner/historical_full_market_daily_panel.csv

Problem:

- only 2024-2026
- only 500 tickers
- poor coverage for 10-year feature research

## Recent-only source

Path:

data/research/full_market_scanner_full_universe/daily_rvol_extreme_tail/base_rows_with_forward_metrics.csv

Use for:

- 2024-2026 recent validation only

Do not use as full 10-year proof.
