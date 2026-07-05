# Strategy Research Status

## Global rule

Any pre-entry strategy using same-day full-session daily volume or same-day daily RVOL is suspect until rerun live-safe.

Danger fields:

- dollar_volume_rvol_20d
- volume_rvol_20d
- dollar_volume_regime
- volume_regime

Safe fields before entry:

- gap_pct
- premarket features
- first 5m / first 15m features
- avg_dollar_volume_20d_prior
- true previous-day dollar volume joined by ticker + prev_trade_date

## $5-$50 gap-down shorts

Script:

scripts/stocks/test_mid_5_to_50_gapdown_short_rrr_rgr_rrg.py

Canonical daily source:

data/research/full_market_scanner_10y/historical_full_market_daily_panel.csv

Current validation status:

- full 2016-2026 live-safe validation complete
- prev_day_volume_metric coverage: 100%
- base rows: 8,474
- missing cache: 0

## Official candidates

### Official A

GD_SHORT_RRR_clean  
gap_down_10_to_5  
LOWER_le_2x  
target/stop: 4/5

Full-sample result:

- trades: 39
- avg_net: +1.78%
- median_net: +3.90%
- win_rate: 74.36%
- target_rate: 61.54%
- stop_rate: 12.82%

Interpretation:

High quality, lower frequency.

### Official A-

GD_SHORT_RRR_continuation  
gap_down_5_to_2  
LOWER_le_2x  
target/stop: 4/5

Full-sample result:

- trades: 115
- avg_net: +1.05%
- median_net: +2.03%
- win_rate: 65.22%
- target_rate: 45.22%
- stop_rate: 13.91%

Interpretation:

Best practical frequency / quality balance.

### Official B

GD_SHORT_RRR_clean  
gap_down_5_to_2  
LOWER_le_2x  
target/stop: 4/5

Full-sample result:

- trades: 85
- avg_net: +0.96%
- median_net: +2.09%
- win_rate: 62.35%
- target_rate: 44.71%
- stop_rate: 15.29%

## Watch candidates

### Watch A

GD_SHORT_RRG_weak_bounce  
gap_down_5_to_2  
LOWER_le_2x  
target/stop: 4/5

Full-sample result:

- trades: 56
- avg_net: +0.79%
- median_net: +3.33%
- win_rate: 62.50%
- target_rate: 48.21%
- stop_rate: 19.64%

### Watch B

GD_SHORT_RRG_failed_reclaim  
gap_down_5_to_2  
LOWER_le_2x  
target/stop: 4/5

Full-sample result:

- trades: 79
- avg_net: +0.77%
- median_net: +2.82%
- win_rate: 60.76%
- target_rate: 44.30%
- stop_rate: 17.72%

## Deprioritized

- RGR setups: not strong enough versus RRR.
- EXTREME volume buckets: too small for official rules.
- HIGH_2_to_5x buckets: interesting but less clean than LOWER_le_2x.
- gap_down_2_to_0: higher sample size, but weaker full 10-year edge than recent-only test suggested.
