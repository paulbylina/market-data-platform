<h1 align="center">NinjaTrader Tick Data Recording Flow Diagram</h1>

```mermaid
flowchart TD
    NT[NinjaTrader 8] --> REC[Raw Data Recorder]

    REC -->|L1| L1REC[nt8/TickRecorder.cs]
    REC -->|L2| L2REC[nt8/Level2Recorder.cs]

    L1REC --> RAW_WIN[Raw CSV file]
    L2REC --> RAW_WIN

    RAW_WIN --> COPY[Copy file to Linux Environment]

    COPY -->|WSL| WSL_SCRIPT[scripts/import_ninjatrader_csv_wsl.sh]
    COPY -->|Dual-boot Ubuntu| UBUNTU_SCRIPT[scripts/import_ninjatrader_csv_ubuntu.sh]

    WSL_SCRIPT --> RAW_LAKE[ninja-lake/raw/ninjatrader]
    UBUNTU_SCRIPT --> RAW_LAKE

    RAW_LAKE --> CONVERT[CSV to Parquet Converter]

    CONVERT --> |L1| L1_CON[src/standardization/L1_csv_to_parquet.py]
    CONVERT --> |L2| L2_CON[src/standardization/L2_csv_to_parquet.py]

    L1_CON --> PARQUET[ninja-lake/parquet/ninjatrader]
    L2_CON --> PARQUET

    PARQUET --> ANALYSIS[Polars / Pandas / DuckDB Analysis]
    ANALYSIS --> FEATURES[Feature Engineering]
    FEATURES --> RESEARCH[Signal Research / Backtesting]
```

## Notes

- NinjaTrader records raw L1/L2 market data as CSV files.
- Import scripts move the raw CSV files into the project data lake on Linux environment.
- Standardization scripts convert raw CSV into Parquet.
- Analysis scripts use the Parquet files for feature engineering, signal research, and backtesting.

## Future Improvements

- Add automated scheduled imports
- Add validation checks for missing timestamps / session gaps
- Add optional cloud backup for raw and Parquet data
- Add metadata tracking for instrument, session, and feed type