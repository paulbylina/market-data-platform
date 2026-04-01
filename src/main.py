import argparse

from src.pipelines.daily_eod_pipeline import run_daily_eod_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the daily EOD market pipeline.")
    parser.add_argument("--symbol", required=True, help="Stock symbol, for example AAPL")
    parser.add_argument("--start-date", required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True, help="End date in YYYY-MM-DD format")

    args = parser.parse_args()

    print(
        f"Starting daily EOD pipeline for {args.symbol} "
        f"from {args.start_date} to {args.end_date}..."
    )

    try:
        run_daily_eod_pipeline(
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        print("Pipeline completed successfully.")
    except Exception as exc:
        print(f"Pipeline failed: {exc}")
        raise


if __name__ == "__main__":
    main()