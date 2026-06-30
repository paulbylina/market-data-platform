from pathlib import Path
import json
import sys
import pandas as pd


CONFIG_PATH = Path("configs/scanners/rs_scanner.json")


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_expected_symbols(config: dict) -> list[dict]:
    rows = []

    benchmark = config["benchmark_symbol"]

    rows.append(
        {
            "symbol": benchmark,
            "group": "benchmark",
        }
    )

    for symbol in config["stock_symbols"]:
        rows.append(
            {
                "symbol": symbol,
                "group": "stock",
            }
        )

    sector_etfs = sorted(set(config.get("sector_etf_by_symbol", {}).values()))

    for symbol in sector_etfs:
        rows.append(
            {
                "symbol": symbol,
                "group": "sector_etf",
            }
        )

    # Deduplicate while preserving first group assignment.
    seen = set()
    deduped = []

    for row in rows:
        if row["symbol"] in seen:
            continue

        seen.add(row["symbol"])
        deduped.append(row)

    return deduped


def read_dates(path: Path) -> pd.Series:
    df = pd.read_parquet(path, columns=["date"])
    return pd.to_datetime(df["date"]).sort_values().reset_index(drop=True)


def main() -> int:
    config = load_config()

    data_root = Path(config["data_root"])
    timeframe = config["timeframe"]
    start = config["start_date"]
    end = config["end_date"]

    curated_dir = data_root / "curated" / "market" / timeframe

    expected = get_expected_symbols(config)
    benchmark_symbol = config["benchmark_symbol"]

    benchmark_path = curated_dir / f"{benchmark_symbol}_{start}_{end}_curated.parquet"

    if not benchmark_path.exists():
        print(f"ERROR: Missing benchmark file: {benchmark_path}")
        return 1

    benchmark_dates = read_dates(benchmark_path)
    benchmark_date_set = set(benchmark_dates)

    rows = []

    for item in expected:
        symbol = item["symbol"]
        group = item["group"]

        path = curated_dir / f"{symbol}_{start}_{end}_curated.parquet"

        if not path.exists():
            rows.append(
                {
                    "symbol": symbol,
                    "group": group,
                    "status": "MISSING",
                    "rows": None,
                    "first": None,
                    "last": None,
                    "benchmark_rows": len(benchmark_dates),
                    "missing_vs_benchmark": None,
                    "extra_vs_benchmark": None,
                    "path": str(path),
                }
            )
            continue

        dates = read_dates(path)
        date_set = set(dates)

        missing_vs_benchmark = len(benchmark_date_set - date_set)
        extra_vs_benchmark = len(date_set - benchmark_date_set)

        same_calendar = (
            len(dates) == len(benchmark_dates)
            and missing_vs_benchmark == 0
            and extra_vs_benchmark == 0
            and dates.min() == benchmark_dates.min()
            and dates.max() == benchmark_dates.max()
        )

        status = "OK" if same_calendar else "CALENDAR_MISMATCH"

        rows.append(
            {
                "symbol": symbol,
                "group": group,
                "status": status,
                "rows": len(dates),
                "first": dates.min().date(),
                "last": dates.max().date(),
                "benchmark_rows": len(benchmark_dates),
                "benchmark_first": benchmark_dates.min().date(),
                "benchmark_last": benchmark_dates.max().date(),
                "missing_vs_benchmark": missing_vs_benchmark,
                "extra_vs_benchmark": extra_vs_benchmark,
                "path": str(path),
            }
        )

    report = pd.DataFrame(rows)

    output_dir = Path("data/validation")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "rs_data_coverage.csv"
    report.to_csv(output_path, index=False)

    print("=== RS data coverage validation ===")
    print(f"Config: {CONFIG_PATH}")
    print(f"Curated dir: {curated_dir}")
    print(f"Benchmark: {benchmark_symbol}")
    print(f"Benchmark rows: {len(benchmark_dates)}")
    print(f"Benchmark first: {benchmark_dates.min().date()}")
    print(f"Benchmark last: {benchmark_dates.max().date()}")
    print(f"Saved report: {output_path}")
    print()

    print("=== Group summary ===")
    print(
        report
        .groupby(["group", "status"])
        .size()
        .reset_index(name="count")
        .to_string(index=False)
    )

    print("\n=== Problems ===")
    problems = report[report["status"] != "OK"].copy()

    if len(problems) == 0:
        print("No problems found.")
    else:
        print(
            problems[
                [
                    "symbol",
                    "group",
                    "status",
                    "rows",
                    "first",
                    "last",
                    "benchmark_rows",
                    "benchmark_first",
                    "benchmark_last",
                    "missing_vs_benchmark",
                    "extra_vs_benchmark",
                ]
            ]
            .sort_values(["group", "rows", "symbol"], na_position="first")
            .to_string(index=False)
        )

    print("\n=== Lowest row counts ===")
    print(
        report[
            [
                "symbol",
                "group",
                "status",
                "rows",
                "first",
                "last",
                "missing_vs_benchmark",
                "extra_vs_benchmark",
            ]
        ]
        .sort_values(["rows", "symbol"], na_position="first")
        .head(30)
        .to_string(index=False)
    )

    # Hard fail only for stock/benchmark problems.
    # Sector ETFs are reported, but they may be intentionally shorter, like XLC.
    blocking = report[
        (report["group"].isin(["benchmark", "stock"]))
        & (report["status"] != "OK")
    ]

    if len(blocking) > 0:
        print("\nFAIL: Stock/benchmark calendar problems found.")
        return 1

    print("\nPASS: All stock and benchmark files match the benchmark calendar.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
