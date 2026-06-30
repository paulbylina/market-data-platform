from pathlib import Path
import json
import pandas as pd


CONFIG_PATH = Path("configs/scanners/rs_scanner.json")
RS_DIR = Path("data/serving/scanners/rs")


def load_config() -> dict:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def read_dates(path: Path) -> pd.Series:
    df = pd.read_parquet(path, columns=["date"])
    return pd.to_datetime(df["date"]).sort_values().reset_index(drop=True)


def main() -> int:
    config = load_config()

    symbols = config["stock_symbols"]
    benchmark = config["benchmark_symbol"]
    start = config["start_date"]
    end = config["end_date"]

    rows = []

    reference_dates = None
    reference_symbol = None

    for symbol in symbols:
        path = RS_DIR / f"{symbol}_vs_{benchmark}_{start}_{end}_rs_scan.parquet"

        if not path.exists():
            rows.append(
                {
                    "symbol": symbol,
                    "status": "MISSING",
                    "rows": None,
                    "first": None,
                    "last": None,
                    "missing_vs_reference": None,
                    "extra_vs_reference": None,
                    "path": str(path),
                }
            )
            continue

        dates = read_dates(path)

        if reference_dates is None:
            reference_dates = dates
            reference_symbol = symbol

        reference_set = set(reference_dates)
        date_set = set(dates)

        missing_vs_reference = len(reference_set - date_set)
        extra_vs_reference = len(date_set - reference_set)

        same_calendar = (
            len(dates) == len(reference_dates)
            and missing_vs_reference == 0
            and extra_vs_reference == 0
            and dates.min() == reference_dates.min()
            and dates.max() == reference_dates.max()
        )

        status = "OK" if same_calendar else "CALENDAR_MISMATCH"

        rows.append(
            {
                "symbol": symbol,
                "status": status,
                "rows": len(dates),
                "first": dates.min().date(),
                "last": dates.max().date(),
                "reference_symbol": reference_symbol,
                "reference_rows": len(reference_dates),
                "reference_first": reference_dates.min().date(),
                "reference_last": reference_dates.max().date(),
                "missing_vs_reference": missing_vs_reference,
                "extra_vs_reference": extra_vs_reference,
                "path": str(path),
            }
        )

    report = pd.DataFrame(rows)

    output_dir = Path("data/validation")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "rs_serving_coverage.csv"
    report.to_csv(output_path, index=False)

    print("=== RS serving coverage validation ===")
    print(f"Config: {CONFIG_PATH}")
    print(f"RS dir: {RS_DIR}")
    print(f"Benchmark: {benchmark}")
    print(f"Symbols expected: {len(symbols)}")
    print(f"Reference symbol: {reference_symbol}")
    print(f"Saved report: {output_path}")
    print()

    print("=== Status summary ===")
    print(
        report
        .groupby("status")
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
                    "status",
                    "rows",
                    "first",
                    "last",
                    "reference_rows",
                    "reference_first",
                    "reference_last",
                    "missing_vs_reference",
                    "extra_vs_reference",
                ]
            ]
            .sort_values(["rows", "symbol"], na_position="first")
            .to_string(index=False)
        )

    print("\n=== Lowest row counts ===")
    print(
        report[
            [
                "symbol",
                "status",
                "rows",
                "first",
                "last",
                "missing_vs_reference",
                "extra_vs_reference",
            ]
        ]
        .sort_values(["rows", "symbol"], na_position="first")
        .head(30)
        .to_string(index=False)
    )

    if len(problems) > 0:
        print("\nFAIL: RS serving calendar problems found.")
        return 1

    print("\nPASS: All RS serving files match the reference calendar.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
