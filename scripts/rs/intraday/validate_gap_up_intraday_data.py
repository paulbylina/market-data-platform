from pathlib import Path

import pandas as pd

from src.utils.path_builders import build_market_curated_output_path


TASKS_PATH = Path("data/research/intraday_gap_up/minute_download_tasks_top3.csv")
OUTPUT_PATH = Path("data/research/intraday_gap_up/intraday_15m_validation.csv")


def get_rth(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["bar_start_utc"] = pd.to_datetime(out["bar_start"], utc=True)
    out["bar_start_et"] = out["bar_start_utc"].dt.tz_convert("America/New_York")
    out["time_et"] = out["bar_start_et"].dt.time

    return out[
        (out["time_et"] >= pd.Timestamp("09:30").time())
        & (out["time_et"] < pd.Timestamp("16:00").time())
    ].copy()


def main() -> None:
    tasks = pd.read_csv(TASKS_PATH)

    rows = []

    for _, row in tasks.iterrows():
        ticker = row["ticker"]
        start_date = row["start_date"]
        end_date = row["end_date"]

        path_1m = build_market_curated_output_path(
            symbol=ticker,
            start_date=start_date,
            end_date=end_date,
            timeframe="1m",
        )

        path_15m = build_market_curated_output_path(
            symbol=ticker,
            start_date=start_date,
            end_date=end_date,
            timeframe="15m",
        )

        status = "OK"
        problem = ""

        if not path_1m.exists():
            status = "MISSING_1M"
            problem = str(path_1m)
            rows.append(
                {
                    "ticker": ticker,
                    "trade_date": start_date,
                    "status": status,
                    "problem": problem,
                    "rows_1m": 0,
                    "rows_15m": 0,
                    "rth_rows_15m": 0,
                    "first_rth_et": None,
                    "last_rth_et": None,
                }
            )
            continue

        if not path_15m.exists():
            status = "MISSING_15M"
            problem = str(path_15m)
            rows.append(
                {
                    "ticker": ticker,
                    "trade_date": start_date,
                    "status": status,
                    "problem": problem,
                    "rows_1m": 0,
                    "rows_15m": 0,
                    "rth_rows_15m": 0,
                    "first_rth_et": None,
                    "last_rth_et": None,
                }
            )
            continue

        df_1m = pd.read_parquet(path_1m)
        df_15m = pd.read_parquet(path_15m)

        rth = get_rth(df_15m)

        expected_full_day_rows = 26
        expected_early_close_rows = 15

        if len(rth) not in {expected_full_day_rows, expected_early_close_rows}:
            status = "BAD_RTH_COUNT"
            problem = f"Expected 26 full-day or 15 early-close RTH 15m bars, got {len(rth)}"

        first_rth_et = None
        last_rth_et = None

        if len(rth) > 0:
            first_rth_et = str(rth["bar_start_et"].iloc[0])
            last_rth_et = str(rth["bar_start_et"].iloc[-1])

            first_time = rth["bar_start_et"].iloc[0].time()
            last_time = rth["bar_start_et"].iloc[-1].time()

            if first_time != pd.Timestamp("09:30").time():
                status = "BAD_FIRST_RTH_BAR"
                problem = f"First RTH bar is {first_time}"

            valid_last_times = {
                pd.Timestamp("15:45").time(),
                pd.Timestamp("13:00").time(),
            }

            if last_time not in valid_last_times:
                status = "BAD_LAST_RTH_BAR"
                problem = f"Last RTH bar is {last_time}"

        rows.append(
            {
                "ticker": ticker,
                "trade_date": start_date,
                "status": status,
                "problem": problem,
                "rows_1m": len(df_1m),
                "rows_15m": len(df_15m),
                "rth_rows_15m": len(rth),
                "first_rth_et": first_rth_et,
                "last_rth_et": last_rth_et,
            }
        )

    report = pd.DataFrame(rows)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(OUTPUT_PATH, index=False)

    print("=== Gap-up intraday 15m validation ===")
    print(f"Tasks: {len(tasks)}")
    print(f"Saved: {OUTPUT_PATH}")
    print()
    print("=== Status summary ===")
    print(report["status"].value_counts().to_string())
    print()

    problems = report[report["status"] != "OK"].copy()

    if problems.empty:
        print("No problems found.")
    else:
        print("=== Problems ===")
        print(problems.to_string(index=False))


if __name__ == "__main__":
    main()
