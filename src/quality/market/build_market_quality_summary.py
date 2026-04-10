from pathlib import Path

import pandas as pd

from src.utils.settings import QUALITY_DATA_DIR


def _extract_symbol_from_summary_filename(
    path: Path,
    start_date: str,
    end_date: str,
) -> str:
    suffix = f"_{start_date}_{end_date}_validation_summary.parquet"
    name = path.name
    if not name.endswith(suffix):
        raise ValueError(f"Unexpected summary filename format: {name}")
    return name.removesuffix(suffix)


def _build_output_paths(start_date: str, end_date: str) -> tuple[Path, Path, Path]:
    base_dir = QUALITY_DATA_DIR / "market" / "run_summary"
    base_dir.mkdir(parents=True, exist_ok=True)

    by_symbol_timeframe_path = (
        base_dir / f"{start_date}_{end_date}_by_symbol_timeframe.parquet"
    )
    by_timeframe_path = base_dir / f"{start_date}_{end_date}_by_timeframe.parquet"
    overall_path = base_dir / f"{start_date}_{end_date}_overall.parquet"

    return by_symbol_timeframe_path, by_timeframe_path, overall_path


def build_market_quality_summary(
    start_date: str,
    end_date: str,
    timeframes: tuple[str, ...],
) -> dict:
    """
    Roll up per-symbol market quality summary files into run-level summary outputs.

    Expected input files:
      data/quality/market/<timeframe>/<symbol>_<start>_<end>_validation_summary.parquet
    """
    summary_frames: list[pd.DataFrame] = []

    for timeframe in timeframes:
        timeframe_dir = QUALITY_DATA_DIR / "market" / timeframe
        if not timeframe_dir.exists():
            continue

        pattern = f"*_{start_date}_{end_date}_validation_summary.parquet"
        for path in sorted(timeframe_dir.glob(pattern)):
            df = pd.read_parquet(path).copy()

            if df.empty:
                continue

            df["symbol"] = _extract_symbol_from_summary_filename(
                path=path,
                start_date=start_date,
                end_date=end_date,
            )

            if "timeframe" not in df.columns:
                df["timeframe"] = timeframe

            summary_frames.append(df)

    by_symbol_columns = [
        "symbol",
        "timeframe",
        "total_rows",
        "valid_rows",
        "failure_count",
        "warning_count",
    ]

    if summary_frames:
        by_symbol_timeframe_df = pd.concat(summary_frames, ignore_index=True)

        for column in by_symbol_columns:
            if column not in by_symbol_timeframe_df.columns:
                by_symbol_timeframe_df[column] = pd.NA

        by_symbol_timeframe_df = by_symbol_timeframe_df[by_symbol_columns]

        by_timeframe_df = (
            by_symbol_timeframe_df.groupby("timeframe", as_index=False)
            .agg(
                symbol_count=("symbol", "nunique"),
                total_rows=("total_rows", "sum"),
                valid_rows=("valid_rows", "sum"),
                failure_count=("failure_count", "sum"),
                warning_count=("warning_count", "sum"),
            )
            .sort_values("timeframe")
            .reset_index(drop=True)
        )

        overall_df = pd.DataFrame(
            [
                {
                    "timeframe_count": by_timeframe_df["timeframe"].nunique(),
                    "symbol_count": by_symbol_timeframe_df["symbol"].nunique(),
                    "total_rows": by_symbol_timeframe_df["total_rows"].sum(),
                    "valid_rows": by_symbol_timeframe_df["valid_rows"].sum(),
                    "failure_count": by_symbol_timeframe_df["failure_count"].sum(),
                    "warning_count": by_symbol_timeframe_df["warning_count"].sum(),
                }
            ]
        )
    else:
        by_symbol_timeframe_df = pd.DataFrame(columns=by_symbol_columns)
        by_timeframe_df = pd.DataFrame(
            columns=[
                "timeframe",
                "symbol_count",
                "total_rows",
                "valid_rows",
                "failure_count",
                "warning_count",
            ]
        )
        overall_df = pd.DataFrame(
            [
                {
                    "timeframe_count": 0,
                    "symbol_count": 0,
                    "total_rows": 0,
                    "valid_rows": 0,
                    "failure_count": 0,
                    "warning_count": 0,
                }
            ]
        )

    (
        by_symbol_timeframe_path,
        by_timeframe_path,
        overall_path,
    ) = _build_output_paths(start_date=start_date, end_date=end_date)

    by_symbol_timeframe_df.to_parquet(by_symbol_timeframe_path, index=False)
    by_timeframe_df.to_parquet(by_timeframe_path, index=False)
    overall_df.to_parquet(overall_path, index=False)

    return {
        "by_symbol_timeframe_path": str(by_symbol_timeframe_path),
        "by_timeframe_path": str(by_timeframe_path),
        "overall_path": str(overall_path),
        "row_count": len(by_symbol_timeframe_df),
    }