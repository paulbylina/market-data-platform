from pathlib import Path

import pandas as pd

from src.utils.settings import CURATED_DATA_DIR, QUALITY_DATA_DIR


def _extract_symbol_from_curated_filename(
    path: Path,
    start_date: str,
    end_date: str,
) -> str:
    suffix = f"_{start_date}_{end_date}_curated.parquet"
    name = path.name
    if not name.endswith(suffix):
        raise ValueError(f"Unexpected curated filename format: {name}")
    return name.removesuffix(suffix)


def build_60m_gap_audit(
    start_date: str,
    end_date: str,
    minimum_bar_threshold: int = 60,
) -> dict:
    """
    Build a first-pass 60m coverage audit from curated 60m parquet files.

    This focuses on practical usability for the app:
    - actual 60m bars per symbol
    - pass/fail against a minimum bar threshold
    """
    curated_dir = CURATED_DATA_DIR / "market" / "60m"
    output_dir = QUALITY_DATA_DIR / "market" / "run_summary"
    output_dir.mkdir(parents=True, exist_ok=True)

    by_symbol_output_path = output_dir / f"{start_date}_{end_date}_60m_gap_audit_by_symbol.parquet"
    failed_output_path = output_dir / f"{start_date}_{end_date}_60m_gap_audit_failed_symbols.parquet"
    overall_output_path = output_dir / f"{start_date}_{end_date}_60m_gap_audit_overall.parquet"

    pattern = f"*_{start_date}_{end_date}_curated.parquet"
    files = sorted(curated_dir.glob(pattern))

    rows: list[dict] = []

    for path in files:
        symbol = _extract_symbol_from_curated_filename(
            path=path,
            start_date=start_date,
            end_date=end_date,
        )

        df = pd.read_parquet(path)
        actual_bars = len(df)
        passes_threshold = actual_bars >= minimum_bar_threshold

        rows.append(
            {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "actual_bars": actual_bars,
                "minimum_bar_threshold": minimum_bar_threshold,
                "bars_below_threshold": max(minimum_bar_threshold - actual_bars, 0),
                "passes_threshold": passes_threshold,
            }
        )

    by_symbol_df = pd.DataFrame(
        rows,
        columns=[
            "symbol",
            "start_date",
            "end_date",
            "actual_bars",
            "minimum_bar_threshold",
            "bars_below_threshold",
            "passes_threshold",
        ],
    )

    if by_symbol_df.empty:
        failed_df = pd.DataFrame(columns=by_symbol_df.columns)
        overall_df = pd.DataFrame(
            [
                {
                    "symbol_count": 0,
                    "minimum_bar_threshold": minimum_bar_threshold,
                    "symbols_below_threshold": 0,
                    "average_actual_bars": 0.0,
                    "median_actual_bars": 0.0,
                    "min_actual_bars": 0,
                    "max_actual_bars": 0,
                }
            ]
        )
    else:
        by_symbol_df = by_symbol_df.sort_values(
            ["passes_threshold", "actual_bars", "symbol"],
            ascending=[True, True, True],
        ).reset_index(drop=True)

        failed_df = by_symbol_df[~by_symbol_df["passes_threshold"]].reset_index(drop=True)

        overall_df = pd.DataFrame(
            [
                {
                    "symbol_count": len(by_symbol_df),
                    "minimum_bar_threshold": minimum_bar_threshold,
                    "symbols_below_threshold": int((~by_symbol_df["passes_threshold"]).sum()),
                    "average_actual_bars": float(by_symbol_df["actual_bars"].mean()),
                    "median_actual_bars": float(by_symbol_df["actual_bars"].median()),
                    "min_actual_bars": int(by_symbol_df["actual_bars"].min()),
                    "max_actual_bars": int(by_symbol_df["actual_bars"].max()),
                }
            ]
        )

    by_symbol_df.to_parquet(by_symbol_output_path, index=False)
    failed_df.to_parquet(failed_output_path, index=False)
    overall_df.to_parquet(overall_output_path, index=False)

    return {
        "by_symbol_output_path": str(by_symbol_output_path),
        "failed_output_path": str(failed_output_path),
        "overall_output_path": str(overall_output_path),
        "symbol_count": len(by_symbol_df),
    }