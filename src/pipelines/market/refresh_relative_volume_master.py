from datetime import date, timedelta

from src.pipelines.market.batch_daily_eod_pipeline import run_batch_daily_eod_pipeline
from src.pipelines.market.build_serving_dataset import build_serving_dataset
from src.utils.path_builders import build_relative_volume_serving_output_path
from src.utils.settings import CONFIG_DIR


def refresh_relative_volume_master(
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int = 60,
) -> None:
    if end_date is None:
        end_date = date.today().isoformat()

    if start_date is None:
        start_date = (date.today() - timedelta(days=lookback_days)).isoformat()

    symbols_file = CONFIG_DIR / "symbols.txt"
    output_path = build_relative_volume_serving_output_path()

    run_batch_daily_eod_pipeline(
        symbols_file=symbols_file,
        start_date=start_date,
        end_date=end_date,
    )

    build_serving_dataset(
        symbols_file=symbols_file,
        start_date=start_date,
        end_date=end_date,
        output_path=output_path,
    )

    print(f"Relative volume master refreshed at {output_path}")
    print(f"Date range: {start_date} to {end_date}")