from pathlib import Path

from src.pipelines.market.batch_market_timeframe_refresh import (
    run_batch_market_timeframe_refresh,
)
from src.utils.path_builders import build_market_curated_output_path


def test_run_batch_market_timeframe_refresh_with_temp_symbols_file(tmp_path: Path) -> None:
    symbols_file = tmp_path / "symbols_test.txt"
    symbols_file.write_text("AAPL\nMSFT\n", encoding="utf-8")

    start_date = "2024-01-02"
    end_date = "2024-01-02"

    summary = run_batch_market_timeframe_refresh(
        symbols_file=symbols_file,
        start_date=start_date,
        end_date=end_date,
        source_timeframes=("1m",),
        derived_timeframes=("5m", "15m", "60m"),
    )

    assert summary["symbol_count"] == 2
    assert summary["failure_count"] == 0

    for symbol in ["AAPL", "MSFT"]:
        minute_path = build_market_curated_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe="1m",
        )
        five_minute_path = build_market_curated_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe="5m",
        )
        fifteen_minute_path = build_market_curated_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe="15m",
        )
        sixty_minute_path = build_market_curated_output_path(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe="60m",
        )

        assert minute_path.exists()
        assert five_minute_path.exists()
        assert fifteen_minute_path.exists()
        assert sixty_minute_path.exists()