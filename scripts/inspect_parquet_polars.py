from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.utils.df_info import print_features, print_head
import polars
polars.Config.set_tbl_rows(100)

from pathlib import Path
import matplotlib.pyplot as plt


level = "L2"
contract_month = "ES_06-26"
session_date = "2026-04-29"
parquet_file = f"{session_date}.parquet"
file_path = f"../ninja-lake/parquet/ninjatrader/{level}/{contract_month}/{session_date}/{parquet_file}"
print(file_path)

df = polars.read_parquet(file_path)

# Print features/columns
print_features(df)

# Select features/columns
cols = ["event_time", "instrument", "side", "operation", "position", "price", "volume"]

print_head(df.select(cols), 50)

best_bid_ask_df = df.filter(polars.col("position") == 0).select(cols)
print_head(best_bid_ask_df, 50)


# -----------------------------
# Build top-of-book quote stream
# -----------------------------

top0 = (
    df
    .filter(polars.col("position") == 0)
    .filter(polars.col("operation") != "Remove")
    .sort("event_time")
)

bid_updates = (
    top0
    .filter(polars.col("side") == "Bid")
    .select([
        "event_time",
        polars.col("price").alias("best_bid"),
        polars.col("volume").alias("bid_size"),
    ])
)

ask_updates = (
    top0
    .filter(polars.col("side") == "Ask")
    .select([
        "event_time",
        polars.col("price").alias("best_ask"),
        polars.col("volume").alias("ask_size"),
    ])
)

quote_stream = (
    polars.concat(
        [
            bid_updates.with_columns([
                polars.lit(None, dtype=polars.Float64).alias("best_ask"),
                polars.lit(None, dtype=polars.Int64).alias("ask_size"),
            ]),
            ask_updates.with_columns([
                polars.lit(None, dtype=polars.Float64).alias("best_bid"),
                polars.lit(None, dtype=polars.Int64).alias("bid_size"),
            ]),
        ],
        how="diagonal",
    )
    .sort("event_time")
    .with_columns([
        polars.col("best_bid").forward_fill(),
        polars.col("best_ask").forward_fill(),
        polars.col("bid_size").forward_fill(),
        polars.col("ask_size").forward_fill(),
    ])
    .drop_nulls(["best_bid", "best_ask"])
    .with_columns([
        ((polars.col("best_bid") + polars.col("best_ask")) / 2).alias("mid_price"),
        (polars.col("best_ask") - polars.col("best_bid")).alias("spread"),
    ])
)


# Minute bars
minute_bars = (
    quote_stream
    .sort("event_time")
    .group_by_dynamic(
        index_column="event_time",
        every="1m",
    )
    .agg([
        polars.col("mid_price").first().alias("open"),
        polars.col("mid_price").max().alias("high"),
        polars.col("mid_price").min().alias("low"),
        polars.col("mid_price").last().alias("close"),

        polars.col("best_bid").last().alias("last_bid"),
        polars.col("best_ask").last().alias("last_ask"),
        polars.col("bid_size").last().alias("last_bid_size"),
        polars.col("ask_size").last().alias("last_ask_size"),

        polars.col("spread").mean().alias("avg_spread"),
        polars.len().alias("quote_updates"),
    ])
    .sort("event_time")
)


print("\n--- Top-of-book quote stream preview ---")
print(quote_stream.head(25))

print("\n--- 1-minute top-of-book bars ---")
print(minute_bars.head(50))

# -----------------------------
# Plot 1-minute top-of-book bars
# -----------------------------

chart_dir = Path("reports/charts")
chart_dir.mkdir(parents=True, exist_ok=True)

chart_path = chart_dir / "l2_top_of_book_mid_1m.png"

plt.figure(figsize=(14, 6))

plt.plot(
    minute_bars["event_time"].to_list(),
    minute_bars["close"].to_list(),
    label="1m close mid-price",
)

plt.title("L2 Top-of-Book Mid-Price - 1 Minute Bars")
plt.xlabel("Time")
plt.ylabel("Mid Price")
plt.grid(True)
plt.legend()
plt.tight_layout()

plt.savefig(chart_path, dpi=150)

print(f"\nSaved chart to: {chart_path}")



# -----------------------------
# Sanity checks for minute bars
# -----------------------------

minute_checks = (
    minute_bars
    .with_columns([
        polars.col("event_time").diff().alias("time_gap"),
        (polars.col("high") - polars.col("low")).alias("bar_range"),
    ])
    .select([
        "event_time",
        "open",
        "high",
        "low",
        "close",
        "avg_spread",
        "quote_updates",
        "time_gap",
        "bar_range",
    ])
)

print("\n--- Minute bar sanity check ---")
print(minute_checks.head(20))

print("\n--- Largest time gaps ---")
print(
    minute_checks
    .sort("time_gap", descending=True)
    .head(10)
)

print("\n--- Largest spreads ---")
print(
    minute_checks
    .sort("avg_spread", descending=True)
    .head(10)
)

print("\n--- Largest bar ranges ---")
print(
    minute_checks
    .sort("bar_range", descending=True)
    .head(10)
)