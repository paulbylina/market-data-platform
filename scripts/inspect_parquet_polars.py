from pathlib import Path
import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.utils.df_info import print_features, print_head
import polars
polars.Config.set_tbl_rows(100)


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