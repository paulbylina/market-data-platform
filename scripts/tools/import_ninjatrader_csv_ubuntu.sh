#!/usr/bin/env bash
set -euo pipefail

MEDIA_ROOT="/media/$USER"
WINDOWS_DRIVE="$(ls "$MEDIA_ROOT" | head -n 1)"

WINDOWS_NINJA_ROOT="$MEDIA_ROOT/$WINDOWS_DRIVE/REPOSITORY/trading-dev-framework/ninja-lake/raw/ninjatrader/"

command -v zenity >/dev/null 2>&1 || {
    echo "zenity is required but not installed."
    echo "Install it with: sudo apt install zenity"
    exit 1
}

CSV_FILE=$(
    zenity --file-selection \
        --title="Select NinjaTrader CSV" \
        --filename="$WINDOWS_NINJA_ROOT/L2/*"
)
if [[ -z "$CSV_FILE" ]]; then
    echo "No file selected."
    exit 1
fi

filename="$(basename "$CSV_FILE")"

if [[ "$filename" =~ ^(.+)_(L1|L2)_([0-9]{8})_[0-9]{6}\.csv$ ]]; then
    instrument="${BASH_REMATCH[1]}"
    feed_type="${BASH_REMATCH[2]}"
    raw_date="${BASH_REMATCH[3]}"
else
    echo "Could not parse NinjaTrader filename: $filename"
    echo "Expected something like:"
    echo "  ES_06-26_L2_20260429_071242.csv"
    exit 1
fi

date="${raw_date:0:4}-${raw_date:4:2}-${raw_date:6:2}"

raw_dest_dir="../ninja-lake/raw/ninjatrader/${feed_type}"
raw_dest_path="${raw_dest_dir}/${filename}"

parquet_dest_path="../ninja-lake/parquet/ninjatrader/${feed_type}/${instrument}/${date}/${date}.parquet"
converter="src/standardization/tick/${feed_type}_csv_to_parquet.py"

if [[ ! -f "$converter" ]]; then
    echo "Converter not found: $converter"
    exit 1
fi

mkdir -p "$raw_dest_dir"

echo "Selected:"
echo "  $CSV_FILE"
echo
echo "Copying to Ubuntu ninja-lake:"
echo "  $raw_dest_path"

cp -v "$CSV_FILE" "$raw_dest_path"

echo
echo "Converting to Parquet:"
echo "  $parquet_dest_path"

uv run python "$converter" "$raw_dest_path" "$parquet_dest_path"

echo
echo "Done."
