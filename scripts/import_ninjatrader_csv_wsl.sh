#!/usr/bin/env bash
set -euo pipefail

selected_windows_path="$(
powershell.exe -NoProfile -Command '
Add-Type -AssemblyName System.Windows.Forms

$dialog = New-Object System.Windows.Forms.OpenFileDialog
$dialog.InitialDirectory = "C:\REPOSITORY\trading-dev-framework\ninja-lake\raw\ninjatrader"
$dialog.Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*"
$dialog.Multiselect = $false

if ($dialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {
    $dialog.FileName
}
'
)"

selected_windows_path="$(echo "$selected_windows_path" | tr -d '\r')"

if [[ -z "$selected_windows_path" ]]; then
    echo "No file selected."
    exit 1
fi

selected_wsl_path="$(wslpath -u "$selected_windows_path")"
filename="$(basename "$selected_wsl_path")"

if [[ "$filename" =~ ^(.+)_(L1|L2)_([0-9]{8})_[0-9]{6}\.csv$ ]]; then
    instrument="${BASH_REMATCH[1]}"
    feed_type="${BASH_REMATCH[2]}"
    raw_date="${BASH_REMATCH[3]}"
else
    echo "Could not parse NinjaTrader filename: $filename"
    exit 1
fi

date="${raw_date:0:4}-${raw_date:4:2}-${raw_date:6:2}"

raw_dest_dir="../ninja-lake/raw/ninjatrader/${feed_type}"
raw_dest_path="${raw_dest_dir}/${filename}"

parquet_dest_path="../ninja-lake/parquet/ninjatrader/${feed_type}/${instrument}/${date}/${date}.parquet"
converter="src/standardization/tick/${feed_type}_csv_to_parquet.py"

mkdir -p "$raw_dest_dir"

echo "Selected:"
echo "  $selected_windows_path"
echo
echo "Copying to WSL:"
echo "  $raw_dest_path"

cp -v "$selected_wsl_path" "$raw_dest_path"

echo
echo "Converting to Parquet:"
echo "  $parquet_dest_path"

uv run python "$converter" "$raw_dest_path" "$parquet_dest_path"

echo
echo "Done."
