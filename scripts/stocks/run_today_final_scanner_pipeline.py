from pathlib import Path
import subprocess
import sys

COMMANDS = [
    [
        sys.executable,
        "scripts/stocks/enrich_today_scanner_with_massive_fundamentals_v2.py",
        "--input",
        "data/reference/stocks/today_all_confirmed_scanner_rows_latest.csv",
        "--output",
        "data/reference/stocks/today_all_confirmed_scanner_rows_fundamentals_latest.csv",
    ],
    [
        sys.executable,
        "scripts/stocks/add_today_scanner_risk_labels.py",
        "--input",
        "data/reference/stocks/today_all_confirmed_scanner_rows_fundamentals_latest.csv",
        "--output",
        "data/reference/stocks/today_all_confirmed_scanner_rows_fundamentals_risk_latest.csv",
    ],
    [
        sys.executable,
        "scripts/stocks/add_today_validated_setup_labels.py",
    ],
    [
        sys.executable,
        "scripts/stocks/build_today_final_scanner_view.py",
    ],
]

for cmd in COMMANDS:
    print()
    print("=" * 100)
    print("running:", " ".join(cmd))
    print("=" * 100)

    result = subprocess.run(cmd)

    if result.returncode != 0:
        raise SystemExit(result.returncode)

print()
print("DONE")
print("final view: data/reference/stocks/today_final_scanner_view_latest.csv")
