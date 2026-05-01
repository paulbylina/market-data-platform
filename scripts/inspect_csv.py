import argparse
from pathlib import Path

import polars as pl


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="Path to CSV file")
    parser.add_argument("--rows", type=int, default=100_000, help="Rows to load for describe/unique display")
    parser.add_argument("--max-unique", type=int, default=20, help="Max unique values to print per column")
    args = parser.parse_args()

    path = Path(args.file)

    if not path.exists():
        print(f"File not found: {path}")
        raise SystemExit(1)

    df = pl.read_csv(path, n_rows=args.rows)

    print("\nSHAPE:")
    print(f"rows loaded: {df.height}")
    print(f"columns: {df.width}")
        
    print("\nINFO:")
    for col in df.columns:
        dtype = df[col].dtype
        non_null = df[col].len() - df[col].null_count()
        nulls = df[col].null_count()
        unique_count = df[col].n_unique()

        print(f"{col}: {dtype} | non-null: {non_null} | nulls: {nulls} | unique: {unique_count}")

    print("\nUNIQUE VALUES:\n")
    select_columns = ["side", "operation"]
    for col in df.columns:
        if col not in select_columns:
            continue
    
        unique_values = df[col].unique().sort()

        print(col)
        print(f"unique count: {unique_values.len()}")

        if unique_values.len() > args.max_unique:
            print(f"showing first {args.max_unique} unique values:")
            print(unique_values.head(args.max_unique))
        else:
            print(unique_values)


if __name__ == "__main__":
    main()
