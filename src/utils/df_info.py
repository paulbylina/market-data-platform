import polars


def print_features(df: polars.DataFrame, with_dtypes: bool = False) -> None:
    """
    Print Polars DataFrame feature/column names
    """

    if not isinstance(df, polars.DataFrame):
        raise TypeError(f"Expected polars.DataFrame, got {type(df)}")

    if with_dtypes:
        for name, dtype in df.schema.items():
            print(f"{name}: {dtype}")
        return
    
    print("---------Features---------")
    for col in df.columns:
        print(col)


def print_head(df: polars.DataFrame, head_len: int) -> None:
    """
    Prints Polars DataFrame using head
    """

    if not isinstance(df, polars.DataFrame):
        raise TypeError(f"Expected polars.DataFrame, got {type(df)}")

    
    print(f"---------Head {head_len}---------")
    print(df.head(head_len))