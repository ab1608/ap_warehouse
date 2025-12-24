from pathlib import Path

import pandas as pd
from pandas import DataFrame


def convert_col_dtype(df: DataFrame, original_type: str, target_type: str) -> DataFrame:
    """
    Convert the data type of a specific column in a DataFrame.

    Args:
        df (DataFrame): The DataFrame containing the column to convert.
        original_type (str): The original data type of the column.
        target_type (str): The target data type to convert the column to.

    Returns:
        DataFrame: A new DataFrame with the specified column converted to the target data type.
    """
    df_converted = df.copy()
    for col in df.columns:
        if df[col].dtype == original_type:
            df_converted[col] = df[col].astype(target_type)
    return df_converted


def list_files_by_extension(directory: Path, extension: str) -> list[Path]:
    """List all files in a directory with a specific file extension.

    Args:
        directory (Path): The directory to search.
        extension (str): The file extension to filter by.

    Returns:
        list[Path]: A list of file paths matching the specified extension.
    """
    return list(directory.glob(f"*.{extension}"))
