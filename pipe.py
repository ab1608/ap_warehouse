from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
from pandas import DataFrame, Float64Dtype, Int64Dtype, StringDtype

PROCESSED_LOG_TABLE = "ingested_files"

RAW_NEO_DTYPES = defaultdict(
    StringDtype,
    {
        "Company Code": StringDtype(),
        "Structure": StringDtype(),
        "Semantic Tag": StringDtype(),
        "Signature": StringDtype(),
        "Signature Code": StringDtype(),
        "Signature2": StringDtype(),
        "Accounting doc type": StringDtype(),
        "Fiscal Year": Int64Dtype(),
        "Fiscal Period": Int64Dtype(),
        "Ledger": StringDtype(),
        "Profit Center": StringDtype(),
        "Profit Center Name": StringDtype(),
        "Distribution Channel": Int64Dtype(),
        "Material": StringDtype(),
        "G/L Account": Int64Dtype(),
        "G/L Account Name": StringDtype(),
        "G/L Account Type": StringDtype(),
        "Journal Entry Type": StringDtype(),
        "JE Type Name": StringDtype(),
        "Journal Entry Item Text": StringDtype(),
        "Amount in Company Code Currency": Float64Dtype(),
        "Purchasing Document": StringDtype(),
        "Purchasing Doc. Item": Int64Dtype(),
        "Cost Center": StringDtype(),
        "Cost Center Name": StringDtype(),
        "Project": StringDtype(),
        "Project Name": StringDtype(),
        "WBS Element": StringDtype(),
        "WBS Element Name": StringDtype(),
        "Product": StringDtype(),
        "Reference Document Category": StringDtype(),
        "Object Type": StringDtype(),
        "Project definition": StringDtype(),
        "Object": StringDtype(),
        "Cost element": Int64Dtype(),
        "Cost element descr.": StringDtype(),
        "Value in Obj. Crcy": Float64Dtype(),
        "Val.in rep.cur.": Float64Dtype(),
        "Total Quantity": Float64Dtype(),
        "Quantity/Plan": Float64Dtype(),
        "Object Currency": StringDtype(),
        "Unit of Measure": StringDtype(),
        "Value TranCurr": Float64Dtype(),
        "Val/COArea Crcy": Float64Dtype(),
        "User Name": StringDtype(),
        "Supplier": StringDtype(),
        "Ref. document number": StringDtype(),
        "Reference Item": Int64Dtype(),
        "Reference Doc. Type": StringDtype(),
        "Name": StringDtype(),
        "Period": Int64Dtype(),
        "Business Transaction": StringDtype(),
        "CO Object Name": StringDtype(),
    },
)


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


def track_processed_files(conn: duckdb.DuckDBPyConnection) -> None:
    """Creates a table to track files we have already processed."""
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {PROCESSED_LOG_TABLE} (
            filename TEXT PRIMARY KEY,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def get_new_files(conn, all_files) -> list[Path]:
    """Filters out files that are already in the database log."""
    processed = conn.execute(f"SELECT filename FROM {PROCESSED_LOG_TABLE}").fetchall()
    processed_set = {row[0] for row in processed}
    return [f for f in all_files if f.name not in processed_set]


def run_import(conn: duckdb.DuckDBPyConnection, data_files: list[Path]) -> None:
    # 1. Check for new files to process
    track_processed_files(conn)
    files_to_process: list[Path] = get_new_files(conn, data_files)

    if not files_to_process:
        print("No new data files found to ingest. Exiting program.")
        return

    # 2. Categorize each file into its respective table
    master_tables: dict[str, list[Path]] = {
        "actuals": [],
        "committed": [],
        "cost_center_details": [],
    }
    for data_file in files_to_process:
        fname = data_file.name.lower()
        if "ccdet" in fname:
            master_tables["cost_center_details"].append(data_file)
        elif "commit" in fname:
            master_tables["committed"].append(data_file)
        else:
            master_tables["actuals"].append(data_file)

    # 3. Process each bucket of files into their respective tables
    for table_key, file_list in master_tables.items():
        if not file_list:
            print(f"No new files to process for table {table_key}. Skipping.")
            continue

        print(f"Moving {len(file_list)} files to table {table_key}...")
        for file_path in file_list:
            try:
                df: DataFrame = pd.read_csv(
                    file_path,
                    dtype=RAW_NEO_DTYPES,
                    encoding="ISO-8859-1",
                    index_col=False,
                )
                df["source_file"] = file_path.name

                # Create copy of DF with string columns as object dtype
                df_typed = convert_col_dtype(df, "string", "object")
                conn.register("df_typed", df_typed)

                conn.execute("BEGIN TRANSACTION")

                # If table does not exist, create column headers
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_key} AS SELECT * FROM df_typed WHERE 1=0"
                )
                # If it exists, append the DF to the table
                conn.execute(f"INSERT INTO {table_key} SELECT * FROM df_typed")
                conn.unregister("df_typed")

                # Log the processed file into the tracking table
                conn.execute(
                    f"""
                    INSERT INTO {PROCESSED_LOG_TABLE} (filename) 
                    VALUES (?)""",
                    [file_path.name],
                )
                conn.execute("COMMIT")

                print(f"Successfully ingested: {file_path.name} to table {table_key}.")

            except Exception as e:
                conn.execute("ROLLBACK")
                print(f"Error processing file {file_path.name}: {e}")

    # 4. Close connection
    conn.close()
    print("Data import complete.")
