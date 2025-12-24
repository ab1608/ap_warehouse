from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
from pandas import DataFrame, Float64Dtype, Int64Dtype, StringDtype

RAW_NEO_DTYPES = defaultdict(
    StringDtype,
    {
        "Accounting doc type": StringDtype(),
        "Amount in Company Code Currency": Float64Dtype(),
        "Business Transaction": StringDtype(),
        "CO Object Name": StringDtype(),
        "Company Code": StringDtype(),
        "Cost Center": StringDtype(),
        "Cost Center Name": StringDtype(),
        "Cost element": StringDtype(),
        "Cost element descr.": StringDtype(),
        "Distribution Channel": Int64Dtype(),
        "Fiscal Period": Int64Dtype(),
        "Fiscal Year": Int64Dtype(),
        "G/L Account": Int64Dtype(),
        "G/L Account Name": StringDtype(),
        "G/L Account Type": StringDtype(),
        "JE Type Name": StringDtype(),
        "Journal Entry Item Text": StringDtype(),
        "Journal Entry Type": StringDtype(),
        "Ledger": StringDtype(),
        "Material": StringDtype(),
        "Name": StringDtype(),
        "Object": StringDtype(),
        "Object Currency": StringDtype(),
        "Object Type": StringDtype(),
        "Partner Cost Center": StringDtype(),
        "Period": Int64Dtype(),
        "Product": StringDtype(),
        "Profit Center": StringDtype(),
        "Profit Center Name": StringDtype(),
        "Project": StringDtype(),
        "Project External ID": StringDtype(),
        "Project Name": StringDtype(),
        "Project definition": StringDtype(),
        "Purchasing Doc. Item": StringDtype(),
        "Purchasing Document": StringDtype(),
        "Quantity/Plan": Float64Dtype(),
        "Ref. document number": StringDtype(),
        "Reference Doc. Type": StringDtype(),
        "Reference Document Category": StringDtype(),
        "Reference Item": Int64Dtype(),
        "Semantic Tag": StringDtype(),
        "Signature": StringDtype(),
        "Signature Code": StringDtype(),
        "Signature2": StringDtype(),
        "Structure": StringDtype(),
        "Supplier": StringDtype(),
        "Total Quantity": Float64Dtype(),
        "Unit of Measure": StringDtype(),
        "User Name": StringDtype(),
        "Val.in rep.cur.": Float64Dtype(),
        "Val/COArea Crcy": Float64Dtype(),
        "Value TranCurr": Float64Dtype(),
        "Value in Obj. Crcy": Float64Dtype(),
        "WBS Element": StringDtype(),
        "WBS Element Name": StringDtype(),
        "WBS Element External ID": StringDtype(),
    },
)

PROCESSED_LOG_TABLE = "ingested_files"


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
        "commit_cc": [],
        "commit_wbs": [],
        "cost_center_details": [],
    }
    for data_file in files_to_process:
        fname = data_file.name.lower()
        if "ccdet" in fname:
            master_tables["cost_center_details"].append(data_file)
        elif "commit_cc" in fname:
            master_tables["commit_cc"].append(data_file)
        elif "commit_wbs" in fname:
            master_tables["commit_wbs"].append(data_file)
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
                # Check the file extension to determine whether to read as CSV or Parquet
                if file_path.suffix.lower() == ".parquet":
                    df: DataFrame = pd.read_parquet(file_path, engine="pyarrow")
                    df["source_file"] = file_path.name
                else:
                    df: DataFrame = pd.read_csv(
                        file_path,
                        dtype=RAW_NEO_DTYPES,
                        encoding="ISO-8859-1",
                        index_col=False,
                    )
                    df["source_file"] = file_path.name

                conn.register("df_typed", df)
                conn.execute("BEGIN TRANSACTION")
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table_key} AS SELECT * FROM df_typed WHERE 1=0"
                )
                conn.execute(f"INSERT INTO {table_key} SELECT * FROM df_typed")
                conn.unregister("df_typed")

                conn.execute(
                    f""" INSERT INTO {PROCESSED_LOG_TABLE} (filename) VALUES (?)""",
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
