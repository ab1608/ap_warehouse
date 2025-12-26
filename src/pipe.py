import gc
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from duckdb import DuckDBPyConnection
from pandas import DataFrame, Float64Dtype, Int64Dtype, StringDtype
from tqdm import tqdm


class FinancePipeline:
    RAW_DATA_TYPES = defaultdict(
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

    COLUMN_RENAME: dict[str, str] = {
        "Cost Center": "Cost Center Code",
        "Cost element": "G/L Account",
        "G/L Account Long Name": "G/L Account Name",
        "Material": "Material Code",
        "Partner Cost Center": "Partner Cost Center Code",
        "Profit Center": "Profit Center Code",
        "Product": "Product Code",
        "Product Description": "Product Name",
        "Project External ID": "Project Code",
        "Period": "Fiscal Period",
        "Signature": "Signature Code",
        "Value in Obj. Crcy": "Amount in Company Code Currency",
        "WBS Element": "WBS Element Code",
        "WBS Element External ID": "WBS Element Code",
        "WBS element": "WBS Element Code",
    }

    def __init__(
        self,
        conn: DuckDBPyConnection,
    ) -> None:
        self.conn = conn
        self.master_tables: dict[str, list[Path]] = {
            "actuals": [],
            "commit_cc": [],
            "commit_wbs": [],
            "cost_center_details": [],
            "wbs_budget": [],
            "forecast_budget": [],
            "forecast_live_estimate": [],
            "forecast_pre_budget": [],
            "forecast_trend": [],
        }

    PROCESSED_LOG_TABLE = "ingested_files"

    def track_processed_files(self) -> None:
        """Creates a table to track files we have already processed."""
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.PROCESSED_LOG_TABLE} (
                filename TEXT PRIMARY KEY,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def get_new_files(self, data_files: list[Path]) -> list[Path]:
        """Filters out files that are already in the database log."""
        processed = self.conn.execute(
            f"SELECT filename FROM {self.PROCESSED_LOG_TABLE}"
        ).fetchall()
        processed_set = {row[0] for row in processed}
        return [f for f in data_files if f.name not in processed_set]

    def enhance_wbs_elements(
        self, wbs_elements: DataFrame, wbs_codification: DataFrame
    ) -> DataFrame:
        # Create WBS Parents
        level_one_mask = wbs_elements["WBS Level"] == 1
        wbs_elements.loc[level_one_mask, "WBS Parent Code"] = wbs_elements.loc[
            level_one_mask, "WBS Element Code"
        ]

        wbs_elements["WBS Parent Code"] = wbs_elements["WBS Parent Code"].ffill()

        wbs_elements.loc[level_one_mask, "WBS Parent Name"] = wbs_elements.loc[
            level_one_mask, "WBS Element Name"
        ]
        wbs_elements["WBS Parent Name"] = wbs_elements["WBS Parent Name"].ffill()

        # Get first character from each WBS Element to create Type Char
        wbs_elements["WBS Type Char"] = wbs_elements["WBS Element Code"].str[0:1]

        # Create WBS Bucket using codifications
        wbs_elements = pd.merge(
            wbs_elements,
            wbs_codification,
            how="left",
            on="WBS Type Char",
            validate="many_to_one",
        )

        return wbs_elements

    def link_profit_center_to_signatures(
        self, profit_centers: DataFrame, signatures: DataFrame
    ) -> DataFrame:
        return pd.merge(
            profit_centers,
            signatures,
            on="Signature Code",
            how="left",
            validate="many_to_one",
        )

    def link_cost_center_to_compass(
        self, cost_centers: DataFrame, node_to_compass: DataFrame
    ):
        return pd.merge(
            cost_centers,
            node_to_compass,
            on="Standard Hierarchy Node",
            how="inner",
            validate="many_to_one",
        ).drop(columns=["Standard Hierarchy Node"])

    def link_gl_to_compass(self, gl_accounts, gl_to_compass: DataFrame) -> DataFrame:
        return pd.merge(
            gl_accounts,
            gl_to_compass,
            how="left",
            on="G/L Account",
            validate="one_to_one",
        )

    def determine_fiscal_type(self, data_table: DataFrame) -> DataFrame:
        data_table["Fiscal Type"] = np.select(
            [
                data_table["WBS Element Code"].notna(),
                (data_table["Cost Center Code"].notna())
                | (data_table["Partner Cost Center Code"].notna()),
                (data_table["WBS Element Code"].isna())
                & (data_table["Product Code"].notna()),
            ],
            ["WBS", "COST CENTER", "NO WBS"],
            default="FINANCE",
        )
        return data_table

    def run_import(self, data_files: list[Path]) -> None:
        # 1. Check for new files to process
        self.track_processed_files()
        files_to_process: list[Path] = self.get_new_files(data_files)

        if not files_to_process:
            print("No new data files found to ingest. Exiting program.")
            return

        # 2. Categorize each file into its respective table

        for data_file in files_to_process:
            fname = data_file.name.lower()
            if "ccdet" in fname:
                self.master_tables["cost_center_details"].append(data_file)
            elif "commit_cc" in fname:
                self.master_tables["commit_cc"].append(data_file)
            elif "commit_wbs" in fname:
                self.master_tables["commit_wbs"].append(data_file)
            elif "wbs_budget" in fname:
                self.master_tables["wbs_budget"].append(data_file)
            elif "_le_" in fname:
                self.master_tables["forecast_live_estimate"].append(data_file)
            elif "_prebud_" in fname:
                self.master_tables["forecast_pre_budget"].append(data_file)
            elif "_bud_" in fname:
                self.master_tables["forecast_budget"].append(data_file)
            elif "_t0" in fname:
                self.master_tables["forecast_trend"].append(data_file)
            else:
                self.master_tables["actuals"].append(data_file)

        # 3. Process each bucket of files into their respective tables
        for table_key, file_list in self.master_tables.items():
            if not file_list:
                print(f"No new files to process for table {table_key}. Skipping.")
                continue

            print(f"Moving {len(file_list)} files to table {table_key}...")
            for file_path in file_list:
                try:
                    df = DataFrame()
                    if file_path.suffix.lower() == ".parquet":
                        df = pd.read_parquet(file_path, engine="pyarrow")
                        df["source_file"] = file_path.name
                    else:
                        df = pd.read_csv(
                            file_path,
                            dtype=self.RAW_DATA_TYPES,
                            encoding="ISO-8859-1",
                            index_col=False,
                        )
                        df["source_file"] = file_path.name

                    self.conn.register("df_typed", df)
                    self.conn.execute("BEGIN TRANSACTION")
                    self.conn.execute(
                        f"CREATE TABLE IF NOT EXISTS {table_key} AS SELECT * FROM df_typed WHERE 1=0"
                    )
                    self.conn.execute(f"INSERT INTO {table_key} SELECT * FROM df_typed")
                    self.conn.unregister("df_typed")

                    self.conn.execute(
                        f""" INSERT INTO {self.PROCESSED_LOG_TABLE} (filename) VALUES (?)""",
                        [file_path.name],
                    )
                    self.conn.execute("COMMIT")

                    print(
                        f"Successfully ingested: {file_path.name} to table {table_key}."
                    )
                except Exception as e:
                    self.conn.execute("ROLLBACK")
                    print(f"Error processing file {file_path.name}: {e}")

    def run_transformation(self, output_path: Path) -> None:
        """Run transformation on data found in the database.

        Args:
            output_path (Path): Path to the output directory.

        Returns:
            None
        """
        # 1. Load all master tables from database
        master_data: dict[str, DataFrame] = {}

        with tqdm(total=1, desc="Fetching from DuckDB") as pbar:
            for master_table in self.master_tables.keys():
                master_df: DataFrame = self.conn.execute(
                    f"SELECT * FROM {master_table}"
                ).df()
                master_data.update({master_table: master_df})
                pbar.update(1)

        # 2. Instantiate metadata and enhance where necessary
        compass_codes: DataFrame = self.conn.execute(
            """
            SELECT
                "Financial Statement Item" AS "Compass Code",
                "Text" AS "P&L Line Text"
            FROM meta_fs_items
            """
        ).df()

        cost_centers: DataFrame = self.conn.execute(
            """
            SELECT
                "Cost Center" AS "Cost Center Code",
                "Profit Center" AS "Profit Center Code",
                "Standard Hierarchy Node",
            FROM meta_cost_centers
            """
        ).df()

        node_to_compass: DataFrame = self.conn.execute(
            """
            SELECT
                "Group cost center code" AS "Standard Hierarchy Node",
                "P&L line code" AS "Compass Code"
            FROM meta_node_to_compass
            """
        ).df()

        fiscal_periods: DataFrame = self.conn.execute(
            """
            SELECT
                "Fiscal Period",
                "Fiscal Period Text"
            FROM meta_fiscal_periods;
            """
        ).df()

        gl_accounts: DataFrame = self.conn.execute(
            """
            SELECT
                "G/L Account",
                "G/L Acct Long Text"
            FROM meta_gl_accounts
            """
        ).df()

        gl_accounts_to_compass: DataFrame = self.conn.execute(
            """
            SELECT
                "Financial Statement Item" AS "Compass Code",
                "Account To" AS "G/L Account"
            FROM meta_gl_to_compass
            """
        ).df()

        profit_centers: DataFrame = self.conn.execute(
            """
            SELECT
                "Profit Center" AS "Profit Center Code",
                "Segment" AS "Division Abbreviation",
                "Segment (2)" AS "Division",
                "Standard Hierarchy Node",
                "SAP Signature" AS "Signature Code"
            FROM meta_profit_centers
            """
        ).df()

        signatures: DataFrame = self.conn.execute(
            """
            SELECT
                "Signature Code",
                "Signature Description"
            FROM meta_signatures
            """
        ).df()

        wbs_codification: DataFrame = self.conn.execute(
            """
            SELECT
                "Type Char" AS "WBS Type Char",
                "Type" AS "WBS Type",
                "Type Local" AS "WBS Typ Local"
            FROM meta_wbs_codification;
            """
        ).df()

        wbs_elements: DataFrame = self.conn.execute(
            """
            SELECT
                "WBS Element" AS "WBS Element Code",
                "WBS Element Name",
                "Level" AS "WBS Level",
                "P&L_Destination" AS "WBS G/L Account",
                "Profit Center" AS "WBS Profit Center Code"
            FROM meta_wbs_elements
            """
        ).df()

        wbs_enhanced: DataFrame = self.enhance_wbs_elements(
            wbs_elements, wbs_codification
        )
        profit_centers_to_signatures: DataFrame = self.link_profit_center_to_signatures(
            profit_centers, signatures
        )
        cost_center_to_compass: DataFrame = self.link_cost_center_to_compass(
            cost_centers, node_to_compass
        )
        gl_to_compass: DataFrame = self.link_gl_to_compass(
            gl_accounts, gl_accounts_to_compass
        )

        # 3. Load all data and append tables where necessary

        # New Map to re-categroize each table as a "Scenario" based on the table name
        final_labels = {
            "actuals": "Actuals",
            "commit_cc": "Committed",
            "commit_wbs": "Committed",
            "cost_center_details": "Cost Center Details",
            "forecast_budget": "Budget",
            "forecast_live_estimate": "Live Estimate",
            "forecast_pre_budget": "Pre-Budget",
            "forecast_trend": "Trend",
        }
        for table_label, table in master_data.items():
            table["Scenario"] = final_labels.get(table_label, "Unknown")

        # Start the transformation process

        # Start with actuals

        # ----- Process Data -----
        # Business Logic:
        # 1. Records posessing a WBS Element Code should be merged with the WBS Elements metdata
        # to retrieve their Profit Center and G/L Account details from there. These attributes should
        # override any existing values in the native data.
        # 2. Compass Codes are retrieved using the G/L Accounts when G/L Accounts are present
        # 3. If G/L Accounts are not present, Cost Centers can be used to retrieve Compass Codes instead
        # by looking up the Cost Center to Compass mapping table that used the Standard Hierarchy Node to bridge both tables.

        # ---- Begin with actulals ----
        actuals: DataFrame = master_data["actuals"].rename(columns=self.COLUMN_RENAME)
        # Let's drop existing WBS Element fields to avoid conflicts except WBS Element Code
        actuals = actuals.drop(
            columns=[col for col in wbs_enhanced.columns if col != "WBS Element Code"],
            errors="ignore",
        )

        actuals = actuals.merge(
            wbs_enhanced, how="left", on="WBS Element Code", validate="many_to_one"
        )

        actuals["Native G/L Account"] = actuals["G/L Account"]
        actuals["G/L Account"] = actuals["WBS G/L Account"].fillna(
            actuals["WBS G/L Account"]
        )
        actuals["Profit Center Code"] = actuals["WBS Profit Center Code"].fillna(
            actuals["Profit Center Code"]
        )
        actuals = actuals.drop(columns=["WBS Profit Center Code", "WBS G/L Account"])

        actuals = actuals.merge(
            gl_to_compass, how="left", on="G/L Account", validate="many_to_one"
        )
        actuals = actuals.merge(
            compass_codes, how="left", on="Compass Code", validate="many_to_one"
        )

        actuals["Amount in Company Code Currency"] *= -1

        actuals_fiscal_types = self.determine_fiscal_type(actuals)

        del actuals
        gc.collect()

        # Let's save actuals to a parquet files that has been partitioned by year and period
        # using DuckDB
        # Instead of creating a table, export directly to a Parquet folder
        self.conn.register("gold_actuals", actuals_fiscal_types)
        self.conn.execute(f"""
            COPY (SELECT * FROM gold_actuals)
            TO '{output_path}'
            (FORMAT PARQUET, PARTITION_BY ("Fiscal Year", "Fiscal Period"), OVERWRITE_OR_IGNORE 1)
        """)
