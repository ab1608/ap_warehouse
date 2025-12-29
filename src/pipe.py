import gc
from collections import defaultdict
from pathlib import Path
from typing import TypedDict

import numpy as np
import pandas as pd
from duckdb import DuckDBPyConnection
from pandas import DataFrame, Float64Dtype, Int64Dtype, StringDtype


class ActualsMetadata(TypedDict):
    wbs_enhanced: DataFrame
    gl_to_compass: DataFrame
    cost_center_to_compass: DataFrame
    compass_codes: DataFrame
    profit_centers_to_signatures: DataFrame


class CostCenterMetadata(TypedDict):
    wbs_enhanced: DataFrame
    gl_to_compass: DataFrame
    cost_center_to_compass: DataFrame
    compass_codes: DataFrame
    profit_centers_to_signatures: DataFrame


class CommitWBSMetadata(TypedDict):
    wbs_enhanced: DataFrame
    gl_to_compass: DataFrame
    compass_codes: DataFrame


class CommitCostCenterMetadat(TypedDict):
    gl_to_compass: DataFrame
    cost_center_to_compass: DataFrame
    compass_codes: DataFrame


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
            "Cost element": Int64Dtype(),
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

    def get_wbs_attributes(
        self, frame: DataFrame, wbs_elements: DataFrame
    ) -> DataFrame:
        frame = frame.drop(
            columns=[col for col in wbs_elements.columns if col != "WBS Element Code"],
            errors="ignore",
        )
        frame = frame.merge(
            wbs_elements, how="left", on="WBS Element Code", validate="many_to_one"
        )

        frame["Native G/L Account"] = frame["G/L Account"]
        frame["G/L Account"] = frame["WBS G/L Account"].fillna(frame["G/L Account"])
        frame["Profit Center Code"] = frame["WBS Profit Center Code"].fillna(
            frame["Profit Center Code"]
        )
        frame = frame.drop(columns=["WBS Profit Center Code", "WBS G/L Account"])
        return frame

    def run_import(self, data_files: list[Path]) -> None:
        # 1. Check for new files to process
        self.track_processed_files()
        files_to_process: list[Path] = self.get_new_files(data_files)

        if not files_to_process:
            print("No new data files found to ingest.")
            return
        else:
            print(f"Found {len(files_to_process)} new files to process.")

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
                print(f"No new {table_key} files to process. Skipping.")
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

                    # Create PartitionDate column
                    if "Period" in df.columns:
                        df["PartitionDate"] = pd.to_datetime(
                            df[["Fiscal Year", "Period"]]
                            .rename(columns={"Fiscal Year": "year", "Period": "month"})
                            .assign(day=1)
                        )
                    elif "Fiscal Period" in df.columns:
                        df["PartitionDate"] = pd.to_datetime(
                            df[["Fiscal Year", "Fiscal Period"]]
                            .rename(
                                columns={
                                    "Fiscal Year": "year",
                                    "Fiscal Period": "month",
                                }
                            )
                            .assign(day=1)
                        )

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

    # 3. Process Actuals
    def make_gold_actuals(
        self, actuals: DataFrame, meta_frames: ActualsMetadata
    ) -> DataFrame:
        # Define required keys
        required_keys = {
            "wbs_enhanced",
            "gl_to_compass",
            "cost_center_to_compass",
            "compass_codes",
            "profit_centers_to_signatures",
        }

        # Check for missing keys
        missing = required_keys - set(meta_frames.keys())
        if missing:
            raise ValueError(f"Missing required meta_frames: {', '.join(missing)}")

        actuals = actuals.rename(columns=self.COLUMN_RENAME)
        actuals["Amount in Company Code Currency"] *= -1
        actuals["Scenario"] = "Actuals"
        actuals_wbs: DataFrame = self.get_wbs_attributes(
            actuals, meta_frames["wbs_enhanced"]
        )

        del actuals
        gc.collect()

        # Get Compass Code
        actuals_wbs = actuals_wbs.merge(
            meta_frames["gl_to_compass"],
            how="left",
            on="G/L Account",
            validate="many_to_one",
        )
        actuals_wbs = actuals_wbs.rename(columns={"Compass Code": "G/L Compass Code"})

        # Compass Code using Cost Center
        actuals_wbs = actuals_wbs.merge(
            meta_frames["cost_center_to_compass"],
            how="left",
            on="Cost Center Code",
            validate="many_to_one",
            suffixes=("_native", "_cc"),
        )
        actuals_wbs["Compass Code"] = actuals_wbs["Compass Code"].fillna(
            actuals_wbs["G/L Compass Code"]
        )
        # Get Compass Code Text
        actuals_wbs = actuals_wbs.merge(
            meta_frames["compass_codes"],
            how="left",
            on="Compass Code",
            validate="many_to_one",
        )

        actuals_wbs["Profit Center Code"] = actuals_wbs[
            "Profit Center Code_native"
        ].fillna(actuals_wbs["Profit Center Code_cc"])

        actuals_wbs = actuals_wbs.drop(
            columns=[
                "G/L Compass Code",
                "Profit Center Code_native",
                "Profit Center Code_cc",
            ]
        )
        # Get Signature Description
        actuals_wbs = actuals_wbs.merge(
            meta_frames["profit_centers_to_signatures"],
            how="left",
            on="Profit Center Code",
            validate="many_to_one",
        )

        # Split actuals into "non-M" WBS Element Codes and "M" WBS Element Codes
        actuals_non_m = actuals_wbs.loc[actuals_wbs["WBS Type Char"] != "M"].copy()
        actuals_m = actuals_wbs.loc[actuals_wbs["WBS Type Char"] == "M"].copy()

        del actuals_wbs
        gc.collect()

        # Fiscal Type for "non-M" follows normal logic
        gold_actuals_non_m: DataFrame = self.determine_fiscal_type(actuals_non_m)

        # Fiscal Type for "M" WBS Element Codes should disregard the presence
        # of WBS Element Codes
        actuals_m["WBS Element Code Temp"] = actuals_m["WBS Element Code"]
        actuals_m["WBS Element Code"] = pd.NA
        gold_actuals_m = self.determine_fiscal_type(actuals_m)
        gold_actuals_m["WBS Element Code"] = actuals_m["WBS Element Code Temp"].astype(
            StringDtype()
        )
        gold_actuals_m = gold_actuals_m.drop(columns=["WBS Element Code Temp"])

        return pd.concat([gold_actuals_m, gold_actuals_non_m])

    def make_gold_cc_details(
        self, cc_details: DataFrame, meta_frames: CostCenterMetadata
    ) -> DataFrame:
        cc_details = cc_details.rename(columns=self.COLUMN_RENAME)
        cc_details["Scenario"] = "Cost Center Details"
        cc_details["Amount in Company Code Currency"] *= -1
        cc_details_wbs: DataFrame = self.get_wbs_attributes(
            cc_details, meta_frames["wbs_enhanced"]
        )

        del cc_details
        gc.collect()

        # Get Compass Code using G/L Account
        cc_details_wbs = cc_details_wbs.merge(
            meta_frames["gl_to_compass"],
            how="left",
            on="G/L Account",
            validate="many_to_one",
        )

        # Rename these first Compass attributes with as "G/L" to signify their origins
        cc_details_wbs = cc_details_wbs.rename(
            columns={"Compass Code": "G/L Compass Code"}
        )

        # Let's get additional Compass Codes using the Standard Hierarchy Node
        cc_details_wbs = cc_details_wbs.merge(
            meta_frames["cost_center_to_compass"],
            how="left",
            on="Cost Center Code",
            validate="many_to_one",
            suffixes=("_native", "_cc"),
        )

        cc_details_wbs["Profit Center Code"] = cc_details_wbs[
            "Profit Center Code_native"
        ].fillna(cc_details_wbs["Profit Center Code_cc"])

        cc_details_wbs["Compass Code"] = cc_details_wbs["Compass Code"].fillna(
            cc_details_wbs["G/L Compass Code"]
        )
        cc_details_wbs = cc_details_wbs.drop(
            columns=[
                "G/L Compass Code",
                "Profit Center Code_native",
                "Profit Center Code_cc",
            ]
        )

        # Get Compass Code Text
        cc_details_wbs = cc_details_wbs.merge(
            meta_frames["compass_codes"],
            how="left",
            on="Compass Code",
            validate="many_to_one",
        )
        # Get Signature Descriptions
        cc_details_wbs = cc_details_wbs.merge(
            meta_frames["profit_centers_to_signatures"],
            how="left",
            on="Profit Center Code",
            validate="many_to_one",
        )

        return self.determine_fiscal_type(cc_details_wbs)

    def make_gold_commit_wbs(
        self, commit_wbs: DataFrame, meta_frame: CommitWBSMetadata
    ) -> DataFrame:
        commit_wbs = commit_wbs.rename(columns=self.COLUMN_RENAME)

        # Find columns that contain the word "date" and format as datetime
        for col in commit_wbs.columns:
            if "date" in col.lower():
                commit_wbs[col] = pd.to_datetime(
                    commit_wbs[col], errors="coerce", format="%m/%d/%Y"
                )

        commit_wbs["Fiscal Type"] = "WBS"
        commit_wbs["Profit Center Code"] = pd.NA
        commit_wbs_enhanced: DataFrame = self.get_wbs_attributes(
            commit_wbs, meta_frame["wbs_enhanced"]
        )

        del commit_wbs
        gc.collect()

        # Get Compass Codes using G/L Account
        commit_wbs_enhanced = commit_wbs_enhanced.merge(
            meta_frame["gl_to_compass"],
            on="G/L Account",
            how="left",
            validate="many_to_one",
        )

        # Get Compass Text
        return commit_wbs_enhanced.merge(
            meta_frame["compass_codes"],
            on="Compass Code",
            how="left",
            validate="many_to_one",
        )

    def make_gold_commit_cc(
        self, commit_cc: DataFrame, meta_frame: CommitCostCenterMetadat
    ):
        commit_cc = commit_cc.rename(columns=self.COLUMN_RENAME)

        for col in commit_cc.columns:
            if "date" in col.lower():
                commit_cc[col] = pd.to_datetime(
                    commit_cc[col], errors="coerce", format="%m/%d/%Y"
                )

        commit_cc["Fiscal Type"] = "COST CENTER"
        commit_cc = commit_cc.merge(
            meta_frame["gl_to_compass"],
            how="left",
            on="G/L Account",
            validate="many_to_one",
        )
        commit_cc = commit_cc.rename(columns={"Compass Code": "G/L Compass Code"})

        # Get Compass Codes using Cost Center
        commit_cc = commit_cc.merge(
            meta_frame["cost_center_to_compass"],
            on="Cost Center Code",
            how="left",
            validate="many_to_one",
        )
        commit_cc["Compass Code"] = commit_cc["Compass Code"].fillna(
            commit_cc["G/L Compass Code"]
        )
        commit_cc = commit_cc.drop(columns=["G/L Compass Code"])

        # Get Compass Text
        return commit_cc.merge(
            meta_frame["compass_codes"],
            how="left",
            on="Compass Code",
            validate="many_to_one",
        )

    def run_transformation(self, output_path: Path) -> None:
        """Run transformation on data found in the database.

        Args:
            output_path (Path): Path to the output directory.

        Returns:
            None
        """

        # 1. Instantiate metadata and enhance where necessary
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

        # Business Logic:
        # 1. Records posessing a WBS Element Code should be merged with the WBS Elements metdata
        # to retrieve their Profit Center and G/L Account details from there. These attributes should
        # override any existing values in the native data.
        # 2. Compass Codes are retrieved using the G/L Accounts when G/L Accounts are present
        # 3. If G/L Accounts are not present, Cost Centers can be used to retrieve Compass Codes instead
        # by looking up the Cost Center to Compass mapping table that used the Standard Hierarchy Node to bridge both tables.

        actuals: DataFrame = self.conn.execute(
            """
            SELECT * FROM actuals
            """
        ).df()

        meta_frame: ActualsMetadata = {
            "compass_codes": compass_codes,
            "cost_center_to_compass": cost_center_to_compass,
            "gl_to_compass": gl_to_compass,
            "profit_centers_to_signatures": profit_centers_to_signatures,
            "wbs_enhanced": wbs_enhanced,
        }
        gold_actuals = self.make_gold_actuals(actuals, meta_frame)

        del actuals
        gc.collect()

        # ---- Process Cost Center Details ----
        cc_details: DataFrame = self.conn.execute(
            """
            SELECT * FROM cost_center_details
            """
        ).df()
        gold_cc_details = self.make_gold_cc_details(
            cc_details, CostCenterMetadata(meta_frame)
        )

        del cc_details
        gc.collect()

        # ---- Process WBS Committed ----
        meta_frame_wbs: CommitWBSMetadata = {
            "wbs_enhanced": wbs_enhanced,
            "compass_codes": compass_codes,
            "gl_to_compass": gl_to_compass,
        }
        commit_wbs: DataFrame = self.conn.execute(
            """
            SELECT * FROM commit_wbs
            """
        ).df()
        gold_commit_wbs: DataFrame = self.make_gold_commit_wbs(
            commit_wbs, meta_frame_wbs
        )

        # ---- Process Cost Center Committed ----
        meta_frame_cc: CommitCostCenterMetadat = {
            "gl_to_compass": gl_to_compass,
            "cost_center_to_compass": cost_center_to_compass,
            "compass_codes": compass_codes,
        }
        commit_cc: DataFrame = self.conn.execute(
            """
            SELECT * FROM commit_cc
            """
        ).df()
        gold_commit_cc: DataFrame = self.make_gold_commit_cc(commit_cc, meta_frame_cc)

        # ---- Final Processing for Committed ----
        gold_committed: DataFrame = pd.concat(
            [gold_commit_wbs, gold_commit_cc], ignore_index=True
        )
        del commit_wbs
        del commit_cc
        del gold_commit_wbs
        del gold_commit_cc
        gc.collect()

        # Get Signature Codes using Profit Center Codes
        gold_committed = gold_committed.merge(
            profit_centers_to_signatures,
            how="left",
            on="Profit Center Code",
            validate="many_to_one",
        )
        gold_committed["Scenario"] = "Committed"

        # ---- Store transformed data ----
        gold_dataset = pd.concat(
            [gold_actuals, gold_cc_details, gold_committed], ignore_index=True
        )
        gold_dataset["Year"] = gold_dataset["Fiscal Year"]
        gold_dataset["Month"] = gold_dataset["Fiscal Period"]
        gold_dataset["PartitionDate"] = pd.to_datetime(
            gold_dataset[["Year", "Month"]]
            .rename(columns={"Year": "year", "Month": "month"})
            .assign(day=1)
        )
        gold_dataset.insert(0, "Index", range(1, len(gold_dataset) + 1))
        gold_dataset["Index"] = gold_dataset["Index"].astype(Int64Dtype())

        del gold_actuals
        del gold_cc_details
        del gold_committed
        gc.collect()

        self.conn.register("gold_df_view", gold_dataset)
        self.conn.execute(
            f"""
         COPY (
            SELECT
                * EXCLUDE ("PartitionDate"),
                CAST(PartitionDate AS TIMESTAMP) AS PartitionDate
            FROM
                gold_df_view
        ) TO '{output_path}' (FORMAT PARQUET, PARTITION_BY ("Year", "Month"), OVERWRITE_OR_IGNORE 1)
            """
        )
        self.conn.execute(
            """
            CREATE OR REPLACE TABLE gold_dataset AS
            (SELECT
                * EXCLUDE ("PartitionDate"),
                CAST(PartitionDate AS TIMESTAMP) AS PartitionDate
            FROM
                gold_df_view)
            """
        )
        self.conn.unregister("gold_df_view")
