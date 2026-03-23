from collections import defaultdict
from pathlib import Path
from typing import TypedDict

import numpy as np
import pandas as pd
from duckdb import DuckDBPyConnection
from pandas import (
    DataFrame,
    Float64Dtype,
    Int8Dtype,
    Int16Dtype,
    Int64Dtype,
    StringDtype,
)


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
    profit_centers_to_signatures: DataFrame


class CommitCostCenterMetadat(TypedDict):
    gl_to_compass: DataFrame
    cost_center_to_compass: DataFrame
    compass_codes: DataFrame
    profit_centers_to_signatures: DataFrame


class FinancePipeline:
    RAW_DATA_SCHEMA = defaultdict(
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
            "Value": Float64Dtype(),
            "WBS Element": StringDtype(),
            "WBS Element Name": StringDtype(),
            "WBS Element External ID": StringDtype(),
            "YEAR": Int64Dtype(),
            "PERIOD": StringDtype(),
            "P&L DESTINATION ACCOUNT": Int64Dtype(),
        },
    )
    SAP_COLUMN_RENAME = {
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
    FORECAST_COLUMN_RENAME = {
        "AXE": "Axe",
        "BRAND": "Brand",
        "BUDGET OWNER": "Budget Owner",
        "CODE (L1_CC_Oth)": "Code 1",
        "CODE DESCRIPTION  (L1_CC_Oth)": "Code 1 Description",
        "CODE (L2)": "Code 2",
        "CODE DESCRIPTION (L2)": "Code 2 Description",
        "COMPANY CODE": "Company Code",
        "P&L LINE COMPASS CODE_FS": "Compass Code",
        "TYPE": "Detailed Type",
        "YEAR": "Fiscal Year",
        "P&L DESTINATION ACCOUNT": "G/L Account",
        "SIGNATURE CODE": "Signature Code",
        "SUB BRAND": "Sub Brand",
        "SUB-AXE": "Sub-Axe",
        "Value": "Amount in Company Code Currency",
        "PRODUCT CODE": "Product Code",
        "SPEND TYPE": "Spend Type",
    }
    STAGE_FINANCE_SCHEMA = {
        "PartitionDate": "datetime64[ms]",
        "Debit Date": "datetime64[ms]",
        "Reference date": "datetime64[ms]",
        "Document Date": "datetime64[ms]",
        "Total Quantity": Float64Dtype(),
        "Quantity/Plan": Float64Dtype(),
        "Value TranCurr": Float64Dtype(),
        "Val/COArea Crcy": Float64Dtype(),
        "Reference Item": Float64Dtype(),
        "Val.in rep.cur.": Float64Dtype(),
        "Native G/L Account": Int64Dtype(),
        "G/L Account": Int64Dtype(),
        "WBS Level": Int8Dtype(),
        "Fiscal Year": Int16Dtype(),
        "Fiscal Period": Int8Dtype(),
        "Distribution Channel": Int8Dtype(),
        "Year": Int16Dtype(),
        "Month": Int8Dtype(),
        "Company Code": StringDtype(),
        "Company Code Name": StringDtype(),
        "Ledger": StringDtype(),
        "Ledger Name": StringDtype(),
        "Profit Center Name": StringDtype(),
        "G/L Account Name": StringDtype(),
        "G/L Account Type": StringDtype(),
        "Journal Entry Type": StringDtype(),
        "JE Type Name": StringDtype(),
        "Journal Entry Item Text": StringDtype(),
        "Amount in Company Code Currency": Float64Dtype(),
        "Company Code Currency": StringDtype(),
        "Purchasing Document": StringDtype(),
        "Purchasing Doc. Item": StringDtype(),
        "Partner Cost Center Code": StringDtype(),
        "Cost Center Code": StringDtype(),
        "Cost Center Name": StringDtype(),
        "Project Code": StringDtype(),
        "Project Name": StringDtype(),
        "WBS Element Code": StringDtype(),
        "Product Code": StringDtype(),
        "Product Name": StringDtype(),
        "source_file": StringDtype(),
        "Scenario": StringDtype(),
        "WBS Element Name": StringDtype(),
        "WBS Parent Code": StringDtype(),
        "WBS Parent Name": StringDtype(),
        "WBS Type Char": StringDtype(),
        "WBS Type": StringDtype(),
        "WBS Typ Local": StringDtype(),
        "G/L Acct Long Text": StringDtype(),
        "Compass Code": StringDtype(),
        "P&L Line Text": StringDtype(),
        "Profit Center Code": StringDtype(),
        "Division Abbreviation": StringDtype(),
        "Division": StringDtype(),
        "Standard Hierarchy Node": StringDtype(),
        "Signature Code": StringDtype(),
        "Signature Description": StringDtype(),
        "Fiscal Type": StringDtype(),
        "Project definition": StringDtype(),
        "Reference Document Category": StringDtype(),
        "Object Type": StringDtype(),
        "Object": StringDtype(),
        "CO Object Name": StringDtype(),
        "Cost element descr.": StringDtype(),
        "Object Currency": StringDtype(),
        "Unit of Measure": StringDtype(),
        "User Name": StringDtype(),
        "Supplier": StringDtype(),
        "Ref. document number": StringDtype(),
        "Reference Doc. Type": StringDtype(),
        "Name": StringDtype(),
        "Business Transaction": StringDtype(),
        "Code 1": StringDtype(),
        "Code 1 Description": StringDtype(),
        "Spend Type": StringDtype(),
        "Code 2": StringDtype(),
        "Code 2 Description": StringDtype(),
        "Budget Owner": StringDtype(),
        "P&L LINE COMPASS DESCRIPTION_FS": StringDtype(),
        "Brand": StringDtype(),
        "Sub Brand": StringDtype(),
        "Axe": StringDtype(),
        "Sub-Axe": StringDtype(),
        "REFERENCE": StringDtype(),
        "CUSTOMER CU CODE": StringDtype(),
        "CUSTOMER CC LABEL": StringDtype(),
        "BUD NATURE": StringDtype(),
        "BU PARTNER": StringDtype(),
        "PERIOD": StringDtype(),
        "Last Refresh": StringDtype(),
        "Detailed Type": StringDtype(),
        "P&L Line Check": StringDtype(),
        "Adj NEO Semantic Tag to Active Compass Code": StringDtype(),
        "Code 1 Concatenated": StringDtype(),
        "Code 2 Concatenated": StringDtype(),
        "WBS Profit Center Code": StringDtype(),
    }
    STAGE_ACTUALS_SCHEMA = {
        "Company Code": StringDtype(),
        "Company Code Name": StringDtype(),
        "Fiscal Year": Int16Dtype(),
        "Fiscal Period": Int8Dtype(),
        "Ledger": StringDtype(),
        "Ledger Name": StringDtype(),
        "Profit Center Name": StringDtype(),
        "Distribution Channel": Int8Dtype(),
        "G/L Account": Int64Dtype(),
        "G/L Account Name": StringDtype(),
        "G/L Account Type": StringDtype(),
        "Journal Entry Type": StringDtype(),
        "JE Type Name": StringDtype(),
        "Journal Entry Item Text": StringDtype(),
        "Amount in Company Code Currency": Float64Dtype(),
        "Company Code Currency": StringDtype(),
        "Purchasing Document": StringDtype(),
        "Purchasing Doc. Item": StringDtype(),
        "Partner Cost Center Code": StringDtype(),
        "Cost Center Code": StringDtype(),
        "Cost Center Name": StringDtype(),
        "Project Code": StringDtype(),
        "Project Name": StringDtype(),
        "WBS Element Code": StringDtype(),
        "Product Code": StringDtype(),
        "Product Name": StringDtype(),
        "source_file": StringDtype(),
        "PartitionDate": "datetime64[ms]",
        "Scenario": StringDtype(),
        "WBS Element Name": StringDtype(),
        "WBS Level": Int8Dtype(),
        "WBS Parent Code": StringDtype(),
        "WBS Parent Name": StringDtype(),
        "WBS Type Char": StringDtype(),
        "WBS Type": StringDtype(),
        "WBS Typ Local": StringDtype(),
        "Native G/L Account": Int64Dtype(),
        "G/L Acct Long Text": StringDtype(),
        "Compass Code": StringDtype(),
        "P&L Line Text": StringDtype(),
        "Profit Center Code": StringDtype(),
        "Division Abbreviation": StringDtype(),
        "Division": StringDtype(),
        "Standard Hierarchy Node": StringDtype(),
        "Signature Code": StringDtype(),
        "Signature Description": StringDtype(),
        "Fiscal Type": StringDtype(),
    }
    STAGE_COST_CENTER_DETAILS_SCHEMA = {
        "Company Code": StringDtype(),
        "Company Code Name": StringDtype(),
        "Fiscal Year": Int16Dtype(),
        "Fiscal Period": Int8Dtype(),
        "Ledger": StringDtype(),
        "Ledger Name": StringDtype(),
        "Profit Center Name": StringDtype(),
        "Distribution Channel": Int8Dtype(),
        "G/L Account": Int64Dtype(),
        "G/L Account Name": StringDtype(),
        "G/L Account Type": StringDtype(),
        "Journal Entry Type": StringDtype(),
        "JE Type Name": StringDtype(),
        "Journal Entry Item Text": StringDtype(),
        "Amount in Company Code Currency": Float64Dtype(),
        "Company Code Currency": StringDtype(),
        "Purchasing Document": StringDtype(),
        "Purchasing Doc. Item": StringDtype(),
        "Partner Cost Center Code": StringDtype(),
        "Cost Center Code": StringDtype(),
        "Cost Center Name": StringDtype(),
        "Project Code": StringDtype(),
        "Project Name": StringDtype(),
        "WBS Element Code": StringDtype(),
        "Product Code": StringDtype(),
        "Product Name": StringDtype(),
        "source_file": StringDtype(),
        "PartitionDate": "datetime64[ms]",
        "Scenario": StringDtype(),
        "WBS Element Name": StringDtype(),
        "WBS Level": Int8Dtype(),
        "WBS Parent Code": StringDtype(),
        "WBS Parent Name": StringDtype(),
        "WBS Type Char": StringDtype(),
        "WBS Type": StringDtype(),
        "WBS Typ Local": StringDtype(),
        "Native G/L Account": Int64Dtype(),
        "G/L Acct Long Text": StringDtype(),
        "Compass Code": StringDtype(),
        "Profit Center Code": StringDtype(),
        "P&L Line Text": StringDtype(),
        "Division Abbreviation": StringDtype(),
        "Division": StringDtype(),
        "Standard Hierarchy Node": StringDtype(),
        "Signature Code": StringDtype(),
        "Signature Description": StringDtype(),
        "Fiscal Type": StringDtype(),
    }
    STAGE_FORECAST_SCHEMA = {
        "Code 1": StringDtype(),
        "Code 1 Description": StringDtype(),
        "Spend Type": StringDtype(),
        "Code 2": StringDtype(),
        "Code 2 Description": StringDtype(),
        "Budget Owner": StringDtype(),
        "G/L Account": Int64Dtype(),
        "P&L LINE COMPASS DESCRIPTION_FS": StringDtype(),
        "Brand": StringDtype(),
        "Sub Brand": StringDtype(),
        "Axe": StringDtype(),
        "Sub-Axe": StringDtype(),
        "REFERENCE": StringDtype(),
        "Product Code": StringDtype(),
        "CUSTOMER CU CODE": StringDtype(),
        "CUSTOMER CC LABEL": StringDtype(),
        "BUD NATURE": StringDtype(),
        "BU PARTNER": StringDtype(),
        "Name": StringDtype(),
        "PERIOD": StringDtype(),
        "Amount in Company Code Currency": Float64Dtype(),
        "Last Refresh": StringDtype(),
        "Detailed Type": StringDtype(),
        "Fiscal Year": Int16Dtype(),
        "Company Code": StringDtype(),
        "Signature Code": StringDtype(),
        "P&L Line Check": StringDtype(),
        "Adj NEO Semantic Tag to Active Compass Code": StringDtype(),
        "source_file": StringDtype(),
        "Fiscal Period": Int8Dtype(),
        "PartitionDate": "datetime64[ms]",
        "Scenario": StringDtype(),
        "Fiscal Type": StringDtype(),
        "Code 1 Concatenated": StringDtype(),
        "Code 2 Concatenated": StringDtype(),
        "Cost Center Code": StringDtype(),
        "Profit Center Code": StringDtype(),
        "WBS Element Code": StringDtype(),
        "WBS Element Name": StringDtype(),
        "WBS Level": Int8Dtype(),
        "WBS Profit Center Code": StringDtype(),
        "WBS Parent Code": StringDtype(),
        "WBS Parent Name": StringDtype(),
        "WBS Type Char": StringDtype(),
        "WBS Type": StringDtype(),
        "WBS Typ Local": StringDtype(),
        "Compass Code": StringDtype(),
    }
    STAGE_COMMIT_SCHEMA = {
        "Company Code": StringDtype(),
        "Project definition": StringDtype(),
        "Reference Document Category": StringDtype(),
        "Debit Date": "datetime64[ms]",
        "Object Type": StringDtype(),
        "Object": StringDtype(),
        "CO Object Name": StringDtype(),
        "G/L Account": Int64Dtype(),
        "Cost element descr.": StringDtype(),
        "Amount in Company Code Currency": Float64Dtype(),
        "Total Quantity": Float64Dtype(),
        "Quantity/Plan": Float64Dtype(),
        "Object Currency": StringDtype(),
        "Unit of Measure": StringDtype(),
        "WBS Element Code": StringDtype(),
        "Value TranCurr": Float64Dtype(),
        "Val/COArea Crcy": Float64Dtype(),
        "User Name": StringDtype(),
        "Supplier": StringDtype(),
        "Ref. document number": StringDtype(),
        "Reference Item": Float64Dtype(),
        "Reference Doc. Type": StringDtype(),
        "Reference date": "datetime64[ms]",
        "Name": StringDtype(),
        "Fiscal Year": Int16Dtype(),
        "Fiscal Period": Int8Dtype(),
        "Document Date": "datetime64[ms]",
        "Business Transaction": StringDtype(),
        "source_file": StringDtype(),
        "PartitionDate": "datetime64[ms]",
        "Scenario": StringDtype(),
        "Fiscal Type": StringDtype(),
        "Profit Center Code": StringDtype(),
        "WBS Element Name": StringDtype(),
        "WBS Level": Int8Dtype(),
        "WBS Parent Code": StringDtype(),
        "WBS Parent Name": StringDtype(),
        "WBS Type Char": StringDtype(),
        "WBS Type": StringDtype(),
        "WBS Typ Local": StringDtype(),
        "Native G/L Account": Int64Dtype(),
        "G/L Acct Long Text": StringDtype(),
        "Compass Code": StringDtype(),
        "Division Abbreviation": StringDtype(),
        "Division": StringDtype(),
        "Standard Hierarchy Node": StringDtype(),
        "Signature Code": StringDtype(),
        "Signature Description": StringDtype(),
        "P&L Line Text": StringDtype(),
        "Cost Center Code": StringDtype(),
        "Val.in rep.cur.": Float64Dtype(),
    }
    STAGE_NET_SALES_SCHEMA = {
        "BU_Central": StringDtype(),
        "PERIOD": StringDtype(),
        "PRODUCT_BI_CENTRAL": StringDtype(),
        "Amount in Company Code Currency": Float64Dtype(),
        "Fiscal Year": Int16Dtype(),
        "source_file": StringDtype(),
        "Fiscal Period": Int8Dtype(),
        "Origin": StringDtype(),
        "Signature Code": StringDtype(),
        "Signature Description": StringDtype(),
        "PartitionDate": "datetime64[ms]",
    }
    FILE_TYPE_TO_FISCAL_TYPE: dict[str, str] = {
        "raw_actuals": "actuals",
        "raw_commit_cc": "commit",
        "raw_commit_wbs": "commit",
        "raw_cost_center_details": "cost_center_details",
        "raw_wbs_budget": "wbs_budget",
        "raw_forecast_budget": "forecast",
        "raw_forecast_live_estimate": "forecast",
        "raw_forecast_pre_budget": "forecast",
        "raw_forecast_trend": "forecast",
        "raw_net_sales": "net_sales",
    }

    def __init__(
        self,
        conn: DuckDBPyConnection,
    ) -> None:
        self.conn = conn
        self.raw_tables: dict[str, list[Path]] = {
            "raw_actuals": [],
            "raw_commit_cc": [],
            "raw_commit_wbs": [],
            "raw_cost_center_details": [],
            "raw_wbs_budget": [],
            "raw_forecast_budget": [],
            "raw_forecast_live_estimate": [],
            "raw_forecast_pre_budget": [],
            "raw_forecast_trend": [],
            "raw_net_sales": [],
        }
        self.compass_codes: DataFrame
        self.cost_centers: DataFrame
        self.node_to_compass: DataFrame
        self.fiscal_periods: DataFrame
        self.gl_accounts: DataFrame
        self.gl_accounts_to_compass: DataFrame
        self.profit_centers: DataFrame
        self.signatures: DataFrame
        self.wbs_codification: DataFrame
        self.wbs_elements: DataFrame
        self.wbs_enhanced: DataFrame
        self.profit_centers_to_signatures: DataFrame
        self.cost_centers_to_compass: DataFrame
        self.gl_to_compass: DataFrame

    PROCESSED_LOG_TABLE = "ingested_files"

    def track_processed_files(self) -> None:
        """Creates a table to track files we have already processed."""
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.PROCESSED_LOG_TABLE} (
                filename TEXT PRIMARY KEY,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def create_stage_tables(self) -> None:
        """Creates empty stage tables."""

        stage_tables = {
            "stg_actuals": DataFrame(
                columns=list(self.STAGE_ACTUALS_SCHEMA.keys())
            ).astype(self.STAGE_ACTUALS_SCHEMA),
            "stg_cost_center_details": DataFrame(
                columns=self.STAGE_COST_CENTER_DETAILS_SCHEMA.keys()  # type: ignore
            ).astype(self.STAGE_COST_CENTER_DETAILS_SCHEMA),
            "stg_forecast": DataFrame(columns=self.STAGE_FORECAST_SCHEMA.keys()).astype(  # type: ignore
                self.STAGE_FORECAST_SCHEMA
            ),
            "stg_commit": DataFrame(columns=self.STAGE_COMMIT_SCHEMA.keys()).astype(  # type: ignore
                self.STAGE_COMMIT_SCHEMA
            ),
            "stg_net_sales": DataFrame(
                columns=self.STAGE_NET_SALES_SCHEMA.keys()
            ).astype(  # type: ignores
                self.STAGE_NET_SALES_SCHEMA
            ),
        }

        for table_name, df in stage_tables.items():
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS (SELECT * FROM df WHERE 1=0)
            """)

    def update_gold_dataset(self, output_path: str) -> None:
        self.conn.execute(
            """
            CREATE OR REPLACE TABLE gold_dataset AS (
                SELECT
                    * EXCLUDE ("PartitionDate", "Debit Date", "Reference Date", "Document Date"),
                    CAST("PartitionDate" AS TIMESTAMP) AS "PartitionDate",
                    CAST("Debit Date" AS TIMESTAMP) AS "Debit Date",
                    CAST("Reference Date" AS TIMESTAMP) AS "Reference Date",
                    CAST("Document Date" AS TIMESTAMP) AS "Document Date",
                    "Fiscal Period" AS "Month",
                    "Fiscal Year" AS "Year"
                FROM
                    (
                    SELECT
                        *
                    FROM
                        stg_actuals
                    UNION ALL
                    BY NAME
                    SELECT
                        *
                    FROM
                        stg_commit
                    UNION ALL
                    BY NAME
                    SELECT
                        *
                    FROM
                        stg_cost_center_details
                    UNION ALL
                    BY NAME
                    SELECT
                        *
                    FROM
                        stg_forecast
                    UNION ALL
                    BY NAME
                    SELECT 
                        *
                    FROM 
                        stg_net_sales
                    )
                );
            """
        )
        self.conn.execute(
            """COPY gold_dataset TO ? (FORMAT parquet, OVERWRITE_OR_IGNORE TRUE, PARTITION_BY ("Year", "Month"))""",
            [str(output_path)],
        )

    def get_new_files(self, data_files: list[Path]) -> list[Path]:
        """Filters out files that are already in the database log."""
        processed = self.conn.execute(
            f"SELECT filename FROM {self.PROCESSED_LOG_TABLE}"
        ).fetchall()
        processed_set = {row[0] for row in processed}
        return [f for f in data_files if f.name not in processed_set]

    def create_partition_date(self, df: DataFrame, type: str) -> DataFrame:
        """Create a PartitionDate column for the type of data passed.

        Args:
            df (DataFrame): Raw dataframe.
            type (str, optional):
                Choose between "actuals", "commit", "forecast" or "net sales"
                to run rule-based creation of PartitionDate
                Defaults to "actual".

        Returns:
            DataFrame: DataFrame with PartitionDate column as "YYYY/MM/DD"
        """
        if type == "actual":
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
        elif type == "commit":
            df["PartitionDate"] = pd.to_datetime(
                df[["Fiscal Year", "Period"]]
                .rename(columns={"Fiscal Year": "year", "Period": "month"})
                .assign(day=1)
            )
        elif type == "forecast":
            df["Fiscal Period"] = df["PERIOD"].str.extract("(\d+)")
            df["Fiscal Period"] = pd.to_numeric(df["Fiscal Period"], errors="coerce")
            df["Fiscal Period"] = df["Fiscal Period"].fillna(0).astype(Int8Dtype())

            df.loc[df["Fiscal Period"] != 0, "PartitionDate"] = pd.to_datetime(
                df.loc[df["Fiscal Period"] != 0, ["YEAR", "Fiscal Period"]]
                .rename(columns={"YEAR": "year", "Fiscal Period": "month"})
                .assign(day=1)
            )
        elif type == "net_sales":
            month_to_num: dict[str, int] = {
                "January": 1,
                "February": 2,
                "March": 3,
                "April": 4,
                "May": 5,
                "June": 6,
                "July": 7,
                "August": 8,
                "September": 9,
                "October": 10,
                "November": 11,
                "December": 12,
            }
            df["Fiscal Period"] = df["PERIOD"].map(month_to_num)
            df["Fiscal Period"] = df["Fiscal Period"].fillna(0).astype(Int8Dtype())
            df.loc[df["Fiscal Period"] != 0, "PartitionDate"] = pd.to_datetime(
                df.loc[df["Fiscal Period"] != 0, ["YEAR", "Fiscal Period"]]
                .rename(columns={"YEAR": "year", "Fiscal Period": "month"})
                .assign(day=1)
            )
        else:
            raise ValueError(
                """Choose a fiscal type of actuals, commit, forecast or net sales 
                to create a PartitionDate column."""
            )

        return df

    def load_metadata(self) -> None:
        self.compass_codes = self.conn.execute(
            """
            SELECT
                "Financial Statement Item" AS "Compass Code",
                "Text" AS "P&L Line Text"
            FROM meta_fs_items
            """
        ).df()

        self.cost_centers = self.conn.execute(
            """
            SELECT
                "Cost Center" AS "Cost Center Code",
                "Profit Center" AS "Profit Center Code",
                "Standard Hierarchy Node",
            FROM meta_cost_centers
            """
        ).df()

        self.node_to_compass = self.conn.execute(
            """
            SELECT
                "Group cost center code" AS "Standard Hierarchy Node",
                "P&L line code" AS "Compass Code"
            FROM meta_node_to_compass
            """
        ).df()

        self.fiscal_periods = self.conn.execute(
            """
            SELECT
                "Fiscal Period",
                "Fiscal Period Text"
            FROM meta_fiscal_periods;
            """
        ).df()

        self.gl_accounts = self.conn.execute(
            """
            SELECT
                "G/L Account",
                "G/L Acct Long Text"
            FROM meta_gl_accounts
            """
        ).df()

        self.gl_accounts_to_compass = self.conn.execute(
            """
            SELECT
                "Financial Statement Item" AS "Compass Code",
                "Account To" AS "G/L Account"
            FROM meta_gl_to_compass
            """
        ).df()

        self.profit_centers = self.conn.execute(
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

        self.signatures = self.conn.execute(
            """
            SELECT
                "Signature Code",
                "Signature Description"
            FROM meta_signatures
            """
        ).df()

        self.wbs_codification = self.conn.execute(
            """
            SELECT
                "Type Char" AS "WBS Type Char",
                "Type" AS "WBS Type",
                "Type Local" AS "WBS Typ Local"
            FROM meta_wbs_codification;
            """
        ).df()

        self.wbs_elements = self.conn.execute(
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

        self.wbs_enhanced = self.enhance_wbs_elements(
            self.wbs_elements, self.wbs_codification
        )
        self.profit_centers_to_signatures = self.link_profit_center_to_signatures(
            self.profit_centers, self.signatures
        )
        self.cost_center_to_compass = self.link_cost_center_to_compass(
            self.cost_centers, self.node_to_compass
        )
        self.gl_to_compass = self.link_gl_to_compass(
            self.gl_accounts, self.gl_accounts_to_compass
        )

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
                self.raw_tables["raw_cost_center_details"].append(data_file)
            elif "commit_cc" in fname:
                self.raw_tables["raw_commit_cc"].append(data_file)
            elif "commit_wbs" in fname:
                self.raw_tables["raw_commit_wbs"].append(data_file)
            elif "wbs_budget" in fname:
                self.raw_tables["raw_wbs_budget"].append(data_file)
            elif "_le_" in fname:
                self.raw_tables["raw_forecast_live_estimate"].append(data_file)
            elif "_prebud_" in fname:
                self.raw_tables["raw_forecast_pre_budget"].append(data_file)
            elif "_bud_" in fname:
                self.raw_tables["raw_forecast_budget"].append(data_file)
            elif "_t0" in fname:
                self.raw_tables["raw_forecast_trend"].append(data_file)
            elif "net sales" in fname:
                self.raw_tables["raw_net_sales"].append(data_file)
            else:
                self.raw_tables["raw_actuals"].append(data_file)

        # 3. Process each bucket of files into their respective tables
        for table_key, file_list in self.raw_tables.items():
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
                            dtype=self.RAW_DATA_SCHEMA,
                            encoding="ISO-8859-1",
                            index_col=False,
                        )  # type: ignore
                        df["source_file"] = file_path.name

                    df_cols = df.columns.to_list()
                    date_cols = [col for col in df_cols if "date" in col.lower()]
                    for date_col in date_cols:
                        df[date_col] = pd.to_datetime(df[date_col], format="%m/%d/%Y")

                    df = self.create_partition_date(
                        df, self.FILE_TYPE_TO_FISCAL_TYPE[table_key]
                    )

                    # if "Period" in df_cols:
                    #     df["PartitionDate"] = pd.to_datetime(
                    #         df[["Fiscal Year", "Period"]]
                    #         .rename(columns={"Fiscal Year": "year", "Period": "month"})
                    #         .assign(day=1)
                    #     )
                    # elif "Fiscal Period" in df_cols:
                    #     df["PartitionDate"] = pd.to_datetime(
                    #         df[["Fiscal Year", "Fiscal Period"]]
                    #         .rename(
                    #             columns={
                    #                 "Fiscal Year": "year",
                    #                 "Fiscal Period": "month",
                    #             }
                    #         )
                    #         .assign(day=1)
                    #     )
                    # # TODO: Confirm if "Last Refresh" is required
                    # elif "Last Refresh" in df_cols:
                    #     df["Fiscal Period"] = df["PERIOD"].str.extract("(\d+)")
                    #     df["Fiscal Period"] = pd.to_numeric(
                    #         df["Fiscal Period"], errors="coerce"
                    #     )
                    #     df["Fiscal Period"] = (
                    #         df["Fiscal Period"].fillna(0).astype(Int64Dtype())
                    #     )

                    #     df.loc[df["Fiscal Period"] != 0, "PartitionDate"] = (
                    #         pd.to_datetime(
                    #             df.loc[
                    #                 df["Fiscal Period"] != 0, ["YEAR", "Fiscal Period"]
                    #             ]
                    #             .rename(
                    #                 columns={"YEAR": "year", "Fiscal Period": "month"}
                    #             )
                    #             .assign(day=1)
                    #         )
                    #     )

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
        self, range_start: str, range_end: str, meta_frames: ActualsMetadata
    ) -> DataFrame:
        actuals: DataFrame = self.conn.execute(
            """
            SELECT * FROM raw_actuals
            WHERE
                "PartitionDate" >= ?
                AND "PartitionDate" < ?
            """,
            [range_start, range_end],
        ).df()
        if actuals.empty:
            return actuals

        actuals = actuals.rename(columns=self.SAP_COLUMN_RENAME)
        actuals["Amount in Company Code Currency"] *= -1
        actuals["Scenario"] = "Actual"
        actuals_wbs: DataFrame = self.get_wbs_attributes(
            actuals, meta_frames["wbs_enhanced"]
        )

        del actuals

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

        # Fiscal Type for "non-M" follows normal logic
        gold_actuals_non_m: DataFrame = self.determine_fiscal_type(actuals_non_m)

        # Fiscal Type for "M" WBS Element Codes should not use WBS Element Codes
        actuals_m["WBS Element Code Temp"] = actuals_m["WBS Element Code"]
        actuals_m["WBS Element Code"] = pd.NA
        gold_actuals_m = self.determine_fiscal_type(actuals_m)
        gold_actuals_m["WBS Element Code"] = actuals_m["WBS Element Code Temp"].astype(
            StringDtype()
        )
        gold_actuals_m = gold_actuals_m.drop(columns=["WBS Element Code Temp"])

        return pd.concat([gold_actuals_m, gold_actuals_non_m], ignore_index=True)

    def make_gold_cc_details(
        self, range_start: str, range_end: str, meta_frames: CostCenterMetadata
    ) -> DataFrame:
        cc_details: DataFrame = self.conn.execute(
            """
            SELECT * FROM raw_cost_center_details
            WHERE
                "PartitionDate" >= ?
                AND "PartitionDate" < ?
            """,
            [range_start, range_end],
        ).df()
        if cc_details.empty:
            return cc_details

        cc_details = cc_details.rename(columns=self.SAP_COLUMN_RENAME)
        cc_details["Scenario"] = "Cost Center Details"
        cc_details["Amount in Company Code Currency"] *= -1
        cc_details_wbs: DataFrame = self.get_wbs_attributes(
            cc_details, meta_frames["wbs_enhanced"]
        )

        del cc_details

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

    def make_gold_commit_wbs(self, meta_frame: CommitWBSMetadata) -> DataFrame:
        commit_wbs: DataFrame = self.conn.execute(
            """
            SELECT
                *,
                'Committed' AS "Scenario"
            FROM raw_commit_wbs
            """
        ).df()
        if commit_wbs.empty:
            return commit_wbs
        commit_wbs = commit_wbs.rename(columns=self.SAP_COLUMN_RENAME)

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

        # Get Compass Codes using G/L Account
        commit_wbs_enhanced = commit_wbs_enhanced.merge(
            meta_frame["gl_to_compass"],
            on="G/L Account",
            how="left",
            validate="many_to_one",
        )
        commit_wbs_enhanced = commit_wbs_enhanced.merge(
            meta_frame["profit_centers_to_signatures"],
            how="left",
            on="Profit Center Code",
            validate="many_to_one",
        )

        # Get Compass Text
        return commit_wbs_enhanced.merge(
            meta_frame["compass_codes"],
            on="Compass Code",
            how="left",
            validate="many_to_one",
        )

    def make_gold_commit_cc(self, meta_frame: CommitCostCenterMetadat):
        commit_cc: DataFrame = self.conn.execute(
            """
            SELECT
                *,
                'Committed' AS "Scenario"
            FROM raw_commit_cc
            """
        ).df()
        if commit_cc.empty:
            return commit_cc

        commit_cc = commit_cc.rename(columns=self.SAP_COLUMN_RENAME)

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
        commit_cc = commit_cc.merge(
            meta_frame["profit_centers_to_signatures"],
            how="left",
            on="Profit Center Code",
            validate="many_to_one",
        )

        # Get Compass Text
        return commit_cc.merge(
            meta_frame["compass_codes"],
            how="left",
            on="Compass Code",
            validate="many_to_one",
        )

    def make_gold_forecast(
        self, range_start: str, range_end: str, meta_frames
    ) -> DataFrame:
        # ---- Load Forecast Types ----
        live_estimate = self.conn.execute(
            """
            SELECT
                *,
                'Live Estimate' AS "Scenario",
                "SPEND TYPE" AS 'Fiscal Type'
            FROM 
                raw_forecast_live_estimate
            WHERE
                "PERIOD" NOT IN ('TOTAL', 'TOTAL_B', 'TOTAL_T')
                AND "PartitionDate" >= ?
                AND "PartitionDate" < ?
            """,
            [range_start, range_end],
        ).df()

        pre_budget = self.conn.execute(
            """
            SELECT
                *,
                'Pre-Budget' AS "Scenario",
                "SPEND TYPE" AS 'Fiscal Type'
            FROM 
                raw_forecast_pre_budget
            WHERE
                "PERIOD" NOT IN ('TOTAL', 'TOTAL_B', 'TOTAL_T')
                AND "PartitionDate" >= ?
                AND "PartitionDate" < ?
            """,
            [range_start, range_end],
        ).df()

        budget = self.conn.execute(
            """
            SELECT
                *,
                'Budget' AS "Scenario",
                "SPEND TYPE" AS 'Fiscal Type'
            FROM
                raw_forecast_budget
            WHERE
                "PERIOD" NOT IN ('TOTAL', 'TOTAL_B', 'TOTAL_T')
                AND "PartitionDate" >= ?
                AND "PartitionDate" < ?
            """,
            [range_start, range_end],
        ).df()

        trend = self.conn.execute(
            """
            SELECT
            *,
            CASE
                WHEN 'T03' in "source_file" THEN 'Trend 3'
                WHEN 'T05' in "source_file" THEN 'Trend 5'
                WHEN 'T09' in "source_file" THEN 'Trend 9'
                ELSE NULL
            END AS 'Scenario',
            "SPEND TYPE" AS 'Fiscal Type'
            FROM
                raw_forecast_trend
            WHERE
                "PERIOD" NOT IN ('TOTAL', 'TOTAL_B', 'TOTAL_T')
                AND "PartitionDate" >= ?
                AND "PartitionDate" < ?
            """,
            [range_start, range_end],
        ).df()

        forecast = pd.concat(
            [live_estimate, pre_budget, budget, trend], ignore_index=True
        )
        if forecast.empty:
            return forecast

        forecast = forecast.rename(columns=self.FORECAST_COLUMN_RENAME)
        forecast["Amount in Company Code Currency"] *= -1

        # PERIODs in forecasted are formatted as "M04_T", "M10_B", etc.
        # so let's extract the numerical portion only
        forecast["Code 1 Concatenated"] = (
            forecast["Code 1"] + forecast["Code 1 Description"]
        )
        forecast["Code 2 Concatenated"] = (
            forecast["Code 2"] + forecast["Code 2 Description"]
        )

        # Code 1 and Code 2 contain Cost Center Codes and WBS Element Codes, respectively
        gold_forecast = forecast.merge(
            meta_frames["cost_center_to_compass"],
            how="left",
            left_on="Code 1",
            right_on="Cost Center Code",
            validate="many_to_one",
            suffixes=("_native", "_cc"),
        )

        del forecast

        gold_forecast = gold_forecast.merge(
            meta_frames["wbs_enhanced"],
            how="left",
            left_on="Code 2",
            right_on="WBS Element Code",
            validate="many_to_one",
            suffixes=("_native", "_wbs"),
        )

        gold_forecast["G/L Account"] = gold_forecast["G/L Account"].fillna(
            gold_forecast["WBS G/L Account"]
        )

        # Fill Profit Center Code given by CC with WBS
        gold_forecast["Profit Center Code"] = (
            gold_forecast["Profit Center Code"]
            .fillna(gold_forecast["WBS Profit Center Code"])
            .astype(StringDtype())
        )

        # Fill Compass Code found in original dataset with that obtained from CC
        gold_forecast["Compass Code"] = (
            gold_forecast["Compass Code_native"]
            .fillna(gold_forecast["Compass Code_cc"])
            .astype(StringDtype())
        )

        return gold_forecast.drop(
            columns=[
                "Compass Code_native",
                "Compass Code_cc",
                "WBS G/L Account",
            ]
        )

    def make_gold_net_sales(
        self, range_start: str, range_end: str, meta_frames
    ) -> DataFrame:
        net_sales = self.conn.execute(
            """
            SELECT
                *,
                'Net Sales' AS "Origin",
            FROM 
                raw_net_sales
            WHERE
                "PERIOD" != 'Annual trend'
                AND "PartitionDate" >= ?
                AND "PartitionDate" < ?
            """,
            [range_start, range_end],
        ).df()

        net_sales["Signature Description"] = (
            net_sales["PRODUCT_BI_CENTRAL"]
            .str.extract(r"\-\s(.*)\s\++", expand=False)
            .str.strip()
            .str.upper()
            .astype(StringDtype())
        ).astype(StringDtype())

        net_sales = net_sales.merge(
            meta_frames["signature_descriptions"],
            on="Signature Description",
            how="left",
        )
        net_sales = net_sales.rename(
            columns={
                "IR1IND15000T - CONSO NET SALES Magnitude phasing": "Amount in Company Code Currency",
                "YEAR": "Fiscal Year",
            }
        )
        return net_sales

    def migrate_to_db(
        self,
        incoming_df: DataFrame,
        target_table: str,
        range_start: str,
        range_end: str,
    ) -> None:
        self.conn.register("incoming_df_view", incoming_df)
        try:
            self.conn.execute("BEGIN TRANSACTION")
            self.conn.execute(
                f"""
                DELETE FROM {target_table}
                WHERE
                    PartitionDate >= ? AND
                    PartitionDate < ?
                """,
                [range_start, range_end],
            )
            self.conn.execute(
                f"""
                INSERT INTO {target_table} BY NAME
                    SELECT
                        *
                    FROM
                        incoming_df_view
                    WHERE
                        "Fiscal Period" != 0
                """
            )
            self.conn.unregister("incoming_df_view")
            self.conn.execute("COMMIT")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            print(f"Ingestion failed: {e}")

    def make_golden_view(self) -> None:
        self.conn.execute("""
                          
            SELECT * FROM stg_actuals
            UNION ALL BY NAME
            SELECT * FROM stg_commit
            UNION ALL BY NAME 
            SELECT * FROM stg_cost_center_details
            UNION ALL BY NAME
            SELECT * FROM stg_forecast""")

    def run_pipeline(
        self,
        fiscal_type: str,
        range_start: str,
        range_end: str,
        output_path: str,
    ) -> None:
        """Run data transformations and migration to database.

        Args:
            fiscal_type (str): Finance category
            range_start (str): Beginning (inclusive) of date range used to filter data
            range_end (str): End (exclusive) of date range used to filter data
            output_path (str): Output directory where Parquet files will be stored.

        Raises:
            ValueError: If fiscal_type is not in ["actual", "commit", "cost_center_details", "forecast"]
        """
        self.load_metadata()
        self.create_stage_tables()
        print(f"Transforming data from {range_start}-{range_end}")

        # Business Logic:
        # 1. Records posessing a WBS Element Code should be merged with the WBS Elements metdata
        # to retrieve their Profit Center and G/L Account details from there. These attributes should
        # override any existing values in the native data.
        # 2. Compass Codes are retrieved using the G/L Accounts when G/L Accounts are present
        # 3. If G/L Accounts are not present, Cost Centers can be used to retrieve Compass Codes instead
        # by looking up the Cost Center to Compass mapping table that used the Standard Hierarchy Node to bridge both tables.

        if fiscal_type.lower() == "actual":
            actuals_meta_frame: ActualsMetadata = {
                "compass_codes": self.compass_codes,
                "cost_center_to_compass": self.cost_center_to_compass,
                "gl_to_compass": self.gl_to_compass,
                "profit_centers_to_signatures": self.profit_centers_to_signatures,
                "wbs_enhanced": self.wbs_enhanced,
            }
            gold_actuals = self.make_gold_actuals(
                range_start, range_end, actuals_meta_frame
            ).astype(self.STAGE_ACTUALS_SCHEMA)
            self.migrate_to_db(gold_actuals, "stg_actuals", range_start, range_end)
        elif fiscal_type.lower() == "cost_center_details":
            # ---- Process Cost Center Details ----
            cc_meta_frame: CostCenterMetadata = {
                "compass_codes": self.compass_codes,
                "cost_center_to_compass": self.cost_center_to_compass,
                "gl_to_compass": self.gl_to_compass,
                "profit_centers_to_signatures": self.profit_centers_to_signatures,
                "wbs_enhanced": self.wbs_enhanced,
            }
            gold_cc_details = self.make_gold_cc_details(
                range_start, range_end, cc_meta_frame
            ).astype(self.STAGE_COST_CENTER_DETAILS_SCHEMA)
            self.migrate_to_db(
                gold_cc_details,
                "stg_cost_center_details",
                range_start,
                range_end,
            )
        elif fiscal_type.lower() == "forecast":
            # ---- Process Forecasted Data ----
            meta_forecast = {
                "cost_center_to_compass": self.cost_center_to_compass,
                "wbs_enhanced": self.wbs_enhanced,
            }
            gold_forecast = self.make_gold_forecast(
                range_start, range_end, meta_forecast
            ).astype(self.STAGE_FORECAST_SCHEMA)
            self.migrate_to_db(gold_forecast, "stg_forecast", range_start, range_end)
        elif fiscal_type.lower() == "commit":
            meta_frames_wbs: CommitWBSMetadata = {
                "wbs_enhanced": self.wbs_enhanced,
                "compass_codes": self.compass_codes,
                "gl_to_compass": self.gl_to_compass,
                "profit_centers_to_signatures": self.profit_centers_to_signatures,
            }

            meta_frames_cc: CommitCostCenterMetadat = {
                "gl_to_compass": self.gl_to_compass,
                "cost_center_to_compass": self.cost_center_to_compass,
                "compass_codes": self.compass_codes,
                "profit_centers_to_signatures": self.profit_centers_to_signatures,
            }
            gold_commit = pd.concat(
                [
                    self.make_gold_commit_wbs(meta_frames_wbs),
                    self.make_gold_commit_cc(meta_frames_cc),
                ],
                ignore_index=True,
            ).astype(self.STAGE_COMMIT_SCHEMA)
            self.migrate_to_db(gold_commit, "stg_commit", range_start, range_end)
        elif fiscal_type.lower() == "net_sales":
            meta_frame_net_sales = {"signature_descriptions": self.signatures}
            gold_net_sales: DataFrame = self.make_gold_net_sales(
                range_start, range_end, meta_frame_net_sales
            )
            self.migrate_to_db(gold_net_sales, "stg_net_sales", range_start, range_end)
        else:
            raise ValueError(f"Invalid fiscal type: {fiscal_type}")

        self.update_gold_dataset(output_path)
