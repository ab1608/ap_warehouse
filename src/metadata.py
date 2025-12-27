from collections import defaultdict
from pathlib import Path

import pandas as pd
from duckdb import DuckDBPyConnection
from pandas import DataFrame, Float64Dtype, Int64Dtype, StringDtype


class FinanceMetadata:
    META_DTYPES = defaultdict(
        StringDtype,
        {
            "Fiscal Year": Int64Dtype(),
            "Fiscal Period": Int64Dtype(),
            "Amount in Company Code Currency": Float64Dtype(),
            "Distribution Channel": Int64Dtype(),
            "G/L Account": Int64Dtype(),
            "Level": Int64Dtype(),
            "FS Version": StringDtype(),
            "ID": Int64Dtype(),
            "Parent ID": Int64Dtype(),
        },
    )

    def __init__(self, metadata_dir: Path):
        self.metadata_dir = metadata_dir

    def __str__(self):
        return f"Meta(metadata_dir={self.metadata_dir})"

    def load_fs_items(self):
        """Load FAGL_011QT.csv."""
        fs_items_path: Path = self.metadata_dir / "FAGL_011QT.csv"

        fs_items = pd.read_csv(
            fs_items_path,
            dtype=StringDtype(),
            index_col=False,
        )

        return fs_items

    def load_gl_accounts(self) -> DataFrame:
        """Load SKAT.csv.

        Returns:
            DataFrame: DataFrame containing the G/L Accounts
        """
        gl_accounts_path: Path = self.metadata_dir / "SKAT.csv"
        gl_accounts: DataFrame = pd.read_csv(
            gl_accounts_path,
            dtype=defaultdict(StringDtype, {"G/L Account": Int64Dtype()}),
            index_col=False,
        )

        return gl_accounts

    def load_fs_parent_levels(self):
        """Load FAGL_011PC.csv:"""

        fs_levels = pd.read_csv(
            self.metadata_dir / "FAGL_011PC.csv",
            dtype=defaultdict(
                StringDtype,
                {
                    "ID": Int64Dtype(),
                    "ID2": Int64Dtype(),
                },
            ),
            index_col=False,
        )

        return fs_levels

    def load_gl_to_compass(self) -> DataFrame:
        """Load FAGL_011ZC.csv.

        Returns:
            DataFrame: The financial statement item linked to to G/L accounts.
        """
        fs_to_gl_path: Path = self.metadata_dir / "FAGL_011ZC.csv"
        fs_gl_link = pd.read_csv(
            fs_to_gl_path,
            dtype=defaultdict(
                StringDtype,
                {
                    "Account To": Int64Dtype(),
                },
            ),
            index_col=False,
        )

        return fs_gl_link

    def load_fs_hiearchy(self) -> DataFrame:
        """
        Build the financial statement hierarchy from the joining of
        Financial Statement Items (FAGL_011QT) and Financial Statement Parent Levels (FAGL_011PC).
        """
        # Load FS items (FAGL_011QT)
        # Note: Include PL24 as an FS Item in the file
        fs_items = self.load_fs_items()
        fs_items = fs_items.loc[:, ["Financial Statement Item", "Text"]]

        # Load FS parent levels (FAGL_011PC)
        fs_levels = self.load_fs_parent_levels()
        fs_levels = fs_levels.loc[:, ["ID", "Financial Statement Item", "ID2"]]
        fs_levels = fs_levels.sort_values("ID")

        fs_item_levels: DataFrame = pd.merge(
            fs_levels,
            fs_items,
            on="Financial Statement Item",
            how="inner",
            validate="one_to_one",
        )
        fs_item_levels["Text"] = fs_item_levels["Text"].fillna(
            fs_item_levels["Financial Statement Item"]
        )

        # Create a map of Financial Statement ID -> Text
        fs_id_text: dict[int, str] = fs_item_levels.set_index("ID")["Text"].to_dict()

        # Create a map of Financial Statement ID -> ID2 (Parent ID)
        fs_child_parent_pairs: dict[int, int] = fs_item_levels.set_index("ID")[
            "ID2"
        ].to_dict()

        def get_path(category_id):
            path = []
            current_id = category_id
            # Keep track of the number of paths taken
            paths_taken: int = 0

            while current_id:
                # Get the FS Text given an ID and
                # append it to all the FS Items associcated with current_id
                path.append(fs_id_text[current_id])
                # Get the parent of the current FS Item
                parent_id = fs_child_parent_pairs[current_id]
                # Set new ID2 as the current_id to continue
                # iterating only if a parent_id exists
                current_id = parent_id if parent_id else None
                paths_taken += 1 if parent_id else 0

            # Reverse the paths and join with " > " as the delimiter
            return " > ".join(reversed(path)), paths_taken

        # Apply the function to create hierarchy
        fs_item_levels["Hierarchy Stats"] = fs_item_levels["ID"].apply(get_path)
        fs_item_levels[["Hierarchy", "Level"]] = pd.DataFrame(
            fs_item_levels["Hierarchy Stats"].tolist(), index=fs_item_levels.index
        )
        fs_item_levels = fs_item_levels.drop(columns=["Hierarchy Stats"])

        # Make new set of columns that split each hierarchy path
        hierarchy_cols = fs_item_levels["Hierarchy"].str.split(" > ", expand=True)

        # Rename hierarchy columns and merge back to widen frame with new columns
        hierarchy_cols = hierarchy_cols.rename(columns=lambda x: f"Level {str(x)} Text")
        fs_hierarchy = pd.concat([fs_item_levels, hierarchy_cols], axis="columns")

        return fs_hierarchy

    def load_chart_of_accounts(self) -> DataFrame:
        """Load the chart of accounts by merging financial statement hierarchy,
        financial statement to G/L account links, and G/L account details.
        """
        fs_parent_children = self.load_fs_hiearchy()
        fs_to_gl = self.load_gl_to_compass()
        fs_to_gl = fs_to_gl.rename(columns={"Account To": "G/L Account"})
        gl_accounts = self.load_gl_accounts()

        # Merge GL with FS-to-GL to get additional G/L Account information
        # such as Short Text and Long Text
        fs_and_gl = pd.merge(
            fs_to_gl,
            gl_accounts,
            on="G/L Account",
            how="inner",
            validate="one_to_one",
        )

        # Merge FS-GL with FS Parent Children
        tcoa: DataFrame = pd.merge(
            fs_parent_children,
            fs_and_gl,
            on="Financial Statement Item",
            how="left",
            validate="one_to_many",
        )

        # Set column order
        level_cols: list[str] = fs_parent_children.loc[
            :, fs_parent_children.columns.str.contains("Level")
        ].columns.to_list()

        col_order = [
            "ID",
            "Financial Statement Item",
            "Text",
            "G/L Account",
            "Short Text",
            "G/L Acct Long Text",
            "Hierarchy",
        ] + level_cols

        tcoa = tcoa.loc[:, col_order]

        return tcoa

    def load_wbs_elements(self) -> DataFrame:
        """Load the following columns from the WBS Elements metadata file, i.e. WBS_ELEMENTS.xlsx:
        - WBS Element
        - WBS Element Name
        - Level
        - P&L_Destination
        - Ufield 1 WBS Element
        - Profit Center

        Returns:
            DataFrame: The WBS elements.
        """
        wbs_elements_path: Path = self.metadata_dir / "wbs_elements.csv"

        wbs_elements: DataFrame = pd.read_csv(
            wbs_elements_path,
            dtype=defaultdict(
                StringDtype, {"Level": Int64Dtype(), "P&L_Destination": Int64Dtype()}
            ),
            index_col=False,
        )

        return wbs_elements

    def load_wbs_codification(self) -> DataFrame:
        wbs_code_path: Path = self.metadata_dir / "wbs_codification.csv"
        wbs_codification: DataFrame = pd.read_csv(
            wbs_code_path,
            dtype=StringDtype(),
            index_col=False,
        )

        return wbs_codification

    def load_custom_wbs_elements(self) -> DataFrame:
        """Load a custom version of WBS elements with additional columns for parent codes and buckets."""
        wbs_elements: DataFrame = self.load_wbs_elements()
        wbs_custom = wbs_elements.copy()
        wbs_custom = wbs_custom.rename(
            columns={
                "WBS Element": "WBS Element Code",
                "P&L_Destination": "WBS G/L Account",
                "Level": "WBS Level",
                "Profit Center": "WBS Profit Center Code",
            }
        )
        wbs_codification = self.load_wbs_codification()
        wbs_codification = wbs_codification.rename(
            columns={
                "Type Char": "WBS Type Char",
                "Type": "WBS Type",
                "Type Local": "WBS Type Local",
            }
        )

        # Create WBS Parents
        level_one_mask = wbs_custom["WBS Level"] == 1
        wbs_custom.loc[level_one_mask, "WBS Parent Code"] = wbs_custom.loc[
            level_one_mask, "WBS Element Code"
        ]

        wbs_custom["WBS Parent Code"] = wbs_custom["WBS Parent Code"].ffill()

        wbs_custom.loc[level_one_mask, "WBS Parent Name"] = wbs_custom.loc[
            level_one_mask, "WBS Element Name"
        ]
        wbs_custom["WBS Parent Name"] = wbs_custom["WBS Parent Name"].ffill()

        # Get first character from each WBS Element to create Type Char
        wbs_custom["WBS Type Char"] = wbs_custom["WBS Element Code"].str[0:1]

        # Create WBS Bucket using codifications
        wbs_custom = pd.merge(
            wbs_custom,
            wbs_codification,
            how="left",
            on="WBS Type Char",
            validate="many_to_one",
        )

        return wbs_custom

    def load_profit_centers(self) -> DataFrame:
        """Load profit_centers.csv.:

        Returns:
            DataFrame: DataFrame containing the profit centers.
        """
        profit_centers_path: Path = self.metadata_dir / "profit_centers.csv"

        profit_centers: DataFrame = pd.read_csv(
            profit_centers_path,
            dtype=StringDtype(),
            index_col=False,
        )

        return profit_centers

    def load_signatures(self) -> DataFrame:
        """Load signature_codes.csv.:

        Returns:
            DataFrame: DataFrame containing the signature codes.
        """
        signatures_path: Path = self.metadata_dir / "signature_codes.csv"
        signatures: DataFrame = pd.read_csv(
            signatures_path,
            usecols=["Signature Code", "Signature Description"],
            dtype=StringDtype(),
            index_col=False,
        )

        return signatures

    def load_cost_centers(self) -> DataFrame:
        """Load cost_centers.csv."""

        cost_centers_path: Path = self.metadata_dir / "cost_centers.csv"
        cost_centers: DataFrame = pd.read_csv(
            cost_centers_path,
            dtype=StringDtype(),
            index_col=False,
        )
        return cost_centers

    def load_standard_node_to_compass(self) -> DataFrame:
        """Load REFSAP06.csv:"""

        cost_center_to_compass_path: Path = self.metadata_dir / "REFSAP06.csv"
        cost_center_to_compass: DataFrame = pd.read_csv(
            cost_center_to_compass_path,
            dtype=StringDtype(),
            index_col=False,
        )
        return cost_center_to_compass

        cost_center_to_compass: DataFrame = pd.merge(
            cost_centers,
            node_to_compass,
            on="Standard Hierarchy Node",
            how="inner",
            validate="many_to_one",
        )

        cost_center_to_compass = cost_center_to_compass.drop(
            columns=["Standard Hierarchy Node"]
        )

        # Add any custom processing here if needed
        return cost_center_to_compass

    def load_fiscal_periods(self) -> DataFrame:
        """Load fiscal_periods.csv."""

        fiscal_periods_path: Path = self.metadata_dir / "fiscal_periods.csv"
        fiscal_periods: DataFrame = pd.read_csv(
            fiscal_periods_path,
            dtype=defaultdict(StringDtype, {"Fiscal Period": Int64Dtype()}),
            index_col=False,
        )
        return fiscal_periods

    def load_fiscal_scenarios(self) -> DataFrame:
        """Load fiscal_scenarios.csv."""

        fiscal_scenarios_path: Path = self.metadata_dir / "fiscal_scenarios.csv"
        fiscal_scenarios: DataFrame = pd.read_csv(
            fiscal_scenarios_path,
            dtype=defaultdict(StringDtype, {"Fiscal Order": Int64Dtype()}),
            index_col=False,
        )
        return fiscal_scenarios

    def load_company_divisions(self) -> DataFrame:
        """Load company_divisions.csv"""

        return pd.read_csv(
            self.metadata_dir / "company_divisions.csv",
            dtype=StringDtype(),
            index_col=False,
        )

    def move_data_to_db(self, db_conn: DuckDBPyConnection) -> None:
        """
        Move data to database and save as Parquet files.

        Args:
            db_conn (DuckDBPyConnection): Connection to DuckDB instance.

        Returns:
            None
        """
        meta_data: dict[str, DataFrame] = {
            "meta_company_divisions": self.load_company_divisions(),
            "meta_fs_items": self.load_fs_items(),
            "meta_fs_parent_children": self.load_fs_parent_levels(),
            "meta_fs_hierachy": self.load_fs_hiearchy(),
            "meta_gl_accounts": self.load_gl_accounts(),
            "meta_gl_to_compass": self.load_gl_to_compass(),
            "meta_wbs_elements": self.load_wbs_elements(),
            "meta_wbs_codification": self.load_wbs_codification(),
            "meta_profit_centers": self.load_profit_centers(),
            "meta_signatures": self.load_signatures(),
            "meta_cost_centers": self.load_cost_centers(),
            "meta_node_to_compass": self.load_standard_node_to_compass(),
            "meta_fiscal_periods": self.load_fiscal_periods(),
            "meta_fiscal_scenarios": self.load_fiscal_scenarios(),
        }

        # Write metadata tables to DuckDB
        for table_name, df in meta_data.items():
            db_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            db_conn.register(table_name, df)
            db_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")
            db_conn.unregister(table_name)
            print(f"Updated metadata table: {table_name} with {len(df)} records")
            parquet_file: Path = self.metadata_dir / f"{table_name}.parquet"
            df.to_parquet(parquet_file, engine="pyarrow")
