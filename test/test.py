# %%
import os
import sys
from pathlib import Path

import pandas as pd
import polars as pl
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)
database_path = os.getenv("DATABASE_PATH")
output_path = os.getenv("OUTPUT_PATH")

from src.utils import list_files_by_extension

# %%
# db_conn = duckdb.connect(str(database_path))
# pipeline = FinancePipeline(db_conn)
source_path = Path(
    "C:/Users/abraham.briones/OneDrive - L'Oréal/01 Finance AP/AP Tracker/Inputs/DEV_002A"
)
data_files = list_files_by_extension(source_path, extension="csv")
raw_tables: dict[str, list[Path]] = {
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
# 2. Categorize each file into its respective table
for data_file in data_files:
    fname = data_file.name.lower()
    if "ccdet" in fname:
        raw_tables["raw_cost_center_details"].append(data_file)
    elif "commit_cc" in fname:
        raw_tables["raw_commit_cc"].append(data_file)
    elif "commit_wbs" in fname:
        raw_tables["raw_commit_wbs"].append(data_file)
    elif "wbs_budget" in fname:
        raw_tables["raw_wbs_budget"].append(data_file)
    elif "_le_" in fname:
        raw_tables["raw_forecast_live_estimate"].append(data_file)
    elif "_prebud_" in fname:
        raw_tables["raw_forecast_pre_budget"].append(data_file)
    elif "_bud_" in fname:
        raw_tables["raw_forecast_budget"].append(data_file)
    elif "_t0" in fname:
        raw_tables["raw_forecast_trend"].append(data_file)
    elif "net sales" in fname:
        raw_tables["raw_net_sales"].append(data_file)
    else:
        raw_tables["raw_actuals"].append(data_file)

# pipeline.run_import(data_files=data_files)
# pipeline.run_pipeline(
#     fiscal_type="net_sales",
#     range_start="2025/01/01",
#     range_end="2025/02/01",
#     output_path=str(output_path),
# )

# db_conn.close()

# %%
# Read actuals data into Polars DataFrames
# using encoding ISO-8859-1 to handle special characters in the data

raw_actuals: list[pd.DataFrame] = [
    df.to_pandas()
    for df in [
        pl.read_csv(
            str(f),
            encoding="iso-8859-1",
        )
        for f in raw_tables["raw_actuals"]
    ]
]
# %%
raw_cc_det: list[pd.DataFrame] = [
    df.to_pandas()
    for df in [
        pl.read_csv(
            str(f),
            encoding="iso-8859-1",
        )
        for f in raw_tables["raw_cost_center_details"]
    ]
]

# %%
# Save raw actuals data into two separate CSV files
output_path = Path(
    "C:/Users/abraham.briones/OneDrive - L'Oréal/01 Finance AP/AP Tracker/Inputs/DEV_002A_Trunc"
)
# %%
if raw_actuals:
    combined_actuals = pd.concat(raw_actuals)
    mid_point = len(combined_actuals) // 2
    file_1 = output_path / "002A_2025_COMPLETE_ACTUALS_1_2.csv"
    file_2 = output_path / "002A_2025_COMPLETE_ACTUALS_2_2.csv"
    combined_actuals.head(mid_point).to_csv(file_1, index=False)
    combined_actuals.tail(len(combined_actuals) - mid_point).to_csv(file_2, index=False)

# %%
# Save raw cost center details data into a two separate CSV files

combined_cc_det = pd.concat(raw_cc_det)
mid_point = len(combined_cc_det) // 2
file_1cc = output_path / "002A_2025_COMPLETE_CCDET_1_2.csv"
file_2cc = output_path / "002A_2025_COMPLETE_CCDET_2_2.csv"
combined_cc_det.head(mid_point).to_csv(file_1cc, index=False)
combined_cc_det.tail(len(combined_cc_det) - mid_point).to_csv(file_2cc, index=False)

# %%
