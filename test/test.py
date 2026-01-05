import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)
database_path = os.getenv("DATABASE_PATH")
output_path = os.getenv("OUTPUT_PATH")

from src.pipe import FinancePipeline
from src.utils import list_files_by_extension

db_conn = duckdb.connect(str(database_path))
pipeline = FinancePipeline(db_conn)
source_path = Path(".")
data_files = list_files_by_extension(source_path, extension="csv")
pipeline.run_import(data_files=data_files)
pipeline.run_transformation(
    str(output_path), range_start="2025/01/01", range_end="2025/02/01"
)

db_conn.close()
