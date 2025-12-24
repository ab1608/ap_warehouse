import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from src.pipe import run_import
from src.utils import list_files_by_extension


def test_run_import(
    db_conn: duckdb.DuckDBPyConnection, source_path: Path, data_format: str
) -> None:
    # Setup DuckDB connection
    source_files = list_files_by_extension(source_path, data_format)

    # Run the import function
    run_import(db_conn, source_files)

    # Clean up: close the connection
    db_conn.close()


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)
database_path = os.getenv("DATABASE_PATH")
project_path = os.getenv("PROJECT_PATH")

db_conn = duckdb.connect(str(database_path))
test_run_import(db_conn, Path(str(project_path)) / "Inputs" / "TEST", "parquet")
