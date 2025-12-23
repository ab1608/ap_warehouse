import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from pipe import list_files_by_extension, run_import


def test_run_import(db_conn: duckdb.DuckDBPyConnection, source_path: Path) -> None:
    # Setup DuckDB connection
    source_path = Path(str(project_path)) / source_path
    source_files = list_files_by_extension(source_path, "csv")

    # Run the import function
    run_import(db_conn, source_files)

    # Clean up: close the connection
    db_conn.close()


load_dotenv()
database_path = os.getenv("DATABASE_PATH")
project_path = os.getenv("PROJECT_PATH")
db_conn = duckdb.connect(str(database_path))
test_run_import(db_conn, Path(str(project_path)) / "Inputs" / "TEST")
