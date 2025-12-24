import argparse
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from src.pipe import run_import
from src.utils import list_files_by_extension


def main(argv=None) -> None:
    # env_path = Path(__file__) / ".env"
    load_dotenv()

    project_default = os.getenv("PROJECT_PATH", None)
    database_default = os.getenv("DATABASE_PATH", None)

    parser = argparse.ArgumentParser(
        description="Move data from source folders to warehouse"
    )

    parser.add_argument(
        "--project-path",
        type=Path,
        nargs="?" if project_default else None,
        default=project_default,
        help="Path to the project directory. If not provided, uses PROJECT_PATH from .env file.",
    )

    parser.add_argument(
        "--database-path",
        type=Path,
        nargs="?" if database_default else None,
        default=database_default,
        help="Path to the DuckDB database file. If not provided, uses DATABASE_PATH from .env file.",
    )

    parser.add_argument(
        "--source-path",
        type=str,
        action="append",
        help="Source directories containing CSV or Parquet files.",
        required=True,
    )

    parser.add_argument(
        "--input-format",
        type=str,
        default="csv",
        required=False,
        help="Input format of the files (csv or parquet). Default is 'csv'.",
    )
    args = parser.parse_args(argv)

    database_path = Path(args.database_path)
    conn = duckdb.connect(database=str(database_path))
    print(f"Connected to database at {database_path}")

    data_files: list[Path] = []

    for path in args.source_path:
        dir_path = (
            Path(path) if not args.project_path else Path(args.project_path) / path
        )
        if dir_path.exists() and dir_path.is_dir():
            dir_files: list[Path] = list_files_by_extension(dir_path, args.input_format)
            data_files.extend(dir_files)
        else:
            print(f"Warning: Source path {dir_path} does not exist or is not a dir.")

    if data_files:
        print(f"Found {len(data_files)} data files to in directory.")
        run_import(conn, data_files)
    else:
        print("No data files found to process. Exiting program.")
        conn.close()
        return


if __name__ == "__main__":
    main()
