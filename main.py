import argparse
import os
import sys
import time
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from src.metadata import FinanceMetadata
from src.pipe import FinancePipeline
from src.utils import list_files_by_extension


def main(argv=None) -> None:
    # env_path = Path(__file__) / ".env"
    load_dotenv()

    project_default = os.getenv("PROJECT_PATH", None)
    database_default = os.getenv("DATABASE_PATH", None)
    metadata_default = os.getenv("METADATA_PATH", None)
    output_default = os.getenv("OUTPUT_PATH", None)

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "--project-path",
        type=Path,
        nargs="?" if project_default else None,
        default=project_default,
        help="Path to the project directory. If not provided, uses PROJECT_PATH from .env file.",
    )

    parent_parser.add_argument(
        "--database-path",
        type=Path,
        nargs="?" if database_default else None,
        default=database_default,
        help="Path to the DuckDB database file. If not provided, uses DATABASE_PATH from .env file.",
    )

    parser = argparse.ArgumentParser(prog="Finance CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    import_parser = subparsers.add_parser(
        "import", parents=[parent_parser], help="Import data from source folders"
    )
    import_parser.add_argument(
        "--source-path",
        type=str,
        action="append",
        help="Source directories containing CSV or Parquet files.",
    )

    import_parser.add_argument(
        "--input-format",
        type=str,
        default="csv",
        required=False,
        help="Input format of the files (csv or parquet). Default is 'csv'.",
    )

    metadata_parser = subparsers.add_parser(
        "metadata",
        parents=[parent_parser],
        help="Replace existing metadata in database.",
    )
    metadata_parser.add_argument(
        "--metadata-path",
        type=Path,
        nargs="?" if metadata_default else None,
        default=metadata_default,
        help="Path to the metadata file describing the data schema. If not provided, uses METADATA_PATH from .env file.",
    )

    transform_parser = subparsers.add_parser(
        "transform", parents=[parent_parser], help="Transform data in warehouse"
    )
    transform_parser.add_argument(
        "--output-path",
        type=Path,
        nargs="?" if output_default else None,
        default=output_default,
        help="Path in which transformed data will be stored. If not provided, uses OUTPUT_PATH from .env file.",
    )

    args = parser.parse_args(argv)

    if args.command == "metadata":
        database_path = Path(str(args.database_path))
        conn = duckdb.connect(database=database_path)
        print(f"Connected to database at {args.database_path}")
        metadata_path = Path(str(args.metadata_path))

        if metadata_path.exists() and metadata_path.is_dir():
            metadata = FinanceMetadata(metadata_path)
            print(f"Reading metadata from {metadata_path}")
            metadata.move_data_to_db(conn)
            print("Metadata import complete. Exiting program")
        else:
            print(
                f"Warning: Metadata path {metadata_path} does not exist or is not a directory. Exiting program."
            )

        conn.close()
        sys.exit(0)

    elif args.command == "import":
        database_path = Path(str(args.database_path))
        conn = duckdb.connect(database=database_path)
        print(f"Connected to database at {database_path}")
        pipeline = FinancePipeline(conn)

        data_files: list[Path] = []
        for path in args.source_path:
            dir_path = (
                Path(path) if not args.project_path else Path(args.project_path) / path
            )
            if dir_path.exists() and dir_path.is_dir():
                dir_files: list[Path] = list_files_by_extension(
                    dir_path, args.input_format
                )
                data_files.extend(dir_files)
            else:
                print(
                    f"Warning: Source path {dir_path} does not exist or is not a dir."
                )

        if data_files:
            print(f"Found {len(data_files)} data files to in directory.")
            pipeline.run_import(data_files)
            print("Data import complete. Closing database and exiting program.")
        else:
            print("No data files found to process. Exiting program.")

        conn.close()
        sys.exit(0)

    elif args.command == "transform":
        database_path = Path(str(args.database_path))
        conn = duckdb.connect(database=str(database_path))
        print(f"Connected to database at {database_path}")
        pipeline = FinancePipeline(conn)
        tic = time.perf_counter()
        print("Starting transformations...")
        pipeline.run_transformation(Path(str(args.output_path)))
        toc = time.perf_counter()
        print(
            f"Transformation took {toc - tic:0.2f} seconds. Closing database and exiting program."
        )
        conn.close()
        sys.exit(0)

    else:
        print("Invalid command. Exiting program.")
        sys.exit(1)


if __name__ == "__main__":
    main()
