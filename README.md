# Finance ETL Pipeline

Finance ETL pipeline for loading raw finance files, enriching them with metadata, and publishing curated outputs using DuckDB + Parquet.

## Overview

This project supports three main workflows through a CLI:

- **`import`**: Ingest raw source files (CSV/Parquet) into DuckDB.
- **`metadata`**: Load metadata reference tables into DuckDB.
- **`transform`**: Build transformed finance outputs by fiscal type and optional date range.

Core modules:

- `main.py` — CLI entrypoint and command routing.
- `src/pipe.py` — transformation pipeline logic (`FinancePipeline`).
- `src/metadata.py` — metadata loaders and DB load routines (`FinanceMetadata`).
- `src/utils.py` — helper utilities (file listing, dtype conversion).

---

## ETL Process

### 1) Extract
- Reads source files from one or more directories.
- Supports `csv` and `parquet`.
- Discovers files recursively by extension.

### 2) Transform
- Normalizes and renames source columns.
- Applies schema typing for staging models.
- Enriches records with metadata (WBS, GL mapping, cost/profit centers, signatures, etc.).
- Supports fiscal scopes:
  - `actual`
  - `forecast`
  - `commit`
  - `cost_center_details`
  - `net_sales`
- Applies optional date range filtering (`--range-start`, `--range-end`).

### 3) Load
- Writes staged/transformed results into DuckDB-managed outputs.
- Produces partition-ready data (year/month fields used downstream).
- Exports transformed datasets to the configured output path.

---

## Configuration

Create a `.env` file in the project root (or set env vars in your shell):

- `PROJECT_PATH`
- `DATABASE_PATH`
- `METADATA_PATH`
- `OUTPUT_PATH`

These values are used as defaults by the CLI arguments.

---

## Usage

From the project root:

### Load metadata
```bash
python main.py metadata --database-path "<path\to\warehouse.duckdb>" --metadata-path "<path\to\metadata_dir>"
```

### Import raw files
```bash
python main.py import --database-path "<path\to\warehouse.duckdb>" --source-path "data\actuals" --source-path "data\commit" --input-format csv
```

### Run transformations
```bash
python main.py transform --database-path "<path\to\warehouse.duckdb>" --fiscal-type actual --output-path "<path\to\output>" --range-start 2025/01/01 --range-end 2025/12/31
```

---

## Notes

- Paths are Windows-friendly and accept relative paths when `PROJECT_PATH` is configured.
- Metadata loading expects a metadata directory with required source files (CSV inputs referenced in `src/metadata.py`).
- The pipeline exits with status code `0` on successful command completion.

---

## Quick Project Structure

```text
ap_warehouse/
├─ main.py
├─ src/
│  ├─ pipe.py
│  ├─ metadata.py
│  └─ utils.py
└─ README.md
```
