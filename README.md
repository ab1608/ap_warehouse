# Extraction and Load

This simple CLI is designed to move CSV or Parquet data from your local directories to a dedicated DuckDB instance.

# How to use

1. (Optional) Create a ".env" file in your project directory that points to a DATABASE_PATH and PROJECT_PATH. Example:

```
DATABASE_PATH="your/data/base/path"
PROJECT_PATH="your/project/path"
```

2. Pass one or more source directories using the `--source-path` argument.
3. (Optional) Give your tables a prefix by passing the `--table-prefix` argument. This can be useful if you want to distinguish your tables with some logic or if your tables start with a non-letter character such as a a number. _Default value is an underscore, "\_"_.

# Example

Here is an example in which the directory has a ".env". with DATABASE_PATH and PROJECT_PATH present.

```
python main.py transport --source-path "my/source/path"
```
