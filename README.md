# hscida

`hscida` is a small Python/R data access package for local or remote data files. It provides lazy dataframe and SQL access to datasets such as Parquet or CSV, including files stored remotely in object storage.

By default, the package reads configuration details such as dataset locations and access credentials from `.env`/`.env.secret` files. Then, users can access the configured data using the same conventions from Python or R:

- DuckDB discovers dataset files from a configurable `GLOB_PATTERN`.
- Remote HTTPS/S3 access is configured with DuckDB `INIT_SQL`.
- Datasets are registered lazily as DuckDB views only when first requested.
- Python callers can work with lazy DuckDB relations, SQLFrame (PySpark) dataframes, and
  Narwhals lazy frames, pulling these into Polars dataframes, Pandas dataframes, or rendering the lazy queries as SQL strings.
- R callers get a DBI/DuckDB connection and lazy `dplyr` tables.

## Installation

From PyPI: https://pypi.org/project/hscida/
From R-Universe: https://hsci-r.r-universe.dev/hscida

## Configuration

Data access is configured through environment variables. In normal downstream
projects these come from `.env` and `.env.secret` files in the project root.

`.env` is usually checked in and contains non-sensitive defaults, such as public
S3/HTTPS paths, DuckDB config, and DuckDB initialization SQL.

`.env.secret` is not checked in. It typically contains credentials for remote
data access, such as object storage keys or service tokens. Place it in the
project root next to `.env`:

```text
my-analysis-project/
|-- .env
|-- .env.secret
`-- ...
```

Ask the downstream project maintainer for the expected `.env.secret` contents.

### Environment Variables

`GLOB_PATTERN` is a DuckDB table expression used to discover files for a dataset.
It may use `{projroot}` and `{dataset}` placeholders. For example:

```dotenv
GLOB_PATTERN=glob('{projroot}/data/{dataset}/*.parquet')
```

`LIST_DATASETS_QUERY` is an optional DuckDB query used to list the datasets
available at the configured destination, using DuckDB's `glob()` capability. It
may use the `{projroot}` placeholder. The query must return the dataset names
as its first (and only expected) column. If it is unset, dataset listing is
disabled. For example:

```dotenv
LIST_DATASETS_QUERY=SELECT DISTINCT regexp_extract(file, '{projroot}/([^/]+)', 1) AS dataset FROM glob('{projroot}/*')
```

`INIT_SQL`, plus any other variables whose names start with `INIT_SQL`, are
concatenated in sorted key order and run when the DuckDB connection starts. This
is useful for loading extensions, creating secrets, setting S3 endpoints, or
installing project-specific macros. The ordering is lexical, so use zero-padded
suffixes such as `INIT_SQL_010` and `INIT_SQL_020` if fragments may reach two
digits.

```dotenv
INIT_SQL=INSTALL httpfs; LOAD httpfs;
INIT_SQL_010=CREATE SECRET s3_secret (...);
INIT_SQL_020=SET s3_region='auto';
```

`DUCKDB_CONFIG` is a comma-separated list of DuckDB configuration values:

```dotenv
DUCKDB_CONFIG=parquet_metadata_cache=true,preserve_insertion_order=false,enable_fsst_vectors=true
```

`PROJROOT` overrides the project root used for `{projroot}` substitution. If it
is unset, `hscida` uses the current project root detected by the language-specific
helper (`here` in R, `hereutil` in Python).

## Remote and Local Data

By default, downstream projects commonly read data directly from remote object
storage. Public read-only data can often be accessed through HTTPS without
credentials. S3-compatible access, private buckets, or write access usually
requires credentials in `.env.secret` and matching DuckDB startup SQL.

Project may also support a local mirror to avoid repeated remote fetches. For
example, a project may use a `GLOB_PATTERN` that prefers `data/filter/` if files
exist locally, while falling back to remote paths otherwise. A typical mirror
command might look like:

```sh
rclone sync s3://project-data data/local
```

For very large datasets, it can be useful to limit the local mirror:

```sh
rclone sync s3://project-data data/local --max-size 500M
```

The local data directory should usually be gitignored. Re-running the same
`rclone sync` command keeps the mirror up to date.

## Python Usage

```python
from hscida import DataAccess

with DataAccess() as da:
    rel = da.duckdb_dataframe("my_dataset")
    preview = rel.limit(10).pl()
```

If `LIST_DATASETS_QUERY` is configured, you can discover which datasets are
available before loading one:

```python
with DataAccess() as da:
    available = da.list_datasets()  # or da.ld()
```

`DataAccess` can expose the same dataset through several lazy dataframe APIs:

```python
with DataAccess() as da:
    duckdb_rel = da.duckdb_dataframe("my_dataset")
    sqlframe_df = da.spark_dataframe("my_dataset")
    narwhals_rel = da.narwhals_duckdb_dataframe("my_dataset")
    narwhals_sqlframe = da.narwhals_spark_dataframe("my_dataset")
```

You can also start from SQL and convert between supported representations:

```python
with DataAccess() as da:
    query = da.duckdb_dataframe_from_sql("SELECT * FROM my_dataset LIMIT 10")
    polars_df = da.to_polars(query)
    sql = da.to_sql(query, pretty=True)
```

Short aliases are available for interactive work:

- `da.ddf()` / `da.duckdb_dataframe()`
- `da.sdf()` / `da.spark_dataframe()`
- `da.nddf()` / `da.narwhals_duckdb_dataframe()`
- `da.nsdf()` / `da.narwhals_spark_dataframe()`
- `da.d()` / `da.to_duckdb()`
- `da.s()` / `da.to_spark()`
- `da.n()` / `da.to_narwhals()`
- `da.p()` / `da.to_polars()`
- `da.q()` / `da.to_sql()`
- `da.ld()` / `da.list_datasets()`

## R Usage

```r
library(hscida)

da <- data_access()
on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

tbl <- da$f("my_dataset")

tbl |>
  dplyr::filter(year >= 2020) |>
  dplyr::collect()
```

`da$f("my_dataset")` lazily registers the dataset as a DuckDB view, then returns
a `dplyr` table backed by DuckDB. You can pass explicit paths to bypass
`GLOB_PATTERN`:

```r
tbl <- da$f("my_dataset", "data/my_dataset/part-000.parquet", replace = TRUE)
```

If `LIST_DATASETS_QUERY` is configured, you can discover which datasets are
available before loading one:

```r
available <- da$list_datasets()
```

## Development

Run the Python tests with:

```sh
uv run pytest
```

Run the R tests against the local source tree with:

```sh
Rscript -e 'pkgload::load_all(export_all = FALSE); testthat::test_dir("tests/testthat")'
```
