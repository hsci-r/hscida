from dataclasses import dataclass, field
from typing import Any, cast
from hereutil import here
import narwhals as nw
import duckdb
import os
import polars as pl

from dotenv import dotenv_values

_DEFAULT_DUCKDB_CONFIG = dict(parquet_metadata_cache="true", preserve_insertion_order="false", enable_fsst_vectors="true")
_PROJROOT = str(here())

@dataclass
class DataAccessConfig:
    glob_pattern: str
    init_sql: str
    duckdb_config: dict[str, Any] = field(default_factory=lambda: _DEFAULT_DUCKDB_CONFIG)
    projroot: str = _PROJROOT

def config_from_env() -> DataAccessConfig:
    c = {
        **dotenv_values(),
        **os.environ,
    }
    return DataAccessConfig(
        glob_pattern=c.get('GLOB_PATTERN', ''),
        init_sql=c.get('INIT_SQL', ''),
        duckdb_config={k: v for k, v in [pair.split('=') for pair in c['DUCKDB_CONFIG'].split(',')] } if 'DUCKDB_CONFIG' in c else _DEFAULT_DUCKDB_CONFIG,
        projroot=c.get('PROJROOT', _PROJROOT)
    )

class DataAccess:
    def __init__(self, config: DataAccessConfig = config_from_env()) -> None:
        self.con = duckdb.connect(config=config.duckdb_config)
        self.con.sql(config.init_sql)
        self.datasets = dict[str, nw.LazyFrame[duckdb.DuckDBPyRelation]]()
        self.config = config

    def register_files_as_view(self, table_name: str, *paths: str, replace: bool = False) -> None:
        self.con.sql(f"CREATE {('OR REPLACE' if replace else '')} VIEW {'IF NOT EXISTS' if not replace else ''} {table_name} AS FROM read_{'parquet' if paths[0].endswith('.parquet') else 'csv'}(['{"', '".join(paths)}'], hive_partitioning=true);")

    def f(self, dataset: str, *paths: str,replace: bool = False, debug: bool = False) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
        if dataset not in self.datasets or replace:
            if not paths:
                paths = tuple(path[0] for path in self.con.sql("FROM "+self.config.glob_pattern.format(dataset=dataset,projroot=self.config.projroot)).fetchall())
            if debug:
                print(f"DEBUG: Found paths for dataset {dataset}: {paths}")
            if not paths:
                print(f"No files found for dataset {dataset} in {self.config.glob_pattern.format(dataset=dataset,projroot=self.config.projroot)}")
                return cast(nw.LazyFrame[duckdb.DuckDBPyRelation], None)
            self.register_files_as_view(dataset, *paths, replace=replace)
            self.datasets[dataset] = nw.from_native(self.con.sql(f'FROM {dataset}'))
        return self.datasets[dataset]

c = nw.col
l = nw.lit

def to_narwhals(duckdb_relation: duckdb.DuckDBPyRelation) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
    return nw.from_native(duckdb_relation)

n = to_narwhals

def to_duckdb(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]) -> duckdb.DuckDBPyRelation:
    return lnf.to_native()

d = to_duckdb

def to_polars(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]) -> pl.DataFrame:
    return lnf.collect(backend='polars').to_native()

p = to_polars

def to_pandas(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]):
    return d(lnf).df()

__all__ = [ "DataAccess", "c", "l", "to_narwhals", "n", "to_duckdb", "d", "to_polars", "p","to_pandas" ]