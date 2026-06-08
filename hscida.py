from dataclasses import dataclass, field
from typing import Any, Callable, cast, overload
from hereutil import here
import narwhals as nw
import duckdb
from sqlframe.duckdb import DuckDBDataFrame, DuckDBSession
from sqlframe.duckdb import functions as F
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
        **dotenv_values(here(".env.secret")),
        **os.environ,
    }
    init_sql = c.get('INIT_SQL', '')
    i = 1
    while f"INIT_SQL_{i}" in c:
        init_sql += c[f"INIT_SQL_{i}"]
        i += 1
    return DataAccessConfig(
        glob_pattern=c.get('GLOB_PATTERN', ''),
        init_sql=init_sql,
        duckdb_config={k: v for k, v in [pair.split('=') for pair in c['DUCKDB_CONFIG'].split(',')] } if 'DUCKDB_CONFIG' in c else _DEFAULT_DUCKDB_CONFIG,
        projroot=c.get('PROJROOT', _PROJROOT)
    )

def data_access(config: DataAccessConfig = config_from_env()) -> tuple[Callable[..., nw.LazyFrame[duckdb.DuckDBPyRelation]], duckdb.DuckDBPyConnection, DuckDBSession]:
    con = duckdb.connect(config=config.duckdb_config)
    con.sql(config.init_sql)
    datasets = dict[str, nw.LazyFrame[duckdb.DuckDBPyRelation]]()

    def register_files_as_view(table_name: str, *paths: str, replace: bool = False) -> None:
        con.sql(f"CREATE {('OR REPLACE' if replace else '')} VIEW {'IF NOT EXISTS' if not replace else ''} {table_name} AS FROM read_{'parquet' if paths[0].endswith('.parquet') else 'csv'}(['{"', '".join(paths)}'], hive_partitioning=true);")

    def f(dataset: str, *paths: str,replace: bool = False, debug: bool = False) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
        if dataset not in datasets or replace:
            if not paths:
                paths = tuple(path[0] for path in con.sql("FROM "+config.glob_pattern.format(dataset=dataset,projroot=config.projroot)).fetchall())
            if debug:
                print(f"DEBUG: Found paths for dataset {dataset}: {paths}")
            if not paths:
                print(f"No files found for dataset {dataset} in {config.glob_pattern.format(dataset=dataset,projroot=config.projroot)}")
                return cast(nw.LazyFrame[duckdb.DuckDBPyRelation], None)
            register_files_as_view(dataset, *paths, replace=replace)
            datasets[dataset] = nw.from_native(con.sql(f'FROM {dataset}'))
        return datasets[dataset]
    return f, con, DuckDBSession(conn=con)

c = nw.col
l = nw.lit

@overload
def to_narwhals(duckdb_relation: DuckDBDataFrame) -> nw.LazyFrame[DuckDBDataFrame]: ...

@overload
def to_narwhals(duckdb_relation: duckdb.DuckDBPyRelation) -> nw.LazyFrame[duckdb.DuckDBPyRelation]: ...
    
def to_narwhals(duckdb_relation: DuckDBDataFrame|duckdb.DuckDBPyRelation) -> nw.LazyFrame[DuckDBDataFrame]|nw.LazyFrame[duckdb.DuckDBPyRelation]:
    if isinstance(duckdb_relation, DuckDBDataFrame):
        return nw.from_native(duckdb_relation)
    else:
        return nw.from_native(duckdb_relation)

n = to_narwhals

def to_duckdb(lnf: DuckDBDataFrame|nw.LazyFrame[duckdb.DuckDBPyRelation]|nw.LazyFrame[DuckDBDataFrame]) -> duckdb.DuckDBPyRelation:
    if isinstance(lnf, DuckDBDataFrame):
        return cast(duckdb.DuckDBPyConnection, cast(DuckDBSession, lnf.session)._conn).sql(lnf.sql(dialect="duckdb"))
    elif lnf.implementation.is_duckdb():
        return cast(duckdb.DuckDBPyRelation, lnf.to_native())
    else:
        dbf = cast(DuckDBDataFrame, lnf.to_native())
        return cast(duckdb.DuckDBPyConnection, cast(DuckDBSession, dbf.session)._conn).sql(dbf.sql(dialect="duckdb"))

d = to_duckdb

@overload
def to_spark(lnf: duckdb.DuckDBPyRelation|nw.LazyFrame[duckdb.DuckDBPyRelation], s: DuckDBSession) -> DuckDBDataFrame: ...

@overload
def to_spark(lnf: nw.LazyFrame[DuckDBDataFrame]) -> DuckDBDataFrame: ...

def to_spark(lnf: duckdb.DuckDBPyRelation|nw.LazyFrame[DuckDBDataFrame]|nw.LazyFrame[duckdb.DuckDBPyRelation], s: DuckDBSession | None = None) -> DuckDBDataFrame: 
    if isinstance(lnf, duckdb.DuckDBPyRelation):
        return cast(DuckDBSession, s).sql(lnf.sql_query(), dialect="duckdb")
    elif lnf.implementation.is_duckdb():
        dbf = cast(duckdb.DuckDBPyRelation, lnf.to_native())
        return cast(DuckDBSession, s).sql(dbf.sql_query(), dialect="duckdb")
    else:
        return cast(DuckDBDataFrame, lnf.to_native())

s = to_spark

def to_polars(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]|nw.LazyFrame[DuckDBDataFrame]) -> pl.DataFrame:
    return lnf.collect(backend='polars').to_native()

p = to_polars

def to_pandas(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]|nw.LazyFrame[DuckDBDataFrame]):
    return to_duckdb(lnf).df()

__all__ = [ "data_access", "nw", "c", "l", "to_narwhals", "n", "to_duckdb", "d", "to_spark", "F", "s", "to_polars", "p", "to_pandas" ]