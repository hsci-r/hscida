from dataclasses import dataclass, field
from typing import Any, cast, overload
from hereutil import here
import narwhals as nw
import duckdb
from duckdb import DuckDBPyRelation
from sqlframe import activate_context
from sqlframe.duckdb import DuckDBDataFrame, DuckDBSession
from sqlframe.duckdb import functions as F
import os
import polars as pl

from dotenv import dotenv_values
from sqlglot import Dialect

type DuckDBackedBDataFrameLike = DuckDBDataFrame | DuckDBPyRelation | nw.LazyFrame[DuckDBDataFrame] | nw.LazyFrame[DuckDBPyRelation]

@overload
def to_narwhals(df: DuckDBDataFrame) -> nw.LazyFrame[DuckDBDataFrame]: ...

@overload
def to_narwhals(df: DuckDBPyRelation) -> nw.LazyFrame[DuckDBPyRelation]: ...

@overload
def to_narwhals(df: nw.LazyFrame[DuckDBDataFrame]) -> nw.LazyFrame[DuckDBDataFrame]: ...

@overload
def to_narwhals(df: nw.LazyFrame[DuckDBPyRelation]) -> nw.LazyFrame[DuckDBPyRelation]: ...
    
def to_narwhals(df: DuckDBackedBDataFrameLike) -> nw.LazyFrame[DuckDBDataFrame]|nw.LazyFrame[DuckDBPyRelation]:
    if isinstance(df, nw.LazyFrame):
        return df
    elif isinstance(df, DuckDBDataFrame):
        return nw.from_native(df)
    else:
        return nw.from_native(df)

n = to_narwhals

def to_duckdb(lnf: DuckDBackedBDataFrameLike, optimize: bool = False, pretty: bool = False) -> DuckDBPyRelation:
    if isinstance(lnf, DuckDBPyRelation):
        return lnf
    elif isinstance(lnf, DuckDBDataFrame):
        return cast(duckdb.DuckDBPyConnection, cast(DuckDBSession, lnf.session)._conn).sql(lnf.sql(optimize=optimize, pretty=pretty))
    elif lnf.implementation.is_duckdb():
        return cast(DuckDBPyRelation, lnf.to_native())
    else:
        dbf = cast(DuckDBDataFrame, lnf.to_native())
        return cast(duckdb.DuckDBPyConnection, cast(DuckDBSession, dbf.session)._conn).sql(dbf.sql(optimize=optimize, pretty=pretty))

d = to_duckdb

@overload
def to_spark(lnf: DuckDBPyRelation|nw.LazyFrame[DuckDBPyRelation], session: DuckDBSession) -> DuckDBDataFrame: ...

@overload
def to_spark(lnf: DuckDBDataFrame|nw.LazyFrame[DuckDBDataFrame], session: DuckDBSession | None = None) -> DuckDBDataFrame: ...

def to_spark(lnf: DuckDBackedBDataFrameLike, session: DuckDBSession | None = None) -> DuckDBDataFrame: 
    if isinstance(lnf, DuckDBPyRelation):
        return cast(DuckDBSession, session).sql(lnf.sql_query())
    elif isinstance(lnf, DuckDBDataFrame):
        return lnf
    elif lnf.implementation.is_duckdb():
        dbf = cast(DuckDBPyRelation, lnf.to_native())
        return cast(DuckDBSession, session).sql(dbf.sql_query())
    else:
        return cast(DuckDBDataFrame, lnf.to_native())

s = to_spark

def to_polars(lnf: DuckDBackedBDataFrameLike) -> pl.DataFrame:
    return to_duckdb(lnf).pl()

p = to_polars

def to_pandas(lnf: DuckDBackedBDataFrameLike):
    return to_duckdb(lnf).df()

@overload
def to_sql(lnf: DuckDBPyRelation|nw.LazyFrame[DuckDBPyRelation], optimize: bool = False, pretty: bool = False) -> str: ...

@overload
def to_sql(lnf: DuckDBDataFrame|nw.LazyFrame[DuckDBDataFrame], optimize: bool = False, pretty: bool = False) -> str: ...

def to_sql(lnf: DuckDBackedBDataFrameLike, optimize: bool = False, pretty: bool = False) -> str:
    if isinstance(lnf, DuckDBPyRelation):
        return lnf.sql_query()
    elif isinstance(lnf, DuckDBDataFrame):
        return lnf.sql(optimize=optimize, pretty=pretty)
    elif lnf.implementation.is_duckdb():
        return cast(DuckDBPyRelation, lnf.to_native()).sql_query()
    else:
        return cast(DuckDBDataFrame, lnf.to_native()).sql(optimize=optimize, pretty=pretty)

q = to_sql

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
    init_sql = ""
    for k in sorted(k for k in c.keys() if k.startswith("INIT_SQL")):
        init_sql += c[k]
    return DataAccessConfig(
        glob_pattern=c.get('GLOB_PATTERN', ''),
        init_sql=init_sql,
        duckdb_config={k: v for k, v in [pair.split('=') for pair in c['DUCKDB_CONFIG'].split(',')] } if 'DUCKDB_CONFIG' in c else _DEFAULT_DUCKDB_CONFIG,
        projroot=c.get('PROJROOT', _PROJROOT)
    )

class DataAccess:

    def __init__(self, config: DataAccessConfig = config_from_env()) -> None:
        self.config = config
        self.con = duckdb.connect(config=config.duckdb_config)
        self.con.sql(config.init_sql)
        self.session = DuckDBSession(conn=self.con)
        self.session.input_dialect = Dialect.get_or_raise("duckdb")
        self.session.output_dialect = Dialect.get_or_raise("duckdb")
        self.datasets = dict[str, tuple[nw.LazyFrame[DuckDBPyRelation], nw.LazyFrame[DuckDBDataFrame]]]()

    def ensure_dataset(self, dataset: str, *paths: str, replace: bool = False, debug: bool = False) -> None:
        if dataset not in self.datasets or replace:
            if not paths:
                paths = tuple(path[0] for path in self.con.sql("FROM "+self.config.glob_pattern.format(dataset=dataset,projroot=self.config.projroot)).fetchall())
            if debug:
                print(f"DEBUG: Found paths for dataset {dataset}: {paths}")
            if not paths:
                print(f"No files found for dataset {dataset} in {self.config.glob_pattern.format(dataset=dataset,projroot=self.config.projroot)}")
                self.datasets[dataset] = cast(tuple[nw.LazyFrame[DuckDBPyRelation], nw.LazyFrame[DuckDBDataFrame]], (None, None))
            self.con.sql(f"CREATE {('OR REPLACE' if replace else '')} VIEW {'IF NOT EXISTS' if not replace else ''} {dataset} AS FROM read_{'parquet' if paths[0].endswith('.parquet') else 'csv'}(['{"', '".join(paths)}'], hive_partitioning=true);")
            self.datasets[dataset] = (nw.from_native(self.con.sql(f'FROM {dataset}')), nw.from_native(self.session.table(dataset)))

    def narwhals_duckdb_dataframe(self, dataset: str, *paths: str,replace: bool = False, debug: bool = False) -> nw.LazyFrame[DuckDBPyRelation]:
        self.ensure_dataset(dataset, *paths, replace=replace, debug=debug)
        return self.datasets[dataset][0]
    
    nddf = narwhals_duckdb_dataframe

    def duckdb_dataframe(self, dataset: str, *paths: str, replace: bool = False, debug: bool = False) -> DuckDBPyRelation:
        self.ensure_dataset(dataset, *paths, replace=replace, debug=debug)
        return to_duckdb(self.datasets[dataset][0])
    
    ddf = duckdb_dataframe

    def narwhals_spark_dataframe(self, dataset: str, *paths: str, replace: bool = False, debug: bool = False) -> nw.LazyFrame[DuckDBDataFrame]:
        self.ensure_dataset(dataset, *paths, replace=replace, debug=debug)
        return self.datasets[dataset][1]
    
    nsdf = narwhals_spark_dataframe
    
    def spark_dataframe(self, dataset: str, *paths: str, replace: bool = False, debug: bool = False) -> DuckDBDataFrame:
        self.ensure_dataset(dataset, *paths, replace=replace, debug=debug)
        return to_spark(self.datasets[dataset][1])
    
    sdf = spark_dataframe

    def spark_dataframe_from_sql(self, sql: str) -> DuckDBDataFrame:
        return self.session.sql(sql)
    
    def duckdb_dataframe_from_sql(self, sql: str) -> DuckDBPyRelation:
        return self.con.sql(sql)
    
    def narwhals_duckdb_dataframe_from_sql(self, sql: str) -> nw.LazyFrame[DuckDBPyRelation]:
        return to_narwhals(self.con.sql(sql))
    
    def narwhals_spark_dataframe_from_sql(self, sql: str) -> nw.LazyFrame[DuckDBDataFrame]:
        return to_narwhals(self.session.sql(sql))
    
    def close(self) -> None:
        self.session.stop()
        self.con.close()

    def __enter__(self) -> "DataAccess":
        return self
    
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def to_narwhals(self, df: DuckDBackedBDataFrameLike) -> nw.LazyFrame[DuckDBDataFrame]|nw.LazyFrame[DuckDBPyRelation]:
        return to_narwhals(df)

    n = to_narwhals

    def to_duckdb(self, lnf: DuckDBackedBDataFrameLike, optimize: bool = False, pretty: bool = False) -> DuckDBPyRelation:
        return to_duckdb(lnf, optimize=optimize, pretty=pretty)

    d = to_duckdb

    def to_spark(self, lnf: DuckDBackedBDataFrameLike) -> DuckDBDataFrame:
        return to_spark(lnf, session=self.session)

    s = to_spark

    def to_polars(self, lnf: DuckDBackedBDataFrameLike) -> pl.DataFrame:
        return to_polars(lnf)

    p = to_polars

    def to_pandas(self, lnf: DuckDBackedBDataFrameLike):
        return to_pandas(lnf)

    def to_sql(self, lnf: DuckDBackedBDataFrameLike, optimize: bool = False, pretty: bool = False) -> str:
        return to_sql(lnf, optimize=optimize, pretty=pretty)

    q = to_sql

__all__ = [ "DataAccess", "nw", "to_narwhals", "n", "to_duckdb", "d", "to_spark", "F", "s", "to_polars", "p", "to_pandas", "to_sql", "q", "DuckDBDataFrame", "DuckDBSession", "DuckDBPyRelation", "DuckDBackedBDataFrameLike", "pl" ]