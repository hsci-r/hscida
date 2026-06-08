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

def to_duckdb(lnf: DuckDBDataFrame|nw.LazyFrame[duckdb.DuckDBPyRelation]|nw.LazyFrame[DuckDBDataFrame], optimize: bool = False, pretty: bool = False) -> duckdb.DuckDBPyRelation:
    if isinstance(lnf, DuckDBDataFrame):
        return cast(duckdb.DuckDBPyConnection, cast(DuckDBSession, lnf.session)._conn).sql(lnf.sql(dialect="duckdb", optimize=optimize, pretty=pretty))
    elif lnf.implementation.is_duckdb():
        return cast(duckdb.DuckDBPyRelation, lnf.to_native())
    else:
        dbf = cast(DuckDBDataFrame, lnf.to_native())
        return cast(duckdb.DuckDBPyConnection, cast(DuckDBSession, dbf.session)._conn).sql(dbf.sql(dialect="duckdb", optimize=optimize, pretty=pretty))

d = to_duckdb

@overload
def to_spark(lnf: duckdb.DuckDBPyRelation|nw.LazyFrame[duckdb.DuckDBPyRelation], session: DuckDBSession) -> DuckDBDataFrame: ...

@overload
def to_spark(lnf: nw.LazyFrame[DuckDBDataFrame]) -> DuckDBDataFrame: ...

def to_spark(lnf: duckdb.DuckDBPyRelation|nw.LazyFrame[DuckDBDataFrame]|nw.LazyFrame[duckdb.DuckDBPyRelation], session: DuckDBSession | None = None) -> DuckDBDataFrame: 
    if isinstance(lnf, duckdb.DuckDBPyRelation):
        return cast(DuckDBSession, session).sql(lnf.sql_query(), dialect="duckdb")
    elif lnf.implementation.is_duckdb():
        dbf = cast(duckdb.DuckDBPyRelation, lnf.to_native())
        return cast(DuckDBSession, session).sql(dbf.sql_query(), dialect="duckdb")
    else:
        return cast(DuckDBDataFrame, lnf.to_native())

s = to_spark

def to_polars(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]|nw.LazyFrame[DuckDBDataFrame]) -> pl.DataFrame:
    return lnf.collect(backend='polars').to_native()

p = to_polars

def to_pandas(lnf: nw.LazyFrame[duckdb.DuckDBPyRelation]|nw.LazyFrame[DuckDBDataFrame]):
    return to_duckdb(lnf).df()

@overload
def to_sql(lnf: duckdb.DuckDBPyRelation|nw.LazyFrame[duckdb.DuckDBPyRelation]) -> str: ...

@overload
def to_sql(lnf: DuckDBDataFrame|nw.LazyFrame[DuckDBDataFrame], optimize: bool = False, pretty: bool = False) -> str: ...

def to_sql(lnf: duckdb.DuckDBPyRelation|DuckDBDataFrame|nw.LazyFrame[duckdb.DuckDBPyRelation]|nw.LazyFrame[DuckDBDataFrame], optimize: bool = False, pretty: bool = False) -> str:
    if isinstance(lnf, duckdb.DuckDBPyRelation):
        return lnf.sql_query()
    elif isinstance(lnf, DuckDBDataFrame):
        return lnf.sql(dialect="duckdb", optimize=optimize, pretty=pretty)
    elif lnf.implementation.is_duckdb():
        return cast(duckdb.DuckDBPyRelation, lnf.to_native()).sql_query()
    else:
        return cast(DuckDBDataFrame, lnf.to_native()).sql(dialect="duckdb", optimize=optimize, pretty=pretty)

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

class DataAccess:

    def __init__(self, config: DataAccessConfig = config_from_env()) -> None:
        self.config = config
        self.con = duckdb.connect(config=config.duckdb_config)
        self.con.sql(config.init_sql)
        self.session = DuckDBSession(conn=self.con)
        self.datasets = dict[str, tuple[nw.LazyFrame[duckdb.DuckDBPyRelation], nw.LazyFrame[DuckDBDataFrame]]]()

    def ensure_dataset(self, dataset: str, *paths: str, replace: bool, debug: bool) -> bool:
        if dataset not in self.datasets or replace:
            if not paths:
                paths = tuple(path[0] for path in self.con.sql("FROM "+self.config.glob_pattern.format(dataset=dataset,projroot=self.config.projroot)).fetchall())
            if debug:
                print(f"DEBUG: Found paths for dataset {dataset}: {paths}")
            if not paths:
                print(f"No files found for dataset {dataset} in {self.config.glob_pattern.format(dataset=dataset,projroot=self.config.projroot)}")
                return False
            self.con.sql(f"CREATE {('OR REPLACE' if replace else '')} VIEW {'IF NOT EXISTS' if not replace else ''} {dataset} AS FROM read_{'parquet' if paths[0].endswith('.parquet') else 'csv'}(['{"', '".join(paths)}'], hive_partitioning=true);")
            self.datasets[dataset] = (nw.from_native(self.con.sql(f'FROM {dataset}')), nw.from_native(self.session.table(dataset)))
        return True

    def df(self, dataset: str, *paths: str,replace: bool = False, debug: bool = False) -> nw.LazyFrame[duckdb.DuckDBPyRelation]:
        if self.ensure_dataset(dataset, *paths, replace=replace, debug=debug):
            return self.datasets[dataset][0]
        else:
            return cast(nw.LazyFrame[duckdb.DuckDBPyRelation], None)
    
    def sf(self, dataset: str, *paths: str, replace: bool = False, debug: bool = False) -> nw.LazyFrame[DuckDBDataFrame]:
        if self.ensure_dataset(dataset, *paths, replace=replace, debug=debug):
            return self.datasets[dataset][1]
        else:
            return cast(nw.LazyFrame[DuckDBDataFrame], None)
        
    f = sf

    def close(self) -> None:
        self.session.stop()
        self.con.close()

    def __enter__(self) -> "DataAccess":
        return self
    
    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def to_spark(self, lnf: duckdb.DuckDBPyRelation|nw.LazyFrame[DuckDBDataFrame]|nw.LazyFrame[duckdb.DuckDBPyRelation]) -> DuckDBDataFrame: 
        if isinstance(lnf, duckdb.DuckDBPyRelation):
            return cast(DuckDBSession, self.session).sql(lnf.sql_query(), dialect="duckdb")
        elif lnf.implementation.is_duckdb():
            dbf = cast(duckdb.DuckDBPyRelation, lnf.to_native())
            return cast(DuckDBSession, self.session).sql(dbf.sql_query(), dialect="duckdb")
        else:
            return cast(DuckDBDataFrame, lnf.to_native())
        
    s = to_spark

__all__ = [ "DataAccess", "nw", "c", "l", "to_narwhals", "n", "to_duckdb", "d", "to_spark", "F", "s", "to_polars", "p", "to_pandas", "to_sql", "q" ]