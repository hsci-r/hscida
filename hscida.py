from dataclasses import dataclass, field
from typing import Any, cast, overload
from hereutil import here
import narwhals as nw
import duckdb
from duckdb import DuckDBPyRelation
from sqlframe.duckdb import DuckDBDataFrame, DuckDBSession
from sqlframe.duckdb import functions as F
import os
import polars as pl
import pandas as pd

from dotenv import dotenv_values
from sqlglot import Dialect
from sqlglot.errors import ParseError

type DuckDBackedBDataFrameLike = DuckDBDataFrame | DuckDBPyRelation | nw.LazyFrame[DuckDBDataFrame] | nw.LazyFrame[DuckDBPyRelation]
type DataFrameLike = DuckDBackedBDataFrameLike | pl.DataFrame | pd.DataFrame | pl.LazyFrame | nw.LazyFrame[pl.LazyFrame] | nw.DataFrame[pl.DataFrame] | nw.DataFrame[pd.DataFrame]

@overload
def to_narwhals(df: DuckDBDataFrame|nw.LazyFrame[DuckDBDataFrame]) -> nw.LazyFrame[DuckDBDataFrame]: ...

@overload
def to_narwhals(df: DuckDBPyRelation|nw.LazyFrame[DuckDBPyRelation]) -> nw.LazyFrame[DuckDBPyRelation]: ...

def to_narwhals(df: DuckDBackedBDataFrameLike) -> nw.LazyFrame[DuckDBDataFrame]|nw.LazyFrame[DuckDBPyRelation]:
    if isinstance(df, nw.LazyFrame):
        return df
    elif isinstance(df, DuckDBDataFrame):
        return nw.from_native(df)
    else:
        return nw.from_native(df)

n = to_narwhals

def to_duckdb(lnf: DataFrameLike, optimize: bool = False, pretty: bool = False) -> DuckDBPyRelation:
    if isinstance(lnf, DuckDBPyRelation):
        return lnf
    elif isinstance(lnf, DuckDBDataFrame):
        return cast(duckdb.DuckDBPyConnection, cast(DuckDBSession, lnf.session)._conn).sql(lnf.sql(optimize=optimize, pretty=pretty))
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_duckdb():
        return cast(DuckDBPyRelation, lnf.to_native())
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_sqlframe():
        dbf = cast(DuckDBDataFrame, lnf.to_native())
        return cast(duckdb.DuckDBPyConnection, cast(DuckDBSession, dbf.session)._conn).sql(dbf.sql(optimize=optimize, pretty=pretty))
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_polars():
        return duckdb.from_arrow(cast(pl.LazyFrame, lnf.to_native()).collect().to_arrow())
    elif isinstance(lnf, nw.DataFrame) and lnf.implementation.is_pandas():
        return duckdb.from_df(cast(pd.DataFrame, lnf.to_native()))
    elif isinstance(lnf, nw.DataFrame) and lnf.implementation.is_polars():
        return duckdb.from_arrow(cast(pl.DataFrame, lnf.to_native()).to_arrow())
    elif isinstance(lnf, pl.DataFrame):
        return duckdb.from_arrow(lnf.to_arrow())
    elif isinstance(lnf, pl.LazyFrame):
        return duckdb.from_arrow(lnf.collect().to_arrow())
    elif isinstance(lnf, pd.DataFrame):
        return duckdb.from_df(lnf)
    else:
        raise TypeError(f"Unsupported type for to_duckdb: {type(lnf)}")

d = to_duckdb

def _duckdb_relation_to_spark(lnf: DuckDBPyRelation, session: DuckDBSession) -> DuckDBDataFrame:
    try:
        return session.sql(lnf.sql_query())
    except ParseError:
        return session.createDataFrame(lnf.df())

@overload
def to_spark(lnf: DuckDBPyRelation|nw.LazyFrame[DuckDBPyRelation]|pl.DataFrame|pl.LazyFrame|pd.DataFrame|nw.LazyFrame[pl.LazyFrame]|nw.DataFrame[pl.DataFrame]|nw.DataFrame[pd.DataFrame], session: DuckDBSession) -> DuckDBDataFrame: ...

@overload
def to_spark(lnf: DuckDBDataFrame|nw.LazyFrame[DuckDBDataFrame], session: DuckDBSession | None = None) -> DuckDBDataFrame: ...

def to_spark(lnf: DataFrameLike, session: DuckDBSession | None = None) -> DuckDBDataFrame: 
    if isinstance(lnf, DuckDBPyRelation):
        return _duckdb_relation_to_spark(lnf, cast(DuckDBSession, session))
    elif isinstance(lnf, DuckDBDataFrame):
        return lnf
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_duckdb():
        dbf = cast(DuckDBPyRelation, lnf.to_native())
        return _duckdb_relation_to_spark(dbf, cast(DuckDBSession, session))
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_sqlframe():
        return cast(DuckDBDataFrame, lnf.to_native())
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_polars():
        return cast(DuckDBSession, session).createDataFrame(cast(pl.LazyFrame, lnf.to_native()).collect().to_pandas(use_pyarrow_extension_array=True))
    elif isinstance(lnf, nw.DataFrame) and lnf.implementation.is_polars():
        return cast(DuckDBSession, session).createDataFrame(cast(pl.DataFrame, lnf.to_native()).to_pandas(use_pyarrow_extension_array=True))
    elif isinstance(lnf, nw.DataFrame) and lnf.implementation.is_pandas():
        return cast(DuckDBSession, session).createDataFrame(cast(pd.DataFrame, lnf.to_native()))
    elif isinstance(lnf, pl.DataFrame):
        return cast(DuckDBSession, session).createDataFrame(lnf.to_pandas(use_pyarrow_extension_array=True))
    elif isinstance(lnf, pl.LazyFrame):
        return cast(DuckDBSession, session).createDataFrame(lnf.collect().to_pandas(use_pyarrow_extension_array=True))
    elif isinstance(lnf, pd.DataFrame):
        return cast(DuckDBSession, session).createDataFrame(lnf)
    else:
        raise TypeError(f"Unsupported type for to_spark: {type(lnf)}")

s = to_spark

def to_polars(lnf: DataFrameLike) -> pl.DataFrame:
    if isinstance(lnf, DuckDBPyRelation):
        return lnf.pl()
    elif isinstance(lnf, DuckDBDataFrame):
        return cast(pl.DataFrame, pl.from_arrow(lnf.toArrow()))
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_duckdb():
        return cast(DuckDBPyRelation, lnf.to_native()).pl()
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_sqlframe():
        return cast(pl.DataFrame, pl.from_arrow(cast(DuckDBDataFrame, lnf.to_native()).toArrow()))
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_polars():
        return cast(pl.DataFrame, cast(pl.LazyFrame, lnf.to_native()).collect())
    elif isinstance(lnf, nw.DataFrame) and lnf.implementation.is_polars():
        return cast(pl.DataFrame, lnf.to_native())
    elif isinstance(lnf, nw.DataFrame) and lnf.implementation.is_pandas():
        return pl.from_pandas(cast(pd.DataFrame, lnf.to_native()))
    elif isinstance(lnf, pl.DataFrame):
        return lnf
    elif isinstance(lnf, pl.LazyFrame):
        return lnf.collect()
    elif isinstance(lnf, pd.DataFrame):
        return pl.from_pandas(lnf)
    else:
        raise TypeError(f"Unsupported type for to_polars: {type(lnf)}")

p = to_polars

def to_pandas(lnf: DataFrameLike) -> pd.DataFrame:
    if isinstance(lnf, DuckDBPyRelation):
        return lnf.df()
    elif isinstance(lnf, DuckDBDataFrame):
        return lnf.toArrow().to_pandas(split_blocks=True, self_destruct=True)
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_duckdb():
        return cast(DuckDBPyRelation, lnf.to_native()).df()
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_sqlframe():
        return cast(DuckDBDataFrame, lnf.to_native()).toArrow().to_pandas(split_blocks=True, self_destruct=True)
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_polars():
        return cast(pl.LazyFrame, lnf.to_native()).collect().to_pandas()
    elif isinstance(lnf, nw.DataFrame) and lnf.implementation.is_pandas():
        return cast(pd.DataFrame, lnf.to_native())
    elif isinstance(lnf, nw.DataFrame) and lnf.implementation.is_polars():
        return cast(pl.DataFrame, lnf.to_native()).to_pandas()
    elif isinstance(lnf, pl.DataFrame):
        return lnf.to_pandas()
    elif isinstance(lnf, pl.LazyFrame):
        return lnf.collect().to_pandas()
    elif isinstance(lnf, pd.DataFrame):
        return lnf
    else:
        raise TypeError(f"Unsupported type for to_pandas: {type(lnf)}")

import sqlglot

def to_sql(lnf: DuckDBackedBDataFrameLike, optimize: bool = False, pretty: bool = False) -> str:
    if isinstance(lnf, DuckDBPyRelation):
        if pretty:
            return ';\n'.join(sqlglot.transpile(lnf.sql_query(), read="duckdb", write="duckdb", pretty=True))
        else:
            return lnf.sql_query()
    elif isinstance(lnf, DuckDBDataFrame):
        return lnf.sql(optimize=optimize, pretty=pretty)
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_duckdb():
        if pretty:
            return ';\n'.join(sqlglot.transpile(cast(DuckDBPyRelation, lnf.to_native()).sql_query(), read="duckdb", write="duckdb", pretty=True))
        else:
            return cast(DuckDBPyRelation, lnf.to_native()).sql_query()
    else:
        return cast(DuckDBDataFrame, lnf.to_native()).sql(optimize=optimize, pretty=pretty)

q = to_sql

@overload
def to_table(name: str, lnf: DuckDBPyRelation, con: duckdb.DuckDBPyConnection, temporary: bool = False, replace: bool = False) -> DuckDBPyRelation: ...

@overload
def to_table(name: str, lnf: nw.LazyFrame[DuckDBPyRelation], con: duckdb.DuckDBPyConnection, temporary: bool = False, replace: bool = False) -> nw.LazyFrame[DuckDBPyRelation]: ...

@overload
def to_table(name: str, lnf: DuckDBDataFrame, con: duckdb.DuckDBPyConnection | None= None, temporary: bool = False, replace: bool = False) -> DuckDBDataFrame: ...

@overload
def to_table(name: str, lnf: nw.LazyFrame[DuckDBDataFrame], con: duckdb.DuckDBPyConnection | None = None, temporary: bool = False, replace: bool = False) -> nw.LazyFrame[DuckDBDataFrame]: ...

def to_table(name: str, lnf: DuckDBackedBDataFrameLike, con: duckdb.DuckDBPyConnection | None = None, temporary: bool = False, replace: bool = False) -> DuckDBackedBDataFrameLike:
    sql = f"CREATE{" OR REPLACE" if replace else ''}{" TEMPORARY" if temporary else ''} TABLE{" IF NOT EXISTS" if not replace else ''} {name} AS {to_sql(lnf)}"
    if isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_duckdb():
        con = cast(duckdb.DuckDBPyConnection, con)
        con.sql(sql)
        return nw.from_native(con.table(name))
    elif isinstance(lnf, DuckDBPyRelation):
        con = cast(duckdb.DuckDBPyConnection, con)
        con.sql(sql)
        return con.table(name)
    elif isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_sqlframe():
        lnf = lnf.to_native()
        session = cast(DuckDBSession, lnf.session)
        session._execute(sql)
        return nw.from_native(session.table(name))
    elif isinstance(lnf, DuckDBDataFrame):
        session = cast(DuckDBSession, lnf.session)
        session._execute(sql)
        return session.table(name)
    else:
        raise TypeError(f"Unsupported type for to_table: {type(lnf)}")

t = to_table

_PROJROOT = str(here())

@dataclass
class DataAccessConfig:
    init_sql: str = ""
    path_query: str = ""
    list_datasets_query: str = ""
    view_definition_query: str = "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS FROM {source};"
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
        init_sql=init_sql,
        path_query=c.get('PATH_QUERY', ""),
        list_datasets_query=c.get('LIST_DATASETS_QUERY', ""),
        view_definition_query=c.get('VIEW_DEFINITION_QUERY', "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS FROM {source};"),
        projroot=c.get('PROJROOT', _PROJROOT)
    )

class DataAccess:

    def __init__(self, config: DataAccessConfig = config_from_env()) -> None:
        self.config = config
        self.con = duckdb.connect()
        self.con.sql(config.init_sql)
        self.session = DuckDBSession(conn=self.con)
        self.session.input_dialect = Dialect.get_or_raise("duckdb")
        self.session.output_dialect = Dialect.get_or_raise("duckdb")
        self.datasets = dict[str, tuple[nw.LazyFrame[DuckDBPyRelation], nw.LazyFrame[DuckDBDataFrame]]]()

    def ensure_dataset(self, dataset: str, *paths: str, replace: bool = False, debug: bool = False) -> None:
        if dataset not in self.datasets or replace:
            if not paths:
                paths = tuple(path[0] for path in self.con.sql(self.config.path_query.format(dataset=dataset,projroot=self.config.projroot)).fetchall())
            if debug:
                print(f"DEBUG: Found paths for dataset {dataset}: {paths}")
            if not paths:
                print(f"No files found for dataset {dataset} in {self.config.path_query.format(dataset=dataset,projroot=self.config.projroot)}")
                self.datasets[dataset] = cast(tuple[nw.LazyFrame[DuckDBPyRelation], nw.LazyFrame[DuckDBDataFrame]], (None, None))
                return
            if len(paths) == 1:
                source = f"'{paths[0]}'"
            else:
                reader = "read_csv" if paths[0].endswith((".tsv", ".tsv.gz", ".csv.gz")) else f"read_{paths[0].rsplit('.', 1)[-1]}"
                paths_sql = "', '".join(paths)
                source = f"{reader}(['{paths_sql}'], hive_partitioning=true)"
            self.con.sql(self.config.view_definition_query.format(or_replace=(' OR REPLACE' if replace else ''), if_not_exists=(' IF NOT EXISTS' if not replace else ''), dataset=dataset, source=source))
            self.datasets[dataset] = (nw.from_native(self.con.sql(f'FROM {dataset}')), nw.from_native(self.session.table(dataset)))

    def list_datasets(self, debug: bool = False) -> list[str]:
        if not self.config.list_datasets_query:
            print("No LIST_DATASETS_QUERY configured; set the LIST_DATASETS_QUERY environment variable to enable dataset discovery.")
            return []
        sql = self.config.list_datasets_query.format(projroot=self.config.projroot)
        if debug:
            print(f"DEBUG: Listing datasets with query: {sql}")
        return sorted({row[0] for row in self.con.sql(sql).fetchall()})

    ld = list_datasets

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

    @overload
    def to_narwhals(self, df: DuckDBDataFrame|nw.LazyFrame[DuckDBDataFrame]) -> nw.LazyFrame[DuckDBDataFrame]: ...

    @overload
    def to_narwhals(self, df: DuckDBPyRelation|nw.LazyFrame[DuckDBPyRelation]) -> nw.LazyFrame[DuckDBPyRelation]: ...

    def to_narwhals(self, df: DuckDBackedBDataFrameLike) -> nw.LazyFrame[DuckDBDataFrame]|nw.LazyFrame[DuckDBPyRelation]:
        return to_narwhals(df)

    n = to_narwhals

    def to_duckdb(self, lnf: DataFrameLike, optimize: bool = False, pretty: bool = False) -> DuckDBPyRelation:
        return to_duckdb(lnf, optimize=optimize, pretty=pretty)

    d = to_duckdb

    def to_spark(self, lnf: DataFrameLike) -> DuckDBDataFrame:
        return to_spark(lnf, session=self.session)

    s = to_spark

    def to_polars(self, lnf: DataFrameLike) -> pl.DataFrame:
        return to_polars(lnf)

    p = to_polars

    def to_pandas(self, lnf: DataFrameLike) -> pd.DataFrame:
        return to_pandas(lnf)

    def to_sql(self, lnf: DuckDBackedBDataFrameLike, optimize: bool = False, pretty: bool = False) -> str:
        return to_sql(lnf, optimize=optimize, pretty=pretty)

    q = to_sql

    @overload
    def to_table(self, name: str, lnf: DuckDBPyRelation, temporary: bool = False, replace: bool = False) -> DuckDBPyRelation: ...

    @overload
    def to_table(self, name: str, lnf: nw.LazyFrame[DuckDBPyRelation], temporary: bool = False, replace: bool = False) -> nw.LazyFrame[DuckDBPyRelation]: ...

    @overload
    def to_table(self, name: str, lnf: DuckDBDataFrame, temporary: bool = False, replace: bool = False) -> DuckDBDataFrame: ... 

    @overload
    def to_table(self, name: str, lnf: nw.LazyFrame[DuckDBDataFrame], temporary: bool = False, replace: bool = False) -> nw.LazyFrame[DuckDBDataFrame]: ...

    def to_table(self, name: str, lnf: DuckDBackedBDataFrameLike, temporary: bool = False, replace: bool = False) -> DuckDBackedBDataFrameLike:
        if isinstance(lnf, DuckDBPyRelation) or (isinstance(lnf, nw.LazyFrame) and lnf.implementation.is_duckdb()):
            return to_table(name, cast(DuckDBPyRelation|nw.LazyFrame[DuckDBPyRelation], lnf), con=self.con, temporary=temporary, replace=replace)
        else:
            return to_table(name, cast(DuckDBDataFrame|nw.LazyFrame[DuckDBDataFrame], lnf), con=None, temporary=temporary, replace=replace)

    t = to_table

__all__ = [ "DataAccess", "nw", "to_narwhals", "n", "to_duckdb", "d", "to_spark", "F", "s", "to_polars", "p", "to_pandas", "to_sql", "q", "to_table", "t", "DuckDBDataFrame", "DuckDBSession", "DuckDBPyRelation", "DuckDBackedBDataFrameLike", "pl" ]
