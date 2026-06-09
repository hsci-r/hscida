from pathlib import Path

from duckdb import DuckDBPyRelation
import narwhals
from sqlframe.duckdb import DuckDBDataFrame

import hscida as hs
import polars as pl
import pytest

from hscida import DataAccessConfig, config_from_env, DataAccess, to_polars

def test_config_from_env_parses_values(monkeypatch):
    monkeypatch.setenv("GLOB_PATTERN", "glob('{projroot}/{dataset}/*.csv')")
    monkeypatch.setenv("INIT_SQL", "SELECT 1")
    monkeypatch.setenv("DUCKDB_CONFIG", "threads=1,enable_fsst_vectors=true")
    monkeypatch.setenv("PROJROOT", "/tmp/project")

    config = config_from_env()

    assert config.glob_pattern == "glob('{projroot}/{dataset}/*.csv')"
    assert config.init_sql == "SELECT 1"
    assert config.duckdb_config == {"threads": "1", "enable_fsst_vectors": "true"}
    assert config.projroot == "/tmp/project"


def test_config_from_env_reads_dotenv_file(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(hs, "here", lambda *parts: str(tmp_path.joinpath(*parts)))
    original_dotenv_values = hs.dotenv_values
    monkeypatch.setattr(
        hs,
        "dotenv_values",
        lambda path=None: original_dotenv_values(path or str(tmp_path / ".env")),
    )
    monkeypatch.delenv("GLOB_PATTERN", raising=False)
    monkeypatch.delenv("INIT_SQL", raising=False)
    monkeypatch.delenv("DUCKDB_CONFIG", raising=False)
    monkeypatch.delenv("PROJROOT", raising=False)

    (tmp_path / ".env").write_text(
        "GLOB_PATTERN=glob('{projroot}/{dataset}/*.csv')\n"
        "INIT_SQL=SELECT 42\n"
        "DUCKDB_CONFIG=threads=2\n"
        "PROJROOT=/tmp/from-dotenv\n",
        encoding="utf-8",
    )

    config = config_from_env()

    assert config.glob_pattern == "glob('{projroot}/{dataset}/*.csv')"
    assert config.init_sql == "SELECT 42"
    assert config.duckdb_config == {"threads": "2"}
    assert config.projroot == "/tmp/from-dotenv"


def test_config_from_env_reads_dotenv_secret_and_overrides_env(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(hs, "here", lambda *parts: str(tmp_path.joinpath(*parts)))
    original_dotenv_values = hs.dotenv_values
    monkeypatch.setattr(
        hs,
        "dotenv_values",
        lambda path=None: original_dotenv_values(path or str(tmp_path / ".env")),
    )
    monkeypatch.delenv("GLOB_PATTERN", raising=False)
    monkeypatch.delenv("INIT_SQL", raising=False)
    monkeypatch.delenv("DUCKDB_CONFIG", raising=False)
    monkeypatch.delenv("PROJROOT", raising=False)

    (tmp_path / ".env").write_text(
        "GLOB_PATTERN=glob('{projroot}/{dataset}/*.csv')\n"
        "INIT_SQL=SELECT 1\n",
        encoding="utf-8",
    )
    (tmp_path / ".env.secret").write_text(
        "GLOB_PATTERN=glob('{projroot}/{dataset}/*.parquet')\n"
        "INIT_SQL=SELECT 7\n"
        "DUCKDB_CONFIG=threads=4\n"
        "PROJROOT=/tmp/from-secret\n",
        encoding="utf-8",
    )

    config = config_from_env()

    assert config.glob_pattern == "glob('{projroot}/{dataset}/*.parquet')"
    assert config.init_sql == "SELECT 7"
    assert config.duckdb_config == {"threads": "4"}
    assert config.projroot == "/tmp/from-secret"


def test_config_from_env_works_without_dotenv_files(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(hs, "here", lambda *parts: str(tmp_path.joinpath(*parts)))
    original_dotenv_values = hs.dotenv_values
    monkeypatch.setattr(
        hs,
        "dotenv_values",
        lambda path=None: original_dotenv_values(path or str(tmp_path / ".env")),
    )
    monkeypatch.delenv("GLOB_PATTERN", raising=False)
    monkeypatch.delenv("INIT_SQL", raising=False)
    monkeypatch.delenv("DUCKDB_CONFIG", raising=False)
    monkeypatch.delenv("PROJROOT", raising=False)

    config = config_from_env()

    assert config.glob_pattern == ""
    assert config.init_sql == ""
    assert config.duckdb_config == hs._DEFAULT_DUCKDB_CONFIG
    assert config.projroot == hs._PROJROOT


def test_config_from_env_concatenates_numbered_init_sql(monkeypatch):
    monkeypatch.setenv("INIT_SQL", "CREATE TEMP TABLE t (x INTEGER);")
    monkeypatch.setenv("INIT_SQL_1", "INSERT INTO t VALUES (1);")
    monkeypatch.setenv("INIT_SQL_2", "INSERT INTO t VALUES (2);")
    monkeypatch.setenv("INIT_SQL_3", "SELECT * FROM t;")

    config = config_from_env()

    assert config.init_sql == (
        "CREATE TEMP TABLE t (x INTEGER);"
        "INSERT INTO t VALUES (1);"
        "INSERT INTO t VALUES (2);"
        "SELECT * FROM t;"
    )


def test_data_access_loads_csv_as_duckdb_dataframe(tmp_path: Path):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        frame = da.duckdb_dataframe("sample")
        columns = frame.columns
        rows = frame.fetchall()

    assert columns == ["x", "y"]
    assert rows == [(1, "a"), (2, "b")]


def test_data_access_loads_csv_as_spark_dataframe(tmp_path: Path):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        frame = da.spark_dataframe("sample")
        columns = frame.columns
        rows = frame.collect()

    assert columns == ["x", "y"]
    assert [row.asDict() for row in rows] == [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]


@pytest.mark.parametrize("accessor", ["narwhals_duckdb_dataframe", "narwhals_spark_dataframe"])
def test_data_access_loads_csv_as_narwhals_dataframe(tmp_path: Path, accessor: str):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        frame: narwhals.LazyFrame = getattr(da, accessor)("sample")
        columns = frame.collect_schema().names()
        rows = frame.collect().to_native().to_pylist()

    assert columns == ["x", "y"]
    assert rows == [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]


@pytest.mark.parametrize("accessor", ["duckdb_dataframe", "spark_dataframe", "narwhals_duckdb_dataframe", "narwhals_spark_dataframe"])
def test_data_access_caches_relation(tmp_path: Path, accessor: str):
    dataset_path = tmp_path / "cache.csv"
    pl.DataFrame({"v": [10]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        first = getattr(da, accessor)("cache")
        second = getattr(da, accessor)("cache")

    assert first is second


@pytest.mark.parametrize(
    "accessor",
    [
        "duckdb_dataframe_from_sql",
        "spark_dataframe_from_sql",
        "narwhals_duckdb_dataframe_from_sql",
        "narwhals_spark_dataframe_from_sql",
    ],
)
def test_data_access_from_sql_generators_return_expected_data(tmp_path: Path, accessor: str):
    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    sql = "SELECT 1 AS x, 'a' AS y"

    with DataAccess(cfg) as da:
        result = getattr(da, accessor)(sql)
        polars_df = to_polars(result)

    assert polars_df.shape == (1, 2)
    assert polars_df.to_dict(as_series=False) == {"x": [1], "y": ["a"]}


def test_data_access_from_sql_generators_match_across_backends(tmp_path: Path):
    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    sql = "SELECT 1 AS x, 'a' AS y"

    with DataAccess(cfg) as da:
        duckdb_df = da.duckdb_dataframe_from_sql(sql)
        spark_df = da.spark_dataframe_from_sql(sql)
        narwhals_duckdb_df = da.narwhals_duckdb_dataframe_from_sql(sql)
        narwhals_spark_df = da.narwhals_spark_dataframe_from_sql(sql)
        expected = to_polars(duckdb_df)

        assert to_polars(spark_df).equals(expected)
        assert to_polars(narwhals_duckdb_df).equals(expected)
        assert to_polars(narwhals_spark_df).equals(expected)


def test_equality(tmp_path: Path):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        assert to_polars(da.duckdb_dataframe("sample")).equals(to_polars(da.narwhals_duckdb_dataframe("sample")))

def test_conversions(tmp_path: Path):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        for accessor in ["duckdb_dataframe", "spark_dataframe", "narwhals_duckdb_dataframe", "narwhals_spark_dataframe"]:
            df = getattr(da, accessor)("sample")
            if isinstance(df, narwhals.LazyFrame):
                df = df.filter(narwhals.col("x") >=    0).filter(narwhals.col("y") != "b")
            elif isinstance(df, DuckDBDataFrame):
                df = df.filter(df["x"] >= 0).filter(df["y"] != "b")
            elif isinstance(df, DuckDBPyRelation):
                df = df.filter('x >= 0').filter("y != 'b'")
            assert to_polars(df).equals(pl.DataFrame({"x": [1], "y": ["a"]}))
            for converter1 in ["to_duckdb", "to_spark", "to_narwhals"]:
                for converter2 in ["to_duckdb", "to_spark", "to_narwhals"]:
                    for converter3 in ["to_duckdb", "to_spark", "to_narwhals"]:
                        converted1 = getattr(da, converter1)(df)
                        converted2 = getattr(da, converter2)(converted1)
                        converted3 = getattr(da, converter3)(converted2)
                        assert to_polars(df).equals(to_polars(converted1))
                        assert to_polars(df).equals(to_polars(converted2))
                        assert to_polars(df).equals(to_polars(converted3))
