from pathlib import Path
import os

from duckdb import DuckDBPyRelation
import narwhals
from sqlframe.duckdb import DuckDBDataFrame

import hscida as hs
import polars as pl
import pytest

from hscida import DataAccessConfig, config_from_env, DataAccess, to_polars


def test_config_from_env_parses_values(monkeypatch):
    monkeypatch.setenv("PATH_QUERY", "glob('{projroot}/{dataset}/*.csv')")
    monkeypatch.setenv("INIT_SQL", "SELECT 1")
    monkeypatch.setenv("PROJROOT", "/tmp/project")

    config = config_from_env()

    assert config.path_query == "glob('{projroot}/{dataset}/*.csv')"
    assert config.init_sql == "SELECT 1"
    assert config.projroot == "/tmp/project"


def test_config_from_env_reads_dotenv_file(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(hs, "here", lambda *parts: str(tmp_path.joinpath(*parts)))
    original_dotenv_values = hs.dotenv_values
    monkeypatch.setattr(
        hs,
        "dotenv_values",
        lambda path=None: original_dotenv_values(path or str(tmp_path / ".env")),
    )
    monkeypatch.delenv("PATH_QUERY", raising=False)
    monkeypatch.delenv("INIT_SQL", raising=False)
    monkeypatch.delenv("PROJROOT", raising=False)
    monkeypatch.delenv("VIEW_DEFINITION_QUERY", raising=False)

    (tmp_path / ".env").write_text(
        "PATH_QUERY=FROM glob('{projroot}/{dataset}/*.csv')\n"
        "INIT_SQL=SELECT 42\n"
        "VIEW_DEFINITION_QUERY=CREATE{or_replace} VIEW{if_not_exists} {dataset} AS SELECT 42;\n"
        "PROJROOT=/tmp/from-dotenv\n",
        encoding="utf-8",
    )

    config = config_from_env()

    assert config.path_query == "FROM glob('{projroot}/{dataset}/*.csv')"
    assert config.init_sql == "SELECT 42"
    assert config.view_definition_query == "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS SELECT 42;"
    assert config.projroot == "/tmp/from-dotenv"


def test_config_from_env_reads_dotenv_secret_and_overrides_env(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(hs, "here", lambda *parts: str(tmp_path.joinpath(*parts)))
    original_dotenv_values = hs.dotenv_values
    monkeypatch.setattr(
        hs,
        "dotenv_values",
        lambda path=None: original_dotenv_values(path or str(tmp_path / ".env")),
    )
    monkeypatch.delenv("PATH_QUERY", raising=False)
    monkeypatch.delenv("INIT_SQL", raising=False)
    monkeypatch.delenv("PROJROOT", raising=False)
    monkeypatch.delenv("VIEW_DEFINITION_QUERY", raising=False)

    (tmp_path / ".env").write_text(
        "PATH_QUERY=glob('{projroot}/{dataset}/*.csv')\n"
        "INIT_SQL=SELECT 1\n",
        encoding="utf-8",
    )
    (tmp_path / ".env.secret").write_text(
        "PATH_QUERY=FROM glob('{projroot}/{dataset}/*.parquet')\n"
        "INIT_SQL=SELECT 7\n"
        "VIEW_DEFINITION_QUERY=CREATE{or_replace} VIEW{if_not_exists} {dataset} AS SELECT 42;\n"
        "PROJROOT=/tmp/from-secret\n",
        encoding="utf-8",
    )

    config = config_from_env()

    assert config.path_query == "FROM glob('{projroot}/{dataset}/*.parquet')"
    assert config.init_sql == "SELECT 7"
    assert config.view_definition_query == "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS SELECT 42;"
    assert config.projroot == "/tmp/from-secret"


def test_config_from_env_works_without_dotenv_files(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(hs, "here", lambda *parts: str(tmp_path.joinpath(*parts)))
    original_dotenv_values = hs.dotenv_values
    monkeypatch.setattr(
        hs,
        "dotenv_values",
        lambda path=None: original_dotenv_values(path or str(tmp_path / ".env")),
    )
    monkeypatch.delenv("PATH_QUERY", raising=False)
    monkeypatch.delenv("INIT_SQL", raising=False)
    monkeypatch.delenv("PROJROOT", raising=False)
    monkeypatch.delenv("VIEW_DEFINITION_QUERY", raising=False)

    config = config_from_env()

    assert config.path_query == ""
    assert config.init_sql == ""
    assert config.view_definition_query == "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS FROM {source};"
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


def test_config_from_env_builds_init_sql_from_env_var_names(monkeypatch):
    monkeypatch.setattr(hs, "dotenv_values", lambda path=None: {})
    for key in list(os.environ):
        if key.startswith("INIT_SQL"):
            monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("INIT_SQL", "m;")
    monkeypatch.setenv("INIT_SQL_A", "z;")
    monkeypatch.setenv("INIT_SQL_Z", "a;")
    monkeypatch.setenv("UNRELATED_SQL", "INIT_SQL_SHOULD_NOT_APPEAR;")

    config = config_from_env()

    assert config.init_sql == "m;z;a;"


@pytest.mark.parametrize(
    ("source_type", "expected_type"),
    [
        ("duckdb_relation", DuckDBPyRelation),
        ("narwhals_duckdb", DuckDBPyRelation),
        ("spark_dataframe", DuckDBDataFrame),
        ("narwhals_spark", DuckDBDataFrame),
    ],
)
def test_to_table_creates_table_from_supported_inputs(tmp_path: Path, source_type: str, expected_type: type):
    cfg = DataAccessConfig(
        path_query="",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    source_sql = "SELECT * FROM (VALUES (2, 'b'), (1, 'a')) AS t(x, y)"

    with DataAccess(cfg) as da:
        if source_type == "duckdb_relation":
            source = da.duckdb_dataframe_from_sql(source_sql)
        elif source_type == "narwhals_duckdb":
            source = da.narwhals_duckdb_dataframe_from_sql(source_sql)
        elif source_type == "spark_dataframe":
            source = da.spark_dataframe_from_sql(source_sql)
        else:
            source = da.narwhals_spark_dataframe_from_sql(source_sql)

        result = da.to_table(source, f"created_{source_type}")

        assert isinstance(result, expected_type)
        assert da.to_polars(result).sort("x").to_dict(as_series=False) == {
            "x": [1, 2],
            "y": ["a", "b"],
        }
        created = da.duckdb_dataframe_from_sql(f"FROM created_{source_type} ORDER BY x")
        assert da.to_polars(created).to_dict(as_series=False) == {
            "x": [1, 2],
            "y": ["a", "b"],
        }


def test_to_table_keeps_existing_table_unless_replace_is_true(tmp_path: Path):
    cfg = DataAccessConfig(
        path_query="",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )

    with DataAccess(cfg) as da:
        da.to_table(da.duckdb_dataframe_from_sql("SELECT 1 AS x"), "replace_target")
        da.to_table(da.duckdb_dataframe_from_sql("SELECT 2 AS x"), "replace_target")

        assert da.to_polars(da.duckdb_dataframe_from_sql("FROM replace_target")).to_dict(as_series=False) == {"x": [1]}

        result = da.to_table(da.duckdb_dataframe_from_sql("SELECT 2 AS x"), "replace_target", replace=True)

        assert isinstance(result, DuckDBPyRelation)
        assert da.to_polars(result).to_dict(as_series=False) == {"x": [2]}
        assert da.to_polars(da.duckdb_dataframe_from_sql("FROM replace_target")).to_dict(as_series=False) == {"x": [2]}


def test_to_table_can_create_temporary_table(tmp_path: Path):
    cfg = DataAccessConfig(
        path_query="",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )

    with DataAccess(cfg) as da:
        result = da.to_table(
            da.duckdb_dataframe_from_sql("SELECT 1 AS x"),
            "temporary_target",
            temporary=True,
        )

        assert da.to_polars(result).to_dict(as_series=False) == {"x": [1]}
        assert da.to_polars(da.duckdb_dataframe_from_sql("FROM temporary_target")).to_dict(as_series=False) == {"x": [1]}

        metadata = da.duckdb_dataframe_from_sql(
            "SELECT table_type FROM information_schema.tables "
            "WHERE table_name = 'temporary_target'"
        )
        assert metadata.fetchall() == [("LOCAL TEMPORARY",)]


def test_data_access_loads_csv_as_duckdb_dataframe(tmp_path: Path):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        path_query="FROM glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        frame = da.duckdb_dataframe("sample")
        columns = frame.columns
        rows = frame.fetchall()

    assert columns == ["x", "y"]
    assert rows == [(1, "a"), (2, "b")]


def test_data_access_loads_multiple_parquet_paths_with_extension_reader(tmp_path: Path):
    first_path = tmp_path / "part-000.parquet"
    second_path = tmp_path / "part-001.parquet"
    pl.DataFrame({"x": [1], "y": ["a"]}).write_parquet(first_path)
    pl.DataFrame({"x": [2], "y": ["b"]}).write_parquet(second_path)

    cfg = DataAccessConfig(
        path_query="",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        frame = da.duckdb_dataframe("sample", str(first_path), str(second_path))
        columns = frame.columns
        rows = frame.order("x").fetchall()

    assert columns == ["x", "y"]
    assert rows == [(1, "a"), (2, "b")]


def test_data_access_loads_csv_as_spark_dataframe(tmp_path: Path):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        path_query="FROM glob('{projroot}/{dataset}.csv')",
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
        path_query="FROM glob('{projroot}/{dataset}.csv')",
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
        path_query="FROM glob('{projroot}/{dataset}.csv')",
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
        path_query="glob('{projroot}/{dataset}.csv')",
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
        path_query="FROM glob('{projroot}/{dataset}.csv')",
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
        path_query="FROM glob('{projroot}/{dataset}.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    with DataAccess(cfg) as da:
        assert to_polars(da.duckdb_dataframe("sample")).equals(to_polars(da.narwhals_duckdb_dataframe("sample")))

def test_conversions(tmp_path: Path):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        path_query="FROM glob('{projroot}/{dataset}.csv')",
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
