from pathlib import Path

import polars as pl

from hscida import DataAccess, DataAccessConfig, config_from_env, to_pandas, to_polars


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


def test_data_access_loads_csv_and_converts(tmp_path: Path):
    dataset_path = tmp_path / "sample.csv"
    pl.DataFrame({"x": [1, 2], "y": ["a", "b"]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}/*.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    da = DataAccess(cfg)

    lazy = da.f("sample", str(dataset_path))
    polars_df = to_polars(lazy)
    pandas_df = to_pandas(lazy)

    assert polars_df.shape == (2, 2)
    assert pandas_df.shape == (2, 2)
    assert set(polars_df.columns) == {"x", "y"}


def test_data_access_caches_relation(tmp_path: Path):
    dataset_path = tmp_path / "cache.csv"
    pl.DataFrame({"v": [10]}).write_csv(dataset_path)

    cfg = DataAccessConfig(
        glob_pattern="glob('{projroot}/{dataset}/*.csv')",
        init_sql="SELECT 1",
        projroot=str(tmp_path),
    )
    da = DataAccess(cfg)

    first = da.f("cache", str(dataset_path))
    second = da.f("cache")

    assert first is second
