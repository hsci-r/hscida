from pathlib import Path

import hscida as hs
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
