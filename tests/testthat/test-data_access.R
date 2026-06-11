library(hscida)

test_that("config_from_env reads values from .env", {
  Sys.unsetenv(c("GLOB_PATTERN", "INIT_SQL", "DUCKDB_CONFIG", "PROJROOT"))

  tmp_root <- tempfile("dotenv-root-")
  dir.create(tmp_root)

  testthat::local_mocked_bindings(
    here = function(...) file.path(tmp_root, ...),
    .package = "here"
  )

  writeLines(c(
    "GLOB_PATTERN=glob('{projroot}/{dataset}/*.csv')",
    "INIT_SQL=SELECT 42",
    "DUCKDB_CONFIG=threads=1,enable_fsst_vectors=true",
    "PROJROOT=/tmp/from-dotenv"
  ), file.path(tmp_root, ".env"))

  cfg <- hscida:::config_from_env()

  expect_equal(cfg$glob_pattern, "glob('{projroot}/{dataset}/*.csv')")
  expect_equal(cfg$init_sql, "SELECT 42")
  expect_equal(cfg$duckdb_config, list(threads = "1", enable_fsst_vectors = "true"))
  expect_equal(cfg$projroot, "/tmp/from-dotenv")
})

test_that("config_from_env also reads .env.secret", {
  Sys.unsetenv(c("GLOB_PATTERN", "INIT_SQL", "DUCKDB_CONFIG", "PROJROOT"))

  tmp_root <- tempfile("dotenv-root-")
  dir.create(tmp_root)

  testthat::local_mocked_bindings(
    here = function(...) file.path(tmp_root, ...),
    .package = "here"
  )

  writeLines(c(
    "GLOB_PATTERN=glob('{projroot}/{dataset}/*.parquet')",
    "INIT_SQL=SELECT 7",
    "DUCKDB_CONFIG=threads=4",
    "PROJROOT=/tmp/from-secret"
  ), file.path(tmp_root, ".env.secret"))

  cfg <- hscida:::config_from_env()

  expect_equal(cfg$glob_pattern, "glob('{projroot}/{dataset}/*.parquet')")
  expect_equal(cfg$init_sql, "SELECT 7")
  expect_equal(cfg$duckdb_config, list(threads = "4"))
  expect_equal(cfg$projroot, "/tmp/from-secret")
})

test_that("config_from_env concatenates INIT_SQL and INIT_SQL_1,2,3", {
  Sys.unsetenv(c(
    "GLOB_PATTERN", "INIT_SQL", "INIT_SQL_1", "INIT_SQL_2", "INIT_SQL_3", "INIT_SQL_4",
    "DUCKDB_CONFIG", "PROJROOT"
  ))

  Sys.setenv(
    INIT_SQL = "CREATE TEMP TABLE t (x INTEGER);",
    INIT_SQL_1 = "INSERT INTO t VALUES (1);",
    INIT_SQL_2 = "INSERT INTO t VALUES (2);",
    INIT_SQL_3 = "SELECT * FROM t;"
  )

  cfg <- hscida:::config_from_env()

  expect_equal(
    cfg$init_sql,
    "CREATE TEMP TABLE t (x INTEGER);INSERT INTO t VALUES (1);INSERT INTO t VALUES (2);SELECT * FROM t;"
  )
})

test_that("config_from_env builds init_sql from env var names", {
  tmp_root <- tempfile("dotenv-root-")
  dir.create(tmp_root)

  testthat::local_mocked_bindings(
    here = function(...) file.path(tmp_root, ...),
    .package = "here"
  )

  init_sql_names <- names(Sys.getenv())[
    stringr::str_starts(names(Sys.getenv()), "INIT_SQL")
  ]
  env_names <- unique(c(
    init_sql_names,
    "INIT_SQL", "INIT_SQL_A", "INIT_SQL_Z", "UNRELATED_SQL"
  ))
  old_env <- Sys.getenv(env_names, unset = NA_character_)

  on.exit({
    Sys.unsetenv(env_names)
    values_to_restore <- old_env[!is.na(old_env)]
    if (length(values_to_restore) > 0) {
      do.call(Sys.setenv, as.list(values_to_restore))
    }
  }, add = TRUE)

  Sys.unsetenv(env_names)
  Sys.setenv(
    INIT_SQL = "m;",
    INIT_SQL_A = "z;",
    INIT_SQL_Z = "a;",
    UNRELATED_SQL = "INIT_SQL_SHOULD_NOT_APPEAR;"
  )

  cfg <- hscida:::config_from_env()

  expect_equal(cfg$init_sql, "m;z;a;")
})

test_that("data_access can register and query csv", {
  csv_path <- tempfile(fileext = ".csv")
  readr::write_csv(
    tibble::tribble(
      ~x, ~y,
      1, "a",
      2, "b"
    ),
    csv_path
  )

  cfg <- list(
    glob_pattern = "glob('{projroot}/{dataset}/*.csv')",
    init_sql = "SELECT 1",
    duckdb_config = list(),
    projroot = tempdir()
  )

  da <- data_access(cfg)
  on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

  tbl <- da$f("sample", csv_path, replace = FALSE, debug = FALSE)
  out <- dplyr::collect(tbl)

  expect_equal(nrow(out), 2)
  expect_true(all(c("x", "y") %in% names(out)))
})

test_that("data_access caches datasets", {
  csv_path <- tempfile(fileext = ".csv")
  readr::write_csv(
    tibble::tribble(
      ~v,
      10
    ),
    csv_path
  )

  cfg <- list(
    glob_pattern = "glob('{projroot}/{dataset}/*.csv')",
    init_sql = "SELECT 1",
    duckdb_config = list(),
    projroot = tempdir()
  )

  da <- data_access(cfg)
  on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

  first <- da$f("cache", csv_path)
  second <- da$f("cache")

  expect_identical(first, second)
})
