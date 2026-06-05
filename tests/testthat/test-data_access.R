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
