library(hscida)

test_that("config_from_env reads values from .env", {
  Sys.unsetenv(c("PATH_QUERY", "LIST_DATASETS_QUERY", "INIT_SQL", "PROJROOT"))

  tmp_root <- tempfile("dotenv-root-")
  dir.create(tmp_root)

  testthat::local_mocked_bindings(
    here = function(...) file.path(tmp_root, ...),
    .package = "here"
  )

  writeLines(c(
    "PATH_QUERY=FROM glob('{projroot}/{dataset}/*.csv')",
    "LIST_DATASETS_QUERY=SELECT dataset FROM glob('{projroot}/*')",
    "INIT_SQL=SELECT 42",
    "PROJROOT=/tmp/from-dotenv"
  ), file.path(tmp_root, ".env"))

  cfg <- hscida:::config_from_env()

  expect_equal(cfg$path_query, "FROM glob('{projroot}/{dataset}/*.csv')")
  expect_equal(cfg$list_datasets_query, "SELECT dataset FROM glob('{projroot}/*')")
  expect_equal(cfg$init_sql, "SELECT 42")
  expect_equal(cfg$projroot, "/tmp/from-dotenv")
})

test_that("config_from_env also reads .env.secret", {
  Sys.unsetenv(c("PATH_QUERY", "INIT_SQL", "VIEW_DEFINITION_QUERY", "PROJROOT"))

  tmp_root <- tempfile("dotenv-root-")
  dir.create(tmp_root)

  testthat::local_mocked_bindings(
    here = function(...) file.path(tmp_root, ...),
    .package = "here"
  )

  writeLines(c(
    "PATH_QUERY=FROM glob('{projroot}/{dataset}/*.parquet')",
    "INIT_SQL=SELECT 7",
    "VIEW_DEFINITION_QUERY=CREATE{or_replace} VIEW{if_not_exists} {dataset} AS SELECT 42;",
    "PROJROOT=/tmp/from-secret"
  ), file.path(tmp_root, ".env.secret"))

  cfg <- hscida:::config_from_env()

  expect_equal(cfg$path_query, "FROM glob('{projroot}/{dataset}/*.parquet')")
  expect_equal(cfg$init_sql, "SELECT 7")
  expect_equal(cfg$view_definition_query, "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS SELECT 42;")
  expect_equal(cfg$projroot, "/tmp/from-secret")
})

test_that("config_from_env concatenates INIT_SQL and INIT_SQL_1,2,3", {
  Sys.unsetenv(c(
    "PATH_QUERY", "INIT_SQL", "INIT_SQL_1", "INIT_SQL_2", "INIT_SQL_3", "INIT_SQL_4",
    "VIEW_DEFINITION_QUERY", "PROJROOT"
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
    path_query = "FROM glob('{projroot}/{dataset}/*.csv')",
    init_sql = "SELECT 1",
    view_definition_query = "CREATE OR REPLACE VIEW {dataset} AS FROM {source}",
    projroot = tempdir()
  )

  da <- data_access(cfg)
  on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

  tbl <- da$f("sample", csv_path, replace = FALSE, debug = FALSE)
  out <- dplyr::collect(tbl)

  expect_equal(nrow(out), 2)
  expect_true(all(c("x", "y") %in% names(out)))
})

test_that("data_access discovers files without explicit paths", {
  project_root <- tempfile("data-root-")
  dataset_dir <- file.path(project_root, "sample")
  dir.create(dataset_dir, recursive = TRUE)
  readr::write_csv(
    tibble::tribble(
      ~x, ~y,
      1, "a",
      2, "b"
    ),
    file.path(dataset_dir, "part.csv")
  )

  cfg <- list(
    path_query = "FROM glob('{projroot}/{dataset}/*.csv')",
    init_sql = "SELECT 1",
    view_definition_query = "CREATE OR REPLACE VIEW {dataset} AS FROM {source}",
    projroot = project_root
  )

  da <- data_access(cfg)
  on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

  out <- da$f("sample") |>
    dplyr::arrange(x) |>
    dplyr::collect()

  expect_equal(out$x, c(1, 2))
  expect_equal(out$y, c("a", "b"))
})

test_that("data_access can register and query multiple parquet paths", {
  first_path <- tempfile(fileext = ".parquet")
  second_path <- tempfile(fileext = ".parquet")

  writer <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  on.exit(DBI::dbDisconnect(writer, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(writer, "CREATE TABLE first_part AS SELECT 1 AS x, 'a' AS y")
  DBI::dbExecute(writer, "CREATE TABLE second_part AS SELECT 2 AS x, 'b' AS y")
  DBI::dbExecute(writer, glue::glue("COPY first_part TO '{first_path}' (FORMAT PARQUET)"))
  DBI::dbExecute(writer, glue::glue("COPY second_part TO '{second_path}' (FORMAT PARQUET)"))

  cfg <- list(
    path_query = "FROM glob('{projroot}/{dataset}/*.parquet')",
    init_sql = "SELECT 1",
    view_definition_query = "CREATE OR REPLACE VIEW {dataset} AS FROM {source}",
    projroot = tempdir()
  )

  da <- data_access(cfg)
  on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

  tbl <- da$f("sample", first_path, second_path, replace = FALSE, debug = FALSE)
  out <- tbl |>
    dplyr::arrange(x) |>
    dplyr::collect()

  expect_equal(out$x, c(1, 2))
  expect_equal(out$y, c("a", "b"))
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
    path_query = "FROM glob('{projroot}/{dataset}/*.csv')",
    init_sql = "SELECT 1",
    view_definition_query = "CREATE OR REPLACE VIEW {dataset} AS FROM {source}",
    projroot = tempdir()
  )

  da <- data_access(cfg)
  on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

  first <- da$f("cache", csv_path)
  second <- da$f("cache")

  expect_identical(first, second)
})

test_that("list_datasets discovers datasets via glob when configured", {
  project_root <- tempfile("data-root-")
  dir.create(file.path(project_root, "alpha"), recursive = TRUE)
  dir.create(file.path(project_root, "beta"), recursive = TRUE)
  readr::write_csv(tibble::tibble(x = 1), file.path(project_root, "alpha", "part.csv"))
  readr::write_csv(tibble::tibble(x = 1), file.path(project_root, "beta", "part.csv"))

  cfg <- list(
    path_query = "FROM glob('{projroot}/{dataset}/*.csv')",
    list_datasets_query = "SELECT DISTINCT regexp_extract(file, '{projroot}/([^/]+)', 1) AS dataset FROM glob('{projroot}/*/*.csv')",
    init_sql = "SELECT 1",
    view_definition_query = "CREATE OR REPLACE VIEW {dataset} AS FROM {source}",
    projroot = project_root
  )

  da <- data_access(cfg)
  on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

  expect_equal(da$list_datasets(), c("alpha", "beta"))
})

test_that("list_datasets warns and returns empty when not configured", {
  cfg <- list(
    path_query = "FROM glob('{projroot}/{dataset}/*.csv')",
    list_datasets_query = "",
    init_sql = "SELECT 1",
    view_definition_query = "CREATE OR REPLACE VIEW {dataset} AS FROM {source}",
    projroot = tempdir()
  )

  da <- data_access(cfg)
  on.exit(DBI::dbDisconnect(da$con, shutdown = TRUE), add = TRUE)

  expect_warning(result <- da$list_datasets(), "LIST_DATASETS_QUERY")
  expect_equal(result, character(0))
})
