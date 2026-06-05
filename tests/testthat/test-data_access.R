library(hscida)

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
