
#' Read data access configuration from environment variables.
#'
#' Loads values from the process environment (and optional `.env` file), then
#' builds the configuration list consumed by the package data access helpers.
#'
#' @return A list containing `glob_pattern`, `init_sql`, `duckdb_config`, and
#'   `projroot`.
#' @keywords internal
#' @noRd
config_from_env <- function() {
  try(dotenv::load_dot_env(here::here(".env")), silent = TRUE)
  try(dotenv::load_dot_env(here::here(".env.secret")), silent = TRUE)
  duckdb_config <- Sys.getenv("DUCKDB_CONFIG", "parquet_metadata_cache=true,preserve_insertion_order=false,enable_fsst_vectors=true") |>
    stringr::str_split_1(stringr::fixed(",")) |>
    purrr::set_names(\(pair) stringr::str_extract(pair, stringr::regex("^[^=]+"))) |>
    purrr::map(\(pair) stringr::str_extract(pair, stringr::regex("[^=]+$")))
  i <- 1
  init_sql <- Sys.getenv("INIT_SQL", "")
  while(!is.na(Sys.getenv(stringr::str_c("INIT_SQL_", i), unset = NA_character_))) {
    init_sql <- stringr::str_c(init_sql, Sys.getenv(stringr::str_c("INIT_SQL_", i)))
    i <- i + 1
  }
  list(
    glob_pattern = Sys.getenv("GLOB_PATTERN"),
    init_sql = init_sql,
    duckdb_config = duckdb_config,
    projroot = Sys.getenv("PROJROOT", here::here())
  )
}

#' Create a data access object backed by DuckDB.
#'
#' @param config A configuration list (by default from config_from_env()).
#'
#' @return A list with a DBI connection `con` and dataset accessor `f`.
#' @export
#'
#' @examples
#' cfg <- list(
#'   glob_pattern = "glob('{projroot}/{dataset}/*.csv')",
#'   init_sql = "SELECT 1",
#'   duckdb_config = list(),
#'   projroot = tempdir()
#' )
#' da <- data_access(cfg)
#' DBI::dbDisconnect(da$con, shutdown = TRUE)
data_access <- function(config = config_from_env()) {
  if (!requireNamespace("bit64", quietly = TRUE)) {
    stop("Package 'bit64' is required for bigint='integer64'.", call. = FALSE)
  }

  con <- DBI::dbConnect(
    duckdb::duckdb(bigint = "integer64"),
    bigint = "integer64",
    config = config$duckdb_config
  )
  DBI::dbExecute(con, glue::glue(config$init_sql))
  datasets <- new.env(parent = emptyenv())
  register_files_as_view <- function(table_name, paths, replace = FALSE) {
    query <- glue::glue("
      CREATE {ifelse(replace, 'OR REPLACE', '')} VIEW {ifelse(replace, '', 'IF NOT EXISTS')} {table_name} AS 
      FROM read_{ifelse(grepl('\\\\.parquet$', paths[1]), 'parquet', 'csv')}(
        ['{paste(paths, collapse = \"', '\")}'], 
        hive_partitioning = true
      );
    ")
    DBI::dbExecute(con, query)
  }
  f <- function(dataset, ..., replace = FALSE, debug = FALSE) {
    if (!exists(dataset, envir = datasets) || replace) {
      paths <- list(...)
      if (length(paths) == 0)
        paths <- dplyr::tbl(
          dbplyr::src_dbi(con),
          glue::glue_data(list(dataset = dataset, projroot = config$projroot), config$glob_pattern)
        ) |>
          dplyr::pull(file)
      if (debug)
        message(glue::glue("Registering dataset {dataset} with paths: {paths}"))
      if (length(paths) == 0) {
        warning(glue::glue("No files found for dataset {dataset} in {config[['glob_pattern']]}"))
        return(invisible(NULL))
      }
      register_files_as_view(dataset, paths, replace = replace)
      assign(dataset, dplyr::tbl(
        dbplyr::src_dbi(con),
        dataset
      ), envir = datasets)
    }
    get(dataset, envir = datasets)
  }
  list(con = con, f = f)
}