
config_from_env <- function() {
  dotenv::load_dot_env(here::here(".env"))
  duckdb_config <- Sys.getenv("DUCKDB_CONFIG", "parquet_metadata_cache=true,preserve_insertion_order=false,enable_fsst_vectors=true") |>
    stringr::str_split_1(stringr::fixed(",")) |>
    purrr::set_names(\(pair) stringr::str_extract(pair, stringr::regex("^[^=]+"))) |>
    purrr::map(\(pair) stringr::str_extract(pair, stringr::regex("[^=]+$")))
  list(
    glob_pattern = Sys.getenv("GLOB_PATTERN"),
    init_sql = Sys.getenv("INIT_SQL"),
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
data_access <- function(config = config_from_env()) {
  con <- DBI::dbConnect(
    duckdb::duckdb(bigint = "integer64"),
    bigint = "integer64",
    config = config$duckdb_config
  )
  DBI::dbExecute(con, glue::glue(config$init_sql))
  datasets = new.env()
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
    if (!exists(dataset, envir = datasets, inherits = FALSE) || replace) {
      paths <- list(...)
      if (length(paths) == 0)
        paths <- dplyr::tbl(con, glue::glue_data(list(dataset=dataset, projroot=config$projroot), config$glob_pattern)) |>
          dplyr::pull(file)
      if (debug)
        message(glue::glue("Registering dataset {dataset} with paths: {paths}"))
      if (length(paths) == 0) {
        warning(glue::glue("No files found for dataset {dataset} in {config[['glob_pattern']]}"))
        return
      }
      register_files_as_view(dataset, paths, replace = replace)
      assign(dataset, dplyr::tbl(
        con,
        dataset
      ), envir = datasets)
    }
    get(dataset, envir = datasets, inherits = FALSE)
  }
  list(con = con, f = f)
}