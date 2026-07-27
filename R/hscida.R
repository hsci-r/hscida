ignore_unused_imports <- function() {
  dbplyr::sql
}

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
  init_sql <- Sys.getenv() |>
    tibble::enframe() |>
    dplyr::filter(name |> stringr::str_starts("INIT_SQL")) |>
    dplyr::arrange(name) |>
    dplyr::pull(value) |>
    stringr::str_c(collapse = "")
  list(
    path_query = Sys.getenv("PATH_QUERY"),
    init_sql = init_sql,
    view_definition_query = Sys.getenv("VIEW_DEFINITION_QUERY", "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS FROM {source};"),
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
#'   path_query = "FROM glob('{projroot}/{dataset}/*.csv')",
#'   init_sql = "SELECT 1",
#'   view_definition_query = "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS FROM {source};",
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
    bigint = "integer64"
  )
  DBI::dbExecute(con, glue::glue(config$init_sql))
  datasets <- new.env(parent = emptyenv())
  register_files_as_view <- function(dataset, paths, replace = FALSE) {
    if (length(paths) == 1) {
      source <- stringr::str_c("'", paths[1], "'")
    } else {
      reader <- ifelse(
        stringr::str_ends(paths[1], stringr::regex("\\.(tsv|tsv\\.gz|csv\\.gz)$")),
        "read_csv",
        stringr::str_c("read_", stringr::str_extract(paths[1], stringr::regex("[^.]+$")))
      )
      paths_sql <- stringr::str_c("'", paths, "'", collapse = ", ")
      source <- glue::glue("{reader}([{paths_sql}], hive_partitioning = true)")
    }
    query <- glue::glue(config$view_definition_query, .open = "{", .close = "}", or_replace = ifelse(replace, " OR REPLACE", ""), if_not_exists = ifelse(replace, "", " IF NOT EXISTS"), dataset = dataset, source = source)
    DBI::dbExecute(con, query)
  }
  f <- function(dataset, ..., replace = FALSE, debug = FALSE) {
    if (!exists(dataset, envir = datasets) || replace) {
      paths <- unlist(list(...), use.names = FALSE)
      if (length(paths) == 0)
        paths <- DBI::dbGetQuery(
          con,
          glue::glue_data(
            list(dataset = dataset, projroot = config$projroot),
            config$path_query
          )
        ) |>
          dplyr::pull(file)
      if (debug)
        message(glue::glue("Registering dataset {dataset} with paths: {paths}"))
      if (length(paths) == 0) {
        warning(glue::glue("No files found for dataset {dataset} in {config[['path_query']]}"))
        return(invisible(NULL))
      }
      register_files_as_view(dataset, paths, replace = replace)
      assign(dataset, dplyr::tbl(
        con,
        dataset
      ), envir = datasets)
    }
    get(dataset, envir = datasets)
  }
  list(con = con, f = f)
}
