ignore_unused_imports <- function() {
  dbplyr::sql
}

#' Read data access configuration from environment variables.
#'
#' Loads values from the process environment (and optional `.env` file), then
#' builds the configuration list consumed by the package data access helpers.
#'
#' @return A list containing `path_query`, `list_datasets_query`, `init_sql`,
#'   `view_definition_query`, and `projroot`.
#' @importFrom rlang .data
#' @keywords internal
#' @noRd
config_from_env <- function() {
  try(dotenv::load_dot_env(here::here(".env")), silent = TRUE)
  try(dotenv::load_dot_env(here::here(".env.secret")), silent = TRUE)
  init_sql <- Sys.getenv() |>
    tibble::enframe() |>
    dplyr::filter(.data$name |> stringr::str_starts("INIT_SQL")) |>
    dplyr::arrange(.data$name) |>
    dplyr::pull(.data$value) |>
    stringr::str_c(collapse = "")
  list(
    path_query = Sys.getenv("PATH_QUERY"),
    list_datasets_query = Sys.getenv("LIST_DATASETS_QUERY"),
    init_sql = init_sql,
    view_definition_query = Sys.getenv("VIEW_DEFINITION_QUERY", "CREATE{or_replace} VIEW{if_not_exists} {dataset} AS FROM {source};"),
    projroot = Sys.getenv("PROJROOT", here::here())
  )
}

#' Create a data access object backed by DuckDB.
#'
#' @param config A configuration list (by default from config_from_env()).
#'
#' @return A list with a DBI connection `con`, dataset accessor `f`, and
#'   dataset discovery function `list_datasets`.
#' @export
#'
#' @examples
#' cfg <- list(
#'   path_query = "FROM glob('{projroot}/{dataset}/*.csv')",
#'   list_datasets_query = "SELECT DISTINCT regexp_extract(file, '{projroot}/([^/]+)', 1) AS dataset FROM glob('{projroot}/*')",
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
  list_datasets <- function(debug = FALSE) {
    if (identical(config$list_datasets_query, "")) {
      warning("No LIST_DATASETS_QUERY configured; set the LIST_DATASETS_QUERY environment variable to enable dataset discovery.", call. = FALSE)
      return(character(0))
    }
    query <- glue::glue_data(list(projroot = config$projroot), config$list_datasets_query)
    if (debug)
      message(glue::glue("Listing datasets with query: {query}"))
    DBI::dbGetQuery(con, query)[[1]] |>
      unique() |>
      sort()
  }
  list(con = con, f = f, list_datasets = list_datasets)
}
