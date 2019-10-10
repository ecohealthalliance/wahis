.onAttach <- function(libname, pkgname) {
    MonetDBLite::monetdblite_shutdown()
    if (interactive() && Sys.getenv("RSTUDIO") == "1") {
        wahis_pane()
    }
}

#' Remove the local WAHIS database
#'
#' Deletes all tables from the local database
#'
#' @return NULL
#' @export
#' @importFrom DBI dbListTables dbRemoveTable
#'
#' @examples
#' \donttest{
#' \dontrun{
#' wahis_db_delete()
#' }
#' }
wahis_db_delete <- function() {
    for (t in dbListTables(wahis_db())) {
        dbRemoveTable(wahis_db(), t)
    }
    update_wahis_pane()
}

#' Get the status of the current local WAHIS database
#'
#' @param verbose Whether to print a status message
#'
#' @return TRUE if the database exists, FALSE if it is not detected. (invisible)
#' @export
#' @importFrom DBI dbExistsTable
#' @importFrom tools toTitleCase
#' @examples
#' wahis_status()
wahis_status <- function(verbose = TRUE) {
    if (DBI::dbExistsTable(wahis_db(), "wahis_status")) {
        status <- DBI::dbReadTable(wahis_db(), "wahis_status")
        status_msg <-
            paste0(
                "WAHIS database status:\n",
                paste0(toTitleCase(gsub("_", " ", names(status))),
                       ": ", as.matrix(status),
                       collapse = "\n"
                )
            )
        out <- TRUE
    } else {
        status_msg <- "Local WAHIS database empty or corrupt. Download with wahis_db_download()" #nolint
        out <- FALSE
    }
    if (verbose) message(status_msg)
    invisible(out)
}
