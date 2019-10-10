#' @importFrom rappdirs user_data_dir
wahis_path <- function() {
    sys_wahis_path <- Sys.getenv("WAHIS_DB_DIR")
    if (sys_wahis_path == "") {
        return(rappdirs::user_data_dir("wahis"))
    } else {
        return(sys_wahis_path)
    }
}

wahis_check_status <- function() {
    if (!wahis_status(FALSE)) {
        stop("Local WAHIS database empty or corrupt. Download with wahis_db_download()") # nolint
    }
}

#' The local WAHIS database
#'
#' Returns a connection to the local WAHIS database. This is a DBI-compliant
#' [MonetDBLite::MonetDBLite()] database connection. When using **dplyr**-based
#' workflows, one typically accesses tables with functions such as
#' [wahis_shipments()], but this function lets one interact with the database
#' directly via SQL.
#'
#' @param dbdir The location of the database on disk. Defaults to
#' `wahis` under [rappdirs::user_data_dir()], or the environment variable `WAHIS_DB_DIR`.
#'
#' @return A MonetDBLite DBI connection
#' @importFrom DBI dbIsValid dbConnect
#' @importFrom MonetDBLite MonetDBLite
#' @export
wahis_db <- function(dbdir = wahis_path()) {
    db <- mget("wahis_db", envir = wahis_cache, ifnotfound = NA)[[1]]
    if (inherits(db, "DBIConnection")) {
        if (DBI::dbIsValid(db)) {
            return(db)
        }
    }
    dbname <- dbdir
    dir.create(dbname, FALSE)
    
    tryCatch(
        {
            gc(verbose = FALSE)
            db <- DBI::dbConnect(MonetDBLite::MonetDBLite(), dbname = dbdir)
        },
        error = function(e) {
            if (grepl("(Database lock|bad rolemask)", e)) {
                stop(paste(
                    "Local WAHIS database is locked by another R session.\n",
                    "Try closing or running wahis_disconect() in that session."
                ),
                call. = FALSE
                )
            } else {
                stop(e)
            }
        },
        finally = NULL
    )
    
    assign("wahis_db", db, envir = wahis_cache)
    db
}

#' Disconnect from the WAHIS database
#'
#' A utility function for disconnecting from the database.
#'
#' @examples
#' wahis_disconnect()
#' @export
#'
wahis_disconnect <- function() {
    wahis_disconnect_()
}
wahis_disconnect_ <- function(environment = wahis_cache) { # nolint
    db <- mget("wahis_db", envir = wahis_cache, ifnotfound = NA)[[1]]
    if (inherits(db, "DBIConnection")) {
        gc(verbose = FALSE)
        DBI::dbDisconnect(db, shutdown = TRUE)
    }
    observer <- getOption("connectionObserver")
    if (!is.null(observer)) {
        observer$connectionClosed("WAHISDB", "wahis")
    }
}

wahis_cache <- new.env()
reg.finalizer(wahis_cache, wahis_disconnect_, onexit = TRUE)
