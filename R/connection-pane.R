sql_action <- function() {
    if (requireNamespace("rstudioapi", quietly = TRUE) &&
        exists("documentNew", asNamespace("rstudioapi"))) {
        contents <- paste(
            "-- !preview conn=wahis::wahis_db()",
            "",
            "SELECT * FROM annual_reports_metadata LIMIT 100",
            "",
            sep = "\n"
        )
        
        rstudioapi::documentNew(
            text = contents, type = "sql",
            position = rstudioapi::document_position(2, 40),
            execute = FALSE
        )
    }
}

#' Open WAHIS database connection pane in RStudio
#'
#' This function launches the RStudio "Connection" pane to interactively
#' explore the database.
#'
#' @return NULL
#' @export
#'
#' @examples
#' if (!is.null(getOption("connectionObserver"))) wahis_pane()
wahis_pane <- function() {
    observer <- getOption("connectionObserver")
    if (!is.null(observer)) {
        observer$connectionOpened(
            type = "WAHIS",
            host = "wahis",
            displayName = "WAHIS Datapaase Tables",
            icon = system.file("img", "eha_logo.png", package = "wahis"),
            connectCode = "wahis::wahis_pane()",
            disconnect = wahis::wahis_disconnect,
            listObjectTypes = function() {
                list(
                    table = list(contains = "data")
                )
            },
            listObjects = function(type = "datasets") {
                tbls <- DBI::dbListTables(wahis_db())
                data.frame(
                    name = tbls,
                    type = rep("table", length(tbls)),
                    stringsAsFactors = FALSE
                )
            },
            listColumns = function(table) {
                res <- DBI::dbSendQuery(wahis_db(),
                                        paste("SELECT * FROM", table, "LIMIT 1"))
                on.exit(DBI::dbClearResult(res))
                data.frame(
                    name = res@env$info$names, type = res@env$info$types,
                    stringsAsFactors = FALSE
                )
            },
            previewObject = function(rowLimit, table) {  #nolint
                DBI::dbGetQuery(wahis_db(),
                                paste("SELECT * FROM", table, "LIMIT", rowLimit))
            },
            actions = list(
                Status = list(
                    icon = system.file("img", "oie-logo.png", package = "wahis"),
                    callback = wahis_status
                ),
                SQL = list(
                    icon = system.file("img", "edit-sql.png", package = "wahis"),
                    callback = sql_action
                )
            ),
            connectionObject = wahis_db()
        )
    }
}

update_wahis_pane <- function() {
    observer <- getOption("connectionObserver")
    if (!is.null(observer)) {
        observer$connectionUpdated("wahis", "wahis", "")
    }
}
