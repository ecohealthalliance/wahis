#' Download the WAHIS database to your local computer
#'
#' This command downloads the WAHIS shipments database and populates a local
#' database. Note that you need EHA AWS access to do so for now.
#'
#' The database is stored by default under [rappdirs::user_data_dir()], or its
#' location can be set with the environment variable `WAHIS_DB_DIR`.
#'
#' @param destdir Where to download the compressed files.
#' @param cleanup Whether to delete the compressed files after loading into the database.
#' @param verbose Whether to display messages and download progress
#' @param load_aws_credentials Whether to use credentials from [aws.signature::use_credentials()].  If FALSE, AWS credentials must be provided as environment variables.

#'
#' @return NULL
#' @export
#' @importFrom arkdb unark
#' @importFrom aws.s3 save_object get_bucket_df
#' @importFrom fs dir_create path
#'
#' @examples
#' \donttest{
#' \dontrun{
#' wahis_db_download()
#' }
#' }
wahis_db_download <- function(destdir = tempfile(),
                              cleanup = TRUE, verbose = interactive(),
                              load_aws_credentials = TRUE) {
    if(load_aws_credentials && requireNamespace("aws.signature", quietly = TRUE)) {
        aws.signature::use_credentials()
    }
    if (verbose) message("Downloading data...\n")
    purrr::walk(DBI::dbListTables(wahis_db()), ~DBI::dbRemoveTable(wahis_db(), .))
    fs::dir_create(destdir)
    data_files_df <- get_bucket_df("wahis-data", prefix = "data-processed/db")
    purrr::walk(data_files_df$Key, function(key) {
        f = fs::path(destdir, basename(key))
        save_object(key, "wahis-data", file = f)
        arkdb::unark(f, wahis_db(), lines = 100000,
                     try_native = TRUE, overwrite = TRUE)
        if (cleanup) file.remove(f)
    })
    if (verbose) message("Calculating Stats...\n")
    DBI::dbWriteTable(wahis_db(), "wahis_status", make_status_table(),
                 overwrite = TRUE)
    update_wahis_pane()
    if (verbose) message("Done!")
}


#' @importFrom DBI dbGetQuery
#' @importFrom purrr map_dbl
make_status_table <- function() {
    sz <- sum(file.info(list.files(wahis_path(),
                                   all.files = TRUE,
                                   recursive = TRUE,
                                   full.names = TRUE))$size)
    class(sz) <- "object_size"
    tables <- DBI::dbListTables(wahis_db())
    records = sum(map_dbl(tables, ~DBI::dbGetQuery(wahis_db(), paste0("SELECT COUNT(*) FROM ", ., ";"))[[1]]))
    tibble(
        time_imported = Sys.time(),
        number_of_tables = length(tables),
        number_of_records = formatC(records, format = "d", big.mark = ","),
        size_on_disk = format(sz, "auto"),
        location_on_disk = wahis_path()
    )
}
