#' Get tibble of database schema
#' @importFrom readr read_csv
#' @export
repel_schema <- function(){
    readr::read_csv(system.file("schema.csv", package = "wahis"))
}