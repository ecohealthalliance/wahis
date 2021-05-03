
#' Extract Info from WAHIS Weekly Disease Information API
#' @param web_page API url
#' @importFrom jsonlite fromJSON
#' @export
ingest_outbreak_report2 <- function(web_page){
   fromJSON(web_page)
}

#' Function to safely run ingest_outbreak_report
#' @param web_page API url
#' @import purrr
#' @export
safe_ingest_outbreak2 <- function(web_page) {
    out <- safely(ingest_outbreak_report2)(web_page)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(list(ingest_status = paste("ingestion error: ", out$error)))
    }
}
