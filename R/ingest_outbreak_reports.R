
#' Extract Info from WAHIS Weekly Disease Information API
#' @param resp curl API response
#' @importFrom jsonlite fromJSON
#' @export
ingest_outbreak_report <- function(resp){ 
    
    out <- fromJSON(rawToChar(resp$content))
    out$report_info_id <- as.integer(basename(resp$url))
    
    return(out)
}

#' Function to safely run ingest_outbreak_report
#' @param resp curl API response
#' @import purrr
#' @export
safe_ingest_outbreak <- function(resp) {
    out <- safely(ingest_outbreak_report)(resp)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(list(ingest_status = paste("ingestion error: ", out$error)))
    }
}
