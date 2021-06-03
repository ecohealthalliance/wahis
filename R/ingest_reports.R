
#' Extract Info from WAHIS API
#' @param resp curl API response
#' @importFrom jsonlite fromJSON
#' @export
ingest_report <- function(resp){ 
    
    out <- fromJSON(rawToChar(resp$content))
    
    report_info_id <- suppressWarnings(as.integer(basename(resp$url)))
    
    if(!is.na(report_info_id)){
        out$report_info_id <- report_info_id
    }
    
    return(out)
}

#' Function to safely run ingest_outbreak_report
#' @param resp curl API response
#' @import purrr
#' @export
safe_ingest <- function(resp) {
    out <- safely(ingest_report)(resp)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(list(ingest_status = paste("ingestion error: ", out$error)))
    }
}
