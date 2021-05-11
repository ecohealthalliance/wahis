
#' Extract Info from WAHIS Weekly Disease Information API
#' @param report_info_id url id for outbreak report
#' @importFrom httr GET RETRY content
#' @export
ingest_outbreak_report <- function(report_info_id){
    
    web_page <- paste0("https://wahis.oie.int/pi/getReport/", report_info_id) 
    
    req <- httr::RETRY(verb = "GET",
                       url = web_page, 
                       times = 3,
                       encode = "json",
                      httr::add_headers(`Accept-Language` = "en-US;"))
    
    out <- httr::content(req, as="parsed")
    out$report_info_id <- report_info_id
    
    return(out)
}

#' Function to safely run ingest_outbreak_report
#' @param report_info_id url id for outbreak report
#' @import purrr
#' @export
safe_ingest_outbreak2 <- function(report_info_id) {
    out <- safely(ingest_outbreak_report2)(report_info_id)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(list(ingest_status = paste("ingestion error: ", out$error)))
    }
}
