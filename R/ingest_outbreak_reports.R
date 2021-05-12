
#' Extract Info from WAHIS Weekly Disease Information API
#' @param web_page url for outbreak report
#' @importFrom httr GET RETRY content
#' @export
ingest_outbreak_report <- function(web_page){
    
    req <- httr::RETRY(verb = "GET",
                       url = web_page, 
                       times = 3,
                       encode = "json",
                      httr::add_headers(`Accept-Language` = "en-US;"))
    
    out <- httr::content(req, as="parsed")
    out$report_info_id <- as.integer(basename(web_page))
    
    return(out)
}

#' Function to safely run ingest_outbreak_report
#' @param web_page url for outbreak report
#' @import purrr
#' @export
safe_ingest_outbreak <- function(web_page) {
    out <- safely(ingest_outbreak_report)(web_page)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(list(ingest_status = paste("ingestion error: ", out$error)))
    }
}
