
#' Extract Info from WAHIS Weekly Disease Information API
#'
#' @param start_i The report from which to start download
#' @export
#' @importFrom jsonlite fromJSON
ingest_outbreak_report2 <- function(start_i = 1){
    
    url_base <- "https://wahis.oie.int/pi/getReport/"
    
    report_avail <- TRUE
    i <- start_i # 17192
    out <- list()
    
    # reports are sequential
    while(report_avail){
        print(i)
        url <- paste0(url_base, i)
        result <- try(fromJSON(url), silent = TRUE)
        if(class(result) == "try-error"){
            report_avail <- FALSE
        }else{
            out[[i]] <- result
            out[[i]]$report_id <- i
            i <- i + 1
        }
    }
    
    return(out)
}

# transform
