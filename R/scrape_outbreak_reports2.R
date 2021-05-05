#' Extract Info from WAHIS Outbreak Reports
#' @return A tibble of outbreak reports metadata
#' @export
#' @import purrr dplyr tidyr
#' @importFrom janitor clean_names
#' @importFrom httr POST content
#' @importFrom assertthat assert_that

scrape_outbreak_report_list <- function(start_date = "2000-01-01", end_date = Sys.Date()) {
    
    page_size <- 1000000L
    
    body_data <-
        list(
            pageNumber = 1L,
            pageSize = page_size,
            searchText = "",
            sortColName = "",
            sortColOrder = "ASC",
            reportFilters = list(reportDate = list(
                startDate = start_date, endDate = end_date
            )),
            languageChanged = FALSE
        )
    
    report_list_response <- POST(
        "https://wahis.oie.int/pi/getReportList",
        body = body_data,
        encode = "json")
    
    report_list <- content(report_list_response)[[2]]
    
    assertthat::assert_that(length(report_list) < page_size) # otherwise you are not retrieving all the results
    
    reports <- map_dfr(report_list, as_tibble) %>% 
        janitor::clean_names()
    
    return(reports)
}
