#' Generic function to get master list of reports
#' Works for outbreak and six-month reports
#' @return A tibble of reports
#' @noRd
#' @import purrr dplyr tidyr
#' @importFrom janitor clean_names
#' @importFrom httr POST content
#' @importFrom assertthat assert_that
scrape_report_list <- function(post_url) {
    
    page_size <- 1000000L
    # start_date <- "2000-01-01"
    # end_date = Sys.Date()
    
    body_data <-
        list(
            pageNumber = 1L,
            pageSize = page_size,
            searchText = "",
            sortColName = "",
            sortColOrder = "ASC",
            # reportFilters = list(reportDate = list(
            #     startDate = start_date, endDate = end_date
            # )),
            languageChanged = FALSE )
    
    
    report_list_response <- POST(
        post_url,
        body = body_data,
        # add_headers('token' = "#PIPRD202006#",
        #             'accept-language' = "en"),
        encode = "json")
    
    
    report_list <- content(report_list_response)[[2]]
    
    assertthat::assert_that(length(report_list) < page_size) # otherwise you are not retrieving all the results
    
    reports <- map_dfr(report_list, as_tibble) %>% 
        janitor::clean_names()
    
    return(reports)
}

#' Get master list of outbreak reports
#' @return A tibble of reports
#' @export
scrape_outbreak_report_list <- function() {
    scrape_report_list("https://wahis.oie.int/pi/getReportList")
}


#' Get master list of outbreak reports
#' @return A tibble of reports
#' @export
scrape_six_month_report_list <- function() {
    scrape_report_list("https://wahis.oie.int/smr/pi/reports")
}

