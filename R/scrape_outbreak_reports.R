#' Extract Info from WAHIS Outbreak Reports
#' @return A vector of outbreak report IDs
#' @export
#' @import xml2 purrr dplyr tidyr stringr

scrape_outbreak_report_list <- function() {
    weekly_pg <-
        read_html("http://www.oie.int/wahis_2/public/wahid.php/Diseaseinformation/WI")
    
    report_ids <- weekly_pg %>%
        html_nodes(xpath = "//a[contains(@href, 'Reviewreport')]") %>%
        html_attr("href") %>%
        stri_extract_last_regex("(?<=\\,)\\d{3,6}(?=\\))") %>%
        as.numeric() %>%
        sort(decreasing=TRUE)
    
    return(report_ids)
}