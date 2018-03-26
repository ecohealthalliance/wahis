#' Extract Info from WAHIS Weekly Disease Information
#'
#' Extract the information found in the weekly OIE World Animal Health Information
#' System (WAHIS) HTML reports. These reports have to be provided to this function.
#'
#' @param web_page Name of the downloaded web page.
#'
#' @return A list with elements:
#' \item{id}{Record id.}
#' \item{country}{Country of outbreak.}
#' \item{title}{Name of disease.}
#' \item{received}{Information on reporting authority.}
#' \item{Report type}{Type of report}
#' \item{Date of start of the event}{Self-explanatory.}
#' \item{Date of confirmation of the event}{id.}
#' \item{Report date}{id.}
#' \item{Date submitted to OIE}{id.}
#' \item{Reason for notification}{id.}
#' \item{Manifestation of disease}{id.}
#' \item{Causal agent}{id.}
#' \item{Nature of diagnosis}{id.}
#' \item{This event pertains to}{Extent of the outbreak.}
#' \item{Related reports}{Self-explanatory.}
#' \item{Outbreak_i}{i lists of outbreaks with following elements: location,
#' start date of the outbreak, outbreak status, epidemiological unit, and
#' affected animals (as a table).}
#' \item{epi_source}{Source of the outbreak.}
#' \item{control_measures}{Vector with the various control measures put in place.}
#' @examples
#' ##ingest_wahis_record("../inst/raw_wahis_pages/25385.html")
#' @export
#' @import rvest stringi
ingest_wahis_record <- function(web_page) {
    page <- read_html(web_page)
    record <- list()
    if (length(page) < 2) {
        return(NULL)
    }
    record$id <- html_node(page, xpath="//div[@class='MidBigTable']//a") %>% 
        html_attr("name") %>% 
        stri_extract_last_regex("(?<=rep_)\\d+$")
    if(is.na(record$id)) {
        return(NULL)
    }
    title_country <- 
        html_nodes(page, xpath="//div[@class='Rap12-Subtitle']//text()") 
    record$title <- title_country[[1]] %>% html_text() %>% stri_replace_last_regex(",$", "")
    record$country <- title_country[[2]] %>% html_text()
    
    record$received <- html_nodes(page, xpath = "//td[@class='topbigtabletitle27']") %>% 
        html_text(trim = TRUE) %>% 
        stri_replace_all_regex("[\\s\\r\\n]+", " ")
    
    summary_table <- html_node(page, xpath="//table[@class='TableFoyers']") %>% 
        html_table()
    summary_table <- structure(as.list(summary_table[,2]), .Names=summary_table[,1])
    summary_table[["Related reports"]] <- html_nodes(page, xpath="//tr//td//a") %>% 
        html_attr("href") %>% 
        stri_extract_last_regex("(?<=\\')\\d+(?=\\'\\))") %>% 
        sort() %>%
        toString()
    if (!exists('Date of previous occurrence', where = summary_table)) {
        summary_table[["Date of previous occurrence"]] <- ""
    }
        
    outbreaks <- if (length(html_nodes(page, xpath="//tr//td[contains(.,'There are no new outbreaks in this report')]")) !=0) {
                     record$outbreaks = "There are no new outbreaks in this report"
                 } else if (length(html_nodes(page, xpath="//div[@class='ReviewSubmitBox']/table")) == 0) {
                     record$outbreaks = "There are no new outbreaks in this report"
                 } else {
                     outbreak_tables <- html_nodes(page, xpath="//div[@class='ReviewSubmitBox']/table")[-1]
                     outbreak_tables <- outbreak_tables[-length(outbreak_tables)]
                     outbreak_tables <- lapply(outbreak_tables, function(ob) {
                         names <- html_nodes(ob, xpath = "tr/td[1]") %>% html_text()
                         contents <- html_nodes(ob, xpath = "tr/td[2][not(table)]") %>%
                             html_text %>%
                             as.list()
                         cases <- html_nodes(ob, xpath="tr/td/table")[[1]] %>%
                             html_table(header=TRUE)
                         return(structure(c(contents, list(cases)), .Names=names))
                     })
                 }
    names(outbreaks) <- paste("Outbreak_", 1:length(outbreaks), sep = "")

    epi_source <- html_nodes(page, xpath = "//li")
#    control_measures <- epi_source[2:(length(epi_source)-1)] %>%
#        html_text(trim = TRUE)  # avoid getting it for now, not constant across web pages
    epi_source <- epi_source[1] %>% html_text(trim = TRUE)

    record <- c(record, summary_table, outbreaks,
                epi_source = epi_source#, list(control_measures = control_measures)
                )

    return(record)
}
