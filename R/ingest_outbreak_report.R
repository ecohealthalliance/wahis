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
#' @import rvest stringi xml2
ingest_outbreak_report <- function(web_page, encoding = "ISO-8859-1") {
    
    base_page <- basename(web_page)
    
    page <- suppressWarnings(read_xml(web_page, encoding = encoding, as_html = TRUE, options = c("RECOVER", "NOERROR", 
                                                           "NOBLANKS")))
    if (length(page) < 2) {
        return(list(report_status = "blank page", web_page = base_page))
    }
    record <- list()
    record$report_status = "available"
    record$web_page <- base_page
    record$id <- xml_find_first(page, xpath="//div[@class='MidBigTable']//a") %>% 
        xml_attr("name") %>% 
        stri_extract_last_regex("(?<=rep_)\\d+$")
    if(is.na(record$id)) {
        return(list(report_status = "does not exist", web_page = base_page))
    }
    title_country <- 
        xml_find_all(page, xpath="//div[@class='Rap12-Subtitle']//text()") 
    record$title <- title_country[[1]] %>% xml_text() %>% stri_replace_last_regex(",$", "")
    record$country <- title_country[[2]] %>% xml_text()
    
    record$received <- xml_find_first(page, xpath = "//td[@class='topbigtabletitle27']") %>% 
        xml_text(trim = TRUE) %>% 
        stri_replace_all_regex("[\\s\\r\\n]+", " ")
    
    summary_table <- html_node(page, xpath="//table[@class='TableFoyers']") %>% 
        html_table()
    summary_table <- structure(as.list(summary_table[,2]), .Names=summary_table[,1])
    
    summary_table$related_reports <- 
        xml_find_all(page, xpath = "//tr/td[contains(text(),'Related reports')]/following-sibling::td[1]/a") %>%
        xml_attr("href") %>% 
        stri_extract_last_regex("(?<=\\')\\d+(?=\\'\\))") %>% 
        sort()
    
    summary_table$immediate_report <- xml_find_first(page, xpath = "//tr/td[contains(text(),'Related reports')]/following-sibling::td[1]/a[contains(text(), 'Immediate notification')]") %>% 
        xml_attr("href") %>% 
        stri_extract_last_regex("(?<=\\')\\d+(?=\\'\\))") 
    
    if (!exists('Date of previous occurrence', where = summary_table)) {
        summary_table[["Date of previous occurrence"]] <- NA_character_
    }
    if (!exists('Date event resolved', where = summary_table)) {
        summary_table[["Date event resolved"]] <- NA_character_
    }
    if (!exists('Nature of diagnosis', where = summary_table)) {
        summary_table[["Nature of diagnosis"]] <- NA_character_
    }
    if (!exists('Manifestation of disease', where = summary_table)) {
        summary_table[["Manifestation of disease"]] <- NA_character_
    }
    if (!exists('Causal agent', where = summary_table)) {
        summary_table[["Causal agent"]] <- NA_character_
    }
    if (!exists('Serotype', where = summary_table)) {
        summary_table[["Serotype"]] <- NA_character_
    }
        
    record <- c(record, summary_table)
    
if (length(xml_find_first(page, xpath="//tr//td[contains(.,'There are no new outbreaks in this report')]")) !=0) {
                     record$outbreaks = "There are no new outbreaks in this report"
                 } else if (length(xml_find_first(page, xpath="//div[@class='ReviewSubmitBox']/table")) == 0) {
                     record$outbreaks = "There are no new outbreaks in this report"
                 } else {
                     outbreak_tables <- html_nodes(page, xpath="//div[@class='ReviewSubmitBox']/table")[-1]
                     outbreak_tables <- outbreak_tables[-length(outbreak_tables)]
                     outbreak_tables <- lapply(seq_along(outbreak_tables), function(i) {
                         names <- html_nodes(outbreak_tables[i], xpath = "tr/td[1]") %>% xml_text()
                         contents <- html_nodes(outbreak_tables[i], xpath = "tr/td[2][not(table)]") %>%
                             xml_text %>%
                             as.list()
                         cases <- html_nodes(outbreak_tables[i], xpath="tr/td/table")[[1]] %>%
                             table_value(html_table, trim = TRUE, header=TRUE)
                         cases2 <- structure(list(cases), .Names = "Cases")
                         cases2 <- mapply(cbind, cases2, "id" = record$id,
                                          SIMPLIFY = FALSE)  # could also use: Map(cbind, cases2, id = record$id) which is short for mapply()
                         return(c(structure(c(contents, list(cases)), .Names=names),
                                  cases2))
                     })
                     record$outbreaks <- outbreak_tables
                     
                 }

#    record$summary_table <- summary_table
    record$epi_source <- xml_find_first(page, xpath = "//tr/td[contains(text(),'Source of the outbreak')]/following-sibling::td[1]//li") %>% 
        table_value(xml_text, trim = TRUE)
    record$epi_notes <- xml_find_first(page, xpath = "//tr/td[contains(text(),'Epidemiological comments')]/following-sibling::td[1]") %>% 
        table_value(xml_text, trim = TRUE)
    
    record$control_applied <- xml_find_first(page, xpath = "//tr/td[contains(text(),'Measures applied')]/following-sibling::td[1]//li") %>% 
        xml_text(trim=TRUE)
    record$control_to_be_applied <- xml_find_first(page, xpath = "//tr/td[contains(text(),'Measures to be applied')]/following-sibling::td[1]//li") %>% 
        table_value(xml_text, trim = TRUE)
    
    record$diagnostic_tests <- xml_find_first(page, xpath = "//div[contains(text(),'Diagnostic test results')]/following-sibling::table[1]") %>% 
        table_value(html_table, trim = TRUE, header=TRUE)
    
    record$future_reporting <- xml_find_first(page, xpath = "//div[contains(text(),'Future Reporting')]/following-sibling::table[1]/tr/td") %>% 
        table_value(xml_text, trim = TRUE)

    return(record)
}

#' Support function for ingest_outbreak_report to clean html tables
#' @param xml
#' @param extractor XML extract function
#' @noRd

table_value <- function(xml, extractor, ...) {
    if(class(xml) == "xml_missing") {
        return(NA)
    } else {
        return(extractor(xml, ...))
    }
}

#' Function to safely run ingest_outbreak_report
#' @param web_page Name of the downloaded web page
#' @importFrom purrr safely
#' @export

safe_ingest_outbreak <- function(web_page) {
    out <- safely(ingest_outbreak_report)(web_page)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(tibble(file = basename(web_page)))
    }
}

