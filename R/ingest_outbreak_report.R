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
#' \item{disease}{Name of disease.}
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
#' @import rvest stringi xml2 dplyr tidyr
ingest_outbreak_report <- function(web_page, encoding = "ISO-8859-1") {
    
    page <- suppressWarnings(read_xml(web_page, encoding = encoding, as_html = TRUE, options = c("RECOVER", "NOERROR", 
                                                                                                 "NOBLANKS")))
    if (length(page) < 2) {
        return(list(ingest_status = "blank page"))
    }
    record <- list()
    record$ingest_status = "available"
    record$id <- xml_find_first(page, xpath="//div[@class='MidBigTable']//a") %>% 
        xml_attr("name") %>% 
        stri_extract_last_regex("(?<=rep_)\\d+$")
    if(is.na(record$id)) {
        return(list(ingest_status = "does not exist"))
    }
    title_country <- 
        xml_find_all(page, xpath="//div[@class='Rap12-Subtitle']//text()") 
    record$disease <- title_country[[1]] %>% xml_text() %>% stri_replace_last_regex(",$", "")
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
    
    if (length(xml_find_first(page, xpath="//tr//td[contains(.,'There are no new outbreaks in this report')]")) !=0 ||
        length(xml_find_first(page, xpath="//div[@class='ReviewSubmitBox']/table")) == 0) {
        record$outbreak_detail <- "There are no new outbreaks in this report"
        record$outbreak_summary <- "There are no new outbreaks in this report"
        record$total_new_outbreaks <- "0"
    } else {
        
        # get outbreak data
        outbreak <- html_nodes(page, xpath="//div[@class='ReviewSubmitBox']/table")[-1]
        outbreak_summary <- outbreak[length(outbreak)]
        outbreak_detail <- outbreak[-length(outbreak)]
        
        # detail
        outbreak_detail <- map_df(outbreak_detail, function(x){
            
            single <- x %>%
                xml_find_all(xpath = "tr/td[2][not(table)]") 
            
            single_fields <- single  %>%
                xml_siblings() %>%
                xml_text()
            
            single_contents <-  single %>%
                xml_text() 
            
            outbreak_number <- single_fields[1]
            outbreak_location <- single_contents[1]
            
            single_fields <- single_fields[-1]
            single_contents <- single_contents[-1]
            names(single_contents) <- single_fields
            
            out <- tibble(outbreak_number, outbreak_location) 
            
            add <- t(single_contents) %>%
                as.data.frame() %>%
                as_tibble() %>%
                mutate_all(~as.character(.))
            
            out <- bind_cols(out, add)
            
            table <- xml_find_all(x, xpath = "tr/td[2][table]") 
            
            table_contents <- table %>%
                xml_children() %>%
                table_value(html_table, trim = TRUE, header=TRUE) 
            
            table_contents <- table_contents %>% reduce(bind_rows)
            
            out <- crossing(out, table_contents)
        }) %>%
            mutate(id = record$id) %>%
            select(id, everything())
        
        # summary
        table <- xml_find_all(outbreak_summary, xpath = "tr/td[2][table]") 
        
        total_outbreaks <- outbreak_summary %>%
            xml_find_all(xpath = "tr/td[2][not(table)]") %>%
            xml_text() %>%
            str_extract("[0-9]+")
        
        outbreak_summary <- table %>%
            xml_children() %>%
            table_value(html_table, trim = TRUE, header=TRUE) %>%
            reduce(full_join) %>%
            #mutate(outbreaks = total_outbreaks) %>%
            mutate(id = record$id) %>%
            select(id, everything())
        
        names(outbreak_summary)[3:ncol(outbreak_summary)] <- paste0("total_", names(outbreak_summary)[3:ncol(outbreak_summary)])
        
        record$outbreak_detail <- outbreak_detail
        record$outbreak_summary <- outbreak_summary
        record$total_new_outbreaks <- total_outbreaks
        
    }
    
    #    record$summary_table <- summary_table
    record$epi_source <- 
        xml_find_first(page, xpath = "//tr/td[contains(text(),'Source of the outbreak')]") %>% 
        xml_siblings() %>%
        xml_child() %>%
        xml_children()  %>% 
        xml_text(trim = TRUE) %>%
        paste(., collapse = "; ")
    record$epi_notes <- xml_find_first(page, xpath = "//tr/td[contains(text(),'Epidemiological comments')]/following-sibling::td[1]") %>% 
        table_value(xml_text, trim = TRUE)
    
    record$control_applied <- 
        xml_find_first(page, xpath = "//tr/td[contains(text(),'Measures applied')]") %>% #TODO - there are some cases where this is not the first mention of measures applied - eg 11825.html
        xml_siblings() %>%
        xml_child() %>%
        xml_children()  %>% 
        xml_text(trim = TRUE) %>%
        paste(., collapse = "; ")
    record$control_to_be_applied <- 
        xml_find_first(page, xpath = "//tr/td[contains(text(),'Measures to be applied')]") %>% 
        xml_siblings() %>%
        xml_child() %>%
        xml_children()  %>% 
        xml_text(trim = TRUE) %>%
        paste(., collapse = "; ")
    
    record$diagnostic_tests <- xml_find_first(page, xpath = "//div[contains(text(),'Diagnostic test results')]/following-sibling::table[1]") %>% 
        table_value(html_table, trim = TRUE, header=TRUE) 
    if(nrow(record$diagnostic_tests)){
        record$diagnostic_tests <- record$diagnostic_tests %>%
            mutate(id = record$id) %>%
            select(id, everything())
    }
    
    record$future_reporting <- xml_find_first(page, xpath = "//div[contains(text(),'Future Reporting')]/following-sibling::table[1]/tr/td") %>% 
        table_value(xml_text, trim = TRUE)
    
    return(record)
}

#' Support function for ingest_outbreak_report to clean html tables
#' @param xml xml data
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
        return(list(ingest_status = paste("ingestion error: ", out$error)))
    }
}


