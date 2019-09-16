#' Extract Info from WAHIS Annual and Semi-Annual Reports
#'
#' Extract the information found in the Annual and Semi-Annual OIE World Animal Health Information
#' System (WAHIS) HTML reports. These reports have to be provided to this function.
#'
#' @param web_page Name of the downloaded web page.
#'
#' @return A list with elements:
#' \item{id}{Record id.}
#' \item{diseases_present}{Table of disease present in country over time period}
#' @examples
#' ##ingest_wahis_report("../data-raw/raw_wahis_reports/BWA_2016_sem0.html")
#' @export
#' @import rvest 
#' @import stringi 
#' @import xml2 
#' @import dplyr
#' @import tidyr

ingest_wahis_report <- function(web_page) {
    
    # get page
    page <- suppressWarnings(read_xml(web_page, as_html = TRUE, options = c("RECOVER", "NOERROR", 
                                                                            "NOBLANKS")))
    # get country name
    country <- xml_find_first(page, '//td[contains(., "Country:")]') %>% xml_text() %>% stri_extract_first_regex("(?<=:\\s).*")
    
    # get report period
    report_period <- xml_find_first(page, '//td[contains(., "Report period:")]') %>% xml_text() %>% stri_extract_first_regex("(?<=:\\s).*")
    
    # get OIE listed disease table
    tab_title <- xml_find_first(page, '//td[contains(., "Summary on OIE-listed diseases/infections present in")]')
    if(inherits(tab_title, "xml_missing")) return(tibble(country = character(0)))
    parents <- xml_parents(tab_title)
                            
    diseases_present <- parents[[min(which(xml_name(parents) == "table"))]] %>% 
        #xml_find_first(page, '//*[@id="MainContainerBig2"]/div[3]/div[2]/div/table[3]') %>% 
        rvest::html_table() %>%
        tibble::as_tibble() %>%
        magrittr::set_names(.[2,]) %>%
        janitor::clean_names() %>%
        slice(-1:-2) %>%
        mutate_all(funs(na_if(., ""))) %>% 
        fill(oie_listed_disease, .direction = "down") %>% 
        mutate(country = country, report_period = report_period, file = basename(web_page)) %>%
        group_by(oie_listed_disease) %>%
        mutate(oie_listed_disease_row_id = row_number()) %>%
        ungroup()
    
    # get notes into OIE listed disease table
    note_rows <- which(stri_detect_fixed(diseases_present$oie_listed_disease, "note", case_insensitive = TRUE))
    notes <- tibble(oie_listed_disease = diseases_present$oie_listed_disease[note_rows - 1],
                    oie_listed_disease_row_id = 1,
                    notes = diseases_present$oie_listed_disease[note_rows])
    
    if(length(note_rows)) {
    diseases_present <- diseases_present %>%
        slice(-note_rows) %>%
        left_join(notes, by = c("oie_listed_disease", "oie_listed_disease_row_id")) %>%
        select(-oie_listed_disease_row_id)
    }else{
        diseases_present$notes <- NA_character_
    }
    return(diseases_present)
}

safe_ingest <- function(web_page) {
    out <- safely(ingest_wahis_report)(web_page)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(tibble(file = basename(web_page)))
    }
}

