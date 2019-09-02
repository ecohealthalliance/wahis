ingest_wahis_report <- function(web_page) {
    page <- suppressWarnings(read_xml(web_page, as_html = TRUE, options = c("RECOVER", "NOERROR", 
                                                                            "NOBLANKS")))
    country <- xml_find_first(page, '//td[contains(., "Country:")]') %>% xml_text() %>% stri_extract_first_regex("(?<=:\\s).*")
    report_period <- xml_find_first(page, '//td[contains(., "Report period:")]') %>% xml_text() %>% stri_extract_first_regex("(?<=:\\s).*")
    
    tab_title <- xml_find_first(page, '//td[contains(., "Summary on OIE-listed diseases/infections present in")]')
    if(inherits(tab_title, "xml_missing")) return(tibble(country = character(0)))
    parents <- xml_parents(tab_title)
    tab0 <- parents[[min(which(xml_name(parents) == "table"))]] %>% 
        #xml_find_first(page, '//*[@id="MainContainerBig2"]/div[3]/div[2]/div/table[3]') %>% 
        rvest::html_table()
    names(tab0) <- tab0[2,]
    tab1 <- tab0[-(1:2),] %>% as_tibble() %>% 
        janitor::clean_names()
    tab2 <- tab1 %>% 
        mutate_all(funs(na_if(., ""))) %>% 
        fill(oie_listed_disease, .direction = "down") %>% 
        mutate(country = country, report_period = report_period, file = basename(web_page), notes = NA_character_) %>% 
        filter(stri_detect_fixed(oie_listed_disease, "rift", case_insensitive = TRUE))
    
    note_rows <-
        which(stri_detect_fixed(tab2$oie_listed_disease, "note", case_insensitive = TRUE))
    
    if(length(note_rows)) {
        tab2$notes[note_rows - 1] <- tab2$oie_listed_disease[note_rows]
        tab2 <- tab2[-note_rows, ]
    }
    return(tab2)
}

safe_ingest <- function(web_page) {
    out <- safely(ingest_wahis_report)(web_page)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(tibble(file = basename(web_page)))
    }
}

