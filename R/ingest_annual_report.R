#' Support function for ingest_annual_report to clean html tables
#' @param parent output of xml_node text matching
#' @param include_header create new column containing content of table header? Defaults to FALSE
#' @noRd
clean_oie_report_table <- function(parent, include_header = FALSE){
    
    if(is.null(parent) || length(parent)==0){return()}
    
    raw <- parent %>%
        html_table(fill = TRUE) 
    
    header <- raw[1,1]
    
    clean <- raw %>%
        as_tibble() %>%
        set_names(.[2,]) %>%
        janitor::clean_names() %>%
        slice(-1:-2) %>%
        mutate_all(~na_if(., ""))
    
    if(include_header){
        clean <- clean %>%
            mutate(header = header) %>%
            select(header, everything())
    }
    
    return(clean)
}

#' Support function for ingest_annual_report to find between dividers in report
#' @param headers search strings
#' @param siblings xml nodeset to search 
#' @importFrom purrr map compact  
#' @noRd
get_header_index <- function(headers, siblings){
    
    out <- map(headers, function(x){
        p_nodeset <- xml_nodes(siblings, xpath = paste0('//div[contains(text(), "', x, '")]'))
        if(length(p_nodeset)==0){return(NULL)}
        which(siblings %in% p_nodeset) 
    }) 
    
    names(out) <- headers
    compact(out) %>% unlist()
}


#' Support function for ingest_annual_report to find between dividers in report
#' @param siblings xml nodeset to search 
#' @param first_header_name string to match when searching colnames or table headers (can be vector of strings)
#' @param second_header_name string to match when searching colnames or table headers (can be vector of strings)
#' @param end_page for semester reports, data runs to the end of the page
#' @noRd
get_tables_by_index <- function(siblings, first_header_name, second_header_name, end_page = FALSE){
    
    # search page
    first_header_index <- get_header_index(first_header_name, siblings)
    second_header_index <- get_header_index(second_header_name, siblings)
    
    if(end_page) {second_header_index <- length(siblings)}
        
    # if second header is not in siblings nodeset, check if it's a table (eg this is the case for "Zoonotic diseases in humans")
    if(is.null(second_header_index)){
        ns <- xml_find_all(siblings, paste0('//tr[contains(., "', second_header_name,'")]')) %>%
            xml_parent()
        second_header_index <- which(siblings %in% ns)
        # if still not there,return null
        if(length(second_header_index)==0){return(NULL)}
        names(second_header_index) <- second_header_name
    }
    
    # check that there is data between headers 
    check_index <- second_header_index - first_header_index > 1
    if(!check_index || length(check_index)==0){return(NULL)}
    
    # get all indices
    all_index <- (first_header_index+1):(second_header_index-1)
    return(all_index)
}


#' Support function for ingest_annual_report to take notes from row and add to table column
#' @param tbl dataframe with notes as full rows
#' @noRd

add_notes <- function(tbl){
    # get notes into table
    note_rows <- which(stri_detect_fixed(tbl$disease, "note", case_insensitive = TRUE))
    
    if(length(note_rows)) {
        notes <- tibble(disease = tbl$disease[note_rows - 1],
                        disease_row_id = 1,
                        notes = tbl$disease[note_rows])
        
        tbl <- tbl %>%
            group_by(disease) %>%
            mutate(disease_row_id = row_number()) %>%
            ungroup() %>%
            slice(-note_rows) %>%
            left_join(notes, by = c("disease", "disease_row_id"))  %>%
            select(-disease_row_id)
    }else{
        tbl$notes <- NA_character_
    }
    return(tbl)
}

#' Extract Info from WAHIS Annual and Semi-Annual Reports
#'
#' Extract the information found in the Annual and Semi-Annual OIE World Animal Health Information
#' System (WAHIS) HTML reports. These reports have to be provided to this function.
#'
#' @param web_page Name of the downloaded web page
#'
#' @return A list with elements:
#' \item{id}{Record id.}
#' \item{diseases_present}{Table of disease present in country over time period}
#' @examples
#' ##ingest_annual_report("../data-raw/raw_wahis_reports/BWA_2016_sem0.html")
#' @export
#' @import xml2 
#' @importFrom stringi stri_extract_first_regex
#' @importFrom stringr str_remove str_trim str_detect str_sub
#' @importFrom tidyr fill
#' @importFrom magrittr set_names
#' @importFrom purrr map map2 map_df imap_dfr map_dfr map_lgl compact
#' @import dplyr
ingest_annual_report <- function(web_page) {
    
    # get page
    page <- suppressWarnings(read_xml(web_page, as_html = TRUE, options = c("RECOVER", "NOERROR", 
                                                                            "NOBLANKS")))
    # get country name
    country <- try(xml_find_first(page, '//td[contains(., "Country:")]') %>% xml_text() %>% stri_extract_first_regex("(?<=:\\s).*"), silent = TRUE)
    
    # return error if country name is NA - indicates that report was not correctly loaded (eg )
    if(is.na(country)||class(country)=="try-error"){
        
        if(class(country)=="try-error") {
            error <- "blank page"
        }else{
            error <- xml_find_first(page, xpath="//h4['Application Error']") %>% xml_text()
            if(is.na(error)){
                error <-  xml_text(page)
            }
        }
        
        report_info <- basename(web_page)
        country_iso3c <- substr(report_info, 1, 3)
        country = countrycode::countrycode(country_iso3c, origin = "iso3c", destination = "country.name")
        report_year <- substr(report_info, 5, 8)
        report_semester <- substr(report_info, 10, 13)
        report_months <- switch(sem, "sem0" = "Jan-Dec", "sem1" = "Jan-Jun", "sem2" = "Jul-Dec")
        
        metadata <- tibble(country_iso3c, country, report_year, report_semester, report_months, error)
        return(list(
            "metadata" = metadata))
    }
    
    country_iso3c <- substr(basename(web_page), 1, 3)
    
    # get report period
    report_period <- xml_find_first(page, '//td[contains(., "Report period:")]') %>% xml_text() %>% stri_extract_first_regex("(?<=:\\s).*")
    report_year <- str_sub(report_period, start = -4) %>% as.numeric()
    report_months <- str_sub(report_period, end = -6)
    report_semester <- substr(basename(web_page), 10, 13)
    
    # get full report summary
    siblings <- xml_nodes(page, xpath='//div[@class="ContentBigTable"]/*')
    
    first_header_name = c("Report Summary") 
    second_header_name = c("1. Summary on OIE-listed diseases/infections present in ") 
    
    index <- get_tables_by_index(siblings = siblings, 
                        first_header_name = first_header_name, second_header_name = second_header_name)
    
    tabs <- siblings[index]
    metadata <- map_dfr(tabs, function(tb){
        dat <- tb %>% 
            rvest::html_table(fill = TRUE) %>%
            filter(X1 != "") %>%
            as_tibble()
        
        dat <- dat[,1:2] %>%
            set_names(c("submission_info", "submission_value")) %>%
            bind_rows(dat[,3:4] %>% set_names(c("submission_info", "submission_value"))) %>%
            mutate(submission_animal_type = .$submission_value[submission_info == "Animal type"]) %>%
            filter(submission_info != "Animal type") %>% 
            filter(submission_info != "Report Period:") %>% 
            mutate(report_period = report_period,
                   report_semester = report_semester, 
                   country_iso3c = country_iso3c,
                   web_page = web_page) %>%
            select(report_period, everything())
            
    })
    # 1 -----------------------------------------------------------------------
    # OIE- and non-OIE-listed diseases/infections present
    # pre-2014 also includes non-OIE-listed diseases present
    
    header_name <- c("1. Summary on OIE-listed diseases/infections present in", "5. Summary on non OIE-Listed diseases/infections present in")
    
    parent <- map(header_name, ~xml_node(page, xpath=paste0('//table/tr/td[contains(text(),"', ., '")]/../..'))) %>% compact()
    
    diseases_present <- map_df(parent, ~clean_oie_report_table(.))
    
    if(nrow(diseases_present) > 0){ # may be NULL if no diseases were detected
        
        # check that we have the right table
        assertthat::has_name(diseases_present, c("new_outbreaks", "occurrence"))
        
        # temporarily add non_oie_listed_disease or oie_listed_disease fields
        if(!"non_oie_listed_disease" %in% colnames(diseases_present)){diseases_present$non_oie_listed_disease <- NA_character_}
        if(!"oie_listed_disease" %in% colnames(diseases_present)){diseases_present$oie_listed_disease <- NA_character_}
        
        # light cleaning to format tables identically
        diseases_present <- diseases_present %>%
            mutate(disease = coalesce(oie_listed_disease, non_oie_listed_disease)) %>%
            mutate(oie_listed = if_else(!is.na(oie_listed_disease), TRUE,
                                        if_else(!is.na(non_oie_listed_disease), FALSE, NA))) %>%
            select(disease, everything(), -non_oie_listed_disease, -oie_listed_disease) %>%
            fill(disease, .direction = "down") %>%
            fill(oie_listed, .direction = "down")
        
        # get notes into table
        diseases_present <- add_notes(diseases_present)
        
    }
    
    # 2 -----------------------------------------------------------------------
    # OIE- and non-OIE-listed diseases absent 
    # Pulls all tables between top div marker and next section 
    diseases_absent_siblings <- xml_nodes(page, xpath='//div[@class="ContentBigTable"]/*')
    
    first_header_names = c("OIE-listed diseases absent in ",  # set 1
                           "Non OIE-Listed diseases absent in ") # set 2 (only applies to pre-14)
    second_header_names = c("Detailed quantitative information for OIE-listed diseases/infections present in ", # set 1
                            "Detailed quantitative information for non OIE-Listed diseases/infections present in ") # set 2 (only applies to pre-14)
    
    diseases_absent_index <- map2(first_header_names, second_header_names, function(x, y){
        
        get_tables_by_index(siblings = diseases_absent_siblings, 
                            first_header_name = x, second_header_name = y)
    }) %>% 
        set_names(first_header_names) %>%
        compact()
    
    # pull data and include oie_listed and taxa
    diseases_absent <- imap_dfr(diseases_absent_index, function(x, y){
        oie_listed <- switch(y, "OIE-listed diseases absent in " = TRUE, "Non OIE-Listed diseases absent in " = FALSE) 
        tabs <- diseases_absent_siblings[x]
        map_dfr(tabs, function(tb){
            clean_oie_report_table(tb, include_header = TRUE) %>%
                fill(disease, .direction = "down") %>%
                rename(taxa = header) %>%
                mutate(oie_listed = oie_listed)
        })
    })
    
    assertthat::has_name(diseases_absent, c("date_of_last_occurrence", "species"))
    
    diseases_absent <- add_notes(diseases_absent)
    
    # 3 -----------------------------------------------------------------------
    # Detailed quantitative information for OIE-listed diseases/infections present
    # Disease information by State by month
    # Get every table that has "Month" in it.  Disease names are sometimes in table header and sometime in node above.  
    
    parent <- xml_nodes(page, xpath=paste0('//table/tr/th[contains(text(),"New outbreaks")]/../..'))
    exclude_tables <- c("1. Summary on OIE-listed diseases/infections present in", "5. Summary on non OIE-Listed diseases/infections present in")
    
    for(x in exclude_tables){
        exclude <- xml_nodes(parent, xpath=paste0('//table/tr/td[contains(text(),"', x, '")]/../..'))
        parent <- parent[!parent %in% exclude]
    }
    
    if(!is.null(parent)){
        
        diseases_present_detail <- map_df(parent, function(node){
            disease <- html_table(node, fill = TRUE)[1,1] # fill needed b/c weirdness in IRQ_2015_sem0.html "/html/body/div/div[3]/div[2]/div/table[27]"
            disease_check <- str_remove(disease, "Species") %>% str_trim()
            
            # if disease name is not in header, get the node above
            if(disease_check %in% c("Wild", "Domestic")){
                grandparent <- xml_parent(node)
                siblings <- xml_children(grandparent)
                disease <- siblings[which(xml_path(siblings) == xml_path(node)) - 1] %>% html_text()
            }
            
            diseases_present_detail <-  clean_oie_report_table(node)
            
            # find column that represents adm
            cols <- c("month", "serotype_s", "new_outbreaks", "total_outbreaks", "species", "family_name", "latin_name", "measuring_units", "susceptible", "cases", "deaths", "killed_and_disposed_of", "slaughtered", "vaccination_in_response_to_the_outbreak_s")
            adm <- setdiff(colnames(diseases_present_detail), cols)
            
            assertthat::assert_that(length(adm) <= 1)
            
            if(length(adm) == 1){
                diseases_present_detail <- diseases_present_detail %>%
                    mutate(adm_type = adm) %>%
                    rename(adm = !!adm) 
            }else{
                diseases_present_detail <- diseases_present_detail %>%
                    mutate(adm_type = "country") %>%
                    mutate(adm = country) 
            }
            
            if("month" %in% colnames(diseases_present_detail)){
                diseases_present_detail <- diseases_present_detail %>%
                    fill(month, .direction = "down") %>%
                    rename(period = month) %>%
                    mutate(temporal_scale = "month") 
            }else{
                diseases_present_detail <- diseases_present_detail %>%
                    mutate(period = report_months) %>%
                    mutate(temporal_scale = "semester") 
            }
            
            diseases_present_detail <- diseases_present_detail %>%
                mutate(disease = disease) #TODO note that these output diseases do not exactly match the disease_present diseases because of line breaks
        })
        
        # Not keeping semester info for yearly reports
        if(str_detect(web_page, "sem0")){
            diseases_present_detail <- diseases_present_detail %>% filter(temporal_scale != "semester")
        }
    }else{
        diseases_present_detail <- NULL
    }
    
    # 4 -----------------------------------------------------------------------
    # Unreported OIE-listed diseases during the reporting period
    # Pulls all tables between top div marker and next section 
    
    diseases_unreported_siblings <- xml_nodes(page, xpath='//div[@class="ContentBigTable"]/*')
    
    first_header_names <- c("Unreported OIE-listed diseases during the reporting period",  # set 1
                           "Unreported non OIE-Listed diseases") # set 2 (only applies to pre-14)
    second_header_names <- c("Summary on non OIE-Listed diseases/infections present in ", # set 1
                            "Zoonotic diseases in humans") # set 2 (only applies to pre-14)
    
    if(report_year >= 2014){second_header_names[1] <- "Zoonotic diseases in humans"} # special case
    
    diseases_unreported_index <- map2(first_header_names, second_header_names, function(x, y){
        
        end_page <- !str_detect(web_page, "sem0")
        get_tables_by_index(siblings = diseases_unreported_siblings, 
                            first_header_name = x, second_header_name = y,
                            end_page = end_page)
    }) %>% 
        set_names(first_header_names) %>%
        compact()
    
    # pull data and include oie_listed and taxa
    diseases_unreported <- imap_dfr(diseases_unreported_index, function(x, y){
        oie_listed <- switch(y, "Unreported OIE-listed diseases during the reporting period" = TRUE, "Unreported non OIE-Listed diseases" = FALSE) 
        tabs <- diseases_unreported_siblings[x]
        map_dfr(tabs, function(tb){
            dat <- tb %>% 
                html_table(fill = TRUE) %>%
                t()
            tibble(taxa = unique(dat[,1]), disease = c(dat[,2:ncol(dat)]), oie_listed = oie_listed) %>% 
                filter(disease != "")
        })
    })
    
    # assertion that you have at least one absense, presence, or unreported table--they can't all be null
    assertthat::assert_that(!all(map_lgl(list(diseases_present, diseases_absent, diseases_unreported), is.null)))
    
    # 5 -----------------------------------------------------------------------
    # Zoonotic diseases in humans
    tbl_number <- ifelse(report_year >= 2014, 5, 9)
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="', tbl_number, '. Zoonotic diseases in humans"]/../..')) 
    disease_humans <- clean_oie_report_table(parent)
    if(!is.null(disease_humans)){
        assertthat::has_name(disease_humans, c("no_information_available", "disease_absent"))
    }
    
    # 6 -----------------------------------------------------------------------
    # Animal population
    tbl_number <- ifelse(report_year >= 2014, 6, 10)
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="', tbl_number, '. Animal population"]/../..')) 
    animal_population <- clean_oie_report_table(parent)
    if(!is.null(animal_population)){
        assertthat::has_name(animal_population, c("species", "production"))
        animal_population <- animal_population %>%
            fill(species, .direction = "down")
    }
    
    # # 7 -----------------------------------------------------------------------
    # Veterinarians and veterinary para-professionals
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="Veterinarians:"]/../..')) 
    veterinarians <- clean_oie_report_table(parent)
    
    if(!is.null(veterinarians)){
        assertthat::has_name(veterinarians, c("x", "public_sector"))
        
        veterinarians <- veterinarians %>%
            rename(veterinarian_field = x) %>%
            mutate(veterinarian_class = "veterinarians")
        
        note <- veterinarians %>% filter(veterinarian_field == "(specify with a short note)") %>% pull(2)
        
        veterinarians <- veterinarians %>%
            mutate(veterinarian_field = ifelse(veterinarian_field=="Others",
                                               paste0(veterinarian_field, " (", note, ")"),
                                               veterinarian_field)) %>%
            slice(-nrow(.))
        
    }
    
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="Veterinary Paraprofessionals"]/../..')) 
    veterinarian_paraprofessionals <- clean_oie_report_table(parent)
    
    if(!is.null(veterinarian_paraprofessionals)){
        assertthat::has_name(veterinarian_paraprofessionals, c("x", "public_sector"))
        
        veterinarian_paraprofessionals <- veterinarian_paraprofessionals %>%
            rename(veterinarian_field = x) %>%
            mutate(veterinarian_class = "veterinarian paraprofessionals")
        
        note <- veterinarian_paraprofessionals %>% filter(veterinarian_field == "(specify with a short note)") %>% pull(2)
        
        veterinarian_paraprofessionals <- veterinarian_paraprofessionals %>%
            mutate(veterinarian_field = ifelse(veterinarian_field=="Others",
                                               paste0(veterinarian_field, " (", note, ")"),
                                               veterinarian_field)) %>%
            slice(-nrow(.))
        
    }
    veterinarians <- bind_rows(veterinarians, veterinarian_paraprofessionals)
    if(nrow(veterinarians)==0){veterinarians <- NULL}
    # 8 -----------------------------------------------------------------------
    #  National reference laboratories
    tbl_number <- ifelse(report_year >= 2014, 8, 12)
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="', tbl_number, '. National reference laboratories"]/../..')) 
    national_reference_laboratories <- clean_oie_report_table(parent)
    
    if(!is.null(national_reference_laboratories)){
        assertthat::has_name(national_reference_laboratories, c("name", "contacts"))
    }
    
    # 9 -----------------------------------------------------------------------
    # Diagnostic Tests
    tbl_number <- ifelse(report_year >= 2014, 9, 13)
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="', tbl_number, '. Diagnostic Tests"]/../..')) 
    national_reference_laboratories_detail <- clean_oie_report_table(parent)
    
    if(!is.null(national_reference_laboratories_detail)){
        assertthat::has_name(national_reference_laboratories_detail, c("laboratory", "disease", "test_type"))
        
        national_reference_laboratories_detail <- national_reference_laboratories_detail %>%
            fill(laboratory, .direction = "down") %>%
            fill(disease, .direction = "down")
    }
    
    # 10 -----------------------------------------------------------------------
    # Vaccine Manufacturers
    tbl_number <- ifelse(report_year >= 2014, 10, 14)
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="', tbl_number, '. Vaccine Manufacturers"]/../..')) 
    vaccine_manufacturers <- clean_oie_report_table(parent)
    
    if(!is.null(vaccine_manufacturers)){
        assertthat::has_name(vaccine_manufacturers, c("manufacturer", "contacts"))
    }
    
    # 11 -----------------------------------------------------------------------
    # Vaccines
    tbl_number <- ifelse(report_year >= 2014, 11, 15)
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="', tbl_number, '. Vaccines"]/../..')) 
    vaccine_manufacturers_detail <- clean_oie_report_table(parent)
    
    if(!is.null(vaccine_manufacturers_detail)){
        assertthat::has_name(vaccine_manufacturers_detail, c("manufacturer", "disease"))
        
        vaccine_manufacturers_detail <- vaccine_manufacturers_detail %>%
            fill(disease, .direction = "down")
    }
    
    # 12 -----------------------------------------------------------------------
    # Vaccine production
    tbl_number <- ifelse(report_year >= 2014, 12, 16)
    parent <- xml_node(page, xpath=paste0('//table/tr/td[text()="', tbl_number, '. Vaccine production"]/../..')) 
    
    vaccine_production <- clean_oie_report_table(parent)
    
    if(!is.null(vaccine_production)){
        assertthat::has_name(vaccine_production, c("manufacturer", "vaccine", "doses_produced"))
    }
    
    # Output -----------------------------------------------------------------------
    wahis <- list(
        "metadata" = metadata,
        "diseases_present"= diseases_present, 
        "diseases_absent"= diseases_absent,
        "diseases_present_detail"= diseases_present_detail,
        "diseases_unreported" = diseases_unreported,
        "disease_humans" = disease_humans,
        "animal_population" = animal_population,
        "veterinarians" = veterinarians,
        "national_reference_laboratories" = national_reference_laboratories,
        "national_reference_laboratories_detail" = national_reference_laboratories_detail,
        "vaccine_manufacturers" = vaccine_manufacturers,
        "vaccine_manufacturers_detail" = vaccine_manufacturers_detail,
        "vaccine_production" = vaccine_production)
    
    
    wahis <- map(wahis, function(x){
            if(is.null(x)){return()}
            x %>% mutate(country = country,
                         report_year = report_year,
                         report_months = report_months) %>%
                select(country, report_year, report_months, everything())
    })
    
    return(wahis)
}

#' Function to safely run ingest_annual_report
#' @param web_page Name of the downloaded web page
#' @importFrom purrr safely
#' @export

safe_ingest_annual <- function(web_page) {
    out <- safely(ingest_annual_report)(web_page)
    if(!is.null(out$result)) {
        return(out$result)
    } else {
        return(tibble(file = basename(web_page)))
    }
}

