#' Convert a list of scraped ourbreak reports to a list of table
#' @param outbreak_reports a list of outbreak reports produced by [ingest_report]
#' @param report_list produced by scrape_outbreak_report_list()
#' @import dplyr tidyr purrr stringr
#' @importFrom glue glue_collapse
#' @importFrom janitor clean_names
#' @importFrom lubridate dmy myd ymd
#' @importFrom textclean replace_non_ascii
#' @importFrom countrycode countrycode
#' @importFrom assertthat %has_name%
#' @export

transform_outbreak_reports <- function(outbreak_reports,
                                       report_list) {
  
  message("Transforming outbreak reports")
  
  # Preprocessing ---------------------------------------------------
  # outbreak_reports[which(map_int(outbreak_reports, length)==2)]
  
  outbreak_reports2 <- discard(outbreak_reports, function(x){
    !is.null(x$ingest_status) && str_detect(x$ingest_status, "ingestion error") |
      !is.null(x$message) && str_detect(x$message, "Endpoint request timed out") 
  })
  if(!length(outbreak_reports2)) return(NULL)
  
  # Events table ------------------------------------------------------------
  
  outbreak_reports_events <- map_dfr(outbreak_reports2, function(x){
    map_dfc(c("senderDto", "generalInfoDto", "reportDto", "report_info_id"), function(tbl){
      out <- x[[tbl]] %>%
        compact() %>%
        as_tibble()
      if(tbl=="generalInfoDto") out <- out %>% select(-one_of("reportDate"))
      if(tbl=="totalCases" & nrow(out)) out <- out %>% rename(total_cases = value)
      if(tbl=="report_info_id" & nrow(out)) out <- out %>% rename(report_info_id = value)
      return(out)
    })
  }) %>%
    janitor::clean_names()
  
  reports <- report_list %>%
    select(report_info_id, # url
           outbreak_thread_id = event_id_oie_reference) # thread number
  # n_distinct(reports$report_info_id)
  # n_distinct(reports$outbreak_thread_id)
  
  lookup_outbreak_thread_url <-  report_list %>% 
    filter(report_type == "IN") %>% 
    select(outbreak_thread_id = event_id_oie_reference, url_outbreak_thread_id = report_info_id) 
  
  reports <- left_join(reports, lookup_outbreak_thread_url, by = "outbreak_thread_id")
  
  outbreak_reports_events <- outbreak_reports_events %>%
    left_join(reports,  by = "report_info_id") %>% 
    mutate(country_or_territory = case_when(
      country_or_territory == "Central African (Rep.)" ~ "Central African Republic",
      country_or_territory == "Dominican (Rep.)" ~ "Dominican Republic",
      country_or_territory == "Ceuta" ~ "Morocco",
      country_or_territory == "Melilla"~ "Morocco",
      TRUE ~ country_or_territory
    )) %>%
    mutate_if(is.character, tolower) %>%
    mutate(country_iso3c = countrycode::countrycode(country_or_territory, origin = "country.name", destination = "iso3c")) %>%
    rename_all(recode, 
               report_info_id =  "url_report_id",
               country_or_territory = "country",
               disease_name = "disease",
               report_title = "report_type",
               translated_reason = "reason_for_notification",
               confirmed_on = "date_of_confirmation_of_the_event",
               start_date = "date_of_start_of_the_event",
               end_date = "date_event_resolved",
               last_occurance_date = "date_of_previous_occurrence",
               disease_type = "serotype",
               event_description_status = "future_reporting") %>%
    select(suppressWarnings(one_of("report_id",
                                   "url_report_id",
                                   "outbreak_thread_id", 
                                   "url_outbreak_thread_id",
                                   "country",
                                   "country_iso3c",
                                   "disease_category",
                                   "disease",
                                   "is_aquatic",
                                   "report_date",
                                   "report_type",
                                   "reason_for_notification",
                                   "date_of_confirmation_of_the_event",
                                   "date_of_start_of_the_event",
                                   "date_event_resolved",
                                   "date_of_previous_occurrence",
                                   "casual_agent",
                                   "serotype",
                                   "future_reporting")),
           everything() 
    ) 
  
  # Cleaning
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate(follow_up_count = ifelse(str_detect(report_type, "immediate notification"), 0, str_extract(report_type, "[[:digit:]]+"))) %>%
    mutate(is_final_report = str_detect(report_type, "final report")) %>%
    mutate(is_endemic = str_detect(future_reporting, "the event cannot be considered resolved"))
  
  # Dates handling - convert to  ISO-8601
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate_at(vars(contains("date")), ~lubridate::as_datetime(.)) 
  
  # Check for missing date_event_resolved
  if(suppressWarnings(is.null(outbreak_reports_events$date_event_resolved))) outbreak_reports_events$date_event_resolved <- NA
  missing_resolved <- outbreak_reports_events %>%
    filter(is.na(date_event_resolved)) %>%
    filter(is_final_report)
  
  if(nrow(missing_resolved)){
    # Check threads to confirm these are final. If they are, then assume last report is the end date.
    check_final <- outbreak_reports_events %>%
      select(report_id, outbreak_thread_id, report_date) %>%
      filter(outbreak_thread_id %in% missing_resolved$outbreak_thread_id) %>%
      left_join(missing_resolved %>% select(report_id, is_final_report),  by = "report_id") %>%
      mutate(is_final_report = coalesce(is_final_report, FALSE)) %>%
      group_by(outbreak_thread_id) %>%
      mutate(check = report_date == max(report_date)) %>%
      ungroup() %>%
      mutate(confirm_final = is_final_report == check)
    
    check_final_resolved <- check_final %>%
      filter(is_final_report, check)
    check_final_unresolved <- check_final %>%
      filter(is_final_report, !check)
    
    outbreak_reports_events <- outbreak_reports_events %>%
      mutate(date_event_resolved = if_else(report_id %in% check_final_resolved$report_id, report_date, date_event_resolved))
  }
  
  # Disease standardization
  
  # disease_export <- outbreak_reports_events %>%
  #   distinct(disease, causal_agent) %>%
  #   mutate_all(~tolower(trimws(.)))
  # write_csv(disease_export, here::here("inst/diseases/outbreak_report_diseases.csv"))
  
  ando_disease_lookup <- readxl::read_xlsx(system.file("diseases", "disease_lookup.xlsx", package = "wahis")) %>%
    mutate(disease = textclean::replace_non_ascii(disease)) %>%
    rename(disease_class = class_desc) %>%
    filter(report == "animal") %>%
    select(-report, -no_match_found) %>%
    mutate_at(.vars = c("ando_id", "preferred_label", "disease_class"), ~na_if(., "NA"))
  
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate(disease = trimws(disease)) %>%
    mutate(disease = textclean::replace_non_ascii(disease)) %>%
    mutate(disease = ifelse(disease == "", causal_agent, disease)) %>%
    mutate(disease = str_remove_all(disease, "\\s*\\([^\\)]+\\)")) %>% 
    mutate(disease = str_remove(disease, "virus")) %>% 
    mutate(disease = trimws(disease)) %>%
    left_join(ando_disease_lookup, by = "disease") %>%
    mutate(disease = coalesce(preferred_label, disease)) %>%
    select(-preferred_label) %>%
    distinct()
  
  diseases_unmatched <- outbreak_reports_events %>%
    filter(is.na(ando_id)) %>%
    distinct(disease) %>%
    mutate(table = "outbreak_animal")
  
  # write_csv(diseases_unmatched, here::here("inst/diseases/outbreak_report_diseases_unmatched_20210507.csv"))
  
  # Check threads to make sure disease is consistent across thread
  # outbreak_reports_events %>%
  #   group_by(outbreak_thread_id) %>%
  #   mutate(disease_count = n_distinct(disease)) %>%
  #   ungroup() %>%
  #   filter(disease_count > 1) %>%
  #   View() # these are missing immediate reports
  
  ### Understanding  IDs
  # outbreak_reports_events$report_id # unique individual report id (in = initial, fur = follow up report)
  # outbreak_reports_events$url_report_id # unique individual report url value
  # outbreak_reports_events$outbreak_thread_id # outbreak thread identifier - does not correspond to report_id or url_report_id
  
  # Outbreak tables ---------------------------------------------------
  
  # outbreak_reports_detail$oieReference 
  # ^ denotes different locations within one report - not unique because there can be multiple species
  # outbreak_reports_detail$outbreakInfoId  and outbreak_reports_detail$outbreakId
  # seems to be reduntant with oieReference - leaving out for now
  
  process_outbreak_map <- function(outbreak_loc, report_id){
    
    # base dataframe
    outbreak_loc[["geographicCoordinates"]] <- NULL
    outbreak_loc[["newlyAddedCm"]] <- NULL
    outbreak_loc[["administrativeDivisionList"]] <- NULL
    outbreak_loc[["diagSummary"]] <- NULL
    outbreak_loc[["deletedCm"]] <- NULL
    cm <- glue::glue_collapse(unique(outbreak_loc$controlMeasures), sep = "; ")
    outbreak_loc[["controlMeasures"]] <- NULL
    out <- as_tibble(outbreak_loc[which(!sapply(outbreak_loc, is.list))])
    out$report_id <- report_id 
    if(length(cm)) out$control_measures <- cm
    assert_that(nrow(out) == 1)
    
    # add species details
    if(!is.null(outbreak_loc$speciesDetails)){
      sd <- outbreak_loc$speciesDetails[-nrow(outbreak_loc$speciesDetails),]
      out <- bind_cols(out, sd, .name_repair = "minimal")
    }
    
    # add animal category
    if(!is.null(outbreak_loc$animalCategory)){
      out <- bind_cols(out, outbreak_loc$animalCategory, .name_repair = "minimal")
    }
    
    return(out)
  }
  
  outbreak_reports_detail <- imap(outbreak_reports2, function(x, i){
    
    report_id <- x$reportDto$reportId 
    outbreak_map <-  x$eventOutbreakDto$outbreakMap
    print(i)
    
    if(is.null(outbreak_map)) return()
    
    map_dfr(outbreak_map, process_outbreak_map, report_id = report_id)
  })
  
  if(length(outbreak_reports_detail)) {
    
    outbreak_reports_detail <- reduce(outbreak_reports_detail, bind_rows)
    
    outbreak_reports_detail <- outbreak_reports_detail %>%
      mutate_if(is.character, tolower) %>%
      janitor::clean_names()  %>% 
      select(-starts_with("total_")) %>% # these are rolling and values and may cause confusion
      select(-suppressWarnings(one_of("prod_type"))) %>% 
      select(-suppressWarnings(one_of("specie_id")), -suppressWarnings(one_of("morbidity")), -suppressWarnings(one_of("mortality")), -suppressWarnings(one_of("outbreak_info_id")), -suppressWarnings(one_of("outbreak_id"))) %>% 
      rename_with(~str_replace(., "^spicie_name$", "species_name"), suppressWarnings(one_of("spicie_name"))) %>% 
      rename_with( ~str_replace(., "^killed$", "killed_and_disposed"), suppressWarnings(one_of("killed"))) %>% 
      rename_with( ~str_replace(., "^slaughtered$", "slaughtered_for_commercial_use"), suppressWarnings(one_of("slaughtered"))) %>% 
      rename_with( ~str_replace(., "^oie_reference$", "outbreak_location_id"), suppressWarnings(one_of("oie_reference"))) %>% 
      mutate_all(~na_if(., "" )) %>% 
      mutate_at(vars(contains("date")), ~lubridate::as_datetime(.)) %>% 
      mutate_at(vars(suppressWarnings(one_of("susceptible", "cases", "deaths", "killed_and_disposed", "slaughtered_for_commercial_use"))), ~replace_na(., 0))
    
    cnames <- colnames(outbreak_reports_detail)
    
    if("wildlife_type" %in% cnames & "type_of_wildlife" %in% cnames){
      outbreak_reports_detail <- outbreak_reports_detail %>%
        mutate(wildlife_type = coalesce(wildlife_type, type_of_wildlife)) %>% 
        select(-type_of_wildlife)
    }
    if(!"wildlife_type" %in% cnames & "type_of_wildlife" %in% cnames){
      outbreak_reports_detail <- outbreak_reports_detail %>%
        rename(wildlife_type = type_of_wildlife)
    }
    
  }
  
  # Export -----------------------------------------------
  wahis_joined <- list("outbreak_reports_events" = outbreak_reports_events,
                       "outbreak_reports_outbreaks" = outbreak_reports_detail,
                       #"outbreak_reports_outbreaks_summary" = outbreak_reports_summary,
                       #"outbreak_reports_laboratories" = outbreak_reports_laboratories,
                       "outbreak_reports_diseases_unmatched" = diseases_unmatched)
  
  # remove empty tables
  wahis_joined <- keep(wahis_joined, ~nrow(.)>0)
  
  # if(nrow(wahis_joined$outbreak_reports_diseases_unmatched)){warning("Unmatched diseases. Check outbreak_reports_diseases_unmatched table.")}
  
  return(wahis_joined)
}

