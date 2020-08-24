#' Convert a list of scraped ourbreak reports to a list of table
#' @param outbreak_reports a list of outbreak reports produced by [ingest_outbreak_report]
#' @import dplyr tidyr purrr stringr
#' @importFrom janitor clean_names
#' @importFrom lubridate dmy myd ymd
#' @importFrom textclean replace_non_ascii
#' @importFrom countrycode countrycode
#' @importFrom assertthat %has_name%
#' @export

transform_outbreak_reports <- function(outbreak_reports) {
  
  message("Transforming outbreak reports")
  
  # Proprocessing ---------------------------------------------------
  outbreak_reports2 <- keep(outbreak_reports, function(x){
    !is.null(x) && !is.null(x$ingest_status) && x$ingest_status == "available"
  })
  
  if(!length(outbreak_reports2)) return(NULL)
  
  exclude_fields <- c("Related reports", "related_reports", # can be determined from immediatate reports
                      "outbreak_detail", "outbreak_summary", "diagnostic_tests", # addressed in detail below
                      "ingest_status" # all available
  )
  
  outbreak_reports2 <-  modify(outbreak_reports2, function(x){
    x$total_new_outbreaks <- as.character(x$total_new_outbreaks)
    return(x)
  })
  
  # Events table ---------------------------------------------------
  outbreak_reports_events <- map_dfr(outbreak_reports2, function(x){
    exclude_index <- which(names(x) %in% exclude_fields)
    as_tibble(x[-exclude_index])
  }) %>% 
    janitor::clean_names() 
  
  # Get iso3c codes
  country_lookup <- tibble(country = unique(outbreak_reports_events$country)) %>% 
    mutate(country_iso3c = countrycode(sourcevar = country, origin = "country.name", destination = "iso3c"))
  
  # Cleaning
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate_at(vars(-country), ~tolower(.)) %>% 
    mutate(immediate_report = ifelse(str_detect(report_type, "immediate notification"), id, immediate_report)) %>%
    mutate(follow_up = ifelse(str_detect(report_type, "immediate notification"), 0, str_extract(report_type, "[[:digit:]]+"))) %>%
    mutate(final_report = str_detect(report_type, "final report")) %>% 
    mutate(endemic = str_detect(future_reporting, "the event cannot be considered resolved")) %>% 
    left_join(country_lookup, by = "country") %>% 
    select(id, country, country_iso3c, everything())
  
  # New outbreak?
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate(new_outbreak_in_report = map_lgl(outbreak_reports2, ~length(.$outbreak_summary) > 1))
  
  # Dates handling - convert to  ISO-8601 
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate_at(vars(starts_with("date"), "report_date", -"date_of_previous_occurrence"), ~dmy(.)) %>% 
    mutate(date_of_previous_occurrence = messy_dates(date_of_previous_occurrence))
  
  # Check for missing date_event_resolved
  missing_resolved <- outbreak_reports_events %>% 
    filter(is.na(date_event_resolved)) %>% 
    filter(final_report)
  
  if(nrow(missing_resolved)){
    # Check threads to confirm these are final. If they are, then assume last report is the end date.
    check_final <- outbreak_reports_events %>% 
      select(id, immediate_report, report_date) %>% 
      filter(immediate_report %in% missing_resolved$immediate_report) %>% 
      left_join(missing_resolved %>% select(id, final_report),  by = "id") %>% 
      mutate(final_report = coalesce(final_report, FALSE)) %>% 
      group_by(immediate_report) %>% 
      mutate(check = report_date == max(report_date)) %>% 
      ungroup() %>% 
      mutate(confirm_final = final_report == check)
    
    check_final_resolved <- check_final %>% 
      filter(final_report, check)
    check_final_unresolved <- check_final %>% 
      filter(final_report, !check)
    
    outbreak_reports_events <- outbreak_reports_events %>% 
      mutate(date_event_resolved = if_else(id %in% check_final_resolved$id, report_date, date_event_resolved)) 
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
    left_join(ando_disease_lookup, by = "disease") %>% 
    mutate(disease = coalesce(preferred_label, disease)) %>% 
    select(-preferred_label) %>% 
    distinct() 
  
  diseases_unmatched <- outbreak_reports_events %>% 
    filter(is.na(ando_id)) %>% 
    distinct(disease) %>% 
    mutate(table = "outbreak_animal")
  
  # Check threads to make sure disease is consistent across thread
  # outbreak_reports_events %>% 
  #   group_by(immediate_report) %>% 
  #   mutate(disease_count = n_distinct(disease)) %>% 
  #   ungroup() %>% 
  #   filter(disease_count > 1) %>% 
  #   View() # these are missing immediate reports
  
  # Outbreak tables ---------------------------------------------------
  
  outbreak_reports_detail <- map_df(outbreak_reports2, function(x){
    if(length(x$outbreak_detail) == 1){return()}
    x$outbreak_detail })
  
  if(nrow(outbreak_reports_detail)) {
    
    outbreak_reports_detail <- outbreak_reports_detail %>%
      mutate_all(~tolower(.)) %>% 
      janitor::clean_names() %>%
      mutate(outbreak_number = trimws(outbreak_number))
    
    # clean dates
    warn_that(unique(str_length(outbreak_reports_detail$date_of_start_of_the_outbreak)) == 10)
    
    outbreak_reports_detail <- outbreak_reports_detail %>%
      mutate(date_of_start_of_the_outbreak = dmy(date_of_start_of_the_outbreak)) %>% 
      mutate(outbreak_status2 = str_extract(outbreak_status, "resolved|continuing")) %>% 
      mutate(date_outbreak_resolved = case_when(outbreak_status2 == "resolved" ~
                                                  str_extract(outbreak_status, "(?<=\\().+?(?=\\))"))) %>% 
      mutate(date_outbreak_resolved = dmy(date_outbreak_resolved)) %>% 
      select(-outbreak_status) %>% 
      rename(outbreak_status = outbreak_status2)
    
  }
  
  outbreak_reports_summary <- map_df(outbreak_reports2, function(x){
    if(length(x$outbreak_summary) == 1){return()}
    x$outbreak_summary %>% mutate_all(as.character)}) 
  
  if(nrow(outbreak_reports_summary)) {
    outbreak_reports_summary <- outbreak_reports_summary %>%
      mutate_all(~tolower(.)) %>% 
      janitor::clean_names() %>%
      mutate_all(~str_remove(., "%")) %>%
      rename(total_morbidity_perc = total_apparent_morbidity_rate,
             total_mortality_perc = total_apparent_mortality_rate,
             total_case_fatality_perc = total_apparent_case_fatality_rate,
             total_susceptible_animals_lost_perc = total_proportion_susceptible_animals_lost)
  }
  
  # Fixes to mortality and morbidity fields (events and outbreak tables) ---------------------------------
  for(tbl_name in c("outbreak_reports_events", "outbreak_reports_detail")){
    tbl <- get(tbl_name)
    if(nrow(tbl)==0) next()
    
    if(tbl %has_name% "mortality"){
      tbl <- tbl %>% 
        mutate(mortality_val = str_extract(mortality, "scale 0 to 5|%|/")) %>% 
        mutate(mortality_rate = case_when(
          mortality_val == "scale 0 to 5" ~ suppressWarnings(as.numeric(str_remove(mortality, "\\(scale 0 to 5\\)"))) * 0.2,
          mortality_val == "%" ~  suppressWarnings(as.numeric(str_remove(mortality, "%")) / 100),
          mortality_val == "/" ~ suppressWarnings(as.numeric(str_remove_all(mortality, ".*/|%")) / 100))) %>% 
        select(-mortality, -mortality_val)
    }
    
    if(tbl %has_name% "morbidity"){
      tbl <- tbl %>% 
        mutate(morbidity_val = str_extract(morbidity, "scale 0 to 5|%|/")) %>% 
        mutate(morbidity_rate = case_when(
          morbidity_val == "scale 0 to 5" ~ suppressWarnings(as.numeric(str_remove(morbidity, "\\(scale 0 to 5\\)"))) * 0.2,
          morbidity_val == "%" ~  suppressWarnings(as.numeric(str_remove(morbidity, "%")) / 100),
          morbidity_val == "/" ~ suppressWarnings(as.numeric(str_remove_all(morbidity, ".*/|%")) / 100))) %>% 
        select(-morbidity, -morbidity_val)
    }
    assign(tbl_name, tbl)
  }
  
  # Laboratories table ---------------------------------------------------
  outbreak_reports_laboratories <- map_dfr(outbreak_reports2, function(x){
    tests <- x$diagnostic_tests 
    if(is.null(dim(tests))){
      return()
    }
    return(tests)
  }) %>%
    janitor::clean_names() %>% 
    mutate_all(~tolower(.)) %>% 
    mutate(test_date = dmy(test_date))
  
  # Export -----------------------------------------------
  wahis_joined <- list("outbreak_reports_events" = outbreak_reports_events, 
                       "outbreak_reports_outbreaks" = outbreak_reports_detail, 
                       "outbreak_reports_outbreaks_summary" = outbreak_reports_summary,
                       "outbreak_reports_laboratories" = outbreak_reports_laboratories,
                       "outbreak_reports_diseases_unmatched" = diseases_unmatched)
  
  # remove empty tables
  wahis_joined <- keep(wahis_joined, ~nrow(.)>0)
  
  if(nrow(wahis_joined$outbreak_reports_diseases_unmatched)){warning("Unmatched diseases. Check outbreak_reports_diseases_unmatched table.")}
  
  return(wahis_joined)
  
}

