#' Convert a list of scraped ourbreak reports to a list of table
#' @param outbreak_reports a list of outbreak reports produced by [ingest_outbreak_report]
#' @import dplyr tidyr purrr
#' @importFrom janitor clean_names
#' @importFrom stringr str_detect str_extract
#' @importFrom lubridate dmy myd ymd
#' @export

transform_outbreak_reports <- function(outbreak_reports) {
  
  # Remove report errors ---------------------------------------------------
  outbreak_reports2 <- keep(outbreak_reports, function(x){
    !is.null(x) && !is.null(x$ingest_status) && x$ingest_status == "available"
  })
  
  if(!length(outbreak_reports2)) return(NULL)
  
  # Events table ---------------------------------------------------
  exclude_fields <- c("Related reports", "related_reports", # can be determined from immediatate reports
                      "outbreak_detail", "outbreak_summary", "diagnostic_tests" # addressed in detail below
  )
  
  outbreak_reports2 <-  modify(outbreak_reports2, function(x){
    x$total_new_outbreaks <- as.character(x$total_new_outbreaks)
    return(x)
  })
  
  outbreak_reports_events <- map_dfr(outbreak_reports2, function(x){
    exclude_index <- which(names(x) %in% exclude_fields)
    as_tibble(x[-exclude_index])
  }) %>% 
    janitor::clean_names() 
  
  # Immediate report threads ---------------------------------------------------
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate(immediate_report = ifelse(str_detect(report_type, "Immediate notification"), id, immediate_report)) %>%
    mutate(follow_up = ifelse(str_detect(report_type, "Immediate notification"), 0, str_extract(report_type, "[[:digit:]]+"))) %>%
    mutate(final_report = str_detect(report_type, "Final report")) %>% 
    mutate(endemic = str_detect(future_reporting, "The event cannot be considered resolved"))
  
  
  # New outbreak?
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate(new_outbreak_in_report = map_lgl(outbreak_reports2, ~length(.$outbreak_summary) > 1))
  
  # Dates handling - convert to  ISO-8601 
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate_at(vars(starts_with("date"), "report_date", -"date_of_previous_occurrence"), ~dmy(.)) %>% 
    mutate(date_of_previous_occurrence_nchar = as.character(nchar(date_of_previous_occurrence))) %>% 
    mutate(date_of_previous_occurrence_resolution = recode(date_of_previous_occurrence_nchar,
                                                           "4" = "yyyy",
                                                           "7" = "yyyy-mm",
                                                           "10" = "yyyy-mm-dd",
                                                           .default = NA_character_)) %>% 
    mutate(date_of_previous_occurrence_resolution2 = date_of_previous_occurrence_resolution) %>% 
    pivot_wider(names_from = date_of_previous_occurrence_resolution2, values_from = date_of_previous_occurrence) %>% 
    mutate(`yyyy-mm-dd`= dmy(`yyyy-mm-dd`)) %>% 
    mutate(`yyyy-mm` = myd(`yyyy-mm`, truncated = 1)) %>% 
    mutate(`yyyy` = ymd(`yyyy`, truncated = 2)) %>% 
    mutate(date_of_previous_occurrence = coalesce(`yyyy-mm-dd`,`yyyy-mm`, `yyyy`)) %>% 
    select(-`NA`, -date_of_previous_occurrence_nchar, -`yyyy-mm-dd`,-`yyyy-mm`, -`yyyy`) 
  
  # Check for missing date_event_resolved
  missing_resolved <- outbreak_reports_events %>% 
    filter(is.na(date_event_resolved)) %>% 
    filter(final_report)
  
  # Check threads to confirm these are final. If they are, then assume last report is the end date.
  check_final <- outbreak_reports_events %>% 
    select(id, immediate_report, report_date) %>% 
    filter(immediate_report %in% missing_resolved$immediate_report) %>% 
    left_join(missing_resolved %>% select(id, final_report)) %>% 
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
    mutate(date_event_resolved = ifelse(id %in% check_final_resolved$id, report_date, date_event_resolved)) %>% 
    mutate(disease = trimws(tolower(disease)))
  
  # disease_export <- outbreak_reports_events %>% 
  #   distinct(disease, causal_agent) %>% 
  #   mutate_all(~tolower(trimws(.)))
  # write_csv(disease_export, here::here("inst/diseases/outbreak_report_diseases.csv"))
  
  # Read in manual lookup
  ando_disease_lookup <- readxl::read_xlsx(system.file("diseases", "disease_lookup.xlsx", package = "wahis")) %>% 
    rename(disease_class = class_desc) %>% 
    filter(report == "animal") %>% 
    select(-report) %>% 
    separate_rows(preferred_label, sep = ";") %>% 
    mutate_at(.vars = c("ando_id", "preferred_label", "disease_class"), ~na_if(., "NA"))
  
  outbreak_reports_events <- outbreak_reports_events %>% 
    left_join(ando_disease_lookup, by = "disease") %>% 
    mutate(disease = coalesce(preferred_label, disease)) %>% 
    select(-preferred_label) %>% 
    distinct() 
  
  diseases_unmatched <- outbreak_reports_events %>% 
    filter(is.na(ando_id)) %>% 
    distinct(disease) %>% 
    mutate(table = "outbreak_animal")
  
  
  #TODO - handling unresolved cases - currently workflow leaves them as marked final (so far - there are no cases like this 2020-03-19)
  
  # Outbreaks ---------------------------------------------------
  
  outbreak_reports_detail <- map_df(outbreak_reports2, function(x){
    if(length(x$outbreak_detail) == 1){return()}
    x$outbreak_detail })
  
  if(nrow(outbreak_reports_detail)) {
    outbreak_reports_detail <- outbreak_reports_detail %>%
      janitor::clean_names() %>%
      mutate(outbreak_number = trimws(outbreak_number))
  }
  
  outbreak_reports_summary <- map_df(outbreak_reports2, function(x){
    if(length(x$outbreak_summary) == 1){return()}
    x$outbreak_summary %>% mutate_all(as.character)}) 
  
  if(nrow(outbreak_reports_summary)) {
    outbreak_reports_summary <- outbreak_reports_summary %>%
      janitor::clean_names() %>%
      mutate_all(~str_remove(., "%")) %>%
      rename(total_morbidity_perc = total_apparent_morbidity_rate,
             total_mortality_perc = total_apparent_mortality_rate,
             total_case_fatality_perc = total_apparent_case_fatality_rate,
             total_susceptible_animals_lost_perc = total_proportion_susceptible_animals_lost)
  }
  
  
  # Laboratories table ---------------------------------------------------
  outbreak_reports_laboratories <- map_dfr(outbreak_reports2, function(x){
    tests <- x$diagnostic_tests 
    if(is.null(dim(tests))){
      return()
    }
    return(tests)
  }) %>%
    janitor::clean_names() 
  
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

