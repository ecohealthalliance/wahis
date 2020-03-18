#' Convert a list of scraped ourbreak reports to a list of table
#' @param outbreak_reports a list of outbreak reports produced by [ingest_outbreak_report]
#' @import dplyr tidyr purrr
#' @importFrom janitor clean_names
#' @importFrom stringr str_detect str_extract
#' @importFrom lubridate dmy
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
    mutate(final_report = str_detect(report_type, "Final report"))
  
  # New outbreak?
  outbreak_reports_events <- outbreak_reports_events %>%
    mutate(new_outbreak_in_report = map_lgl(outbreak_reports2, ~length(.$outbreak_summary) > 1))
  
  # Dates handling
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
                       "outbreak_reports_laboratories" = outbreak_reports_laboratories)
  return(wahis_joined)
  
}

