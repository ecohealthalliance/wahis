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
  
  reports <- scrape_outbreak_report_list() %>% 
    select(report_info_id, # url
           outbreak_thread_id = event_id_oie_reference) # thread number (does not reference a specific report)

  outbreak_reports_events2 <- outbreak_reports_events %>%
    left_join(reports) %>% 
    mutate(country_or_territory = case_when(
      country_or_territory == "Central African (Rep.)" ~ "Central African Republic",
      country_or_territory == "Dominican (Rep.)" ~ "Dominican Republic",
      country_or_territory == "Ceuta" ~ "Morocco",
      country_or_territory == "Melilla"~ "Morocco",
      TRUE ~ country_or_territory
    )) %>%
    mutate_if(is.character, tolower) %>%
    mutate(country_iso3c = countrycode::countrycode(country_or_territory, origin = "country.name", destination = "iso3c")) %>%
    select(report_id,
           url_report_id = report_info_id, # url
           country = country_or_territory,
           country_iso3c,
           disease_category,
           disease = disease_name,
           is_aquatic,
           report_date,
           report_type = report_title,
           reason_for_notification = translated_reason,
           date_of_confirmation_of_the_event = confirmed_on,
           date_of_start_of_the_event = start_date,
           date_event_resolved = end_date,
           date_of_previous_occurrence = last_occurance_date,
           casual_agent,
           serotype = disease_type,
           future_reporting = event_description_status,
           outbreak_thread_id, 
           everything() 
    ) 
  
  # Cleaning
  outbreak_reports_events2 <- outbreak_reports_events2 %>%
    mutate(follow_up_count = ifelse(str_detect(report_type, "immediate notification"), 0, str_extract(report_type, "[[:digit:]]+"))) %>%
    mutate(is_final_report = str_detect(report_type, "final report")) %>%
    mutate(is_endemic = str_detect(future_reporting, "the event cannot be considered resolved"))
  
  # Dates handling - convert to  ISO-8601
  outbreak_reports_events2 <- outbreak_reports_events2 %>%
    mutate_at(vars(contains("date")), ~lubridate::as_datetime(.)) 
  
  # Check for missing date_event_resolved
  missing_resolved <- outbreak_reports_events2 %>%
    filter(is.na(date_event_resolved)) %>%
    filter(is_final_report)
  
  if(nrow(missing_resolved)){
    # Check threads to confirm these are final. If they are, then assume last report is the end date.
    check_final <- outbreak_reports_events2 %>%
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
    
    outbreak_reports_events2 <- outbreak_reports_events2 %>%
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
  
  outbreak_reports_events2 <- outbreak_reports_events2 %>%
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
  
  diseases_unmatched <- outbreak_reports_events2 %>%
    filter(is.na(ando_id)) %>%
    distinct(disease) %>%
    mutate(table = "outbreak_animal")
  
  # write_csv(diseases_unmatched, here::here("inst/diseases/outbreak_report_diseases_unmatched_20210507.csv"))
  
  # Check threads to make sure disease is consistent across thread
  # outbreak_reports_events2 %>%
  #   group_by(outbreak_thread_id) %>%
  #   mutate(disease_count = n_distinct(disease)) %>%
  #   ungroup() %>%
  #   filter(disease_count > 1) %>%
  #   View() # these are missing immediate reports
  
  ### Understanding  IDs
  # outbreak_reports_events2$report_id # unique individual report id (in = initial, fur = follow up report)
  # outbreak_reports_events2$url_report_id # unique individual report url value
  # outbreak_reports_events2$outbreak_thread_id # outbreak thread identifier - does not correspond to report_id or url_report_id
  
  # Outbreak tables ---------------------------------------------------
  #TODO
  # outbreak table
  # testing to see if this works in our pipeline
  # documentation in readme - when did it change, how does the api work
  # caching
  # 6 month reports
  
  # impact - over event and thread
  # radii of space
  # time course
  # cases - deaths
  # confirm lat/lon
  
  # conn <- repeldata::repel_remote_conn()
  # DBI::dbListTables(conn)
  # outbreak_reports_outbreaks <- DBI::dbReadTable(conn, "outbreak_reports_outbreaks")
  # examp <- janitor::get_dupes(outbreak_reports_outbreaks, id, outbreak_number)
  
  # outbreak_reports_detail$oieReference 
  # ^ denotes different locations within one report - not unique because there can be mltiple species
  # outbreak_reports_detail$outbreakInfoId 
  # seems to be a unique id that is reduntant with oieReference - leaving out for now

  tic()
  outbreak_reports_detail <- map_dfr(outbreak_reports2[1:200], function(x){
    
    report_id <- tibble(report_id = x$reportDto$reportId)
    outbreak_map <-  x$eventOutbreakDto$outbreakMap

    if(is.null(outbreak_map)) return()
    
    map_dfr(outbreak_map, function(xx){   
      out <- xx %>% 
        compact() %>% 
        purrr::keep(., ~!is.list(.x)) %>% 
        as_tibble() %>% 
        distinct() %>% 
        bind_cols(report_id, .)
      
      # add species details
      out <- xx$speciesDetails[-length(xx$speciesDetails)] %>% 
        compact() %>%  
        map_dfr(as_tibble) %>% 
        bind_cols(out, .)
      
      # add animal cat
      out <- xx$animalCategory %>% 
        compact() %>% 
        map_dfr(as_tibble) %>% 
        bind_cols(out, .)
      
      # add control measures
      out <- out %>% 
        mutate(control_measures = paste(xx$controlMeasures, collapse = "; "))
      
      return(out)
    }) 
  })
  toc()
  
  if(nrow(outbreak_reports_detail)) {
    
    # note: vaccinated is missing?????
    outbreak_reports_detail2 <- outbreak_reports_detail %>%
      mutate_if(is.character, tolower) %>%
      janitor::clean_names()  %>% 
      select(-starts_with("total_")) %>% # these are rolling and values and may cause confusion
      select(-specie_id, -morbidity, -mortality, -outbreak_info_id) %>% 
      rename(taxa = spicie_name,
             killed_and_disposed = killed,
             slaughtered_for_commercial_use = slaughtered) %>% 
      mutate_at(vars(susceptible, cases, deaths, killed_and_disposed, slaughtered_for_commercial_use), ~replace_na(., 0))
    
    #TODO
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
  # 
  # outbreak_reports_summary <- map_df(outbreak_reports2, function(x){
  #   if(length(x$outbreak_summary) == 1){return()}
  #   x$outbreak_summary %>% mutate_all(as.character)}) 
  # 
  # if(nrow(outbreak_reports_summary)) {
  #   outbreak_reports_summary <- outbreak_reports_summary %>%
  #     mutate_all(~tolower(.)) %>% 
  #     janitor::clean_names() %>%
  #     mutate_all(~str_remove(., "%")) %>%
  #     rename(total_morbidity_perc = total_apparent_morbidity_rate,
  #            total_mortality_perc = total_apparent_mortality_rate,
  #            total_case_fatality_perc = total_apparent_case_fatality_rate,
  #            total_susceptible_animals_lost_perc = total_proportion_susceptible_animals_lost) %>% 
  #     mutate_at(vars(starts_with("total_")),  ~str_remove_all(., "\\**"))  %>% # note "**" means not calculated because of missing information
  #     mutate_at(vars(starts_with("total_")),  ~str_remove_all(., "-")) 
  # }
  # 
  # 
  # 
  # # Fixes to mortality and morbidity fields (events and outbreak tables) ---------------------------------
  # for(tbl_name in c("outbreak_reports_events", "outbreak_reports_detail")){
  #   tbl <- get(tbl_name)
  #   if(nrow(tbl)==0) next()
  #   
  #   if(tbl %has_name% "mortality"){
  #     tbl <- tbl %>% 
  #       mutate(mortality_val = str_extract(mortality, "scale 0 to 5|%|/")) %>% 
  #       mutate(mortality_rate = case_when(
  #         mortality_val == "scale 0 to 5" ~ suppressWarnings(as.numeric(str_remove(mortality, "\\(scale 0 to 5\\)"))) * 0.2,
  #         mortality_val == "%" ~  suppressWarnings(as.numeric(str_remove(mortality, "%")) / 100),
  #         mortality_val == "/" ~ suppressWarnings(as.numeric(str_remove_all(mortality, ".*/|%")) / 100))) %>% 
  #       select(-mortality, -mortality_val)
  #   }
  #   
  #   if(tbl %has_name% "morbidity"){
  #     tbl <- tbl %>% 
  #       mutate(morbidity_val = str_extract(morbidity, "scale 0 to 5|%|/")) %>% 
  #       mutate(morbidity_rate = case_when(
  #         morbidity_val == "scale 0 to 5" ~ suppressWarnings(as.numeric(str_remove(morbidity, "\\(scale 0 to 5\\)"))) * 0.2,
  #         morbidity_val == "%" ~  suppressWarnings(as.numeric(str_remove(morbidity, "%")) / 100),
  #         morbidity_val == "/" ~ suppressWarnings(as.numeric(str_remove_all(morbidity, ".*/|%")) / 100))) %>% 
  #       select(-morbidity, -morbidity_val)
  #   }
  #   assign(tbl_name, tbl)
  # }
  # 
  # # Laboratories table ---------------------------------------------------
  # outbreak_reports_laboratories <- map_dfr(outbreak_reports2, function(x){
  #   tests <- x$diagnostic_tests 
  #   if(is.null(dim(tests))){
  #     return()
  #   }
  #   return(tests)
  # }) %>%
  #   janitor::clean_names() %>% 
  #   mutate_all(~tolower(.)) %>% 
  #   mutate(test_date = dmy(test_date))
  # 
  # # Export -----------------------------------------------
  # wahis_joined <- list("outbreak_reports_events" = outbreak_reports_events, 
  #                      "outbreak_reports_outbreaks" = outbreak_reports_detail, 
  #                      "outbreak_reports_outbreaks_summary" = outbreak_reports_summary,
  #                      "outbreak_reports_laboratories" = outbreak_reports_laboratories,
  #                      "outbreak_reports_diseases_unmatched" = diseases_unmatched)
  # 
  # # remove empty tables
  # wahis_joined <- keep(wahis_joined, ~nrow(.)>0)
  # 
  # # change some columns to numeric
  # if(!purrr::is_empty(wahis_joined)){
  #   wahis_joined  <- map(wahis_joined, function(tb){
  #     tb %>%
  #       mutate_at(vars(suppressWarnings(one_of("id", "outbreak_thread_id", "total_new_outbreaks", "follow_up",
  #                                              "mortality_rate", "morbidity_rate",
  #                                              "susceptible", "deaths", "killed_and_disposed_of", "cases", "slaughtered",
  #                                              "total_susceptible", "total_deaths", "total_killed_and_disposed_of", "total_cases", "total_slaughtered",
  #                                              "total_morbidity_perc", "total_mortality_perc", "total_case_fatality_perc", "total_susceptible_animals_lost_perc"
  #       ))), as.numeric)
  #   })
  # }
  # 
  # 
  # if(nrow(wahis_joined$outbreak_reports_diseases_unmatched)){warning("Unmatched diseases. Check outbreak_reports_diseases_unmatched table.")}
  # 
  # return(wahis_joined)
  
}

