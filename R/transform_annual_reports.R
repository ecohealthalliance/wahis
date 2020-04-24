# For summing counts 
sum_na <- function(vec) ifelse(all(is.na(vec)), NA_integer_, sum(as.numeric(vec), na.rm = TRUE))


#' Convert a list of scraped annual reports to a list of table
#' @param annual_reports a list of annual reports produced by [ingest_annual_report]
#' @import dplyr tidyr stringr purrr
#' @importFrom assertthat %has_name%
#' @importFrom janitor make_clean_names get_dupes
#' @importFrom readr read_csv
#' @importFrom readxl read_xlsx
#' @export
transform_annual_reports <- function(annual_reports) {
  
  # Initial processing ------------------------------------------------------
  
  ##### Remove report errors
  annual_reports2 <- keep(annual_reports, function(x){
    !is.null(x) && !is.null(x$ingest_status) && x$ingest_status == "available"
  })
  
  if(!length(annual_reports2)) return(NULL)
  
  #####  Extract and rbind tables
  tnames <- c('submission_info', 'diseases_present', 'diseases_absent', 'diseases_present_detail', 'diseases_unreported', 'disease_humans', 'animal_population', 'veterinarians', 'national_reference_laboratories', 'national_reference_laboratories_detail', 'vaccine_manufacturers', 'vaccine_manufacturers_detail', 'vaccine_production')
  
  wahis_joined <- map(tnames, function(name){
    map_dfr(annual_reports2, ~`[[`(., name)) %>% 
      mutate_at(vars(-country, -country_iso3c, -report, -report_months), ~tolower(.))
  })
  
  names(wahis_joined) <- tnames
  tnames_absent <- tnames[!map_lgl(wahis_joined, ~nrow(.)>0)]
  
  if(length(tnames_absent)){
    warning(paste("Following tables are empty:", paste(tnames_absent, collapse = ", ")))
  }
  
  #####  Table name assertions 
  warn_that(wahis_joined$submission_info %has_name%  c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'submission_info', 'submission_value', 'submission_animal_type'))
  warn_that(wahis_joined$diseases_present %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'disease', 'occurrence', 'serotype_s', 'new_outbreaks', 'total_outbreaks', 'species', 'control_measures', 'official_vaccination', 'measuring_units', 'susceptible', 'cases', 'deaths', 'killed_and_disposed_of', 'slaughtered', 'vaccination_in_response_to_the_outbreak_s', 'oie_listed', 'notes'))
  warn_that(wahis_joined$diseases_absent %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'taxa', 'disease', 'date_of_last_occurrence', 'species', 'control_measures', 'official_vaccination', 'oie_listed', 'notes'))
  warn_that(wahis_joined$diseases_present_detail %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'period', 'adm', 'serotype_s', 'new_outbreaks', 'total_outbreaks', 'species', 'family_name', 'latin_name', 'measuring_units', 'susceptible', 'cases', 'deaths', 'killed_and_disposed_of', 'slaughtered', 'vaccination_in_response_to_the_outbreak_s', 'adm_type', 'temporal_scale', 'disease'))
  warn_that(wahis_joined$diseases_unreported %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'taxa', 'disease', 'oie_listed'))
  warn_that(wahis_joined$disease_humans %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'disease', 'no_information_available', 'disease_absent', 'disease_present_number_of_cases_unknown', 'disease_present_number_of_cases_known', 'human_cases', 'human_deaths'))
  warn_that(wahis_joined$animal_population %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'species', 'production', 'total', 'units', 'number', 'units_2'))
  warn_that(wahis_joined$veterinarians %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'veterinarian_field', 'public_sector', 'total', 'private_sector', 'veterinarian_class'))
  warn_that(wahis_joined$national_reference_laboratories %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'name', 'contacts', 'latitude', 'longitude'))
  warn_that(wahis_joined$national_reference_laboratories_detail %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'laboratory', 'disease', 'test_type'))
  warn_that(wahis_joined$vaccine_manufacturers %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'manufacturer', 'contacts', 'year_of_start_of_activity', 'year_of_cessation_of_activity'))
  warn_that(wahis_joined$vaccine_manufacturers_detail %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'disease', 'manufacturer', 'vaccine', 'vaccine_type', 'year_of_start_of_production', 'year_of_end_of_production_if_production_ended'))
  warn_that(wahis_joined$vaccine_production %has_name% c('country', 'country_iso3c', 'report_year', 'report_months', 'report_semester', "report", 'manufacturer', 'vaccine', 'doses_produced', 'doses_exported'))
  
  #####  NA handling
  wahis_joined <- map(wahis_joined, function(x){
    x %>%
      mutate_at(vars(-suppressWarnings(one_of("occurrence"))),  ~ifelse(. %in% c("no", "..."), NA, .)) # warning "Unknown columns: `occurrence`" is ok
  })
  
  ##### Read in lookup tables
  taxa_lookup <- read_csv(system.file("annual_report_lookups", "lookup_species.csv", package = "wahis")) %>%
    rename(taxa_class = group)
  
  suspected_code <- read_csv(system.file("annual_report_lookups", "lookup_occurrence.csv", package = "wahis")) %>%
    mutate(code = str_remove_all(code, "\"")) %>% 
    filter(disease_status == "suspected") %>% 
    pull(code)
  
  control <- read_csv(system.file("annual_report_lookups", "lookup_control.csv", package = "wahis"))
  control$code[control$code_value == "Vaccination in response to the outbreak(s)"] <- "Vr"
  control <- control %>% select(code, code_value) %>% distinct() %>% arrange(code) 
  control_lookup <- structure(as.vector(tolower(control$code_value)), .Names = tolower(control$code))
  
  ando_disease_lookup <- readxl::read_xlsx(system.file("diseases", "disease_lookup.xlsx", package = "wahis")) %>% 
    rename(disease_class = class_desc) %>% 
    separate_rows(preferred_label, sep = ";") %>% 
    mutate_at(.vars = c("ando_id", "preferred_label", "disease_class"), ~na_if(., "NA"))
  
  # Pre-processing animal disease and host tables ----------------------------------------------------
  
  diseases_present <- wahis_joined$diseases_present
  diseases_present_detail <- wahis_joined$diseases_present_detail
  diseases_absent <- wahis_joined$diseases_absent 
  diseases_unreported <- wahis_joined$diseases_unreported
  
  ##### misc fixes (probably should have been handled in ingest function)
  diseases_present <- diseases_present %>% 
    rename(taxa = species, serotype = serotype_s, vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s) %>% 
    group_by(report, disease) %>%
    fill(occurrence, .direction = "down") %>% 
    fill(serotype, .direction = "down") %>% 
    ungroup() %>% 
    mutate(serotype = na_if(serotype, "not typed")) %>% 
    mutate_at(.vars = c("new_outbreaks", "total_outbreaks", "official_vaccination", "susceptible", "cases", "deaths", "killed_and_disposed_of", "slaughtered", "vaccination_in_response_to_the_outbreak"), ~as.numeric(.))
  
  diseases_present_detail <- diseases_present_detail %>% 
    rename(taxa = species, serotype = serotype_s, vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s,
           new_outbreaks_detail = new_outbreaks, total_outbreaks_detail = total_outbreaks) %>% 
    group_by(report, period, disease) %>%
    fill(adm, .direction = "down") %>% 
    fill(serotype, .direction = "down") %>% 
    ungroup() %>% 
    mutate(serotype = na_if(serotype, "not typed")) %>% 
    mutate_at(.vars = c("new_outbreaks_detail", "total_outbreaks_detail", "susceptible", "cases", "deaths", "killed_and_disposed_of", "slaughtered", "vaccination_in_response_to_the_outbreak"), ~as.numeric(.))
  
  diseases_absent <- diseases_absent %>% 
    rename(date_of_last_occurrence_if_absent = date_of_last_occurrence,
           taxa_parent = taxa,  taxa = species) %>%  # parent was extracted from table headers, taxa was from columns) 
    group_by(report, disease) %>%
    fill(date_of_last_occurrence_if_absent) %>% 
    ungroup()
  
  ##### Clean taxa codes
  if(nrow(diseases_present)){
    diseases_present <- diseases_present %>%
      mutate(taxa = str_replace(taxa, "\\(fau\\)", "\\(wild\\)")) %>%
      left_join(taxa_lookup, by = c("taxa" = "code")) %>%
      mutate(taxa = tolower(coalesce(code_value, taxa))) %>% 
      select(-code_value) 
  }
  
  if(nrow(diseases_present_detail)){
    diseases_present_detail <- diseases_present_detail %>%
      mutate(taxa = str_replace(taxa, "\\(fau\\)", "\\(wild\\)")) %>%
      left_join(taxa_lookup, by = c("taxa" = "code")) %>%
      mutate(taxa = tolower(coalesce(code_value, taxa))) %>% 
      select(-code_value) 
  }
  
  if(nrow(diseases_absent)){
    diseases_absent <- diseases_absent %>%
      mutate(taxa = str_replace(taxa, "\\(fau\\)", "\\(wild\\)")) %>%
      mutate(taxa = na_if(taxa, "***")) %>%  # if ***, default to taxa parent
      mutate(taxa = coalesce(taxa, taxa_parent)) %>%  # otherwise default to taxa (from table columns)
      left_join(taxa_lookup, by = c("taxa" = "code")) %>%
      mutate(taxa = tolower(coalesce(code_value, taxa))) %>% 
      select(-code_value, -taxa_parent)
  }
  
  ##### Assume some missing taxa from disease name
  if(nrow(diseases_present)){
    diseases_present <- diseases_present %>% 
      mutate(taxa2 = str_extract(disease, "bov|caprine|swine|sui|porcine|equi|horse|avian|fowl|bees|rabbit")) %>% 
      mutate(taxa2 = recode(taxa2, 
                            "bov" = "cattle",
                            "caprine" = "cattle", 
                            "sui" = "swine",
                            "porcine" = "swine",
                            "equi" = "equidae",
                            "horse" = "equidae",
                            "avian" = "birds",
                            "fowl" = "birds"))
    
    # check assumptions are correct
    # diseases_present %>%
    #     filter(is.na(taxa)) %>%
    #     drop_na(taxa2) %>%
    #     distinct(taxa2, disease) %>%
    #     View()
    
    diseases_present <- diseases_present %>% 
      mutate(taxa = coalesce(taxa, taxa2)) %>% 
      select(-taxa2) %>% 
      distinct()
    
    # how many taxa are still NA?
    # diseases_present %>%
    #     filter(is.na(taxa)) %>%
    #     nrow()
  }
  
  ##### Clean disease names
  disease_vect <- unique(c(unique(diseases_present$disease), 
                           unique(diseases_present_detail$disease),
                           unique(diseases_absent$disease), 
                           unique(diseases_unreported$disease)))
  
  animal_disease_lookup <- tibble(disease = disease_vect) %>%
    arrange(disease) %>% 
    mutate(disease_clean = str_replace(disease, "domesticand wild|domestic andwild", "domestic and wild")) %>%
    mutate(disease_population = str_extract_all(disease_clean, "domestic|wild")) %>%
    mutate(disease_population = map_chr(disease_population, ~paste(sort(unique(.x)), collapse = " and "))) %>%
    mutate(disease_population = ifelse(disease_population=="", "not specified", disease_population)) %>%
    mutate(disease_clean = str_remove(disease_clean, "\\(domestic and wild\\)|\\(domestic\\)|\\(wild\\)"))  %>% 
    mutate(disease_clean = trimws(disease_clean)) %>% 
    rename(disease_old = disease, disease = disease_clean)
  
  # export for manual lookup (see inst/processing_scripts/clean_disease_names.R)
  # animal_disease_export <- animal_disease_lookup %>%
  #   distinct(disease) 
  # write_csv(animal_disease_export, here::here("inst/diseases/annual_report_diseases_animals.csv"))
  
  # join manual ontology lookup into disease lookup
  ando_disease_lookup_animal <- ando_disease_lookup %>% 
    filter(report == "animal") %>% 
    select(-report) 
  
  animal_disease_lookup <- animal_disease_lookup %>% 
    left_join(ando_disease_lookup_animal, by = "disease") %>% 
    distinct()
  
  # save unmatched diseases to database
  wahis_joined$diseases_unmatched <- animal_disease_lookup %>% 
    filter(is.na(ando_id)) %>% 
    distinct(disease) %>% 
    mutate(table = "annual_animal")
  
  # clean disease names in existing tables
  for(tbl_name in c("diseases_present", "diseases_present_detail", "diseases_absent", "diseases_unreported")){
    tbl <- get(tbl_name)
    if(nrow(tbl)==0) next()
    disease_joined <- tbl %>% 
      rename(disease_old = disease) %>% 
      left_join(animal_disease_lookup) %>% 
      mutate(disease = coalesce(preferred_label, disease)) %>% 
      select(-preferred_label, -disease_old) 
    warn_that(!any(is.na(disease_joined$disease)))
    assign(tbl_name, disease_joined)
  }
  
  ##### For present table, handle occurrences/statuses  
  # if disease is listed as both present and suspected, and the counts are all NA for suspected, then remove the suspected
  if(nrow(diseases_present)){
    diseases_present <- diseases_present %>% 
      mutate(disease_status = if_else(occurrence %in% suspected_code, "suspected", "present")) %>% 
      group_by(report, disease, disease_population, taxa) %>% 
      mutate(suspected_and_present = all(c("present", "suspected") %in% disease_status)) %>% 
      ungroup() %>% 
      mutate(suspected_na = suspected_and_present == TRUE & 
               disease_status == "suspected" & 
               is.na(serotype) &
               is.na(new_outbreaks) & 
               is.na(total_outbreaks) &
               is.na(official_vaccination) &
               is.na(susceptible) &
               is.na(cases) &
               is.na(deaths) &
               is.na(killed_and_disposed_of) &
               is.na(slaughtered) &
               is.na(vaccination_in_response_to_the_outbreak)) %>% 
      filter(!suspected_na) %>% 
      select(-suspected_na, -suspected_and_present, -occurrence)
  }
  
  # Split out disease tables  ------------------------------------------
  # aggrate over clean disease names
  
  ##### animal_diseases table
  if(nrow(diseases_present)){
    
    animal_diseases_present <- diseases_present %>% 
      group_by(country, country_iso3c, report, report_year, report_months, report_semester,
               disease, disease_population, disease_class, ando_id, disease_status, serotype) %>%
      summarize(new_outbreaks = sum_na(new_outbreaks),
                total_outbreaks = sum_na(total_outbreaks),
                taxa = paste(sort(unique(na.omit(taxa))), collapse = "; "),
                oie_listed = "true" %in% unique(oie_listed),
                notes = paste(na.omit(notes), collapse = "\n")) %>%
      ungroup() %>%
      mutate(taxa = na_if(taxa, ""),
             notes = na_if(notes, ""))
    
    # sc <- get_dupes(animal_diseases_present, report, disease, disease_population, serotype, taxa)
  }
  
  if(nrow(diseases_absent)){
    animal_diseases_absent <- diseases_absent %>%
      mutate(disease_status = "absent") %>%
      mutate(date_of_last_occurrence_if_absent = messy_dates(date_of_last_occurrence_if_absent)) %>% 
      group_by(country, country_iso3c, report, report_year, report_months, report_semester,
               disease, disease_population,  disease_class, ando_id, disease_status) %>% 
      summarize(taxa = paste(sort(unique(na.omit(taxa))), collapse = "; "),
                oie_listed = "true" %in% unique(oie_listed),
                date_of_last_occurrence_if_absent = suppressWarnings(max(date_of_last_occurrence_if_absent, na.rm = TRUE))) %>% # take latest date if more than one
      ungroup() %>%
      mutate(taxa = na_if(taxa, "")) %>% 
      mutate(new_outbreaks = 0, total_outbreaks = 0) %>% 
      distinct()
    
    #sc <- get_dupes(animal_diseases_absent, report, disease, disease_population)
  }
  
  if(nrow(diseases_unreported)){
    animal_diseases_unreported <- diseases_unreported %>% 
      mutate(disease_status = "unreported") %>%
      group_by(country, country_iso3c, report, report_year, report_months, report_semester,
               disease, disease_population,  disease_class, ando_id, disease_status) %>% 
      summarize(taxa = paste(sort(unique(na.omit(taxa))), collapse = "; "),
                oie_listed = "true" %in% unique(oie_listed)) %>% 
      ungroup() %>%
      mutate(taxa = na_if(taxa, "")) %>% 
      mutate(new_outbreaks = 0, total_outbreaks = 0)  %>% 
      distinct()
    
    #sc <- get_dupes(animal_diseases_unreported, report, disease, disease_population)
  }
  
  animal_diseases <- bind_rows(animal_diseases_present, animal_diseases_absent) %>% 
    bind_rows(animal_diseases_unreported) 
  
  # remove cases where diseases are listed with more than one status. present or suspected > absent > unreported
  
  if(nrow(animal_diseases)){
    animal_diseases <- animal_diseases %>% 
      mutate(disease_status_rank = recode(disease_status, "present" = 1, "suspected" = 1, "absent" = 2, "unreported" = 3)) %>%
      group_by(report, disease, disease_population) %>%
      filter(disease_status_rank == min(disease_status_rank)) %>%
      ungroup() %>% 
      select(-disease_status_rank) %>% 
      mutate(disease_status  = recode(disease_status, "unreported" = "absent")) # for our purposes, assume unreported = absent
    #sc <- get_dupes(animal_diseases, report, disease, disease_population, taxa)
  }
  
  ##### animal_diseases_detail table
  if(nrow(diseases_present_detail)){
    animal_diseases_detail <- diseases_present_detail %>%
      mutate(disease_status = "present") %>%
      group_by(country, country_iso3c, report, report_year, report_months, report_semester,
               disease, disease_population, disease_class, ando_id, disease_status,
               serotype, period, temporal_scale, adm, adm_type) %>% 
      summarize(new_outbreaks_detail = sum_na(new_outbreaks_detail),
                total_outbreaks_detail = sum_na(total_outbreaks_detail),
                taxa = paste(sort(unique(na.omit(taxa))), collapse = "; ")) %>%
      ungroup() %>%
      mutate(taxa = na_if(taxa, ""))
    
    # sc <- get_dupes(animal_diseases_detail, report, disease, disease_population, serotype, period, adm, taxa)
  }
  
  # Split out hosts tables  ------------------------------------------
  
  ##### animal_hosts table
  if(nrow(diseases_present)){
    animal_hosts_present <- diseases_present %>%
      group_by(country, country_iso3c, report, report_year, report_months, report_semester,
               disease, disease_population, disease_class, ando_id, disease_status, serotype, taxa) %>%
      summarize(official_vaccination = sum_na(official_vaccination),
                susceptible = sum_na(susceptible),
                cases = sum_na(cases),
                deaths = sum_na(deaths),
                killed_and_disposed_of = sum_na(killed_and_disposed_of),
                vaccination_in_response_to_the_outbreak = sum_na(vaccination_in_response_to_the_outbreak),
                measuring_units = paste(na.omit(unique(measuring_units)), collapse = " "), # this results in a few weird unit combos
                control_measures = paste(na.omit(unique(control_measures)), collapse = " ") # may result in some dupes, handled below
      ) %>%
      ungroup() %>% 
      mutate(control_measures2 = ifelse(!is.na(vaccination_in_response_to_the_outbreak) & vaccination_in_response_to_the_outbreak > 0,
                                        "Vr", "")) %>% # < there is no code for vaccination_in_response_to_the_outbreak so I am making one up
      unite(control_measures, control_measures, control_measures2, sep = " ") %>% # this is clunky but other approaches with paste and glue were sloooow
      mutate(control_measures = str_trim(control_measures)) %>%
      mutate(control_measures = na_if(control_measures, "")) %>% 
      mutate(measuring_units = na_if(measuring_units, "")) 
    
    # fix seemingly incorrect measurement units (hives for non-bees)
    animal_hosts_present <- animal_hosts_present %>%
      mutate(measuring_units = ifelse(measuring_units == "Hives" & taxa != "api" & taxa != "***",  "Animals",  measuring_units))
    
    # confirming we do not have dupe taxa
    taxa_check <- animal_hosts_present %>%
      group_by(report, disease, disease, disease_population, serotype, disease_status) %>%
      mutate(n = n(), n_taxa = n_distinct(taxa)) %>%
      filter(n != n_taxa) 
    warn_that(nrow(taxa_check)==0)
  }
  
  if(nrow(diseases_absent)){
    animal_hosts_absent <- diseases_absent %>%
      mutate(disease_status = "absent") %>% 
      group_by(country, country_iso3c, report, report_year, report_months, report_semester,
               disease, disease_population,  disease_class, ando_id, disease_status, taxa) %>% 
      summarize(official_vaccination = sum_na(official_vaccination),
                control_measures = paste(na.omit(unique(control_measures)), collapse = " ") # may result in some dupes, handled below
      ) %>% 
      ungroup()
    warn_that(!any(is.na(animal_hosts_absent$taxa)))
  }
  
  if(nrow(diseases_unreported)){
    animal_hosts_unreported <- diseases_unreported %>%
      mutate(disease_status = "absent") 
  }
  
  animal_hosts <- bind_rows(animal_hosts_present, animal_hosts_absent) #%>%
  # bind_rows(animal_hosts_unreported) %>% 
  # distinct()
  
  # Look up control codes
  if(nrow(animal_hosts)){
    animal_hosts <- animal_hosts %>%
      mutate(control_measures = str_split(control_measures, " ")) %>% # make control_measures into list
      mutate(control_measures = map(control_measures, ~control_lookup[.])) %>% # lookup all items in list
      mutate(control_measures = map_chr(control_measures, ~str_flatten(sort(unique(.)), collapse = "; "))) # back to string, now with full code measures
  }
  
  ##### animal_hosts_detail table
  if(nrow(diseases_present_detail)){
    animal_hosts_detail <- diseases_present_detail %>%
      mutate(disease_status = "present") %>%
      mutate(measuring_units = ifelse(measuring_units == "Hives" & taxa != "api" & taxa != "***",  "Animals",  measuring_units)) %>% 
      group_by(country, country_iso3c, report, report_year, report_months, report_semester,
               disease, disease_population, disease_class, ando_id, disease_status, taxa, family_name, latin_name,
               serotype, period, temporal_scale, adm, adm_type) %>% 
      summarize(susceptible_detail = sum_na(susceptible),
                cases_detail = sum_na(cases),
                deaths_detail = sum_na(deaths),
                killed_and_disposed_of_detail = sum_na(killed_and_disposed_of),
                vaccination_in_response_to_the_outbreak_detail = sum_na(vaccination_in_response_to_the_outbreak),
                measuring_units = paste(na.omit(unique(measuring_units)), collapse = " ")) %>%  # this results in a few weird unit combos
      ungroup() 
  }
  
  # Human tables  ------------------------------------------
  
  disease_humans <- wahis_joined$disease_humans
  
  if(nrow(disease_humans)){
    human_disease_lookup <- disease_humans %>%
      distinct(disease) 
    # write_csv(human_disease_lookup, here::here("inst/diseases/annual_report_diseases_humans.csv"))
    
    # Read in manual lookup
    ando_disease_lookup_human <- ando_disease_lookup %>% 
      filter(report == "annual human")  %>% 
      select(-report)
    
    human_disease_lookup <- human_disease_lookup %>% 
      left_join(ando_disease_lookup_human, by = "disease") %>% 
      distinct()
    
    wahis_joined$diseases_unmatched <- bind_rows(wahis_joined$diseases_unmatched, 
                                                 human_disease_lookup %>% 
                                                   filter(is.na(ando_id)) %>% 
                                                   distinct(disease) %>% 
                                                   mutate(table = "annual_human"))
    
    disease_humans <- disease_humans %>%
      left_join(human_disease_lookup) %>% 
      mutate(disease = coalesce(preferred_label, disease)) %>% 
      select(-preferred_label) %>% 
      gather(key = "human_disease_status", value = "value", no_information_available:disease_present_number_of_cases_known) %>%
      drop_na(value) %>% 
      select(-value) %>% 
      mutate(human_disease_status = str_replace_all(human_disease_status, "_", " "))
    
    warn_that(!any(is.na(disease_humans$disease)))
  }
  
  # Add tables to database --------------------------------------------------
  
  index <- names(wahis_joined)[!names(wahis_joined) %in% c("diseases_present", "diseases_present_detail", "diseases_absent", "diseases_unreported", "disease_humans")]
  wahis_joined <- wahis_joined %>% `[`(index)
  wahis_joined$animal_diseases <- animal_diseases
  wahis_joined$animal_hosts <- animal_hosts
  wahis_joined$animal_diseases_detail <- animal_diseases_detail
  wahis_joined$animal_hosts_detail <- animal_hosts_detail
  wahis_joined$disease_humans <- disease_humans
  
  # Misc other cleaning items -----------------------------------------------
  if (nrow(wahis_joined$animal_population)) {
    wahis_joined$animal_population <- wahis_joined$animal_population %>%
      rename(taxa = species, population_count = total, population_units = units, producers_count = number, producers_units = units_2)
  }
  
  if (nrow(wahis_joined$veterinarians)) {
    wahis_joined$veterinarians <- wahis_joined$veterinarians %>%
      rename(public_sector_count = public_sector, private_sector_count = private_sector, total_count = total)
  }
  
  if (nrow(wahis_joined$national_reference_laboratories)) {
    wahis_joined$national_reference_laboratories <- wahis_joined$national_reference_laboratories %>%
      rename(laboratory_name = name, laboratory_contact = contacts, laboratory_latitude = latitude, laboratory_longitude = longitude)
  }
  
  if (nrow(wahis_joined$national_reference_laboratories_detail)) {
    wahis_joined$national_reference_laboratories_detail <- wahis_joined$national_reference_laboratories_detail %>%
      rename(laboratory_name = laboratory)
  }
  
  if (nrow(wahis_joined$vaccine_manufacturers)) {
    wahis_joined$vaccine_manufacturers <- wahis_joined$vaccine_manufacturers %>%
      rename(vaccine_manufacturer = manufacturer, vaccine_contact = contacts, year_start_vaccine_activity = year_of_start_of_activity, year_cessation_vaccine_activity = year_of_cessation_of_activity)
  }
  
  if (nrow(wahis_joined$vaccine_manufacturers_detail)) {
    wahis_joined$vaccine_manufacturers_detail <- wahis_joined$vaccine_manufacturers_detail %>%
      rename(vaccine_manufacturer = manufacturer, year_start_vaccine_production = year_of_start_of_production, year_end_vaccine_production = year_of_end_of_production_if_production_ended)
  }
  
  if (nrow(wahis_joined$vaccine_production)) {
    wahis_joined$vaccine_production <- wahis_joined$vaccine_production %>%
      rename(vaccine_manufacturer = manufacturer)
  }
  
  #TODO note that vaccine_manufacturers_detail and vaccine_production can potentially be joined by vaccine - would need to clean the fields first
  
  wahis_joined$submission_info <- wahis_joined$submission_info %>%
    mutate(submission_animal_type = recode(submission_animal_type, "terrestrial and aquatic" = "aquatic and terrestrial"))
  names(wahis_joined) <- paste0("annual_reports_", names(wahis_joined))
  
  
  # Postprocess -------------------------------------------------------------
  
  # check measurement units
  # wahis_joined$annual_reports_animal_hosts %>% 
  #   count(measuring_units, species) %>%
  #   View
  # wahis_joined$annual_reports_animal_hosts_detail %>% 
  #   count(measuring_units, species) %>%
  #   View
  
  # remove empty tables
  wahis_joined <- keep(wahis_joined, ~nrow(.)>0)
  
  if(nrow(wahis_joined$annual_reports_diseases_unmatched)){warning("Unmatched diseases. Check annual_reports_diseases_unmatched table.")}
  
  return(wahis_joined)
  
}
