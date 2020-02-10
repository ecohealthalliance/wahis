# TODO better scoped imports

#' Convert a list of scraped annual reports to a list of table
#' @param annual_reports a list of annual reports produced by [ingest_annual_report]
#' @import dplyr tidyr stringr
#' @importFrom purrr keep map map_chr map_dfr map_lgl modify_at
#' @importFrom janitor make_clean_names
#' @importFrom readr read_csv
#' @export
transform_annual_reports <- function(annual_reports) {
  
  # Remove report errors ---------------------------------------------------
  annual_reports2 <- keep(annual_reports, function(x){
    !is.null(x) && !is.null(x$ingest_status) && x$ingest_status == "available"
  })
  
  if(!length(annual_reports2)) return(NULL)
  
  
  # Extract and rbind tables ----------------------------------------------------
  tnames <- c('metadata', 'submission_info', 'diseases_present', 'diseases_absent', 'diseases_present_detail', 'diseases_unreported', 'disease_humans', 'animal_population', 'veterinarians', 'national_reference_laboratories', 'national_reference_laboratories_detail', 'vaccine_manufacturers', 'vaccine_manufacturers_detail', 'vaccine_production')
  
  wahis_joined <- map(tnames, function(name){
    map_dfr(annual_reports2, ~`[[`(., name)) 
  })
  
  names(wahis_joined) <- tnames
  
  tnames_absent <- tnames[!map_lgl(wahis_joined, ~nrow(.)>0)]
  
  if(length(tnames_absent)){
    warning(paste("Following tables are empty:", paste(tnames_absent, collapse = ", ")))
  }
  # NA handling -------------------------------------------------------------
  # "empty" = missing/NA/blank in the reports
  # "No information" = "..." or "No" in the reports
  
  wahis_joined <- map(wahis_joined, function(x){
    x %>%
      mutate_all(~replace_na(., "empty")) %>%
      mutate_at(vars(-suppressWarnings(one_of("occurrence"))),  ~ifelse(. %in% c("No", "..."), "no information", .)) # warning "Unknown columns: `occurrence`" is ok
  })
  
  # Animal disease table ----------------------------------------------------
  animal_diseases <- wahis_joined$diseases_present
  
  if(nrow(animal_diseases)){
    animal_diseases <- animal_diseases %>%
      rename(serotype = serotype_s) %>%
      filter(occurrence != "empty") %>% # NAs are from nested species data - this is preserved in animal_hosts table
      mutate(status = "present") %>%
      select(country, country_iso3c, report_year, report_months, report_semester,
             disease, oie_listed, status,
             occurrence, serotype, new_outbreaks, total_outbreaks, notes)
  }
  
  # Add Absent table to animal disease table ----------------------------------------------------
  animal_diseases_absent <- wahis_joined$diseases_absent
  
  if(nrow(animal_diseases_absent)){
    animal_diseases_absent <- animal_diseases_absent %>%
      select(country, country_iso3c, report_year, report_months, report_semester,
             disease, oie_listed, taxa, date_of_last_occurrence_if_absent = date_of_last_occurrence) %>% 
      mutate(date_of_last_occurrence_if_absent = na_if(date_of_last_occurrence_if_absent, "empty")) %>% # filling and then taking distinct, rather than just dropping NAs, because there can be some cases where there is empty date of last occurrence and we do not want to filter out
      group_by(country, report_year, report_months, disease) %>%
      fill(date_of_last_occurrence_if_absent) %>%
      ungroup() %>%
      mutate(date_of_last_occurrence_if_absent = replace_na(date_of_last_occurrence_if_absent, "empty")) %>%
      distinct() %>% 
      mutate(occurrence = "-", new_outbreaks = "0", total_outbreaks = "0", status = "absent") 
  }
  
  animal_diseases <- bind_rows(animal_diseases %>%
                                 mutate(date_of_last_occurrence_if_absent = "disease not absent",
                                        taxa = "see animal_hosts table for taxa for diseases present"), 
                               animal_diseases_absent %>% mutate(serotype = "disease not present"))
  
  # Add Unreported table to animal disease table ----------------------------------------------------
  animal_diseases_unreported <- wahis_joined$diseases_unreported 
  
  if(nrow(animal_diseases_unreported)){
    animal_diseases_unreported <- animal_diseases_unreported %>% 
      mutate(occurrence = "0000", new_outbreaks = "0", total_outbreaks = "0",  status = "unreported") 
  }
  
  animal_diseases <- bind_rows(animal_diseases, animal_diseases_unreported %>% 
                                 mutate(date_of_last_occurrence_if_absent = "disease not absent") %>%
                                 mutate(serotype = "disease not present"))
  
  # Look up occurrence codes ----------------------------------------------------
  occurrence <- read_csv(system.file("annual_report_lookups", "lookup_occurrence.csv", package = "wahis")) %>%
    mutate(code = str_remove_all(code, "\""))
  
  if(nrow(animal_diseases)){
    animal_diseases <- animal_diseases %>%
      left_join(occurrence, by = c("occurrence" = "code")) %>%
      select(-occurrence) %>%
      rename(occurrence = code_value, occurrence_description = code_description) %>%
      mutate(date_of_last_occurrence_if_absent = recode(date_of_last_occurrence_if_absent, 
                                                        "-" = "disease absent",
                                                        "0000" = "never reported")) %>% # same codes as occurrence
      distinct() # Remove dupes
  }
  
  # Handling animal diseases listed with more than once status ----------------------------------------------------
  status_check <- function(x){
    x %>%
      group_by(country,country_iso3c, report_year, report_months, report_semester, oie_listed, disease) %>% 
      filter(n() > 1) %>%
      mutate(status = paste(status, collapse = "; ")) %>%
      mutate(serotype = paste(serotype, collapse = "; ")) %>%
      ungroup()  
  }
  
  sc <- status_check(animal_diseases) %>% 
    pull(serotype) %>% 
    unique() %>%
    str_split( "; ") %>%
    reduce(c) %>%
    unique() 
  assertthat::assert_that(all(sc %in% c("no information", "disease not present", "empty")))
  # ^ if this fails: the differnt statuses may be due to differences in serotype, and should not be filtered out
  
  animal_diseases <- animal_diseases %>%
    mutate(status_rank = recode(status, "present" = 1, "absent" = 2, "unreported" = 3)) %>% 
    group_by(country, country_iso3c, report_year, report_months, report_semester, oie_listed, disease) %>% 
    filter(status_rank == min(status_rank)) %>% 
    ungroup() %>%
    select(-status_rank)
  
  # some remaining diseases listed as absent twice
  #status_check(animal_diseases) 
  
  animal_diseases <- animal_diseases %>%
    mutate(date_rank =  recode(date_of_last_occurrence_if_absent, 
                               "disease absent" = 2, 
                               "never reported" = 3,
                               "empty" = 4,
                               .default = 1)) %>% 
    group_by(country, country_iso3c, report_year, report_months, report_semester, oie_listed, disease) %>% 
    filter(date_rank == min(date_rank)) %>% 
    ungroup() %>%
    select(-date_rank)
  
  # Animal diseases detail --------------------------------------------------
  animal_diseases_detail <- wahis_joined$diseases_present_detail 
  
  if(nrow(animal_diseases_detail)){
    animal_diseases_detail <- animal_diseases_detail %>%
      select(country, country_iso3c, report_year, report_months, report_semester,
             disease, serotype = serotype_s, period,temporal_scale, adm, adm_type, new_outbreaks, total_outbreaks) %>%
      filter(new_outbreaks != "empty") %>%
      mutate(status = "present")
  }
  
  # Animal host table ----------------------------------------------------
  animal_hosts <- wahis_joined$diseases_present 
  
  if(nrow(animal_hosts)){
    animal_hosts <- animal_hosts %>%
      select(country, country_iso3c, report_year, report_months, report_semester,
             disease, oie_listed, 
             species:vaccination_in_response_to_the_outbreak_s) %>%
      rename(vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s) %>%
      group_by(country, report_year, report_months, disease, oie_listed) %>%
      mutate(control_measures = na_if(control_measures, "empty")) %>%
      fill(control_measures, .direction = "down")  %>%
      ungroup() %>%
      mutate(status = "present") %>%
      mutate(control_measures = replace_na(control_measures, "")) %>%
      mutate(vaccination_in_response_to_the_outbreak_num = suppressWarnings(as.numeric(vaccination_in_response_to_the_outbreak))) %>%
      mutate(control_measures2 = ifelse(!is.na(vaccination_in_response_to_the_outbreak_num) & vaccination_in_response_to_the_outbreak_num>0,
                                        "Vr", "")) %>% # < there is no code for vaccination_in_response_to_the_outbreak so I am making one up
      unite(control_measures, control_measures, control_measures2, sep = " ") %>% # this is clunky but other approaches with paste and glue were sloooow
      mutate(control_measures = str_trim(control_measures)) %>%
      select(-vaccination_in_response_to_the_outbreak_num) %>%
      mutate(control_measures = ifelse(control_measures == "", "empty", control_measures))
    
    # fix seemingly incorrect measurement units (hives for non-bees)
    animal_hosts <- animal_hosts %>%
      mutate(measuring_units = ifelse(measuring_units == "Hives" & species != "api" & species != "***",  "Animals",  measuring_units))
  }
  # confirming we do not have dupe species: (ie it's okay not to include serotypes in this table)
  # animal_hosts %>% 
  #     group_by(country, report_months, report_year,  disease, oie_listed) %>%
  #     summarize(n = n(), n_species = n_distinct(species)) %>%
  #     filter(n != n_species)
  
  animal_hosts_absent <- wahis_joined$diseases_absent 
  
  if(nrow(animal_hosts_absent)){
    animal_hosts_absent <- animal_hosts_absent %>%
      select(country, country_iso3c, report_year, report_months, report_semester,
             disease, oie_listed, 
             species:official_vaccination) %>%
      group_by(country, report_year, report_months, disease, oie_listed) %>%
      mutate(control_measures = na_if(control_measures, "empty")) %>%
      fill(control_measures, .direction = "down")  %>%
      ungroup() %>%
      mutate(status = "absent") %>%
      mutate(control_measures = replace_na(control_measures, "empty"))
    
  }
  
  ah_names <- setdiff(names(animal_hosts ), names(animal_hosts_absent))
  animal_hosts <- bind_rows(animal_hosts, animal_hosts_absent) %>%
    mutate_at(.vars = ah_names, ~replace_na(., "disease absent")) 
  
  # Look up species codes -------------- --------------------------------------
  species <- read_csv(system.file("annual_report_lookups", "lookup_species.csv", package = "wahis")) %>%
    rename(species_class = group)
  
  if(nrow(animal_hosts)){
    animal_hosts <- animal_hosts %>%
      mutate(species = str_replace(species, "\\(fau\\)", "\\(wild\\)")) %>% # manual fix
      left_join(species, by = c("species" = "code")) %>%
      mutate(species = tolower(coalesce(code_value, species))) %>% # to capture empty/no info
      select(-code_value) 
  }
  
  # Look up control measure codes ----------------------------------------------------
  control <- read_csv(system.file("annual_report_lookups", "lookup_control.csv", package = "wahis"))
  control$code[control$code_value == "Vaccination in response to the outbreak(s)"] <- "Vr"
  control <- control %>% select(code, code_value) %>% distinct() %>% arrange(code) 
  control_lookup <- structure(as.vector(control$code_value), .Names=control$code)
  control_lookup <- c(control_lookup, "empty" = "empty")
  
  if(nrow(animal_hosts)){
    animal_hosts <- animal_hosts %>%
      mutate(control_measures = str_split(control_measures, " ")) %>% # make control_measures into list
      mutate(control_measures = map(control_measures, ~control_lookup[.])) %>% # lookup all items in list
      mutate(control_measures = map(control_measures, ~replace_na(., "code not recognized"))) %>% # NAs are not recognized
      mutate(control_measures = map_chr(control_measures, ~str_flatten(., collapse = "; "))) %>% # back to string, now with full code measures
      mutate(species_class = ifelse(species == "all susceptible species", "all",
                                    ifelse(species %in% c("empty", "no information"), species, species_class)))
  }
  
  # Animal hosts detail --------------------------------------------------
  animal_hosts_detail <- wahis_joined$diseases_present_detail 
  if(nrow(animal_hosts_detail)){
    # fix seemingly incorrect measurement units (hives for non-bees)
    animal_hosts_detail <- animal_hosts_detail %>%
      mutate(measuring_units = ifelse(measuring_units == "Hives" & species != "api" & species != "***",  "Animals",  measuring_units))
    
    animal_hosts_detail <- animal_hosts_detail %>%
      select(country, country_iso3c, report_year, report_months, report_semester,
             disease, period, temporal_scale, adm, adm_type, 
             species, family_name: vaccination_in_response_to_the_outbreak_s) %>%
      rename(vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s) %>%
      mutate(species = str_replace(species, "\\(fau\\)", "\\(wild\\)")) %>% # manual fix
      left_join(species, by = c("species" = "code")) %>%
      mutate(species = tolower(coalesce(code_value, species))) %>% # to capture empty/no info
      mutate(species_class = ifelse(species == "all susceptible species", "all",
                                    ifelse(species %in% c("empty", "no information"), species, species_class))) %>%
      select(-code_value) 
  }
  # Add tables to database --------------------------------------------------
  
  index <- names(wahis_joined)[!names(wahis_joined) %in% c("diseases_present", "diseases_absent", "diseases_present_detail", "diseases_unreported")]
  wahis_joined <- wahis_joined %>% `[`(index) 
  wahis_joined$animal_diseases <- animal_diseases
  wahis_joined$animal_hosts <- animal_hosts
  wahis_joined$animal_diseases_detail <- animal_diseases_detail
  wahis_joined$animal_hosts_detail <- animal_hosts_detail
  
  # Clean disease names -----------------------------------------------------
  
  # List of al diseases cleaned, with domestic/wild separated out
  if(nrow(animal_diseases)){
    diseases <- animal_diseases %>% 
      dplyr::select(disease)
    
    if(nrow(animal_diseases_detail)){
      diseases <- diseases %>%
        bind_rows(animal_diseases_detail %>% dplyr::select(disease)) 
    }
    
    diseases <- diseases %>%
      distinct() %>%
      mutate(disease_clean = janitor::make_clean_names(disease)) %>%
      mutate(disease_clean = str_replace(disease_clean, "domesticand_wild", "domestic_and_wild")) %>%
      mutate(disease_population = str_extract_all(disease_clean, "domestic|wild")) %>%
      mutate(disease_population = map_chr(disease_population, ~paste(sort(unique(.x)), collapse = " and "))) %>%
      mutate(disease_population = ifelse(disease_population=="", "not specified", disease_population)) %>%
      mutate(disease_clean = str_remove(disease_clean, "_domestic_and_wild|_domestic|_wild")) 
  }
  wahis_joined <- modify_at(wahis_joined, .at = c("animal_diseases", "animal_diseases_detail", "animal_hosts", "animal_hosts_detail"), function(x){ #TODO add in animal_diseases_detail and animal_hosts_detail after fixing disease names
    # note that if you have animal_diseases, you have animal_hosts because they come from the same parent table
    if(nrow(x)==0){return(x)} 
    x %>% left_join(diseases) %>%
      select(-disease) %>%
      rename(disease = disease_clean)
  })
  
  # Do the same for humans
  
  diseases_human <- wahis_joined$disease_humans 
  
  if(nrow(diseases_human)){
    
    diseases_human <- diseases_human %>%
      dplyr::select(disease) %>%
      distinct() %>%
      mutate(disease_clean = janitor::make_clean_names(disease))  
    
    wahis_joined$disease_humans <- wahis_joined$disease_humans %>%
      left_join(diseases_human) %>%
      select(-disease) %>%
      rename(disease = disease_clean) %>%
      gather(key = "occurrence", value = "value", no_information_available:disease_present_number_of_cases_known) %>%
      filter(value != "empty") %>%
      select(-value)
    
  }
  
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
  
  #TODO note that vaccine_manufacturers_detail and vaccine_production can potentially be joined by vaccine
  
  wahis_joined$submission_info <- wahis_joined$submission_info %>%
    mutate(submission_animal_type = recode(submission_animal_type, "Terrestrial and Aquatic" = "Aquatic and terrestrial"))
  
  names(wahis_joined) <- paste0("annual_reports_", names(wahis_joined))
  
  
  # Postprocess -------------------------------------------------------------
  
  # check no NAs (should be "empty" or "no information")
  map(wahis_joined, function(x){
    map_int(x %>% select(-suppressWarnings(one_of("notes"))), ~sum(is.na(.))) %>%
      sum() %>%
      assertthat::are_equal(., 0)})
  
  # now replace "empty" and "no information" with NA
  wahis_joined <- map(wahis_joined, function(x){
    x %>%
      mutate_all(~na_if(., c("empty"))) %>%
      mutate_all(~na_if(., c("no information")))
  })
  
  # check measurement units
  # wahis_joined$annual_reports_animal_hosts %>% 
  #   count(measuring_units, species) %>%
  #   View
  # wahis_joined$annual_reports_animal_hosts_detail %>% 
  #   count(measuring_units, species) %>%
  #   View
  
  return(wahis_joined)
  
}
