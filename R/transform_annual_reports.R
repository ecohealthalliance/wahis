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
  tnames <- c('metadata', 'submission_info', 'diseases_present', 'diseases_absent', 'diseases_present_detail', 'diseases_unreported', 'disease_humans', 'animal_population', 'veterinarians', 'national_reference_laboratories', 'national_reference_laboratories_detail', 'vaccine_manufacturers', 'vaccine_manufacturers_detail', 'vaccine_production')
  
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
  warn_that(wahis_joined$metadata %has_name% c("country", "country_iso3c", "report_year", "report_months",  "report_semester", "report"))
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
  
  diseases_present <- wahis_joined$diseases_present %>% rename(taxa = species)
  diseases_present_detail <- wahis_joined$diseases_present_detail %>% rename(taxa = species)
  diseases_absent <- wahis_joined$diseases_absent %>% rename(taxa_parent = taxa,  taxa = species) # parent was extracted from table headers, taxa was from columns
  diseases_unreported <- wahis_joined$diseases_unreported
  
  ##### misc fix (probably should have been handled in ingest function)
  diseases_present_detail <- diseases_present_detail %>% 
    rename(serotype = serotype_s) %>% 
    group_by(report, period, disease) %>%
    fill(adm, .direction = "down") %>% 
    fill(serotype, .direction = "down") %>% 
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
      select(-preferred_label) 
    warn_that(!any(is.na(disease_joined$disease)))
    assign(tbl_name, disease_joined)
  }
  
  # Split out disease tables  ------------------------------------------
  
  ##### animal_diseases table
  if(nrow(diseases_present)){
    animal_diseases_present <- diseases_present %>%
      mutate(disease_status = "present") %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease_old,
             disease, disease_population, disease_class, ando_id, oie_listed, disease_status, taxa,
             occurrence, serotype = serotype_s, new_outbreaks, total_outbreaks, notes) %>% 
      group_by(report, disease_old, disease, disease_population) %>% 
      fill(serotype, .direction = "down") %>% 
      fill(new_outbreaks, .direction = "down") %>% 
      fill(total_outbreaks, .direction = "down") %>% 
      fill(occurrence, .direction = "down") %>% 
      mutate(taxa = paste(taxa, collapse = "; ")) %>% # note NAs are included
      ungroup() %>% 
      distinct()
  }
  
  if(nrow(diseases_absent)){
    animal_diseases_absent <- diseases_absent %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease_old,
             disease, disease_population,  disease_class, ando_id, oie_listed, taxa,
             date_of_last_occurrence_if_absent = date_of_last_occurrence) %>% 
      group_by(report, disease_old, disease, disease_population) %>%
      mutate(taxa = paste(taxa, collapse = "; ")) %>% 
      fill(date_of_last_occurrence_if_absent) %>%
      ungroup() %>%
      distinct() %>% 
      mutate(occurrence = "-", new_outbreaks = "0", total_outbreaks = "0", disease_status = "absent") 
  }
  
  if(nrow(diseases_unreported)){
    animal_diseases_unreported <- diseases_unreported %>% 
      mutate(occurrence = "0000", new_outbreaks = "0", total_outbreaks = "0",  disease_status = "absent")  %>% 
      distinct()
  }
  
  animal_diseases <- bind_rows(animal_diseases_present, animal_diseases_absent) %>% 
    bind_rows(animal_diseases_unreported) 
  
  # Look up occurrence codes 
  
  if(nrow(animal_diseases)){
    animal_diseases <- animal_diseases %>%
      mutate(disease_status = ifelse(occurrence %in% suspected_code, "suspected", disease_status)) %>% 
      select(-occurrence) %>%
      mutate(date_of_last_occurrence_if_absent = recode(date_of_last_occurrence_if_absent,
                                                        "-" = NA_character_,
                                                        "0000" = NA_character_))  %>%  # same codes as occurrence
      distinct() 
  }
  
  ##### animal_diseases_detail table
  if(nrow(diseases_present_detail)){
    animal_diseases_detail <- diseases_present_detail %>%
      mutate(disease_status = "present") %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease_old,
             disease, disease_population, disease_class, ando_id, disease_status, taxa,
             serotype, period, temporal_scale, adm, adm_type, new_outbreaks, total_outbreaks) %>% 
      group_by(report, period, disease_old, disease, disease_population) %>%
      fill(new_outbreaks, .direction = "down") %>% 
      fill(total_outbreaks, .direction = "down") %>% 
      mutate(taxa = paste(unique(taxa), collapse = "; ")) %>%  # note NAs are included here
      ungroup() %>% 
      distinct()
  }
  
  # Split out hosts tables  ------------------------------------------
  
  ##### animal_hosts table
  if(nrow(diseases_present)){
    animal_hosts_present <- diseases_present %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease_old,
             disease, disease_population, disease_class, ando_id, oie_listed, taxa,
             control_measures:vaccination_in_response_to_the_outbreak_s) %>%
      rename(vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s) %>% 
      group_by(report, disease_old, disease, disease_population) %>%
      fill(control_measures, .direction = "down")  %>%
      ungroup() %>%
      mutate(disease_status = "present") %>%
      mutate(control_measures = replace_na(control_measures, "")) %>%
      mutate(vaccination_in_response_to_the_outbreak_num = suppressWarnings(as.numeric(vaccination_in_response_to_the_outbreak))) %>%
      mutate(control_measures2 = ifelse(!is.na(vaccination_in_response_to_the_outbreak_num) & vaccination_in_response_to_the_outbreak_num>0,
                                        "Vr", "")) %>% # < there is no code for vaccination_in_response_to_the_outbreak so I am making one up
      unite(control_measures, control_measures, control_measures2, sep = " ") %>% # this is clunky but other approaches with paste and glue were sloooow
      mutate(control_measures = str_trim(control_measures)) %>%
      select(-vaccination_in_response_to_the_outbreak_num) %>%
      mutate(control_measures = ifelse(control_measures == "", NA_character_, control_measures))
    
    # fix seemingly incorrect measurement units (hives for non-bees)
    animal_hosts_present <- animal_hosts_present %>%
      mutate(measuring_units = ifelse(measuring_units == "Hives" & taxa != "api" & taxa != "***",  "Animals",  measuring_units))
    
    # confirming we do not have dupe taxa
    taxa_check <- animal_hosts_present %>%
      group_by(report, disease_old, disease, disease_population) %>%
      mutate(n = n(), n_taxa = n_distinct(taxa)) %>%
      filter(n != n_taxa) 
    warn_that(nrow(taxa_check)==0)
  }
  
  if(nrow(diseases_absent)){
    animal_hosts_absent <- diseases_absent %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease_old,
             disease, disease_population,  disease_class, ando_id, oie_listed, taxa,
             control_measures, official_vaccination) %>%
      # control measures applied to all species
      group_by(report, disease_old, disease, disease_population) %>%
      fill(control_measures, .direction = "down")  %>%
      ungroup() %>%
      mutate(disease_status = "absent")
    
    warn_that(!any(is.na(animal_hosts_absent$taxa)))
  }
  
  if(nrow(diseases_unreported)){
    animal_hosts_unreported <- diseases_unreported %>%
      mutate(disease_status = "absent") 
  }
  
  animal_hosts <- bind_rows(animal_hosts_present, animal_hosts_absent) %>%
    bind_rows(animal_hosts_unreported) %>% 
    distinct()
  
  # Look up control codes
  if(nrow(animal_hosts)){
    animal_hosts <- animal_hosts %>%
      mutate(control_measures = str_split(control_measures, " ")) %>% # make control_measures into list
      mutate(control_measures = map(control_measures, ~control_lookup[.])) %>% # lookup all items in list
      mutate(control_measures = map_chr(control_measures, ~str_flatten(., collapse = "; "))) # back to string, now with full code measures
  }
  
  ##### animal_hosts_detail table
  if(nrow(diseases_present_detail)){
    animal_hosts_detail <- diseases_present_detail %>%
      mutate(disease_status = "present") %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease_old,
             disease, disease_population, disease_class, ando_id, disease_status, taxa,
             serotype, period, temporal_scale, adm, adm_type, family_name:vaccination_in_response_to_the_outbreak_s) %>% 
      rename(vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s) %>%
      mutate(measuring_units = ifelse(measuring_units == "Hives" & taxa != "api" & taxa != "***",  "Animals",  measuring_units))
  }
  
  ##### Handling diseases listed more 
  #TODO ranking present > not present
  #TODO aggregate over identical diseases
  #TODO test merge hosts and diseases
  sc <- janitor::get_dupes(diseases_present, report, taxa, disease, disease_population, serotype_s) 
  
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
      gather(key = "occurrence", value = "value", no_information_available:disease_present_number_of_cases_known) %>%
      drop_na(value) %>% 
      select(-value) %>% 
      mutate(occurrence = str_replace_all(occurrence, "_", " "))
    
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
  
  #TODO note that vaccine_manufacturers_detail and vaccine_production can potentially be joined by vaccine
  
  wahis_joined$submission_info <- wahis_joined$submission_info %>%
    mutate(submission_animal_type = recode(submission_animal_type, "Terrestrial and Aquatic" = "Aquatic and terrestrial"))
  
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
