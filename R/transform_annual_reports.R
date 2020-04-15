#' Convert a list of scraped annual reports to a list of table
#' @param annual_reports a list of annual reports produced by [ingest_annual_report]
#' @import dplyr tidyr stringr purrr
#' @importFrom assertthat %has_name%
#' @importFrom janitor make_clean_names
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
  
  # Animal disease and host tables ----------------------------------------------------
  
  diseases_present <- wahis_joined$diseases_present %>% rename(taxa = species)
  diseases_present_detail <- wahis_joined$diseases_present_detail %>% rename(taxa = species)
  diseases_absent <- wahis_joined$diseases_absent %>% rename(taxa_parent = taxa,  taxa = species) # parent was extracted from table headers, taxa was from columns
  diseases_unreported <- wahis_joined$diseases_unreported
  
  ##### Clean taxa codes
  taxa_lookup <- read_csv(system.file("annual_report_lookups", "lookup_species.csv", package = "wahis")) %>%
    rename(taxa_class = group)
  
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
  
  ##### Assume missing taxa from disease name
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
      select(-taxa2)
    
    # how many taxa are still NA?
  # diseases_present %>%
  #     filter(is.na(taxa)) %>%
  #     nrow()

  
  ##### Clean disease names
  disease_vect <- unique(c(unique(diseases_present$disease), 
                           unique(diseases_present_detail$disease),
                           unique(diseases_absent$disease), 
                           unique(diseases_unreported$disease)))
  
  disease_lookup <- tibble(disease = disease_vect) %>%
    arrange(disease) %>% 
    mutate(disease_clean = str_replace(disease, "domesticand wild|domestic andwild", "domestic and wild")) %>%
    mutate(disease_population = str_extract_all(disease_clean, "domestic|wild")) %>%
    mutate(disease_population = map_chr(disease_population, ~paste(sort(unique(.x)), collapse = " and "))) %>%
    mutate(disease_population = ifelse(disease_population=="", "not specified", disease_population)) %>%
    mutate(disease_clean = str_remove(disease_clean, "\\(domestic and wild\\)|\\(domestic\\)|\\(wild\\)"))  %>% 
    #mutate(disease_clean = str_remove_all(disease_clean, "mortality|viral|infectious|infect.|\\(infection with\\)|\\(infectionwith\\)|disease|infestation")) %>% 
    mutate(disease_clean = trimws(disease_clean)) %>% 
    rename(disease_old = disease, disease = disease_clean)
  
  # export for manual lookup (see inst/processing_scripts/clean_disease_names.R)
  # disease_export <- diseases %>%
  #   distinct(disease) 
  # write_csv(disease_export, here::here("inst/diseases/annual_report_diseases_animals.csv"))
  
  # join manual ontology lookup into disease lookup
  ando_disease_lookup <- readxl::read_xlsx(system.file("diseases", "disease_lookup.xlsx", package = "wahis")) %>% 
    rename(disease_class = class_desc) %>% 
    filter(report == "animal") %>% 
    select(-report) %>% 
    separate_rows(preferred_label, sep = ";") %>% 
    mutate_at(.vars = c("ando_id", "preferred_label", "disease_class"), ~na_if(., "NA"))
  
  disease_lookup <- disease_lookup %>% 
    left_join(ando_disease_lookup, by = "disease") %>% 
    distinct()
  
  # save unmatched diseases to database
  wahis_joined$diseases_unmatched <- disease_lookup %>% 
    filter(is.na(ando_id)) %>% 
    distinct(disease) %>% 
    mutate(table = "annual_animal")
  
  # clean disease names in existing tables
  for(tbl_name in c("diseases_present", "diseases_present_detail", "diseases_absent", "diseases_unreported")){
    tbl <- get(tbl_name)
    if(nrow(tbl)==0) next()
    disease_joined <- tbl %>% 
      rename(disease_old = disease) %>% 
      left_join(disease_lookup) %>% 
      mutate(disease = coalesce(preferred_label, disease)) %>% 
      select(-preferred_label, -disease_old) 
    warn_that(!any(is.na(disease_joined$disease)))
    assign(tbl_name, disease_joined)
  }
  ##### Handling diseases listed more than once within separate tables  
  sc <- janitor::get_dupes(diseases_present, report, taxa, disease, disease_population)
  
  ##### Split disease and host tables  
  #make sure to include species in disease tables
  #combine present, absent, unreported
  #selecting diseases listed as present > absent
  #then lookup occurrrence, control measures for separated tables
  
  ##### Human table
  # remove underscores
  
  ##### metadata remove?
  
  ##### reconcile not matched disease names 
  
  ##### yellow highlights
  
  ##### merge vaccine mau with vaccine prod
  
  
  
  
  
  
  
  
  
  
  
  
  
  # TABLE 1 - contains disease occurrence and outbreak counts
  if(nrow(diseases_present)){
    animal_diseases_present <- diseases_present %>%
      rename(serotype = serotype_s) %>%
      # maintain species for joining with hosts
      group_by(report, disease) 
    
    
    
    drop_na(occurrence) %>% 
      mutate(disease_status = "present") %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease, oie_listed, disease_status,
             occurrence, serotype, new_outbreaks, total_outbreaks, notes)
  }
  
  # Add Absent table to animal disease table ----------------------------------------------------
  animal_diseases_absent <- wahis_joined$diseases_absent
  
  if(nrow(animal_diseases_absent)){
    animal_diseases_absent <- animal_diseases_absent %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease, oie_listed, date_of_last_occurrence_if_absent = date_of_last_occurrence) %>% 
      #mutate(date_of_last_occurrence_if_absent = na_if(date_of_last_occurrence_if_absent, "empty")) %>% # filling and then taking distinct, rather than just dropping NAs, because there can be some cases where there is empty date of last occurrence and we do not want to filter out
      group_by(country, report_year, report_months, disease) %>%
      fill(date_of_last_occurrence_if_absent) %>%
      ungroup() %>%
      #mutate(date_of_last_occurrence_if_absent = replace_na(date_of_last_occurrence_if_absent, "empty")) %>%
      distinct() %>% 
      mutate(occurrence = "-", new_outbreaks = "0", total_outbreaks = "0", disease_status = "absent") 
  }
  
  # Add Unreported table to animal disease table ----------------------------------------------------
  animal_diseases_unreported <- wahis_joined$diseases_unreported 
  
  if(nrow(animal_diseases_unreported)){
    animal_diseases_unreported <- animal_diseases_unreported %>% 
      mutate(occurrence = "0000", new_outbreaks = "0", total_outbreaks = "0",  disease_status = "absent")  %>% 
      select(-taxa)
  }
  
  animal_diseases <- bind_rows(animal_diseases_present, animal_diseases_absent) %>% 
    bind_rows(animal_diseases_unreported) 
  
  # Look up occurrence codes ----------------------------------------------------
  suspected_code <- read_csv(system.file("annual_report_lookups", "lookup_occurrence.csv", package = "wahis")) %>%
    mutate(code = str_remove_all(code, "\"")) %>% 
    filter(disease_status == "suspected") %>% 
    pull(code)
  
  if(nrow(animal_diseases)){
    animal_diseases <- animal_diseases %>%
      mutate(disease_status = ifelse(occurrence %in% suspected_code, "suspected", disease_status)) %>% 
      select(-occurrence) %>%
      mutate(date_of_last_occurrence_if_absent = recode(date_of_last_occurrence_if_absent,
                                                        "-" = NA_character_,
                                                        "0000" = NA_character_))  %>%  # same codes as occurrence
      distinct() # Remove dupes
  }
  
  # Animal diseases detail --------------------------------------------------
  # TABLE 2 - contains disease occurrence and at finer spatial (ADM) and temporal (monthly) resolutions (for present disease only)
  
  animal_diseases_detail <- wahis_joined$diseases_present_detail 
  
  if(nrow(animal_diseases_detail)){
    animal_diseases_detail <- animal_diseases_detail %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease, serotype = serotype_s, period,temporal_scale, adm, adm_type, new_outbreaks, total_outbreaks) %>%
      drop_na(new_outbreaks) %>% 
      #filter(new_outbreaks != "empty") %>%
      mutate(disease_status = "present")
  }
  
  # Animal host table ----------------------------------------------------
  # TABLE 3 - contains case data by taxa 
  
  animal_hosts_present <- wahis_joined$diseases_present 
  
  if(nrow(animal_hosts_present)){
    animal_hosts_present <- animal_hosts_present %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease, oie_listed, 
             species:vaccination_in_response_to_the_outbreak_s) %>%
      rename(vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s,
             taxa = species) %>% # these arent true species names
      group_by(country, report_year, report_months, disease, oie_listed) %>%
      #mutate(control_measures = na_if(control_measures, "empty")) %>%
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
  }
  
  # confirming we do not have dupe taxa: (ie it's okay not to include serotypes in this table)
  taxa_check <- animal_hosts_present %>%
    group_by(country, report_months, report_year,  disease, oie_listed) %>%
    summarize(n = n(), n_taxa = n_distinct(taxa)) %>%
    filter(n != n_taxa) 
  warn_that(nrow(taxa_check)==0)
  
  # Add Absent data to animal host table ----------------------------------------------------
  
  animal_hosts_absent <- wahis_joined$diseases_absent 
  
  if(nrow(animal_hosts_absent)){
    animal_hosts_absent <- animal_hosts_absent %>%
      select(country, country_iso3c, report, report_year, report_months, report_semester,
             disease, oie_listed, taxa,
             species:official_vaccination) %>%
      # control measures applied to all species
      group_by(country, report_year, report_months, disease, oie_listed) %>%
      #mutate(control_measures = na_if(control_measures, "empty")) %>%
      fill(control_measures, .direction = "down")  %>%
      ungroup() %>%
      #mutate(control_measures = replace_na(control_measures, "empty")) %>% 
      # disease status
      mutate(disease_status = "absent") %>%
      # combine taxa and species columns
      mutate(species = na_if(species, "***")) %>%  # if species is ***, default to taxa
      mutate(taxa = coalesce(species, taxa)) %>% 
      select(-species)
    
    warn_that(!any(is.na(animal_hosts_absent$taxa)))
  }
  
  # Add unreported data to animal host table ----------------------------------------------------
  
  animal_hosts_unreported <- wahis_joined$diseases_unreported 
  
  if(nrow(animal_hosts_unreported)){
    animal_hosts_unreported <- animal_hosts_unreported %>%
      mutate(disease_status = "absent") 
  }
  
  animal_hosts <- bind_rows(animal_hosts_present, animal_hosts_absent) %>%
    bind_rows(animal_hosts_unreported) %>% 
    distinct()
  
  
  # Look up control measure codes ----------------------------------------------------
  control <- read_csv(system.file("annual_report_lookups", "lookup_control.csv", package = "wahis"))
  control$code[control$code_value == "Vaccination in response to the outbreak(s)"] <- "Vr"
  control <- control %>% select(code, code_value) %>% distinct() %>% arrange(code) 
  control_lookup <- structure(as.vector(tolower(control$code_value)), .Names = tolower(control$code))
  #control_lookup <- c(control_lookup, "empty" = "empty")
  
  if(nrow(animal_hosts)){
    animal_hosts <- animal_hosts %>%
      mutate(control_measures = str_split(control_measures, " ")) %>% # make control_measures into list
      mutate(control_measures = map(control_measures, ~control_lookup[.])) %>% # lookup all items in list
      #mutate(control_measures = map(control_measures, ~replace_na(., "code not recognized"))) %>% # NAs are not recognized
      mutate(control_measures = map_chr(control_measures, ~str_flatten(., collapse = "; "))) # back to string, now with full code measures
  }
  
  # Animal hosts detail --------------------------------------------------
  # TABLE 4 - contains case data by taxa at finer spatial (ADM) and temporal (monthly) resolutions
  
  animal_hosts_detail <- wahis_joined$diseases_present_detail 
  
  if(nrow(animal_hosts_detail)){
    
    animal_hosts_detail <- animal_hosts_detail %>%
      select(country, country_iso3c, report,report_year, report_months, report_semester,
             disease, period, temporal_scale, adm, adm_type, 
             species, family_name: vaccination_in_response_to_the_outbreak_s) %>%
      rename(vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s,
             taxa = species) %>%
      mutate(measuring_units = ifelse(measuring_units == "Hives" & taxa != "api" & taxa != "***",  "Animals",  measuring_units)) %>% 
      mutate(taxa = str_replace(taxa, "\\(fau\\)", "\\(wild\\)")) %>% # manual fix
      left_join(taxa, by = c("taxa" = "code")) %>%
      mutate(taxa = tolower(coalesce(code_value, taxa))) %>% 
      select(-code_value) 
  }
  # Add tables to database --------------------------------------------------
  
  index <- names(wahis_joined)[!names(wahis_joined) %in% c("diseases_present", "diseases_absent", "diseases_present_detail", "diseases_unreported")]
  wahis_joined <- wahis_joined %>% `[`(index)
  wahis_joined$animal_diseases <- animal_diseases
  wahis_joined$animal_hosts <- animal_hosts
  wahis_joined$animal_diseases_detail <- animal_diseases_detail
  wahis_joined$animal_hosts_detail <- animal_hosts_detail
  
  
  # Do the same for humans
  diseases_human <- wahis_joined$disease_humans 
  
  if(nrow(diseases_human)){
    diseases_human <- diseases_human %>%
      distinct(disease) 
    # write_csv(diseases_human, here::here("inst/diseases/annual_report_diseases_humans.csv"))
    
    # Read in manual lookup
    ando_disease_lookup <- readxl::read_xlsx(system.file("diseases", "disease_lookup.xlsx", package = "wahis")) %>% 
      rename(disease_class = class_desc) %>% 
      filter(report == "annual human") %>% 
      select(-report) %>% 
      separate_rows(preferred_label, sep = ";") %>% 
      mutate_at(.vars = c("ando_id", "preferred_label", "disease_class"), ~na_if(., "NA"))
    
    diseases_human <- diseases_human %>% 
      left_join(ando_disease_lookup, by = "disease") %>% 
      distinct()
    
    #janitor::get_dupes(diseases_human, disease)
    
    wahis_joined$diseases_unmatched <- bind_rows(wahis_joined$diseases_unmatched, 
                                                 diseases_human %>% 
                                                   filter(is.na(ando_id)) %>% 
                                                   distinct(disease) %>% 
                                                   mutate(table = "annual_human"))
    
    wahis_joined$disease_humans <- wahis_joined$disease_humans %>%
      left_join(diseases_human) %>% 
      mutate(disease = coalesce(preferred_label, disease)) %>% 
      select(-preferred_label) %>% 
      gather(key = "occurrence", value = "value", no_information_available:disease_present_number_of_cases_known) %>%
      filter(value != "empty") %>%
      select(-value)
    
    warn_that(!any(is.na(wahis_joined$disease_humans$disease)))
    
  }
  
  # Handling animal diseases listed with more than once disease_status ----------------------------------------------------
  
  status_check <- function(x){
    x %>%
      group_by(report, disease, disease_population) %>%
      filter(n() > 1) %>%
      mutate(disease_status = paste(disease_status, collapse = "; ")) %>%
      mutate(serotype = paste(serotype, collapse = "; ")) %>%
      ungroup()
  }
  
  sc <- status_check(wahis_joined$animal_diseases) 
  
  animal_diseases2 <- wahis_joined$animal_diseases %>%
    mutate(disease_status_rank = recode(disease_status, "present" = 1, "suspected" = 2, "absent" = 3)) %>%
    group_by(report, disease, disease_population) %>%
    filter(disease_status_rank == min(disease_status_rank)) %>%
    ungroup() %>%
    select(-disease_status_rank) 
  
  # some remaining diseases listed as absent twice
  sc2 <- status_check(animal_diseases2) 
  # 
  # animal_diseases <- animal_diseases %>%
  #   mutate(date_rank =  ifelse(date_of_last_occurrence_if_absent == "empty", 2, 1)) %>% 
  #   group_by(country, country_iso3c, report, report_year, report_months, report_semester, oie_listed, disease) %>% 
  #   filter(date_rank == min(date_rank)) %>% 
  #   ungroup() %>%
  #   select(-date_rank)
  # 
  # warn_that(nrow(status_check(animal_diseases)) == 0)
  # animal_diseases %>%  distinct(disease, serotype) %>% filter(!serotype %in% c("empty", "no information")) %>% 
  #   group_by(disease) %>% count(sort = TRUE)
  
  
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
      are_equal(., 0)})
  
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
  
  # remove empty tables
  wahis_joined <- keep(wahis_joined, ~nrow(.)>0)
  
  if(nrow(wahis_joined$annual_reports_diseases_unmatched)){warning("Unmatched diseases. Check annual_reports_diseases_unmatched table.")}
  
  return(wahis_joined)
  
}
