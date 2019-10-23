#!/usr/bin/env Rscript

library(tidyverse)
library(fs)
library(here)

# Load files -------------------------------------------------------------
wahis <- readr::read_rds(here::here("data-processed", "processed-annual-reports.rds"))

# Remove report errors ---------------------------------------------------
wahis <- discard(wahis, function(x){
    length(x) == 1
})

# Extract and rbind tables ----------------------------------------------------
wahis_joined <- map(names(wahis[[1]]), function(name){
    map_dfr(wahis, ~magrittr::extract2(., name)) 
})

names(wahis_joined) <- names(wahis[[1]])

# NA handling -------------------------------------------------------------
# "empty" = missing/NA/blank in the reports
# "No information" = "..." or "No" in the reports

wahis_joined <- map(wahis_joined, function(x){
    x %>%
        mutate_all(~replace_na(., "empty")) %>%
        mutate_at(vars(-one_of("occurrence")),  ~ifelse(. %in% c("No", "..."), "no information", .))
})

# Animal disease table ----------------------------------------------------
animal_diseases <- wahis_joined$diseases_present %>%
    rename(serotype = serotype_s) %>%
    filter(occurrence != "empty") %>% # NAs are from nested species data - this is preserved in animal_hosts table
    mutate(status = "present") %>%
    select(country, report_year, report_months, disease, oie_listed, status,
           occurrence, serotype, new_outbreaks, total_outbreaks, notes)

# Add Absent table to animal disease table ----------------------------------------------------
animal_diseases_absent <- wahis_joined$diseases_absent %>%
    select(country, report_year, report_months, disease, oie_listed, taxa, date_of_last_occurrence_if_absent = date_of_last_occurrence) %>% 
    group_by(country, report_year, report_months, disease) %>%
    mutate(date_of_last_occurrence_if_absent = na_if(date_of_last_occurrence_if_absent, "empty")) %>% # filling and then taking distinct, rather than just dropping NAs, because there can be some cases where there is empty date of last occurrence and we do not want to filter out
    fill(date_of_last_occurrence_if_absent) %>%
    ungroup() %>%
    distinct() %>% 
    mutate(occurrence = "-", new_outbreaks = "0", total_outbreaks = "0", status = "absent") 

animal_diseases <- bind_rows(animal_diseases %>%
                                 mutate(date_of_last_occurrence_if_absent = "disease not absent",
                                        taxa = "see animal_hosts table for taxa for diseases present"), 
                             animal_diseases_absent %>% mutate(serotype = "disease not present"))

# Add Unreported table to animal disease table ----------------------------------------------------
animal_diseases_unreported <- wahis_joined$diseases_unreported %>%
    mutate(occurrence = "0000", new_outbreaks = "0", total_outbreaks = "0",  status = "unreported") 

animal_diseases <- bind_rows(animal_diseases, animal_diseases_unreported %>% 
                                 mutate(date_of_last_occurrence_if_absent = "disease not absent") %>%
                                 mutate(serotype = "disease not present"))

# Look up occurrence codes ----------------------------------------------------
occurrence <- read_csv(here::here("data-raw", "annual_report_lookups", "lookup_occurrence.csv")) %>%
    mutate(code = str_remove_all(code, "\""))

animal_diseases <- animal_diseases %>%
    left_join(occurrence, by = c("occurrence" = "code")) %>%
    select(-occurrence) %>%
    rename(occurrence = code_value, occurrence_description = code_description) %>%
    mutate(date_of_last_occurrence_if_absent = recode(date_of_last_occurrence_if_absent, 
                                                      "-" = "disease absent",
                                                      "0000" = "never reported")) # same codes as occurrence

# Remove dupes ------------------------------------------------------------
animal_diseases <- animal_diseases %>%
    distinct()

# Animal diseases detail --------------------------------------------------
animal_diseases_detail <- wahis_joined$diseases_present_detail %>%
    select(country, report_year, report_months, disease, serotype = serotype_s, period,temporal_scale, adm, adm_type, new_outbreaks, total_outbreaks) %>%
    filter(new_outbreaks != "empty") %>%
    mutate(status = "present")

#TODO there's an issue in the scraping, disease names are sometimes column headers
# see e.g.,
# animal_diseases_detail %>% filter(country == "Afghanistan", report_year == "2006", report_months == "Jan-Dec")

#TODO cleaning disease names so can be joined with animal_diseases

#TODO QA check that totals in animal_diseases are the sum of the months in animal_diseases_detail

# Animal host table ----------------------------------------------------
animal_hosts <- wahis_joined$diseases_present %>%
    select(country, report_year, report_months, disease, oie_listed, 
           species:vaccination_in_response_to_the_outbreak_s) %>%
    rename(vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s) %>%
    group_by(country, report_year, report_months, disease, oie_listed) %>%
    mutate(control_measures = na_if(control_measures, "empty")) %>%
    fill(control_measures, .direction = "down")  %>%
    ungroup() %>%
    mutate(status = "present") %>%
    mutate(control_measures = replace_na(control_measures, "")) %>%
    mutate(vaccination_in_response_to_the_outbreak_num = as.numeric(vaccination_in_response_to_the_outbreak)) %>%
    mutate(control_measures2 = ifelse(!is.na(vaccination_in_response_to_the_outbreak_num) & vaccination_in_response_to_the_outbreak_num>0,
                                      "Vr", "")) %>% # < there is no code for vaccination_in_response_to_the_outbreak so I am making one up
    unite(control_measures, control_measures, control_measures2, sep = " ") %>% # this is clunky but other approaches with paste and glue were sloooow
    mutate(control_measures = str_trim(control_measures)) %>%
    select(-vaccination_in_response_to_the_outbreak_num) %>%
    mutate(control_measures = ifelse(control_measures == "", "empty", control_measures))

# confirming we do not have dupe species: (ie it's okay not to include serotypes in this table)
# animal_hosts %>% 
#     group_by(country, report_months, report_year,  disease, oie_listed) %>%
#     summarize(n = n(), n_species = n_distinct(species)) %>%
#     filter(n != n_species)

animal_hosts_absent <- wahis_joined$diseases_absent %>%
    select(country, report_year, report_months, disease, oie_listed, 
           species:official_vaccination) %>%
    group_by(country, report_year, report_months, disease, oie_listed) %>%
    mutate(control_measures = na_if(control_measures, "empty")) %>%
    fill(control_measures, .direction = "down")  %>%
    ungroup() %>%
    mutate(status = "absent") %>%
    mutate(control_measures = replace_na(control_measures, "empty"))

ah_names <- setdiff(names(animal_hosts), names(animal_hosts_absent))
animal_hosts <- bind_rows(animal_hosts, animal_hosts_absent) %>%
    mutate_at(.vars = ah_names, ~replace_na(., "disease absent")) 

# Look up species codes ----------------------------------------------------
species <- read_csv(here::here("data-raw", "annual_report_lookups", "lookup_species.csv"))
animal_hosts <- animal_hosts %>%
    mutate(species = str_replace(species, "\\(fau\\)", "\\(wild\\)")) %>% # manual fix
    left_join(species, by = c("species" = "code")) %>%
    mutate(species = tolower(coalesce(code_value, species))) %>% # to capture empty/no info
    select(-code_value) 

# Look up control measure codes ----------------------------------------------------
control <- read_csv(here::here("data-raw", "annual_report_lookups", "lookup_control.csv"))
control$code[control$code_value == "Vaccination in response to the outbreak(s)"] <- "Vr"
control <- control %>% select(code, code_value) %>% distinct() %>% arrange(code) 
control_lookup <- structure(as.vector(control$code_value), .Names=control$code)
control_lookup <- c(control_lookup, "empty" = "empty")

animal_hosts <- animal_hosts %>%
    mutate(control_measures = str_split(control_measures, " ")) %>% # make control_measures into list
    mutate(control_measures = map(control_measures, ~control_lookup[.])) %>% # lookup all items in list
    mutate(control_measures = map(control_measures, ~replace_na(., "code not recognized"))) %>% # NAs are not recognized
    mutate(control_measures = map_chr(control_measures, ~str_flatten(., collapse = "; "))) %>% # back to string, now with full code measures
    rename(species_class = group) %>%
    mutate(species_class = replace_na(species_class, "all"))

# Animal hosts detail --------------------------------------------------
animal_hosts_detail <- wahis_joined$diseases_present_detail %>%
    select(country, report_year, report_months, disease, period,temporal_scale, adm, adm_type, 
           species, family_name: vaccination_in_response_to_the_outbreak_s) %>%
    rename(vaccination_in_response_to_the_outbreak = vaccination_in_response_to_the_outbreak_s) %>%
    mutate(species = str_replace(species, "\\(fau\\)", "\\(wild\\)")) %>% # manual fix
    left_join(species, by = c("species" = "code")) %>%
    mutate(species = tolower(coalesce(code_value, species))) %>% # to capture empty/no info
    select(-code_value) 

#TODO Same issues with disease names as in animal_diseases_details

# Add tables to database --------------------------------------------------

index <- names(wahis_joined)[!names(wahis_joined) %in% c("diseases_present", "diseases_absent", "diseases_present_detail", "diseases_unreported")]
wahis_joined <- wahis_joined %>%  magrittr::extract(index) 
wahis_joined$animal_diseases <- animal_diseases
wahis_joined$animal_hosts <- animal_hosts
wahis_joined$animal_diseases_detail <- animal_diseases_detail
wahis_joined$animal_hosts_detail <- animal_hosts_detail

# Clean disease names -----------------------------------------------------

# List of al diseases cleaned, with domestic/wild separated out
diseases <- animal_diseases %>% 
    dplyr::select(disease) %>%
    distinct() %>%
    mutate(disease_clean = janitor::make_clean_names(disease)) %>%
    mutate(disease_clean = str_replace(disease_clean, "domesticand_wild", "domestic_and_wild")) %>%
    mutate(disease_population = str_extract_all(disease_clean, "domestic|wild")) %>%
    mutate(disease_population = map_chr(disease_population, ~paste(sort(unique(.x)), collapse = " and "))) %>%
    mutate(disease_population = ifelse(disease_population=="", "not specified", disease_population)) %>%
    mutate(disease_clean = str_remove(disease_clean, "_domestic_and_wild|_domestic|_wild")) 

wahis_joined <- modify_at(wahis_joined, .at = c("animal_diseases", "animal_hosts"), function(x){ #TODO add in animal_diseases_detail and animal_hosts_detail after fixing disease names
    x %>% left_join(diseases) %>%
        select(-disease) %>%
        rename(disease = disease_clean)
})

# Misc other cleaning items -----------------------------------------------
wahis_joined$disease_humans  <- wahis_joined$disease_humans %>%
    gather(key = "occurrence", value = "value", no_information_available:disease_present_number_of_cases_known) %>%
    filter(value != "empty") %>%
    select(-value)

wahis_joined$animal_population <- wahis_joined$animal_population %>%
    rename(taxa = species, population_count = total, population_units = units, producers_count = number, producers_units = units_2)

wahis_joined$veterinarians <- wahis_joined$veterinarians %>%
    rename(public_sector_count = public_sector, private_sector_count = private_sector, total_count = total)

wahis_joined$national_reference_laboratories <- wahis_joined$national_reference_laboratories %>%
    rename(laboratory_name = name, laboratory_contact = contacts, laboratory_latitude = latitude, laboratory_longitude = longitude)

wahis_joined$national_reference_laboratories_detail <- wahis_joined$national_reference_laboratories_detail %>%
    rename(laboratory_name = laboratory)

wahis_joined$vaccine_manufacturers <- wahis_joined$vaccine_manufacturers %>%
    rename(vaccine_manufacturer = manufacturer, vaccine_contact = contacts, year_start_vaccine_activity = year_of_start_of_activity, year_cessation_vaccine_activity = year_of_cessation_of_activity)

wahis_joined$vaccine_manufacturers_detail <- wahis_joined$vaccine_manufacturers_detail %>%
    rename(vaccine_manufacturer = manufacturer, year_start_vaccine_production = year_of_start_of_production, year_end_vaccine_production = year_of_end_of_production_if_production_ended)

wahis_joined$vaccine_production <- wahis_joined$vaccine_production %>%
    rename(vaccine_manufacturer = manufacturer)

#TODO note that vaccine_manufacturers_detail and vaccine_production can potentially be joined by vaccine

wahis_joined$metadata <- wahis_joined$metadata %>%
    mutate(submission_animal_type = recode(submission_animal_type, "Terrestrial and Aquatic" = "Aquatic and terrestrial"))

# Export -----------------------------------------------
dir_create(here("data-processed", "db"))
iwalk(wahis_joined, ~write_csv(.x, here("data-processed", "db", paste0("annual_reports_", .y, ".csv.xz"))))

write_rds(wahis_joined, here("data-processed", "annual-reports-data.rds"), compress = "xz", compression = 9L)

# make dictionary table
# dictionary <- purrr::imap_dfr(wahis_joined, function(x, y){
#     x %>% 
#         summarise_all(class) %>% 
#         gather(variable_name, data_type_r) %>%
#         mutate(table_name = y) 
# })
# dictionary <- dictionary %>%
#     mutate(description = "", data_type_postgres = "",	allowed_values = "") %>%
#     select(table_name, variable_name, description, everything())
# write_csv(dictionary, "dictionary.csv")
