#!/usr/bin/env Rscript

library(tidyverse)
library(fs)
library(here)

wahis_joined <- read_rds(here::here("data-processed", "merged-annual-reports.rds"))

# NA handling -------------------------------------------------------------
# "empty" = missing/NA/blank in the reports
# "No information" = "..." or "-" or "0000" in the reports

wahis_joined <- map(wahis_joined, function(x){
    x %>%
        mutate_all(~replace_na(., "empty")) %>%
        mutate_at(vars(-one_of("occurrence")),  ~ifelse(. %in% c("No", "...",  "-", "0000"), "no information", .))
})

# Animal disease table ----------------------------------------------------
animal_diseases <- wahis_joined$diseases_present %>%
    rename(serotype = serotype_s) %>%
    filter(occurrence != "empty") %>% # NAs are from nested species data - this is preserved in animal_hosts table
    mutate(serotype = recode(serotype, "No" = "no information")) %>%
    mutate(status = "present") %>%
    select(country, report_year, report_months, disease, oie_listed, status,
           occurrence, serotype, new_outbreaks, total_outbreaks, notes)

# Add Absent table to animal disease table ----------------------------------------------------
animal_diseases_absent <- wahis_joined$diseases_absent %>%
    select(country, report_year, report_months, disease, disease_population, oie_listed, taxa, date_of_last_occurrence_if_absent = date_of_last_occurrence) %>% 
    group_by(country, report_year, report_months, disease) %>%
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
    #mutate_at(.vars = c("code_value", "code_description"), ~case_when(occurrence == "0000" ~ "Code not recognized", TRUE ~ .)) %>%
    select(-occurrence) %>%
    rename(occurrence = code_value, occurrence_description = code_description)

# Animal host table ----------------------------------------------------
animal_hosts <- wahis_joined$diseases_present %>%
    select(country, report_year, report_months, disease, oie_listed, 
           species:vaccination_in_response_to_the_outbreak_s) %>%
    rename(taxa = species) %>%
    group_by(country, report_year, report_months, disease, oie_listed) %>%
    fill(control_measures, .direction = "down")  %>%
    ungroup()

# Look up species codes ----------------------------------------------------
species <- read_csv(here::here("data-raw", "annual_report_lookups", "lookup_species.csv"))
animal_hosts <- animal_hosts %>%
    mutate(taxa = str_replace(taxa, "\\(fau\\)", "\\(wild\\)")) %>% # manual fix
    left_join(species, by = c("taxa" = "code")) %>%
    mutate(taxa = tolower(coalesce(code_value, taxa))) %>%
    select(-code_value) 

# Look up control measure codes ----------------------------------------------------
control <- read_csv(here::here("data-raw", "annual_report_lookups", "lookup_control.csv"))
animal_hosts <- animal_hosts %>%
    separate_rows(control_measures, sep = " ") %>%
    left_join(control,  by = c("control_measures" = "code", "group" = "group")) %>% # first join by group
    left_join(control %>%
                  filter(group == "terrestrial") %>% select(-group), # then join by code only for cases where there us no group (ie species not specified). assume codes are terrestrial b/c we dont know species group (and code descriptions are very similar for terrestrial v aquatic)
              by = c("control_measures" = "code")) %>%
    mutate(control_measures = coalesce(code_value.x, code_value.y),
           control_measures_description = coalesce(code_description.x, code_description.y)) %>%
    mutate_at(.vars = c("control_measures", "control_measures_description"), ~replace_na(., "Code not recognized")) %>%
    select(-ends_with(".x"), -ends_with(".y")) %>%
    rename(taxa_class = group)


# Add tables to database --------------------------------------------------

index <- names(wahis_joined)[!names(wahis_joined) %in% c("diseases_present", "diseases_absent", "diseases_present_detail", "diseases_unreported")]
wahis_joined <- wahis_joined %>%  magrittr::extract(index) 
wahis_joined$animal_diseases <- animal_diseases
wahis_joined$animal_hosts <- animal_hosts

# Clean disease names -----------------------------------------------------
diseases <- map_df(wahis_joined[c("animal_diseases", "animal_hosts")], function(x){
    x %>% dplyr::select(disease) %>% distinct()}) %>%
    distinct() %>%
    mutate(disease_clean = janitor::make_clean_names(disease)) %>%
    mutate(disease_clean = str_replace(disease_clean, "domesticand_wild", "domestic_and_wild")) %>%
    mutate(disease_population = str_extract_all(disease_clean, "domestic|wild")) %>%
    mutate(disease_population = map_chr(disease_population, ~paste(sort(unique(.x)), collapse = " and "))) %>%
    mutate(disease_population = ifelse(disease_population=="", "not specified", disease_population)) %>%
    mutate(disease_clean = str_remove(disease_clean, "_domestic_and_wild|_domestic|_wild")) 

wahis_joined <- modify_at(wahis_joined, .at = c("animal_diseases", "animal_hosts"), function(x){
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
