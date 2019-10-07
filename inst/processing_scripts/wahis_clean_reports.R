library(tidyverse)

wahis_joined <- read_rds(here::here("data", "wahis-merged.rds"))

# Clean disease names -----------------------------------------------------
diseases <- map_df(wahis_joined, function(x){
    if(!"disease" %in% colnames(x)){return(NULL)}
    x %>% select(disease) %>% distinct()}) %>%
    distinct() %>%
    mutate(disease_clean = janitor::make_clean_names(disease)) %>%
    mutate(disease_clean = str_replace(disease_clean, "domesticand_wild", "domestic_and_wild")) %>%
    mutate(disease_population = str_extract_all(disease_clean, "domestic|wild")) %>%
    mutate(disease_population = map_chr(disease_population, ~paste(sort(unique(.x)), collapse = " and "))) %>%
    mutate(disease_population = ifelse(disease_population=="", "not specified", disease_population)) %>%
    mutate(disease_clean = str_remove(disease_clean, "_domestic_and_wild|_domestic|_wild")) 


wahis_joined <- purrr::map(wahis_joined, function(x){
    if(!"disease" %in% colnames(x)){return(x)}
    x %>% left_join(diseases) %>%
        select(-disease) %>%
        rename(disease = disease_clean)
})

# Animal disease table ----------------------------------------------------
animal_diseases <- wahis_joined$diseases_present %>%
    drop_na(occurrence) %>%
    mutate_at(.vars = c("serotype_s", "new_outbreaks", "total_outbreaks"), ~replace_na(., "...")) %>%
    mutate(serotype_s = recode(serotype_s, "No" = "...")) %>%
    select(country, report_year, report_months, disease, disease_population, oie_listed, 
           occurrence, serotype_s, new_outbreaks, total_outbreaks, notes)


# Some QA checks on animal disease table
check_counts <- animal_diseases %>%
    group_by(country, report_year, disease, disease_population, serotype_s) %>% 
    mutate(n = n()) %>%
    filter(n == 3) %>%
    mutate(report_months = str_replace(report_months, "-", "_")) %>%
    pivot_wider(names_from = report_months, values_from = c(new_outbreaks, total_outbreaks)) %>%
    mutate(new_check = ifelse(new_outbreaks_Jan_Dec == "...", 
                              all(new_outbreaks_Jan_Dec == new_outbreaks_Jan_Jun, new_outbreaks_Jan_Dec == new_outbreaks_Jul_Dec),
                              sum(as.numeric(new_outbreaks_Jan_Jun), as.numeric(new_outbreaks_Jul_Dec), na.rm = TRUE) == as.numeric(new_outbreaks_Jan_Dec))) %>%
    mutate(total_check = ifelse(total_outbreaks_Jan_Dec == "...", 
                                all(total_outbreaks_Jan_Dec == total_outbreaks_Jan_Jun, total_outbreaks_Jan_Dec == total_outbreaks_Jul_Dec),
                                sum(as.numeric(total_outbreaks_Jan_Jun), as.numeric(total_outbreaks_Jul_Dec), na.rm = TRUE) >= as.numeric(total_outbreaks_Jan_Dec)))

check1 = check_counts %>% filter(new_check == FALSE) # instances where the yearly number of new cases does not equal the sum of the semester new cases
nrow(check1)
check2 = check_counts %>% filter(total_check == FALSE) # instances where the yearly number of total cases is greater than the sum of the semester total cases (ie suggests there are possibly cases that were unreported)
nrow(check2)

#^ `check_counts` only applies to cases where disease, disease_population, and serotype are the same across semesters 
# some reasons for mismatches: animal type may be (domestic) in semester and (domestic and wild) in yearly; missing reports (some created by report errors)
animal_diseases %>%
    group_by(country, report_year, disease, disease_population, serotype_s) %>% 
    count() %>%
    group_by(n) %>%
    count()

# For now, `animal_diseases` will be semester reports only
animal_diseases <- animal_diseases %>%
    filter(report_months != "Jan-Dec")

# Add Absent table to animal disease table ----------------------------------------------------
animal_diseases_absent <- wahis_joined$diseases_absent %>%
    select(country, report_year, report_months, disease, disease_population, oie_listed, taxa, date_of_last_occurrence_if_absent = date_of_last_occurrence) %>% 
    group_by(country, report_year, report_months, disease) %>%
    fill(date_of_last_occurrence_if_absent) %>%
    distinct() %>% 
    filter(report_months != "Jan-Dec") %>% # Filter out semester reports for now
    mutate(occurrence = "-", new_outbreaks = "0", total_outbreaks = "0")

animal_diseases <- bind_rows(animal_diseases, animal_diseases_absent)

# Add Unreported table to animal disease table ----------------------------------------------------
animal_diseases_unreported <- wahis_joined$diseases_unreported %>%
    mutate(occurrence = "0", new_outbreaks = "0", total_outbreaks = "0") %>%
    filter(report_months != "Jan-Dec") # Filter out semester reports for now

animal_diseases <- bind_rows(animal_diseases, animal_diseases_unreported)

# Look up occurrence codes ----------------------------------------------------
occurrence <- read_csv(here::here("data-raw", "wahis_lookup_occurrence.csv")) %>%
    mutate(code = str_remove_all(code, "\""))
animal_diseases <- animal_diseases %>%
    left_join(occurrence, by = c("occurrence" = "code")) %>%
    select(-occurrence) %>%
    rename(occurrence = code_value, occurrence_description = code_description)

# Animal host table ----------------------------------------------------
animal_hosts <- wahis_joined$diseases_present %>%
    select(country, report_year, report_months, disease, disease_population, oie_listed, 
           species:vaccination_in_response_to_the_outbreak_s) %>%
    rename(taxa = species) %>%
    mutate(taxa = replace_na(taxa, "not specified")) %>%
    mutate(taxa = recode(taxa, "..." = "not specified", "***" =  "not specified")) %>%
    group_by(country, report_year, report_months, disease, disease_population, oie_listed) %>%
    fill(control_measures, .direction = "down")  %>%
    ungroup()

# Look up species codes ----------------------------------------------------
species <- read_csv(here::here("data-raw", "wahis_lookup_species.csv"))
animal_hosts <- animal_hosts %>%
    mutate(taxa = str_replace(taxa, "\\(fau\\)", "\\(wild\\)")) %>% # manual fix
    left_join(species, by = c("taxa" = "code")) %>%
    mutate(taxa = tolower(coalesce(code_value, taxa))) %>%
    select(-code_value) 

# Look up control measure codes ----------------------------------------------------
control <- read_csv(here::here("data-raw", "wahis_lookup_control.csv"))
animal_hosts <- animal_hosts %>%
    separate_rows(control_measures, sep = " ") %>%
    left_join(control,  by = c("control_measures" = "code", "group" = "group")) %>% # first join by group
    left_join(control %>%
                  filter(group == "terrestrial") %>% select(-group), # then join by code only for cases where there us no group (ie species not specified). assume codes are terrestrial b/c we dont know species group (and code descriptions are very similar for terrestrial v aquatic)
              by = c("control_measures" = "code")) %>%
    mutate(control_measures = coalesce(code_value.x, code_value.y),
           control_measures_description = coalesce(code_description.x, code_description.y)) %>%
    select(-ends_with(".x"), -ends_with(".y")) %>%
    rename(taxa_class = group)

# Misc other cleaning items -----------------------------------------------
wahis_joined$disease_humans <- wahis_joined$disease_humans %>%
    mutate(disease_population = "human") %>%
    gather(key = "occurrence", value = "value", no_information_available:disease_present_number_of_cases_known) %>%
    drop_na(value) %>%
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

# Export -----------------------------------------------
index <- names(wahis_joined)[!names(wahis_joined) %in% c("diseases_present", "diseases_absent", "diseases_present_detail", "diseases_unreported")]
db <- wahis_joined %>%  magrittr::extract(index) 
db$animal_diseases <- animal_diseases
db$animal_hosts <- animal_hosts

write_rds(db, here::here("data", "wahis-db.rds"))
