# First @rostal, is interested in cases of Crimean Congo Haemorrhagic Fever. I think no country reports animal numbers about this, but there are a variety of occurrence conditions. How can we make a table or plot that shows by country or year/semester, whether the disease is present, absent, suspected, not reported on specifically, or no info because there is no report?

# Also, I think @rostal could use human case counts of CCHF by country/year. I think this is reported and should be pretty easy.

# Second, I'd like to be able to quickly look at a county's reporting history to see if a given year's report is present, present but incomplete (is this a thing)?, invalid, reported but not scraped from the website. (This will require some update in the data my scraping job reports).
# EM - this is in data-processed/report-status.csv

library(tidyverse)
library(here)

db <- read_rds(here("data-processed", "annual-reports-data.rds"))

# Function to search diseases in all tables

find_disease <- function(disease){
    
    disease_clean <- janitor::make_clean_names(disease)
    
    tb_names <- c("animal_diseases", "animal_diseases_detail", "animal_hosts", "animal_hosts_detail", "disease_humans")
    
    tbs <- modify(db[tb_names], function(x){
        x %>%
            filter(disease == disease_clean)
    }) 
    
    return(tbs)
}

cch <- find_disease("Crimean Congo haemorrhagic fever") %>%
    map(., ~filter(., report_semester == "sem0"))

cch_animal <- cch$animal_diseases %>%
    select(country, disease_population, report_year, occurrence, total_outbreaks) %>%
    mutate(total_outbreaks = as.numeric(total_outbreaks)) %>%
    mutate(occurrence = ifelse(is.na(total_outbreaks)|total_outbreaks==0,
                               occurrence,
                               paste0(occurrence, " (total outbreaks = ", total_outbreaks, ")")
                               )) %>%
    select(-total_outbreaks) %>%
    pivot_wider(id_cols = c(country, disease_population), names_from = report_year, values_from = occurrence) %>%
    select(country, disease_population, as.character(2006:2018))
    
write_csv(cch_animal, here("inst/queries/cchf_outputs/cchf_animal.csv"))

cch_human <- cch$disease_humans %>%
    select(country, report_year, occurrence, human_cases, human_deaths)

write_csv(cch_human, here("inst/queries/cchf_outputs/cchf_human.csv"))

          