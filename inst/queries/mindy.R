# First @rostal, is interested in cases of Crimean Congo Haemorrhagic Fever. I think no country reports animal numbers about this, but there are a variety of occurrence conditions. How can we make a table or plot that shows by country or year/semester, whether the disease is present, absent, suspected, not reported on specifically, or no info because there is no report?

# Also, I think @rostal could use human case counts of CCHF by country/year. I think this is reported and should be pretty easy.
# EM - only one human report

# Second, I'd like to be able to quickly look at a county's reporting history to see if a given year's report is present, present but incomplete (is this a thing)?, invalid, reported but not scraped from the website. (This will require some update in the data my scraping job reports).
# EM - this is in data-processed/report-status.csv

library(tidyverse)
library(here)

db <- read_rds(here("data-processed", "annual-reports-data.rds"))

# Function to search diseases in all tables

#TODO - go back and clean human names in transform
db$disease_humans <- db$disease_humans %>%
    mutate(disease = janitor::make_clean_names(disease))

# "Crimean Congo haemorrhagic fever"  

find_disease <- function(disease){
    
    disease_clean <- janitor::make_clean_names(disease)
    
    disease_humans <- db$disease_humans %>% 
        filter(disease == disease_clean)
    
    animal_diseases <- db$animal_diseases %>% 
        filter(disease == disease_clean)
    
    animal_diseases_detail <- db$animal_diseases_detail %>% 
        filter(disease == disease_clean)
    
    return(list(animal_diseases = animal_diseases, animal_diseases_detail=animal_diseases_detail, disease_humans = disease_humans))
    
}

cch <- find_disease("Crimean Congo haemorrhagic fever") %>%
    magrittr::extract2("animal_diseases") %>%
    filter(report_semester == "sem0")

# widget (dt? - filterable?) - select region - heatmap chart by year showing absent, unreported, present - if present, then count
# or: map showing status - filter by year (too hard to see small countries)

