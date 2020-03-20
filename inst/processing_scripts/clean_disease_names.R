# export a single document of disease names for standardization
library(tidyverse)

annual_animal <- read_csv(system.file("diseases", "annual_report_diseases_animals.csv", package = "wahis"))
annual_human <- read_csv(system.file("diseases", "annual_report_diseases_humans.csv", package = "wahis"))
outbreak_animal <- read_csv(system.file("diseases", "outbreak_report_diseases.csv", package = "wahis"))

diseases_for_lookup <- annual_animal %>% 
    select(disease_clean) %>% 
    bind_rows(annual_human) %>% 
    bind_rows(outbreak_animal %>% select(disease_clean = disease)) %>% 
    distinct() %>% 
    arrange(disease_clean)

# get exact matches
ando <-  read_csv(system.file("diseases", "ando_ontology.csv", package = "wahis")) %>% 
    mutate_all(~tolower(.)) %>% 
    mutate(id = basename(class_id)) %>% 
    select(id, preferred_label, synonyms) %>% 
    separate_rows(synonyms, sep = "\\|")

diseases_for_lookup <- diseases_for_lookup %>% 
    left_join(ando %>% distinct(id, preferred_label), by = c("disease_clean" = "preferred_label")) %>%
    left_join(ando %>% distinct(id, synonyms) %>% drop_na(), by = c("disease_clean" = "synonyms")) %>% 
    mutate(id = coalesce(id.x, id.y)) %>% 
    select(-id.x, -id.y) %>% 
    drop_na(disease_clean) %>% 
    distinct()

write_csv(diseases_for_lookup, here::here("inst", "diseases", "disease_lookup.csv"))

