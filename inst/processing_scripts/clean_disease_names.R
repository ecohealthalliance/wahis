# export a single document of disease names for standardization
library(tidyverse)

annual_animal <- read_csv(system.file("diseases", "annual_report_diseases_animals.csv", package = "wahis"))
annual_human <- read_csv(system.file("diseases", "annual_report_diseases_humans.csv", package = "wahis"))
outbreak_animal <- read_csv(system.file("diseases", "outbreak_report_diseases.csv", package = "wahis"))

diseases_for_lookup <- annual_animal %>% 
    select(disease_clean) %>%
    mutate(report = "animal") %>% 
    bind_rows(annual_human %>% mutate(report = "annual human")) %>% 
    bind_rows(outbreak_animal %>% 
                  select(disease_clean = disease) %>% 
                  mutate(report = "animal")) %>% 
    distinct() %>% 
    arrange(disease_clean) %>% 
    rename(disease = disease_clean)

# get exact matches
ando <-  read_csv(system.file("diseases", "ando_ontology.csv", package = "wahis")) %>% 
    mutate_all(~tolower(.)) %>% 
    mutate(id = basename(class_id)) %>% 
    select(id, preferred_label, synonyms) %>% 
    separate_rows(synonyms, sep = "\\|") %>% 
    mutate(class = str_sub(id, 1, 2)) %>% 
    mutate(class_desc = recode(class,
                               "ma" = "disease group",
                               "mh" = "disease specific",
                               "ag" = "pathogen",
                               .default = NA_character_
                               )) %>% 
    select(-class)

write_csv(ando, here::here("inst", "diseases", "ando_lookup.csv"))

# heirarchy here
# http://agroportal.lirmm.fr/ontologies/ANDO/?p=classes&conceptid=http%3A%2F%2Fopendata.inra.fr%2FAnimalDiseasesOnto%2FAG401

diseases_for_lookup <- diseases_for_lookup %>% 
    left_join(ando %>% distinct(id, preferred_label), by = c("disease" = "preferred_label")) %>%
    left_join(ando %>% distinct(id, synonyms) %>% drop_na(), by = c("disease" = "synonyms")) %>% 
    mutate(ando_id = coalesce(id.x, id.y)) %>% 
    select(-id.x, -id.y) %>% 
    drop_na(disease) %>% 
    distinct()

write_csv(diseases_for_lookup, here::here("inst", "diseases", "disease_lookup.csv"))
