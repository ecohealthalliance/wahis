
library(tidyverse)
library(rvest)
library(xml2)
library(stringdist)

page <- read_html("https://www.oie.int/animal-health-in-the-world/oie-listed-diseases-2020/")

parents <- xml2::xml_find_all(page, xpath="//div[@id='oe_articleContent']/div/table/tbody")
headers <- xml2::xml_find_all(parents, xpath="//p[@class='bodytext']")  %>% xml_text()
headers <- headers[map_lgl(headers, ~nchar(.)>1)]
diseases <- parents %>%  xml_children() %>% xml_children() %>% xml_children() %>% xml_children() %>% xml_text()
diseases <- diseases[diseases != ""]

disease_lookup <- tibble(disease = diseases) %>% 
    mutate(taxa = ifelse(disease %in% headers, disease, NA)) %>%
    mutate(disease = ifelse(disease %in% headers, NA, disease)) %>% 
    fill(taxa) %>% 
    drop_na(disease) %>% 
    mutate(disease = trimws(tolower(disease))) %>% 
    arrange(disease) %>% 
    rename(disease_official = disease)

write_csv(disease_lookup, "delete.csv")

annual_disease <- read_csv("inst/annual_report_lookups/lookup_disease.csv")
    #read_csv(system.file("annual_report_lookups", "lookup_disease.csv", package = "wahis"))


matching_fizz <- expand.grid(annual_disease$disease_clean, disease_lookup$disease_official) %>%
    as_tibble() %>% 
    distinct() %>% 
    mutate_all(~as.character(.)) %>% 
    # left_join(articles_db %>% select(title, study_id, author, year, url, volume, doi, edition, language, mex_name), by = c("Var1" = "title")) %>%
    # left_join(articles_db %>% select(title, study_id, author, year, url, volume, doi, edition, language, mex_name), by = c("Var2" = "title")) %>%
    mutate(comp = stringdist(Var1, Var2, method = "osa")) %>%
    filter(comp <= 15) %>%
    arrange(comp) %>%
    mutate(tmp = apply(cbind(Var1, Var2), 1, function(x) paste(sort(x), collapse=" "))) %>%
    filter(!duplicated(tmp)) %>%
    select(-tmp, -comp) %>%
    setNames(gsub("\\.x", "1", colnames(.) )) %>%
    setNames(gsub("\\.y", "2", colnames(.) ))