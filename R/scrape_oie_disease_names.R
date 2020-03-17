
library(tidyverse)
library(rvest)
library(xml2)

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
    mutate(disease = str_remove_all(disease, "Infection with ")) %>% 
    mutate(disease = trimws(tolower(disease))) %>% 
    arrange(disease)
