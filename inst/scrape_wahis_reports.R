library(tidyverse)
library(xml2)
library(rvest)
library(stringi)
library(httr)

#Read the page of weekly reports and get the latest report number
weekly_pg <-
    read_html("http://www.oie.int/wahis_2/public/wahid.php/Diseaseinformation/WI")

report_ids <- weekly_pg %>% 
  html_nodes(xpath = "//a[contains(@href, 'Reviewreport')]") %>% 
  html_attr("href") %>% 
  stri_extract_last_regex("(?<=\\,)\\d{3,6}(?=\\))") %>% 
  as.numeric() %>% 
  sort(decreasing=TRUE)

max_report_id = max(report_ids)

file.size(list.files(here::here("inst", "raw_wahis_pages"), full.names = TRUE))

for(rid in rev(seq_len(max_report_id))) {
    file_name = paste0(rid, ".html")
    if(length(list.files(here::here("inst", "raw_wahis_pages"), paste0("^", file_name))) == 0) {
    response <- RETRY("GET",
          url = paste0("http://www.oie.int/wahis_2/public/wahid.php/Reviewreport/Review?reportid=", as.character(rid)),
          write_disk(here::here("inst", "raw_wahis_pages", file_name)),
          user_agent("R httr extraction script (ross@ecohealthalliance.org)"))
    }
    Sys.sleep(1 + rexp(1, 0.25))
}
