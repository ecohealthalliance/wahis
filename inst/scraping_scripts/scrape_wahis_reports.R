# A script to scrape incident reports from WAHIS

library(tidyverse)
library(xml2)
library(rvest)
library(stringi)
library(httr)
library(furrr)
library(here)
plan(multiprocess, workers = 8)



# First remove empty html files

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

current_pages <- fs::dir_ls(here("data-raw", "raw_wahis_pages")) %>% 
  stri_extract_first_regex("\\d+") %>% as.numeric() %>% as.integer()
to_get <- setdiff(seq_len(max_report_id), current_pages)

responses <- furrr::future_map(to_get, function(rid) {
    Sys.sleep(1 + rexp(1, 0.25))
    file_name = here::here("data-raw", "raw_wahis_pages", paste0(rid, ".html"))
    if(!fs::file_exists(file_name)) {
    response <- RETRY("GET",
          url = paste0("http://www.oie.int/wahis_2/public/wahid.php/Reviewreport/Review?reportid=", as.character(rid)),
          write_disk(file_name),
          user_agent("R httr extraction script (ross@ecohealthalliance.org)"))
    }
    #TODO: Remove file if empty
    return(response)
}, .progress = TRUE)



saveRDS(response, here("responses.rds"))