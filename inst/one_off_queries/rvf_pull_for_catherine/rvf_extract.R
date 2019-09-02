#rstudioapi::viewer("data-raw/raw_wahis_reports/BDI_2006_sem0.html")
library(xml2)
library(stringi)
library(fs)
library(future)
library(furrr)
library(tidyverse)
h <- here::here

plan(multiprocess)


annuals <- dir_ls(h("data-raw", "raw_wahis_reports"), regexp = "sem0\\.html$")

web_page = "data-raw/raw_wahis_reports/KIR_2008_sem0..html"

rift_extract <- future_map_dfr(annuals, safe_ingest, .progress = TRUE)
#write_csv(rift_extract, "rift_extract.csv")
r2 <- rift_extract %>% 
    mutate(country_code = stri_match_first_regex(file, "^\\w{3}"),
           year = stri_match_first_regex(file, "\\d{4}"))
files <- fs::dir_info("data-raw/raw_wahis_reports/") %>% 
    filter(stri_detect_regex(basename(path), "^ZAF")) %>% 
    filter(stri_detect_regex(basename(path), "sem0")) %>% 
    pull(path) %>% basename()

animals <-
    r2 %>% 
    select(-file) %>% 
    select(country, country_code, year, report_period, everything())

pos_codes <- animals %>% 
    select(country, country_code) %>% 
    distinct()

missing_reports <- read_csv("data-raw/available_reports.csv") %>% 
    filter(!reported, semester == 0, yr != 2019 ) %>% 
    rename(country_code = country) %>% 
    select(-semester, -reported) %>% 
    right_join(pos_codes)

write_csv(missing_reports, "missing_reports_from_rvf_countries_2019-02-08.csv")

write_csv(animals, "reported_rvf_from_oie_annual_reports_2019-02-08.csv")


