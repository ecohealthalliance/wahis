#!/usr/bin/env Rscript

# A script to scrape country semi-annual and annal reports from WAHIS

library(httr)
library(tidyverse)
library(xml2)
library(rvest)
library(stringi)
library(httr)
library(glue)
library(future)
library(furrr)
library(digest)

plan(multiprocess, workers = min(parallel::detectCores(), 8))

available_reports <- read_csv(here::here("data-raw", "available_annual_reports.csv")) %>% 
    filter(reported) %>% 
    select(-reported)

if (!dir.exists(here::here("data-raw", "raw_wahis_annual_reports"))) {
    dir.create(here::here("data-raw", "raw_wahis_annual_reports"))
}

current_reports <- 
    tibble(file = basename(fs::dir_ls(here::here("data-raw", "raw_wahis_annual_reports")))) %>% 
    extract(file, into = c("country", "yr", "semester"), regex = "(\\w{3})_(\\d{4})_sem(\\d)\\.html", remove = TRUE) %>% 
    mutate_at(vars(yr, semester), as.double)

reports_to_get <- anti_join(available_reports, current_reports) %>% 
    sample_frac(1)

df <- tibble(country = 1, yr = 1, semester = 1, file = 1, path = 1, status = 1, md5 = 1) %>% filter(country=="yoyo")
write_csv(df, here::here("data-raw", "annual_reports_scraped_progress.csv"))

sRETRY <- safely(RETRY)

cat(glue("Downloading {nrow(reports_to_get)} reports...\n\n"))

fs::dir_info(here::here("data-raw", "raw_wahis_annual_reports")) %>% 
    filter(size == 0) %>% 
    pull(path) %>% 
    fs::file_delete()

if(!dir.exists(here::here("data-raw", "raw_wahis_annual_reports"))) dir.create(here::here("data-raw", "raw_wahis_annual_reports"))
files <- future_pmap_dfr(reports_to_get, function(country, yr, semester) {
    fileout <- here::here("data-raw", "raw_wahis_annual_reports",
                          glue("{country}_{yr}_sem{semester}.html"))
    if(!file.exists(fileout) || file.info(fileout)$size == 0) {
        country_report <- sRETRY(
            "GET",
            url = "http://www.oie.int/wahis_2/public/wahid.php/Reviewreport/semestrial/review",
            query = list(year=yr,
                         semester=semester,
                         wild=0,
                         country=country,
                         this_country_code=country,
                         detailed=1),
            write_disk(fileout, overwrite = TRUE))
        Sys.sleep(min(0.5, rexp(1, 1)))
        if(!is.null(country_report$result)) {
            status = status_code(country_report$result)
            md5 = digest(readLines(fileout))
        } else {
            status = "failed"
            md5 = NA_character_
        }
    } else {
        status = "skipped" 
        md5 = digest(readLines(fileout))
    }
    
    df <- tibble(country = country, yr = yr, semester = semester, file = basename(fileout), path = fileout, status = status, md5 = md5)
    write_csv(df, here::here("data-raw", "wahis_reports_prog.csv"), append = TRUE)
    return(df)
}, .progress = TRUE)
write_csv(files, here::here("data-raw", "wahis_annual_reports_downloaded.csv"))

fs::dir_info(here::here("data-raw", "raw_wahis_annual_reports")) %>% 
    filter(size == 0) %>% 
    pull(path) %>% 
    fs::file_delete()

## TODO - do a clean-up run, single threaded, to get remaining failed hits
