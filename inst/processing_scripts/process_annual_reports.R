#!/usr/bin/env Rscript

library(fs)
library(future)
library(furrr)
library(tidyverse)

# Set up parallel plan  --------------------------------------------------------

plan(multiprocess) # This takes a bit to load on many cores as all the processes are starting
devtools::load_all(here::here()) #doing this as scraping functions may not be exported

# List all files  ---------------------------------------------------------
#! These are example files for testing the scraper functions. Actual files are downloaded and processed in repel-infrastructure/repeldb
filenames <- list.files(here::here("data-raw/wahis-raw-annual-reports"),
                        pattern = "*.html",
                        full.names = TRUE)[1]

# Run ingest (~25 mins) ---------------------------------------------------------
message(paste(length(filenames), "files to process"))
wahis_annual <- future_map(filenames, wahis:::safe_ingest_annual, .progress = TRUE)  

#For testing/profiling
# Rprof("out.prof")
# wahis <- lapply(filenames[1:100], wahis:::safe_ingest)
# Rprof(NULL)
# noamtools::proftable("out.prof")

# Save ingested files   ------------------------------------------------------
dir_create(here::here("data-processed"))
readr::write_rds(wahis_annual, here::here("data-processed", "processed-annual-reports.rds"), compress = "xz", compression = 9L)

# Transform files   ------------------------------------------------------
annual_reports <-  readr::read_rds(here::here("data-processed", "processed-annual-reports.rds"))

assertthat::are_equal(length(filenames), length(annual_reports))
ingest_status_log <- tibble(web_page = basename(filenames), 
                            ingest_status = map_chr(annual_reports, ~.x$ingest_status)) %>%
    mutate(code = substr(web_page, 1, 3),
           report_year = substr(web_page, 5, 8),
           semester = substr(web_page, 13, 13)) %>%
    select(-web_page) %>%
    mutate(in_database = ingest_status == "available") %>%
    mutate(ingest_error = ifelse(!in_database, ingest_status, NA)) %>%
    select(code, report_year, semester, in_database, ingest_error)


annual_reports_transformed <- transform_annual_reports(annual_reports)

# Export transformed files-----------------------------------------------
dir_create( here::here("data-processed", "db"))
purrr::iwalk(annual_reports_transformed, ~readr::write_csv(.x, here::here("data-processed", "db", paste0(.y, ".csv.xz"))))
readr::write_csv(ingest_status_log, here::here("data-processed", "db", "annual_reports_ingest_status_log.csv.xz"))

readr::write_rds(annual_reports_transformed, here::here("data-processed", "annual-reports-data.rds"), compress = "xz", compression = 9L)

