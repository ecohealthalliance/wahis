#!/usr/bin/env Rscript

library(furrr)
library(future)
library(fs)
plan(multiprocess) # This takes a long time in RStudio, but is faster from terminal
devtools::load_all(here::here()) #doing this as scraping functions may not be exported

# List all files  ---------------------------------------------------------
filenames <- list.files(here::here("data-raw/wahis_raw_annual_reports"),
                        pattern = "*.html",
                        full.names = TRUE)

# Run scraper (~1 hr) ---------------------------------------------------------
message(paste(length(filenames), "files to process"))
wahis <- future_map(filenames, wahis:::safe_ingest, .progress = TRUE)  

# Save processed files   ------------------------------------------------------
dir_create(here::here("data-processed"))
readr::write_rds(wahis, here::here("data-processed", "processed-annual-reports.rds"), compress = "xz", compression = 9L)
