#!/usr/bin/env Rscript

library(pbapply) 
library(wahis) #doing this as scraping functions may not be exported

# List all files  ---------------------------------------------------------
filenames <- list.files(here::here("data-raw/wahis_raw_annual_reports"),
                        pattern = "*.html",
                        full.names = TRUE)

# Run scraper (~1 hr) ---------------------------------------------------------
message(paste(length(filenames), "files to process"))
opb <- pboptions(type="timer")
wahis <- pblapply(filenames, wahis:::safe_ingest, cl = parallel::detectCores())  

# Save processed files   ------------------------------------------------------
readr::write_rds(wahis, here::here("data-processed", "processed-annual-reports.rds"), compress = "xz", compression = 9L)
