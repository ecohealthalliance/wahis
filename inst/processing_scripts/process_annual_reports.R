#!/usr/bin/env Rscript

library(fs)
library(future)
library(furrr)
# Set up parallel plan  --------------------------------------------------------

plan(multiprocess) # This takes a bit to load on many cores as all the processes are starting
devtools::load_all(here::here()) #doing this as scraping functions may not be exported

# List all files  ---------------------------------------------------------
filenames <- list.files(here::here("data-raw/wahis-raw-annual-reports"),
                        pattern = "*.html",
                        full.names = TRUE)

# Run scraper (~25 mins) ---------------------------------------------------------
message(paste(length(filenames), "files to process"))
wahis_annual <- future_map(filenames, wahis:::safe_ingest_annual, .progress = TRUE)  

#For testing/profiling
# Rprof("out.prof")
# wahis <- lapply(filenames[1:100], wahis:::safe_ingest)
# Rprof(NULL)
# noamtools::proftable("out.prof")


# Save processed files   ------------------------------------------------------
dir_create(here::here("data-processed"))
readr::write_rds(wahis_annual, here::here("data-processed", "processed-annual-reports.rds"), compress = "xz", compression = 9L)
