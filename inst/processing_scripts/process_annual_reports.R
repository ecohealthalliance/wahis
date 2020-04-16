#!/usr/bin/env Rscript

# This script is for testing. Processing for database is done in repel-infrastructure. 
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
                        full.names = TRUE)

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
readr::write_rds(wahis_annual, here::here("data-processed", "wahis_ingested_annual_reports.rds"), compress = "xz", compression = 9L)

# Transform files   ------------------------------------------------------
annual_reports <-  readr::read_rds(here::here("data-processed", "wahis_ingested_annual_reports.rds"))
annual_reports_transformed <- wahis::transform_annual_reports(annual_reports)

# Export transformed files-----------------------------------------------
dir_create( here::here("data-processed", "db"))
purrr::iwalk(annual_reports_transformed, ~readr::write_csv(.x, here::here("data-processed", "db", paste0(.y, ".csv.xz"))))
readr::write_rds(annual_reports_transformed, here::here("data-processed", "wahis_transformed_annual_reports.rds))
