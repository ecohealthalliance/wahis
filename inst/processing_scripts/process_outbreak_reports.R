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
filenames <- list.files(here::here("data-raw/wahis-raw-outbreak-reports"),
                        pattern = "*.html",
                        full.names = TRUE)

# Run ingest (~25 mins) ---------------------------------------------------------
message(paste(length(filenames), "files to process"))
wahis_outbreak <- future_map(filenames, wahis:::safe_ingest_outbreak, .progress = TRUE)  

#For testing/profiling
# Rprof("out.prof")
# wahis <- lapply(filenames[1:100], wahis:::safe_ingest)
# Rprof(NULL)
# noamtools::proftable("out.prof")

# Save ingested files   ------------------------------------------------------
dir_create(here::here("data-processed"))
readr::write_rds(wahis_outbreak, here::here("data-processed", "wahis-ingested-outbreak-reports.rds"), compress = "xz", compression = 9L)

# Transform files   ------------------------------------------------------
outbreak_reports <-  readr::read_rds(here::here("data-processed", "wahis-ingested-outbreak-reports.rds"))
outbreak_reports_transformed <- transform_outbreak_reports(outbreak_reports)

# Export transformed files-----------------------------------------------
dir_create( here::here("data-processed", "db"))
purrr::iwalk(outbreak_reports_transformed, ~readr::write_csv(.x, here::here("data-processed", "db", paste0(.y, ".csv.xz"))))
readr::write_rds(outbreak_reports_transformed, here::here("data-processed", "wahis-transformed-outbreak-reports.rds"), compress = "xz", compression = 9L)
