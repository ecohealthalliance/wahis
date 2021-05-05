#!/usr/bin/env Rscript

# This script is for testing. Processing for database is done in repel-infrastructure. 
library(fs)
library(future)
library(furrr)
library(tidyverse)
library(tictoc)

# Set up parallel plan  --------------------------------------------------------

plan(multisession) # This takes a bit to load on many cores as all the processes are starting
devtools::load_all(here::here()) #doing this as scraping functions may not be exported

# List all files  ---------------------------------------------------------
reports <- scrape_outbreak_report_list()

# Run ingest (~35 mins) ---------------------------------------------------------
message(paste(n_distinct(reports$report_info_id), "files to process"))
tic()
wahis_outbreak <- future_map(reports$report_info_id, wahis:::safe_ingest_outbreak2, .progress = TRUE)  
toc()

# Save ingested files   ------------------------------------------------------
dir_create(here::here("data-processed"))
readr::write_rds(wahis_outbreak, here::here("data-processed", "wahis_ingested_outbreak_reports2.rds"), compress = "xz", compression = 9L)

# Transform files   ------------------------------------------------------
# outbreak_reports <-  readr::read_rds(here::here("data-processed", "wahis_ingested_outbreak_reports2.rds"))
# outbreak_reports_transformed <- transform_outbreak_reports2(outbreak_reports)
# 
# # Export transformed files-----------------------------------------------
# readr::write_rds(outbreak_reports_transformed, here::here("data-processed", "wahis_transformed_outbreak_reports.rds"), compress = "xz", compression = 9L)
