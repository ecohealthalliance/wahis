#!/usr/bin/env Rscript

# This script is for testing. Processing for database is done in repel-infrastructure. 
library(fs)
library(future)
library(furrr)
library(tidyverse)
library(tictoc)

# Set up parallel plan  --------------------------------------------------------

# plan(multisession) # This takes a bit to load on many cores as all the processes are starting
devtools::load_all(here::here()) #doing this as scraping functions may not be exported

# List all files  ---------------------------------------------------------
reports <- scrape_outbreak_report_list()
web_pages <- paste0("https://wahis.oie.int/pi/getReport/", reports$report_info_id)
    
# Run ingest (~35 mins) ---------------------------------------------------------
message(paste(n_distinct(reports$report_info_id), "files to process"))
tic()
report_resps <- map_curl(
    urls = web_pages[1:20],
    .f = function(x) wahis::safe_ingest_outbreak(x),
    .host_con = 6L, # can turn up
    .delay = 1L, # can turn down
    .timeout = nrow(reports_to_get)*120L,
    .handle_opts = list(low_speed_limit = 100, low_speed_time = 300), # bytes/sec
    .retry = 3
)
toc()

# Save ingested files   ------------------------------------------------------
# dir_create(here::here("data-processed"))
# readr::write_rds(wahis_outbreak, here::here("data-processed", "wahis_ingested_outbreak_reports.rds"), compress = "xz", compression = 9L)

# Transform files   ------------------------------------------------------
# outbreak_reports <-  readr::read_rds(here::here("data-processed", "wahis_ingested_outbreak_reports2.rds"))
tic()
outbreak_reports_transformed <- transform_outbreak_reports(outbreak_reports = report_resps,
                                                           report_list = reports)
toc()
# Export transformed files-----------------------------------------------
readr::write_rds(outbreak_reports_transformed, here::here("data-processed", "wahis_transformed_outbreak_reports.rds"), compress = "xz", compression = 9L)
