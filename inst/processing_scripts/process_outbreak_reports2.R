#!/usr/bin/env Rscript

# This script is for testing. Processing for database is done in repel-infrastructure. 
library(tidyverse)
library(tictoc)

# Set up parallel plan  --------------------------------------------------------

devtools::load_all(here::here()) #doing this as scraping functions may not be exported

# Run ingest ---------------------------------------------------------
tic()
wahis_outbreak <- ingest_outbreak_report2(start_i =  1)  
toc()

# Save ingested files   ------------------------------------------------------
dir_create(here::here("data-processed"))
readr::write_rds(wahis_outbreak, here::here("data-processed", "wahis_ingested_outbreak_reports2.rds"), compress = "xz", compression = 9L)

# # Transform files   ------------------------------------------------------
# outbreak_reports <-  readr::read_rds(here::here("data-processed", "wahis_ingested_outbreak_reports2.rds"))
# outbreak_reports_transformed <- transform_outbreak_reports(outbreak_reports)
# 
# # Export transformed files-----------------------------------------------
# readr::write_rds(outbreak_reports_transformed, here::here("data-processed", "wahis_transformed_outbreak_reports.rds"), compress = "xz", compression = 9L)
