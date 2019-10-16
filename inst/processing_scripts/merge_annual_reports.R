#!/usr/bin/env Rscript

library(tidyverse)

# Load files -------------------------------------------------------------
wahis <- readr::read_rds(here::here("data-processed", "processed-annual-reports.rds"))

# Remove report errors ---------------------------------------------------
wahis <- discard(wahis, function(x){
    length(x) == 1
})

# Extract and rbind tables ----------------------------------------------------
wahis_joined <- map(names(wahis[[1]]), function(name){
    map_dfr(wahis, ~magrittr::extract2(., name)) 
})

names(wahis_joined) <- names(wahis[[1]])

# Save -------------------------------------------------------------
write_rds(wahis_joined, here::here("data-processed", "merged-annual-reports.rds"),
          compress = "xz", compression = 9L)

