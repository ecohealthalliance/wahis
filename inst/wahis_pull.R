#!/usr/bin/env Rscript

library(pbapply)
library(tidyverse)
library(wahis)
h <- here::here


filenames <- list.files(h("data-raw", "raw_wahis_pages"),
                        pattern = "*.html",
                        full.names = TRUE)
wahis <- pblapply(filenames, ingest_wahis_record)  ## 25448 files read
saveRDS(wahis)
#ingest_wahis_record("/Users/noamross/dropbox-eha/projects/wahis/data-raw/raw_wahis_pages/400.html")

## to check where there's a problem:
#library(plyr)  # good'ol plyr
#wahis <- laply(filenames, ingest_wahis_record, .inform = TRUE)

#wahis_fmd <- wahis[which(sapply(wahis, `[[`, 'title') == "Foot and mouth disease")]

## get variable names
#var_names <- wahis_fmd %>%
#    modify_depth(1, function(x) names(x))
#var_names <- unique(unlist(var_names))
#var_outbreaks <- grep("(Outbreak_)\\d+", var_names, value = TRUE)  # outbreaks variables
## "Main" variables from the list, removing outbreaks (which tells there's no outbreak),
## and Summary descritpion which is added to few reports (449, 457, 720, 1047, 1066,
## 1113) which gives some comments on the outbreak (not needed, at least for now)
#var_main <- var_names[!var_names %in% c(var_outbreaks,
     #                                   "outbreaks",
     #                                   "Summary description")]

# wahis_main <- wahis_fmd %>%
#     map_df(., magrittr::extract, var_main)
