#!/usr/bin/env Rscript

# Script to sync data files
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup

wahis:::push_aws(folder = "", filename = "data-processed", bucket = "wahis-data")

# separately, for now, add db files to aws
aws.s3::s3sync(files =  paste0("data-processed/db/", dir("data-processed/db", include.dirs = TRUE)),
       bucket = "wahis-data",
       direction = "upload")
