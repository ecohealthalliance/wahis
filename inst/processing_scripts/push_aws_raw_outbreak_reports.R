#!/usr/bin/env Rscript

# Script to sync data files
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup
library(aws.s3)
if (Sys.getenv("CI_JOB_ID") != "") {
    aws.signature::use_credentials(file = Sys.getenv("AWS_SIGNATURE_PATH"))
} else {
    aws.signature::use_credentials()
}
Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")


if (!bucket_exists("wahis-data")) {
    aws.s3::put_bucket(bucket = "wahis-data", acl = "private")
}

#Compress the raw data 
setwd(here::here("data-raw"))
tar("wahis-raw-outbreak-reports.tar.xz",
    files = "wahis_raw_outbreak_reports",
    compression = "xz",      # xz and level 9 makes this slow, but small!
    compression_level = 9,
    tar = "internal")
# Upload the compressed file
put_object(file = "wahis-raw-outbreak-reports.tar.xz",
           object = "wahis-raw-outbreak-reports.tar.xz",
           bucket = "wahis-data",
           multipart = FALSE,
           verbose = FALSE)
tar("wahis-raw-outbreak-pdfs.tar.xz",
    files = "wahis_raw_outbreak_pdfs",
    compression = "xz",      # xz and level 9 makes this slow, but small!
    compression_level = 9,
    tar = "internal")
# Upload the compressed file
put_object(file = "wahis-raw-outbreak-pdfs.tar.xz",
           object = "wahis-raw-outbreak-pdfs.tar.xz",
           bucket = "wahis-data",
           multipart = FALSE,
           verbose = FALSE)
setwd(here::here())
