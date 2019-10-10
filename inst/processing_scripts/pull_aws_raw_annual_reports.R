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


save_object(object = "wahis-raw-annual-reports.tar.xz", 
            bucket = "wahis-data",
            file = "wahis-raw-annual-reports.tar.xz",
            overwrite = TRUE)
untar("wahis-raw-annual-reports.tar.xz", exdir = "data-raw", tar = "internal")
file.remove("wahis-raw-annual-reports.tar.xz")
