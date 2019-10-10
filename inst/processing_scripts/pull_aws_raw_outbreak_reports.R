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


save_object(object = "wahis-raw-outbreak-reports.tar.xz", 
            bucket = "wahis-data",
            file = "wahis-raw-outbreak-reports.tar.xz",
            overwrite = TRUE)
untar("wahis-raw-outbreak-reports.tar.xz", exdir = "data-raw", tar = "internal")
file.remove("wahis-raw-outbreak-reports.tar.xz")

save_object(object = "wahis-raw-outbreak-pdfs.tar.xz", 
            bucket = "wahis-data",
            file = "wahis-raw-outbreak-pdfs.tar.xz",
            overwrite = TRUE)
untar("wahis-raw-outbreak-pdfs.tar.xz", exdir = "data-raw", tar = "internal")
file.remove("wahis-raw-outbreak-pdfs.tar.xz")
