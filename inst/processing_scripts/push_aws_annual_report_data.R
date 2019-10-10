#!/usr/bin/env Rscript

# Post to AWS -------------------------------------------------------------
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup
library(aws.s3)
if (Sys.getenv("CI_JOB_ID") != "") {
    aws.signature::use_credentials(file = Sys.getenv("AWS_SIGNATURE_PATH"))
} else {
    aws.signature::use_credentials()
}
Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")

if(!bucket_exists("wahis-data")) {
    aws.s3::put_bucket(bucket = "wahis-data", acl = "private")
}

s3sync(files =  paste0("data-processed/db/", dir("data-processed/db", include.dirs = TRUE)),
       bucket = "wahis-data",
       direction = "upload")
