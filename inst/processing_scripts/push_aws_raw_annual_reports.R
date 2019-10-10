# Script to sync data files
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup
library(aws.s3)
aws.signature::use_credentials()

if(!bucket_exists("wahis-data")) {
    aws.s3::put_bucket(bucket = "wahis-data", acl = "private")
}

#Compress the raw data 
tar("wahis-raw-annual-reports.tar.xz",
    files = "data-raw/wahis_raw_annual_reports",
    compression = "xz",      # xz and level 9 makes this slow, but small!
    compression_level = 9,
    tar = "internal")
# Upload the compressed file
put_object(file = "wahis-raw-annual-reports.tar.xz",
           object = "wahis-raw-annual-reports.tar.xz",
           bucket = "wahis-data",
           multipart = FALSE,
           verbose = FALSE)
