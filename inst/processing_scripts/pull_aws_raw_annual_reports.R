# Script to sync data files
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup
library(aws.s3)
aws.signature::use_credentials()

save_object(object = "wahis-raw-annual-reports.tar.xz", 
            bucket = "wahis-data",
            file = "wahis-raw-annual-reports.tar.xz",
            overwrite = TRUE)
untar("wahis-raw-data.tar.xz", exdir = "data-raw", tar = "internal")
file.remove("wahis-raw-annual-reports.tar.xz")
