library(pbapply) 
devtools::load_all()
filenames <- list.files("data-raw/raw_wahis_reports",
                        pattern = "*.html",
                        full.names = TRUE)
# filenames <- filenames[sample(length(filenames), 200)] # for testing data subset

wahis <- pblapply(filenames, safe_ingest, cl=40)  
# readr::write_rds(wahis, here::here("data", "wahis.rds"))

# to retrieve from AWS:
library(aws.s3)
aws.signature::use_credentials()
Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")

save_object(object = "wahis-data.tar.xz", 
            bucket = "wahis-data",
            file = here::here("data", "wahis-data.tar.xz"),
            overwrite = TRUE)
untar(here::here("data", "wahis-data.tar.xz"), exdir = ".", tar = "internal")

# Post to AWS S3
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup
# library(aws.s3)
# aws.signature::use_credentials()
# Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")

# Create the bucket, only done once
# aws.s3::put_bucket(bucket = "wahis-data", acl = "private")

# To deposit updated scraped data
# Compress the data-raw folder
# tar("wahis-data.tar.xz",
#     files = "data",
#     compression = "xz",      # xz and level 9 makes this slow, but small!
#     compression_level = 9,
#     tar = "internal")
# Upload the compressed file
# put_object(file = "wahis-data.tar.xz",
#            object = "wahis-data.tar.xz",
#            bucket = "wahis-data",
#            multipart = TRUE,
#            verbose = TRUE)
