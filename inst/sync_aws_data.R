# Script to sync data files
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup
library(aws.s3)
aws.signature::use_credentials()

# Create the bucket, only done once
# aws.s3::put_bucket(bucket = "wahis-raw-data",
#                   acl = "private")
 
# To deposit updated scraped data
# Compress the data-raw folder
# tar("wahis-raw-data.tar.xz",
#     files = "data-raw",
#     compression = "xz",      # xz and level 9 makes this slow, but small!
#     compression_level = 9,
#     tar = "internal")
# Upload the compressed file
# put_object(file = "wahis-raw-data.tar.xz",
#            object = "wahis-raw-data.tar.xz",
#            bucket = "wahis-raw-data",
#            multipart = TRUE,
#            verbose = TRUE)

# To get current scraped data (>2GB)
save_object(object = "wahis-raw-data.tar.xz", 
            bucket = "wahis-raw-data",
            file = "waihs-raw-data.tar.xz",
            overwrite = TRUE)
untar("wahis-raw-data.tar.xz", exdir = ".", tar = "internal")
