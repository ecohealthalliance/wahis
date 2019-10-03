library(tidyverse)

wahis <- readr::read_rds(here::here("data", "wahis.rds"))

# remove report errors
wahis <- discard(wahis, function(x){
    length(x) == 1
})

# add country and report period to every table
wahis <- purrr::map(wahis, function(x){
    country <- x$metadata$country %>% unique()
    report_year <- x$metadata$report_year %>% unique()
    report_months <- x$metadata$report_months %>% unique()
    purrr::map(x, function(y){
        if(is.null(y)){return()}
        y %>% mutate(country = country,
                     report_year = report_year,
                     report_months = report_months) %>%
            select(country, report_year, report_months, everything())
    })
})

# extract and rbind tables
wahis_joined <- map(names(wahis[[1]]), function(name){
    map_dfr(wahis, ~magrittr::extract2(., name)) 
})
names(wahis_joined) <- names(wahis[[1]])

write_rds(wahis_joined, here::here("data", "wahis-merged.rds"))

# Retrieve from AWS -------------------------------------------------------------
# library(aws.s3)
# aws.signature::use_credentials()
# Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")
# 
# save_object(object = "wahis-data-merged.tar.xz",
#             bucket = "wahis-data",
#             file = here::here("data", "wahis-data-merged.tar.xz"),
#             overwrite = TRUE)
# untar(here::here("data", "wahis-data-merged.tar.xz"), exdir = ".", tar = "internal")
# wahis <- readr::read_rds(here::here("data", "wahis-merged.rds"))

# Post to AWS -------------------------------------------------------------
# See https://ecohealthalliance.github.io/eha-ma-handbook/11-cloud-computing-services.html
# For credentials setup
# library(aws.s3)
# aws.signature::use_credentials()
# Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")
# 
# # To deposit updated scraped data
# # Compress the data-raw folder
# tar("wahis-data-merged.tar.xz",
#     files = "data/wahis-merged.rds",
#     compression = "xz",      # xz and level 9 makes this slow, but small!
#     compression_level = 9,
#     tar = "internal")
# # Upload the compressed file
# put_object(file = "wahis-data-merged.tar.xz",
#            object = "wahis-data-merged.tar.xz",
#            bucket = "wahis-data",
#            multipart = TRUE,
#            verbose = TRUE)

