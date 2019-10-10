library(tidyverse)

# Merge files -------------------------------------------------------------
wahis <- readr::read_rds(here::here("data", "wahis.rds"))

# Remove report errors
wahis <- discard(wahis, function(x){
    length(x) == 1
})

# Add country and report period to every table
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

# Extract and rbind tables
wahis_joined <- map(names(wahis[[1]]), function(name){
    map_dfr(wahis, ~magrittr::extract2(., name)) 
})

names(wahis_joined) <- names(wahis[[1]])

# Save
write_rds(wahis_joined, here::here("data", "wahis-merged.rds"))

