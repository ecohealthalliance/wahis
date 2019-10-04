#!/usr/bin/env Rscript

# A script get the list of availabel annual reports on WAHIS

library(httr)
library(tidyverse)
library(xml2)
library(rvest)
library(stringi)
library(future)
library(furrr)

plan(multiprocess, workers = min(parallel::detectCores(), 8))
cat("Collecting List of Annual Reports\n")

base_page <-read_html("http://www.oie.int/wahis_2/public/wahid.php/Countryinformation/Countryhome")

country_codes <- base_page %>% 
    xml_nodes(xpath='//*[@id="country6"]/option') %>% 
    xml_attrs() %>% 
    map("value") %>% 
    unlist() %>% 
    {`[`(., . != "0")}

country_codes <- sample(country_codes, length(country_codes)) # randomize for even performance

# Make an empty table of headers
df <- tibble(country = 1, yr = 1, semester = 1, reported = 1) %>% filter(country=="yoyo")
write_csv(df, here::here("data-raw", "available_reports_prog.csv"))

available_reports <- future_map_dfr(country_codes, function(country) {
    Sys.sleep(min(0.5, rexp(1, 1)))
    page <- RETRY("POST",
                  url = "https://www.oie.int/wahis_2/public/wahid.php/Countryinformation/reporting/reporthistory",
                  body = list(this_country_code = country, detailed = "1"), encode = "form")
    html_tb <- read_html(content(page, "text", encoding = "ISO-8859-1")) %>% 
        xml_nodes(xpath = '//table[@class="Table27"]/*') %>% 
        as_list()
    out <- map_dfr(html_tb[-1], function(tr) {
        tibble(
            country = country,
            yr = as.integer(stri_trim_both(tr[[1]][[1]])),
            `1` = names(tr[[3]])[2] == "a",
            `2` = names(tr[[5]])[2] == "a",
            `0` = names(tr[[7]])[2] == "a"
        )
    })
    if(nrow(out) == 0) {
        return(df)
    } else {
        out <- gather(out, "semester", "reported", -country, -yr)
        #write_csv(out, here::here("inst", "scraping_scripts", "available_annual_reports.csv"), append = TRUE)
        #write_csv(out, stdout(), append = TRUE)
        cat(format_tsv(out, append = TRUE))
        return(out)
    }
}, .progress = TRUE)

available_reports %>% 
    arrange(country, yr, semester, reported) %>% 
    write_csv(here::here("data-raw", "available_annual_reports.csv"))