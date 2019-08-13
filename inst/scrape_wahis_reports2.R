#!/usr/bin/env Rscript

library(httr)
library(tidyverse)
library(xml2)
library(rvest)
library(stringi)
library(httr)
library(glue)
library(future)
library(furrr)
library(digest)

plan(multiprocess, workers = 2)
if(!file.exists(here::here("data-raw", "available_reports.csv"))) {
    cat("Checking for report availability\n")
    base_page <-read_html("http://www.oie.int/wahis_2/public/wahid.php/Countryinformation/Countryhome")
    country_codes <- base_page %>% 
        xml_nodes(xpath='//*[@id="country6"]/option') %>% 
        xml_attrs() %>% 
        map("value") %>% 
        unlist() %>% 
        {`[`(., . != "0")}
    
    country_codes <- sample(country_codes, length(country_codes)) # randomize for even performance
    
    df <- tibble(country = 1, yr = 1, semester = 1, reported = 1) %>% filter(country=="yoyo")
    write_csv(df, here::here("data-raw", "available_reports_prog.csv"))
    available_reports <- future_map_dfr(country_codes, function(country) {
        page <- RETRY("POST",
                      url = "http://www.oie.int/wahis_2/public/wahid.php/Countryinformation/reporting/reporthistory",
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
        }) %>% 
            gather("semester", "reported", -country, -yr)
        write_csv(out, here::here("data-raw", "available_reports_prog.csv"), append = TRUE)
        Sys.sleep(min(0.5, rexp(1, 1)))
        return(out)
    }, .progress = TRUE)
    
    write_csv(available_reports, here::here("data-raw", "available_reports.csv"))
    
} else {
    available_reports <- read_csv(here::here("data-raw", "available_reports.csv"))
}

reports_to_get <- available_reports %>% 
    filter(reported) %>% 
    select(-reported) %>% 
    sample_frac(1)  # randomizing for even performance across threads

df <- tibble(country = 1, yr = 1, semester = 1, file = 1, path = 1, status = 1, md5 = 1) %>% filter(country=="yoyo")
write_csv(df, here::here("data-raw", "wahis_reports_prog.csv"))

sRETRY <- safely(RETRY)

cat(glue("Downloading {nrow(reports_to_get)} reports...\n\n"))

fs::dir_info(here::here("data-raw", "raw_wahis_reports")) %>% 
    filter(size == 0) %>% 
    pull(path) %>% 
    fs::file_delete()

if(!dir.exists(here::here("data-raw", "raw_wahis_reports"))) dir.create(here::here("data-raw", "raw_wahis_reports"))
files <- future_pmap_dfr(reports_to_get, function(country, yr, semester) {
    fileout <- here::here("data-raw", "raw_wahis_reports",
                          glue("{country}_{yr}_sem{semester}.html"))
    if(!file.exists(fileout) || file.info(fileout)$size == 0) {
        country_report <- sRETRY(
            "GET",
            url = "http://www.oie.int/wahis_2/public/wahid.php/Reviewreport/semestrial/review",
            query = list(year=yr,
                         semester=semester,
                         wild=0,
                         country=country,
                         this_country_code=country,
                         detailed=1),
            write_disk(fileout, overwrite = TRUE))
        Sys.sleep(min(0.5, rexp(1, 1)))
        if(!is.null(country_report$result)) {
            status = status_code(country_report$result)
            md5 = digest(readLines(fileout))
        } else {
            status = "failed"
            md5 = NA_character_
        }
    } else {
        status = "skipped" 
        md5 = digest(readLines(fileout))
    1}
    
    df <- tibble(country = country, yr = yr, semester = semester, file = basename(fileout), path = fileout, status = status, md5 = md5)
    write_csv(df, here::here("data-raw", "wahis_reports_prog.csv"), append = TRUE)
    return(df)
}, .progress = TRUE)
write_csv(files, here::here("data-raw", "wahis_reports.csv"))

fs::dir_info(here::here("data-raw", "raw_wahis_reports")) %>% 
    filter(size == 0) %>% 
    pull(path) %>% 
    fs::file_delete()

## TODO - do a clean-up run, single threaded, to get remaining failed hits
