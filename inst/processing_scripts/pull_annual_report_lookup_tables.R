#!/usr/bin/env Rscript

library(tidyverse)
library(googledrive) 
library(readxl)
library(fs)

drive_auth()

wb <- drive_download("https://docs.google.com/spreadsheets/d/18tgKfu-3oA_xycVO07LGMN7CIEZ1WYnMDz0ZF665zuo",
                     overwrite = TRUE)
dir_create(here::here("inst", "annual_report_lookups"))

walk(excel_sheets(wb$local_path), function(z) {
    message(z)
    write_csv(read_excel(wb$local_path, z),
              here::here("inst", "annual_report_lookups",
                         paste0(z, ".csv")))
})
file_delete(wb$local_path)

