## Extract info on FMD from WAHIS web pages

library(wahis)
library(rvest)
library(stringi)
library(pbapply)
library(purrr)
library(magrittr)
devtools::load_all()
filenames <- list.files("data-raw/raw_wahis_pages",
                        pattern = "*.html",
                        full.names = TRUE)
wahis <- pblapply(filenames, ingest_wahis_record, cl=40)  ## 25448 files read

## to check where there's a problem:
#library(plyr)  # good'ol plyr
#wahis <- laply(filenames, ingest_wahis_record, .inform = TRUE)

wahis_fmd <- wahis[which(sapply(wahis, `[[`, 'title') == "Foot and mouth disease")]

## get variable names
var_names <- wahis_fmd %>%
    modify_depth(1, function(x) names(x))
var_names <- unique(unlist(var_names))
var_outbreaks <- grep("(Outbreak_)\\d+", var_names, value = TRUE)  # outbreaks variables
## "Main" variables from the list, removing outbreaks (which tells there's no outbreak),
## and Summary descritpion which is added to few reports (449, 457, 720, 1047, 1066,
## 1113) which gives some comments on the outbreak (not needed, at least for now)
var_main <- var_names[!var_names %in% c(var_outbreaks,
                                        "outbreaks",
                                        "Summary description")]

wahis_main <- wahis_fmd %>%
    map_df(., magrittr::extract, var_main)
wahis_outbreaks <- wahis_fmd %>%
    map(., magrittr::extract, c("id", var_outbreaks))

## Get outbreaks info: outbreaks metadata
datalist <- list()
pb <- utils::txtProgressBar(min = 0, max = length(var_outbreaks), style = 3)
for (i in 1:length(var_outbreaks)) {
    dat <- tibble::tibble(
        id = map_chr(wahis_outbreaks, "id"),
        outbreak_nbr = i,
        location = wahis_outbreaks %>%
            map(`[`, paste("Outbreak_", i, sep = "")) %>%
            modify_depth(2,
                         magrittr::extract(paste("Outbreak ", i, " ",
                                                 sep = ""))) %>%
            flatten() %>%
            map_chr(., ~ ifelse(is.null(.), NA, .)),
        start = wahis_outbreaks %>%
            map(`[`, paste("Outbreak_", i, sep = "")) %>%
            modify_depth(2,
                         magrittr::extract("Date of start of the outbreak")) %>%
            flatten() %>%
            map_chr(., ~ ifelse(is.null(.), NA, .)),
        status = wahis_outbreaks %>%
            map(`[`, paste("Outbreak_", i, sep = "")) %>%
            modify_depth(2,
                         magrittr::extract("Outbreak status")) %>%
            flatten() %>%
            map_chr(., ~ ifelse(is.null(.), NA, .)),
        epi_unit = wahis_outbreaks %>% 
            map(`[`, paste("Outbreak_", i, sep = "")) %>% 
            modify_depth(2,
                         magrittr::extract("Epidemiological unit")) %>%
            flatten() %>%
            map_chr(., ~ ifelse(is.null(.), NA, .))
    )
    datalist[[i]] <- dat
    utils::setTxtProgressBar(pb, i)
}
close(pb)
wahis_outbreaksMeta <- do.call(rbind, datalist)

## Get outbreaks data: get cases
datalist <- list()
pb <- utils::txtProgressBar(min = 0, max = length(var_outbreaks), style = 3)
for (i in 1:length(var_outbreaks)) {
    data_case <- wahis_outbreaks %>%
        map(`[`, paste("Outbreak_", i, sep = "")) %>%
        modify_depth(2, magrittr::extract("Cases")) %>%
        flatten() %>%
        map_df(., extract, c("id", "Species", "Susceptible", "Cases", "Deaths",
                             "Killed and disposed of", "Slaughtered"))
    data_case$outbreak_nbr <- i
    datalist[[i]] <- data_case
    utils::setTxtProgressBar(pb, i)
}
close(pb)
wahis_cases <- do.call(rbind, datalist)

head(wahis_main)
head(wahis_outbreaksMeta)
head(wahis_cases)