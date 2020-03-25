#' Download fao livestock data
#' @import here
#' @export
download_livestock <- function(){
  suppressWarnings(dir.create(here("data-raw/fao-livestock")))
  download.file("http://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip",
                destfile =  here("data-raw/fao-livestock/fao-livestock.zip"))
}

#' Transform fao data to return pairwise livestock trade values
#' @import dplyr tidyr countrycode here
#' @importFrom janitor clean_names
#' @importFrom vroom vroom
#' @export

transform_livestock <- function(){
  
  # unzip using OS command (based on this: https://stackoverflow.com/questions/42740206/r-possible-truncation-of-4gb-file)
  setwd(here("data-raw/fao-livestock"))
  system2("unzip", args = c("-o", # include override flag
                     here("data-raw/fao-livestock/fao-livestock.zip")),
            stdout = TRUE)
  setwd(here())

  fao <- vroom(here("data-raw/fao-livestock/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv"), 
               col_types = cols(
                 `Reporter Country Code` = col_skip(),
                 `Reporter Countries` = col_character(),
                 `Partner Country Code` = col_skip(),
                 `Partner Countries` = col_character(),
                 `Item Code` = col_double(),
                 Item = col_skip(),
                 `Element Code` = col_skip(),
                 Element = col_character(),
                 `Year Code` = col_skip(),
                 Year = col_double(),
                 Unit = col_character(),
                 Value = col_double(),
                 Flag = col_character()
               )) %>%
    janitor::clean_names()
    
  # filter data
  fao_heads <- fao %>%
    filter(unit %in% c("Head", "1000 Head")) %>%
    drop_na(value) %>%
    mutate(value = ifelse(unit == "1000 Head", value * 1000, value)) %>%
    mutate(unit = "Head")
  
  # get country iso3c
  fao_countries <- unique(c(fao$reporter_countries, fao$partner_countries))
  country_code_lookup <- suppressWarnings(countrycode::countrycode(fao_countries,
                                                  origin = "country.name",
                                                  destination = "iso3c"))
  names(country_code_lookup) <- fao_countries
  #country_code_lookup[is.na(country_code_lookup)]
  country_code_lookup[grepl("d'Ivoire", names(country_code_lookup))] <- "CIV"
  
  fao_bilateral <- fao_heads %>%
    mutate(reporter_iso = country_code_lookup[reporter_countries],
           partner_iso = country_code_lookup[partner_countries]) %>%
    drop_na(reporter_iso, partner_iso) %>%
    filter(reporter_iso != partner_iso) %>%
    select(year, reporter_iso, partner_iso, element, item_code, value)
  
  # assign country destination and origin
  fao_export <- fao_bilateral %>%
    filter(element == "Export Quantity") %>%
    rename(country_origin = reporter_iso, country_destination = partner_iso) %>%
    select(-element)
  
  fao_import <- fao_bilateral %>%
    filter(element == "Import Quantity") %>%
    rename(country_destination = reporter_iso, country_origin = partner_iso) %>%
    select(-element)
  
  # for trades reported more than once, assume max value
  fao_bilateral_long <- bind_rows(fao_export, fao_import) %>%
    group_by(year, item_code, country_origin, country_destination) %>%
    filter(value == max(value)) %>% 
    distinct() %>%
    ungroup() 
  
  # reshape
  fao_bilateral_wide <- fao_bilateral_long %>%
    mutate(item_code = paste0("livestock_heads_", item_code)) %>%
    pivot_wider(names_from = item_code, values_from = value) %>%
    filter(year >= 2000)
  
  return(fao_bilateral_wide)
  
}

#' FAO item lookup table
#' @import dplyr tidyr here
#' @importFrom janitor clean_names
#' @importFrom vroom vroom
#' @export

get_livestock_item_id <- function(){
  
  # unzip using OS command (based on this: https://stackoverflow.com/questions/42740206/r-possible-truncation-of-4gb-file)
  setwd(here("data-raw/fao-livestock"))
  system2("unzip", args = c("-o", # include override flag
                            here("data-raw/fao-livestock/fao-livestock.zip")),
          stdout = TRUE)
  setwd(here())
  
  vroom(here("data-raw/fao-livestock/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv"), 
               col_types = cols(
                 `Reporter Country Code` = col_skip(),
                 `Reporter Countries` = col_skip(),
                 `Partner Country Code` = col_skip(),
                 `Partner Countries` = col_skip(),
                 `Item Code` = col_double(),
                 Item = col_character(),
                 `Element Code` = col_skip(),
                 Element = col_skip(),
                 `Year Code` = col_skip(),
                 Year = col_skip(),
                 Unit = col_character(),
                 Value = col_skip(),
                 Flag = col_skip()
               )) %>%
    janitor::clean_names() %>%
    filter(unit %in% c("Head", "1000 Head")) %>%
    select(-unit) %>%
    distinct()

}
