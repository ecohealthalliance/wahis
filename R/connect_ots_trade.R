#' Download ots data
#' @param directory to save downloaded OTS data
#' @import dplyr here tradestatistics
#' @importFrom readr write_rds
#' @importFrom jsonlite fromJSON
#' @export
download_trade <- function(directory){
  
  product_ids <- left_join(tradestatistics::ots_products, tradestatistics::ots_sections, by = "product_code") %>%
    left_join(tradestatistics::ots_sections_shortnames, by = "section_code") %>% 
    filter(section_shortname_english %in% c("Foodstuffs","Animal and Vegetable Bi-Products", "Vegetable Products", "Animal Hides",  "Animal Products")) %>%
    pull(product_code)
  
  suppressWarnings(dir.create(here("data-raw")))
  
  max_year <- fromJSON("https://api.tradestatistics.io/year_range")[2,]
  
  ots <- ots_create_tidy_data(years = c(2000:max_year), 
                              table = "yrpc",  
                              products = product_ids, 
                              reporters = "all") 
  
  write_rds(ots, here(directory, "ots-trade.rds"))
}

#' Transform ots data to return pairwise trade values
#' @param directory where ots-trade.rds is saved
#' @import dplyr tidyr here
#' @importFrom readr read_rds
#' @export
transform_trade <- function(directory){
  
  ots <- read_rds(here(directory, "ots-trade.rds"))
  
  # assign country destination and origin
  ots_export <- ots %>%
    select(year, product_code, country_origin = reporter_iso, country_destination = partner_iso, value = export_value_usd) %>% 
    as_tibble()
  
  ots_import <- ots %>%
    select(year, product_code, country_origin = partner_iso, country_destination = reporter_iso, value = import_value_usd)%>% 
    as_tibble()
  
  # for trades reported more than once, assume max value
  ots_bilateral_long <- bind_rows(ots_export, ots_import) %>%
    drop_na(value) %>%
    filter(country_origin != country_destination) %>%
    group_by(year, product_code, country_origin, country_destination) %>%
    filter(value == max(value)) %>% 
    distinct() %>%
    ungroup() %>%
    mutate_at(.vars = c("country_origin", "country_destination"), ~toupper(.))
  
  # reshape
  ots_bilateral_wide <- ots_bilateral_long %>%
    mutate(product_code = paste0("trade_dollars_", product_code)) %>%
    pivot_wider(names_from = product_code, values_from = value) 
  
  return(ots_bilateral_wide)
  
}